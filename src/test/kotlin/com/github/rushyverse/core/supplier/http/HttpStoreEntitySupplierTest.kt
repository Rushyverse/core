package com.github.rushyverse.core.supplier.http

import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.container.createRedisContainer
import com.github.rushyverse.core.data.ProfileIdCacheService
import com.github.rushyverse.core.data.ProfileSkinCacheService
import com.github.rushyverse.core.utils.createProfileId
import com.github.rushyverse.core.utils.createProfileSkin
import io.lettuce.core.RedisURI
import io.mockk.coEvery
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

@Testcontainers
class HttpStoreEntitySupplierTest {

    companion object {
        @JvmStatic
        @Container
        private val redisContainer = createRedisContainer()
    }

    private lateinit var cacheClient: CacheClient

    private lateinit var storeEntitySupplier: HttpStoreEntitySupplier
    private lateinit var mockSupplier: IHttpEntitySupplier
    private lateinit var cacheEntitySupplier: HttpCacheEntitySupplier

    @BeforeTest
    fun onBefore() = runBlocking {
        cacheClient = CacheClient {
            uri = RedisURI.create(redisContainer.url)
        }

        mockSupplier = mockk()
        // Use to verify if data is inserted
        cacheEntitySupplier = HttpCacheEntitySupplier(
            ProfileSkinCacheService(cacheClient),
            ProfileIdCacheService(cacheClient)
        )

        storeEntitySupplier = HttpStoreEntitySupplier(cacheEntitySupplier, mockSupplier)
    }

    interface StoreTest {
        fun `data not stored into cache if data not exists`()
        fun `data stored if found`()
    }

    @Nested
    @DisplayName("Get player uuid")
    inner class GetId : StoreTest {

        @Test
        override fun `data not stored into cache if data not exists`() = runBlocking {
            val id = createProfileId()
            val name = id.name
            coEvery { mockSupplier.getUUID(name) } returns null
            assertNull(storeEntitySupplier.getUUID(name))
            assertNull(cacheEntitySupplier.getUUID(name))
        }

        @Test
        override fun `data stored if found`() = runBlocking {
            val id = createProfileId()
            val name = id.name
            coEvery { mockSupplier.getUUID(name) } returns id
            assertEquals(id, storeEntitySupplier.getUUID(name))
            assertEquals(id, cacheEntitySupplier.getUUID(name))
        }

    }

    @Nested
    @DisplayName("Get skin by id")
    inner class GetSkin : StoreTest {

        @Test
        override fun `data not stored into cache if data not exists`() = runBlocking {
            val skin = createProfileSkin()
            val uuid = skin.id
            coEvery { mockSupplier.getSkin(uuid) } returns null
            assertNull(storeEntitySupplier.getSkin(uuid))
            assertNull(cacheEntitySupplier.getSkin(uuid))
        }

        @Test
        override fun `data stored if found`() = runBlocking {
            val skin = createProfileSkin()
            val uuid = skin.id
            coEvery { mockSupplier.getSkin(uuid) } returns skin
            assertEquals(skin, storeEntitySupplier.getSkin(uuid))
            assertEquals(skin, cacheEntitySupplier.getSkin(uuid))
        }

    }
}