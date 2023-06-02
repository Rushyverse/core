package com.github.rushyverse.core.supplier.http

import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.container.createRedisContainer
import com.github.rushyverse.core.data.ProfileIdCacheService
import com.github.rushyverse.core.data.ProfileSkinCacheService
import com.github.rushyverse.core.utils.createProfileId
import com.github.rushyverse.core.utils.createProfileSkin
import com.github.rushyverse.core.utils.getRandomString
import io.lettuce.core.RedisURI
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Timeout
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.util.concurrent.TimeUnit
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

@Timeout(3, unit = TimeUnit.SECONDS)
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
            HttpSupplierConfiguration(
                mockk(),
                ProfileSkinCacheService(cacheClient),
                ProfileIdCacheService(cacheClient)
            )
        )

        storeEntitySupplier = HttpStoreEntitySupplier(cacheEntitySupplier, mockSupplier)
    }

    @Test
    fun `get configuration will get from cache supplier`() = runTest {
        val cacheSupplier = mockk<HttpCacheEntitySupplier>(getRandomString())
        storeEntitySupplier = HttpStoreEntitySupplier(cacheSupplier, mockSupplier)

        val configuration = mockk<HttpSupplierConfiguration>(getRandomString())
        every { cacheSupplier.configuration } returns configuration

        assertEquals(configuration, storeEntitySupplier.configuration)
        verify(exactly = 1) { cacheSupplier.configuration }
    }

    interface StoreTest {
        fun `data not stored into cache if data not exists`()
        fun `data stored if found`()
    }

    @Nested
    @DisplayName("Get player uuid")
    inner class GetId : StoreTest {

        @Test
        override fun `data not stored into cache if data not exists`() = runTest {
            val id = createProfileId()
            val name = id.name
            coEvery { mockSupplier.getIdByName(name) } returns null
            assertNull(storeEntitySupplier.getIdByName(name))
            assertNull(cacheEntitySupplier.getIdByName(name))
        }

        @Test
        override fun `data stored if found`() = runTest {
            val id = createProfileId()
            val name = id.name
            coEvery { mockSupplier.getIdByName(name) } returns id
            assertEquals(id, storeEntitySupplier.getIdByName(name))
            assertEquals(id, cacheEntitySupplier.getIdByName(name))
        }

    }

    @Nested
    @DisplayName("Get skin by id")
    inner class GetSkin : StoreTest {

        @Test
        override fun `data not stored into cache if data not exists`() = runTest {
            val skin = createProfileSkin()
            val uuid = skin.id
            coEvery { mockSupplier.getSkinById(uuid) } returns null
            assertNull(storeEntitySupplier.getSkinById(uuid))
            assertNull(cacheEntitySupplier.getSkinById(uuid))
        }

        @Test
        override fun `data stored if found`() = runTest {
            val skin = createProfileSkin()
            val uuid = skin.id
            coEvery { mockSupplier.getSkinById(uuid) } returns skin
            assertEquals(skin, storeEntitySupplier.getSkinById(uuid))
            assertEquals(skin, cacheEntitySupplier.getSkinById(uuid))
        }

    }
}
