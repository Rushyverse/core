package com.github.rushyverse.core.supplier.http

import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.container.createRedisContainer
import com.github.rushyverse.core.data.ProfileIdCacheService
import com.github.rushyverse.core.data.ProfileSkinCacheService
import com.github.rushyverse.core.utils.createProfileId
import com.github.rushyverse.core.utils.getRandomString
import com.github.rushyverse.mojang.api.MojangAPI
import io.lettuce.core.RedisURI
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Timeout
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.util.concurrent.TimeUnit
import kotlin.test.*

@Timeout(3, unit = TimeUnit.SECONDS)
@Testcontainers
class IHttpEntitySupplierStrategyTest {

    companion object {
        @JvmStatic
        @Container
        private val redisContainer = createRedisContainer()
    }

    private lateinit var cacheClient: CacheClient

    private lateinit var configuration: HttpSupplierConfiguration

    private lateinit var mojangAPI: MojangAPI
    private lateinit var cacheEntitySupplier: HttpCacheEntitySupplier

    @BeforeTest
    fun onBefore() = runBlocking {
        cacheClient = CacheClient {
            uri = RedisURI.create(redisContainer.url)
        }

        mojangAPI = mockk(getRandomString())

        cacheEntitySupplier = HttpCacheEntitySupplier(
            HttpSupplierConfiguration(
                mockk(),
                ProfileSkinCacheService(cacheClient),
                ProfileIdCacheService(cacheClient)
            )
        )

        configuration = HttpSupplierConfiguration(mojangAPI, cacheClient)
    }

    @AfterTest
    fun onAfter() = runBlocking<Unit> {
        cacheClient.closeAsync().await()
    }

    @Test
    fun `rest supplier corresponding to the class`() {
        val configuration = HttpSupplierConfiguration(mockk(), mockk())
        assertEquals(HttpEntitySupplier::class, IHttpEntitySupplier.rest(configuration)::class)
    }

    @Test
    fun `cache supplier corresponding to the class`() {
        val configuration = HttpSupplierConfiguration(mockk(), mockk())
        assertEquals(HttpCacheEntitySupplier::class, IHttpEntitySupplier.cache(configuration)::class)
    }

    @Nested
    @DisplayName("Caching rest")
    inner class CachingRest {

        private lateinit var supplier: IHttpEntitySupplier

        @BeforeTest
        fun onBefore() {
            supplier = IHttpEntitySupplier.cachingRest(configuration)
        }

        @Test
        fun `data found in rest is saved into cache`() = runTest {
            val profileId = createProfileId()
            val name = profileId.name
            coEvery { mojangAPI.getUUID(name) } returns profileId
            assertEquals(profileId, supplier.getIdByName(name))
            coVerify(exactly = 1) { mojangAPI.getUUID(name) }
            assertEquals(profileId, cacheEntitySupplier.getIdByName(name))
        }

        @Test
        fun `data present in cache is not used to find value`() = runTest {
            val profileId = createProfileId()
            val name = profileId.name
            cacheEntitySupplier.save(profileId)

            coEvery { mojangAPI.getUUID(name) } returns profileId
            assertEquals(profileId, supplier.getIdByName(name))
            coVerify(exactly = 1) { mojangAPI.getUUID(name) }
        }
    }

    @Nested
    @DisplayName("Cache with Rest fallback")
    inner class CacheWithRestFallback {

        private lateinit var supplier: IHttpEntitySupplier

        @BeforeTest
        fun onBefore() {
            supplier = IHttpEntitySupplier.cacheWithRestFallback(configuration)
        }

        @Test
        fun `data found in rest is not saved into cache`() = runTest {
            val profileId = createProfileId()
            val name = profileId.name
            coEvery { mojangAPI.getUUID(name) } returns profileId
            assertEquals(profileId, supplier.getIdByName(name))
            coVerify(exactly = 1) { mojangAPI.getUUID(name) }
            assertNull(cacheEntitySupplier.getIdByName(name))
        }

        @Test
        fun `data present in cache is use to avoid rest call`() = runTest {
            val profileId = createProfileId()
            val name = profileId.name
            cacheEntitySupplier.save(profileId)

            coEvery { mojangAPI.getUUID(name) } returns profileId
            assertEquals(profileId, supplier.getIdByName(name))
            coVerify(exactly = 0) { mojangAPI.getUUID(name) }
        }
    }

    @Nested
    @DisplayName("Cache with caching result of Rest fallback")
    inner class CacheWithCachingRestFallback {

        private lateinit var supplier: IHttpEntitySupplier

        @BeforeTest
        fun onBefore() {
            supplier = IHttpEntitySupplier.cacheWithCachingRestFallback(configuration)
        }

        @Test
        fun `data found in rest is saved into cache`() = runTest {
            val profileId = createProfileId()
            val name = profileId.name
            coEvery { mojangAPI.getUUID(name) } returns profileId
            assertEquals(profileId, supplier.getIdByName(name))
            coVerify(exactly = 1) { mojangAPI.getUUID(name) }
            assertEquals(profileId, cacheEntitySupplier.getIdByName(name))
        }

        @Test
        fun `data present in cache is use to avoid rest call`() = runTest {
            val profileId = createProfileId()
            val name = profileId.name
            cacheEntitySupplier.save(profileId)

            coEvery { mojangAPI.getUUID(name) } returns profileId
            assertEquals(profileId, supplier.getIdByName(name))
            coVerify(exactly = 0) { mojangAPI.getUUID(name) }
        }
    }

}
