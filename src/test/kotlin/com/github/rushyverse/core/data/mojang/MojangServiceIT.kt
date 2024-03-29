package com.github.rushyverse.core.data.mojang

import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.container.createRedisContainer
import com.github.rushyverse.core.data.MojangService
import com.github.rushyverse.core.supplier.http.HttpSupplierConfiguration
import com.github.rushyverse.core.supplier.http.IHttpEntitySupplier
import com.github.rushyverse.mojang.api.MojangAPIImpl
import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.serialization.kotlinx.json.*
import io.lettuce.core.RedisURI
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Timeout
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.util.concurrent.TimeUnit
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals

@Timeout(10, unit = TimeUnit.SECONDS)
@Testcontainers
class MojangServiceIT {

    companion object {
        @JvmStatic
        @Container
        private val redisContainer = createRedisContainer()
    }

    private lateinit var cache: CacheClient
    private lateinit var httpClient: HttpClient
    private lateinit var services: HttpSupplierConfiguration

    @BeforeTest
    fun onBefore() = runBlocking {
        cache = CacheClient {
            uri = RedisURI.create(redisContainer.url)
        }
        httpClient = HttpClient(CIO) {
            install(ContentNegotiation) {
                json(Json { ignoreUnknownKeys = true })
            }
        }

        val mojangAPI = MojangAPIImpl(httpClient)
        services = HttpSupplierConfiguration(mojangAPI, cache)
    }

    @AfterTest
    fun onAfter() = runBlocking<Unit> {
        cache.connect {
            it.flushall()
        }

        httpClient.close()
        cache.closeAsync().await()
    }

    @Nested
    inner class GetSkinById {

        @Test
        fun `should store and retrieve from cache`() = runTest {
            val id = "069a79f4-44e9-4726-a5be-fca90e38aaf5"
            val supplier = IHttpEntitySupplier.cacheWithCachingRestFallback(services)
            val serviceWeb = MojangService(supplier)
            val skinFromWeb = serviceWeb.getSkinById(id)

            val serviceCache = serviceWeb.withStrategy(IHttpEntitySupplier::cache)
            val skinFromCache = serviceCache.getSkinById(id)

            assertEquals(skinFromWeb, skinFromCache)
        }

    }


    @Nested
    inner class GetSkinByName {

        @Test
        fun `should store and retrieve from cache`() = runTest {
            val name = "Notch"
            val supplier = IHttpEntitySupplier.cacheWithCachingRestFallback(services)
            val serviceWeb = MojangService(supplier)
            val skinFromWeb = serviceWeb.getSkinByName(name)

            val serviceCache = serviceWeb.withStrategy(IHttpEntitySupplier::cache)
            val skinFromCache = serviceCache.getSkinByName(name)

            assertEquals(skinFromWeb, skinFromCache)
        }

    }

    @Nested
    inner class GetIdByName {

        @Test
        fun `should store and retrieve from cache`() = runTest {
            val name = "Notch"
            val supplier = IHttpEntitySupplier.cacheWithCachingRestFallback(services)
            val serviceWeb = MojangService(supplier)
            val skinFromWeb = serviceWeb.getIdByName(name)

            val serviceCache = serviceWeb.withStrategy(IHttpEntitySupplier::cache)
            val skinFromCache = serviceCache.getIdByName(name)

            assertEquals(skinFromWeb, skinFromCache)
        }

    }
}
