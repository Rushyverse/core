package com.github.rushyverse.core.data.profileId

import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.container.createRedisContainer
import com.github.rushyverse.core.data.ProfileIdCacheService
import com.github.rushyverse.core.utils.createProfileId
import com.github.rushyverse.core.utils.getRandomString
import com.github.rushyverse.core.utils.getTTL
import io.lettuce.core.RedisURI
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.builtins.serializer
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Timeout
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.test.*
import kotlin.time.Duration.Companion.seconds

@Timeout(3, unit = TimeUnit.SECONDS)
@Testcontainers
class ProfileIdCacheServiceTest {

    companion object {
        @JvmStatic
        @Container
        private val redisContainer = createRedisContainer()
    }

    private lateinit var service: ProfileIdCacheService

    private lateinit var cacheClient: CacheClient

    @BeforeTest
    fun onBefore() = runBlocking {
        cacheClient = CacheClient {
            uri = RedisURI.create(redisContainer.url)
        }
        service = ProfileIdCacheService(cacheClient, null, getRandomString())
    }

    @AfterTest
    fun onAfter(): Unit = runBlocking {
        cacheClient.closeAsync().await()
    }

    @Nested
    @DisplayName("Get")
    inner class Get {

        @Test
        fun `data is not into the cache`() = runTest {
            val profile = createProfileId()
            service.save(profile)
            assertNull(service.getIdByName(getRandomString()))
        }

        @Test
        fun `data is retrieved from the cache`() = runTest {
            val profile = createProfileId()
            service.save(profile)
            assertEquals(profile, service.getIdByName(profile.name))
        }

        @Test
        fun `data is retrieved from the cache with name key but serial value is not valid`() = runTest {
            val profile = createProfileId()
            val key = profile.name
            cacheClient.connect {
                val keySerial = cacheClient.binaryFormat.encodeToByteArray(String.serializer(), service.prefixKey + key)
                it.set(keySerial, "test".encodeToByteArray())
            }
            assertNull(service.getIdByName(key))
        }

        @Test
        fun `should not set the expiration of the data if expiration defined`() = runTest {
            val profile = createProfileId()
            service.save(profile)
            assertEquals(-1, cacheClient.getTTL(service, profile.name))
            assertEquals(profile, service.getIdByName(profile.name))
            assertEquals(-1, cacheClient.getTTL(service, profile.name))
        }

        @Test
        fun `should not set the expiration of the data if expiration is not defined`() = runTest {
            val profile = createProfileId()
            service.save(profile)
            assertEquals(-1, cacheClient.getTTL(service, profile.name))

            service = ProfileIdCacheService(cacheClient, 5.seconds, service.prefixKey)
            assertEquals(profile, service.getIdByName(profile.name))
            assertEquals(-1, cacheClient.getTTL(service, profile.name))
        }


    }

    @Nested
    @DisplayName("Save")
    inner class Save {

        @Test
        fun `should define the key`() = runTest {
            val profile = createProfileId()
            val key = profile.name
            service.save(profile)

            cacheClient.connect {
                val expectedKey =
                    cacheClient.binaryFormat.encodeToByteArray(String.serializer(), "${service.prefixKey}$key")
                Assertions.assertThat(it.keys(expectedKey).toList()).hasSize(1)
            }
        }

        @Test
        fun `save identity with key not exists`() = runTest {
            val profile = createProfileId()
            val key = profile.name
            assertNull(service.getIdByName(key))
            service.save(profile)
            assertEquals(profile, service.getIdByName(key))
        }

        @Test
        fun `save identity but key already exists`() = runTest {
            val profile = createProfileId()
            val key = profile.name

            assertNull(service.getIdByName(key))
            service.save(profile)
            assertEquals(profile, service.getIdByName(key))

            val id2 = profile.copy(name = key)
            service.save(id2)
            assertEquals(id2, service.getIdByName(key))
        }

        @Test
        fun `data is saved using the binary format from client`(): Unit = runTest {
            val profile = createProfileId()
            val key = profile.name
            service.save(profile)

            val keySerial = cacheClient.binaryFormat.encodeToByteArray(String.serializer(), service.prefixKey + key)

            val value = cacheClient.connect {
                it.get(keySerial)
            }!!

            val expected = profile.id
            assertEquals(expected, cacheClient.binaryFormat.decodeFromByteArray(String.serializer(), value))
            assertNotEquals(expected, value.decodeToString())
        }

        @Test
        fun `should set the expiration of the data if expiration defined`() = runTest {
            val profile = createProfileId()
            service = ProfileIdCacheService(cacheClient, 40.seconds)
            service.save(profile)
            assertEquals(40, cacheClient.getTTL(service, profile.name))
        }

        @Test
        fun `should not set the expiration of the data if expiration is not defined`() = runTest {
            val profile = createProfileId()
            service = ProfileIdCacheService(cacheClient)
            service.save(profile)
            assertEquals(-1, cacheClient.getTTL(service, profile.name))
        }

    }
}