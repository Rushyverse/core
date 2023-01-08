@file:OptIn(ExperimentalLettuceCoroutinesApi::class)

package com.github.rushyverse.core.data.profileId

import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.container.createRedisContainer
import com.github.rushyverse.core.data.ProfileIdCacheService
import com.github.rushyverse.core.utils.createProfileId
import com.github.rushyverse.core.utils.getRandomString
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.RedisURI
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.builtins.serializer
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import kotlin.test.*
import kotlin.time.Duration.Companion.seconds

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
    fun onAfter() {
        cacheClient.close()
    }

    @Nested
    @DisplayName("Get")
    inner class Get {

        @Test
        fun `data is not into the cache`() = runTest {
            val profile = createProfileId()
            service.save(profile)
            assertNull(service.getByName(getRandomString()))
        }

        @Test
        fun `data is retrieved from the cache`() = runTest {
            val profile = createProfileId()
            service.save(profile)
            assertEquals(profile, service.getByName(profile.name))
        }

        @Test
        fun `data is retrieved from the cache with name key but serial value is not valid`() = runTest {
            val profile = createProfileId()
            val key = profile.name
            cacheClient.connect {
                val keySerial = cacheClient.binaryFormat.encodeToByteArray(String.serializer(), service.prefixKey + key)
                it.set(keySerial, "test".encodeToByteArray())
            }
            assertNull(service.getByName(key))
        }

    }

    @Nested
    @DisplayName("Save")
    inner class Save {

        @Test
        fun `save identity with key not exists`() = runTest {
            val profile = createProfileId()
            val key = profile.name
            assertNull(service.getByName(key))
            service.save(profile)
            assertEquals(profile, service.getByName(key))
        }

        @Test
        fun `save identity but key already exists`() = runTest {
            val profile = createProfileId()
            val key = profile.name

            assertNull(service.getByName(key))
            service.save(profile)
            assertEquals(profile, service.getByName(key))

            val id2 = profile.copy(name = key)
            service.save(id2)
            assertEquals(id2, service.getByName(key))
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

    }

    @Nested
    inner class Expiration {

        @Test
        fun `should can't retrieve data after expiration`() = runBlocking {
            val profile = createProfileId()
            val expiration = 1.seconds
            service = ProfileIdCacheService(cacheClient, expiration)
            service.save(profile)
            assertEquals(profile, service.getByName(profile.name))
            delay(0.5.seconds)
            assertEquals(profile, service.getByName(profile.name))
            delay(0.5.seconds)
            assertNull(service.getByName(profile.name))
        }

    }
}