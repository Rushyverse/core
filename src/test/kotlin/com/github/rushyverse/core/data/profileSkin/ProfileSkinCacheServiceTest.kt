package com.github.rushyverse.core.data.profileSkin

import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.container.createRedisContainer
import com.github.rushyverse.core.data.ProfileSkinCacheService
import com.github.rushyverse.core.utils.createProfileSkin
import com.github.rushyverse.core.utils.getRandomString
import com.github.rushyverse.core.utils.getTTL
import io.github.universeproject.kotlinmojangapi.ProfileSkin
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
import java.util.concurrent.TimeUnit
import kotlin.test.*
import kotlin.time.Duration.Companion.seconds

@Timeout(3, unit = TimeUnit.SECONDS)
@Testcontainers
class ProfileSkinCacheServiceTest {

    companion object {
        @JvmStatic
        @Container
        private val redisContainer = createRedisContainer()
    }

    private lateinit var service: ProfileSkinCacheService

    private lateinit var cacheClient: CacheClient

    @BeforeTest
    fun onBefore() = runBlocking {
        cacheClient = CacheClient {
            uri = RedisURI.create(redisContainer.url)
        }
        service = ProfileSkinCacheService(cacheClient, null, getRandomString())
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
            val profile = createProfileSkin()
            service.save(profile)
            assertNull(service.getSkinById(getRandomString()))
        }

        @Test
        fun `data is retrieved from the cache`() = runTest {
            val profile = createProfileSkin()
            service.save(profile)
            assertEquals(profile, service.getSkinById(profile.id))
        }

        @Test
        fun `data is retrieved from the cache with name key but serial value is not valid`() = runTest {
            val profile = createProfileSkin()
            val key = profile.id
            cacheClient.connect {
                val keySerial = cacheClient.binaryFormat.encodeToByteArray(String.serializer(), service.prefixKey + key)
                it.set(keySerial, "test".encodeToByteArray())
            }
            assertNull(service.getSkinById(key))
        }

        @Test
        fun `should not set the expiration of the data if expiration defined`() = runTest {
            val profile = createProfileSkin()
            service.save(profile)
            assertEquals(-1, cacheClient.getTTL(service, profile.id))

            service = ProfileSkinCacheService(cacheClient, 30.seconds, service.prefixKey)
            service.getSkinById(profile.id)

            assertEquals(-1, cacheClient.getTTL(service, profile.id))
        }

        @Test
        fun `should not set the expiration of the data if expiration is not defined`() = runTest {
            val profile = createProfileSkin()
            service = ProfileSkinCacheService(cacheClient)
            service.save(profile)
            assertEquals(-1, cacheClient.getTTL(service, profile.id))
            service.getSkinById(profile.id)
            assertEquals(-1, cacheClient.getTTL(service, profile.id))
        }

    }

    @Nested
    @DisplayName("Save")
    inner class Save {

        @Test
        fun `should define the key`() = runTest {
            val profile = createProfileSkin()
            val key = profile.id
            service.save(profile)

            cacheClient.connect {
                val expectedKey = "${service.prefixKey}$key".encodeToByteArray()
                Assertions.assertThat(it.keys(expectedKey).toList()).hasSize(1)
            }
        }

        @Test
        fun `save identity with key not exists`() = runTest {
            val profile = createProfileSkin()
            val key = profile.id
            assertNull(service.getSkinById(key))
            service.save(profile)
            assertEquals(profile, service.getSkinById(key))
        }

        @Test
        fun `save identity but key already exists`() = runTest {
            val profile = createProfileSkin()
            val key = profile.id

            assertNull(service.getSkinById(key))
            service.save(profile)
            assertEquals(profile, service.getSkinById(key))

            val profile2 = profile.copy(id = key)
            service.save(profile2)
            assertEquals(profile2, service.getSkinById(key))
        }

        @Test
        fun `data is saved using the human readable format from client`(): Unit = runTest {
            val profile = createProfileSkin()
            val key = profile.id
            service.save(profile)

            val keySerial = (service.prefixKey + key).encodeToByteArray()

            val value = cacheClient.connect {
                it.get(keySerial)
            }!!

            assertEquals(profile, cacheClient.binaryFormat.decodeFromByteArray(ProfileSkin.serializer(), value))
        }

        @Test
        fun `should set the expiration of the data if expiration defined`() = runTest {
            val profile = createProfileSkin()
            service = ProfileSkinCacheService(cacheClient, 30.seconds)
            service.save(profile)
            assertEquals(30, cacheClient.getTTL(service, profile.id))
        }

        @Test
        fun `should not set the expiration of the data if expiration is not defined`() = runTest {
            val profile = createProfileSkin()
            service = ProfileSkinCacheService(cacheClient)
            service.save(profile)
            assertEquals(-1, cacheClient.getTTL(service, profile.id))
        }

    }
}