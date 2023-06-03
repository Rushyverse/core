package com.github.rushyverse.core.cache

import com.github.rushyverse.core.container.createRedisContainer
import com.github.rushyverse.core.serializer.UUIDSerializer
import com.github.rushyverse.core.utils.getRandomString
import com.github.rushyverse.core.utils.getTTL
import io.lettuce.core.RedisURI
import io.mockk.every
import io.mockk.spyk
import io.mockk.verify
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.protobuf.ProtoBuf
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.assertThrows
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.time.Duration.Companion.seconds

@Timeout(3, unit = TimeUnit.SECONDS)
@Testcontainers
class AbstractCacheServiceTest {

    companion object {
        @JvmStatic
        @Container
        private val redisContainer = createRedisContainer()
    }

    private lateinit var cacheClient: CacheClient

    @BeforeTest
    fun onBefore() = runBlocking {
        val cacheClientImpl = CacheClient {
            uri = RedisURI.create(redisContainer.url)
        }

        cacheClient = spyk(cacheClientImpl) {
            every { binaryFormat } returns spyk(ProtoBuf {
                encodeDefaults = false
            })
        }
    }

    @Test
    fun `default prefix key user cache match`() {
        assertEquals("user:%s:", AbstractCacheService.DEFAULT_PREFIX_KEY_USER_CACHE)
    }

    @Nested
    inner class EncodeFormatKey {

        @Test
        fun `with empty prefix key should be the key`() {
            val key = getRandomString()
            val cacheService = object : AbstractCacheService(cacheClient, "") {
                fun function() = encodeFormattedKeyWithPrefix(key)
            }

            val encodedKey = cacheService.function()
            val resultKey = encodedKey.decodeToString()
            assertEquals(key, resultKey)

            val binaryFormat = cacheClient.binaryFormat
            verify(exactly = 0) { binaryFormat.encodeToByteArray(String.serializer(), any()) }
        }

        @Test
        fun `with empty key should be the prefix key`() {
            val prefixKey = getRandomString()
            val cacheService = object : AbstractCacheService(cacheClient, prefixKey) {
                fun function() = encodeFormattedKeyWithPrefix("")
            }

            val encodedKey = cacheService.function()
            val resultKey = encodedKey.decodeToString()
            assertEquals(prefixKey, resultKey)

            val binaryFormat = cacheClient.binaryFormat
            verify(exactly = 0) { binaryFormat.encodeToByteArray(String.serializer(), any()) }
        }

        @Test
        fun `with empty key and empty prefix key should be empty string`() {
            val cacheService = object : AbstractCacheService(cacheClient, "") {
                fun function() = encodeFormattedKeyWithPrefix("")
            }

            val encodedKey = cacheService.function()
            val resultKey = encodedKey.decodeToString()
            assertEquals("", resultKey)

            val binaryFormat = cacheClient.binaryFormat
            verify(exactly = 0) { binaryFormat.encodeToByteArray(String.serializer(), any()) }
        }

        @Test
        fun `with zero format args without template, the prefix key should not be formatted`() {
            val key = getRandomString()
            val prefixKey = getRandomString()
            val cacheService = object : AbstractCacheService(cacheClient, "$prefixKey:%s:") {
                fun function() = encodeFormattedKeyWithPrefix(key)
            }

            assertThrows<MissingFormatArgumentException> {
                cacheService.function()
            }
        }

        @Test
        fun `with zero format args the prefix key should not be formatted`() {
            val key = getRandomString()
            val prefixKey = getRandomString()
            val cacheService = object : AbstractCacheService(cacheClient, "$prefixKey:") {
                fun function() = encodeFormattedKeyWithPrefix(key)
            }
            val expected = "$prefixKey:$key"

            val encodedKey = cacheService.function()
            val resultKey = encodedKey.decodeToString()
            assertEquals(expected, resultKey)

            val binaryFormat = cacheClient.binaryFormat
            verify(exactly = 0) { binaryFormat.encodeToByteArray(String.serializer(), any()) }
        }

        @Test
        fun `with one format args the prefix key should be formatted`() {
            val user = getRandomString()
            val key = getRandomString()
            val prefixKey = getRandomString()
            val cacheService = object : AbstractCacheService(cacheClient, "$prefixKey:%s:") {
                fun function() = encodeFormattedKeyWithPrefix(key, argsFormat = arrayOf(user))
            }
            val expected = "$prefixKey:$user:$key"

            val encodedKey = cacheService.function()
            val resultKey = encodedKey.decodeToString()
            assertEquals(expected, resultKey)

            val binaryFormat = cacheClient.binaryFormat
            verify(exactly = 0) { binaryFormat.encodeToByteArray(String.serializer(), any()) }
        }

        @Test
        fun `with several many format args the prefix key should be formatted`() {
            val user = getRandomString()
            val guild = getRandomString()
            val key = getRandomString()
            val prefixKey = getRandomString()
            val cacheService = object : AbstractCacheService(cacheClient, "$prefixKey:%s:%s:") {
                fun function() = encodeFormattedKeyWithPrefix(key, argsFormat = arrayOf(user, guild))
            }
            val expected = "$prefixKey:$user:$guild:$key"

            val encodedKey = cacheService.function()
            val resultKey = encodedKey.decodeToString()
            assertEquals(expected, resultKey)

            val binaryFormat = cacheClient.binaryFormat
            verify(exactly = 0) { binaryFormat.encodeToByteArray(String.serializer(), any()) }
        }

        @Test
        fun `with too many format args the prefix key should be formatted`() {
            val user = getRandomString()
            val key = getRandomString()
            val prefixKey = getRandomString()
            val cacheService = object : AbstractCacheService(cacheClient, "$prefixKey:%s:") {
                fun function() =
                    encodeFormattedKeyWithPrefix(key, argsFormat = arrayOf(user, getRandomString(), getRandomString()))
            }
            val expected = "$prefixKey:$user:$key"

            val encodedKey = cacheService.function()
            val resultKey = encodedKey.decodeToString()
            assertEquals(expected, resultKey)

            val binaryFormat = cacheClient.binaryFormat
            verify(exactly = 0) { binaryFormat.encodeToByteArray(String.serializer(), any()) }
        }

    }

    @Nested
    inner class EncodeKey {

        @Test
        fun `with empty prefix key should be the key`() {
            val key = getRandomString()
            val cacheService = object : AbstractCacheService(cacheClient, "") {
                fun function() = encodeKeyWithPrefix(key)
            }

            val encodedKey = cacheService.function()
            val resultKey = encodedKey.decodeToString()
            assertEquals(key, resultKey)

            val binaryFormat = cacheClient.binaryFormat
            verify(exactly = 0) { binaryFormat.encodeToByteArray(String.serializer(), key) }
        }

        @Test
        fun `with empty key should be the prefix key`() {
            val prefixKey = getRandomString()
            val cacheService = object : AbstractCacheService(cacheClient, prefixKey) {
                fun function() = encodeKeyWithPrefix("")
            }

            val encodedKey = cacheService.function()
            val resultKey = encodedKey.decodeToString()
            assertEquals(prefixKey, resultKey)

            val binaryFormat = cacheClient.binaryFormat
            verify(exactly = 0) { binaryFormat.encodeToByteArray(String.serializer(), any()) }
        }

        @Test
        fun `with empty key and empty prefix key should be empty string`() {
            val cacheService = object : AbstractCacheService(cacheClient, "") {
                fun function() = encodeKeyWithPrefix("")
            }

            val encodedKey = cacheService.function()
            val resultKey = encodedKey.decodeToString()
            assertEquals("", resultKey)

            val binaryFormat = cacheClient.binaryFormat
            verify(exactly = 0) { binaryFormat.encodeToByteArray(String.serializer(), any()) }
        }

        @Test
        fun `with not empty prefix key should be concatenation of the prefix key with key`() {
            val key = getRandomString()
            val prefixKey = getRandomString()
            val cacheService = object : AbstractCacheService(cacheClient, prefixKey) {
                fun function() = encodeKeyWithPrefix(key)
            }
            val expected = "$prefixKey$key"

            val encodedKey = cacheService.function()
            val resultKey = encodedKey.decodeToString()
            assertEquals(expected, resultKey)

            val binaryFormat = cacheClient.binaryFormat
            verify(exactly = 0) { binaryFormat.encodeToByteArray(String.serializer(), any()) }
        }

    }

    @Nested
    inner class EncodeToByteArray {

        @Test
        fun `should encode string to byte array`() {
            val serializer = String.serializer()
            val key = getRandomString()
            val cacheService = object : AbstractCacheService(cacheClient, "") {
                fun function() = encodeToByteArray(serializer, key)
            }

            val encodedKey = cacheService.function()
            val resultKey = cacheClient.binaryFormat.decodeFromByteArray(serializer, encodedKey)

            assertEquals(key, resultKey)
            val binaryFormat = cacheClient.binaryFormat
            verify(exactly = 1) { binaryFormat.encodeToByteArray(serializer, key) }
        }

        @Test
        fun `should encode uuid to byte array`() {
            val serializer = UUIDSerializer
            val key = UUID.randomUUID()
            val cacheService = object : AbstractCacheService(cacheClient, "") {
                fun function() = encodeToByteArray(serializer, key)
            }

            val encodedKey = cacheService.function()
            val resultKey = cacheClient.binaryFormat.decodeFromByteArray(serializer, encodedKey)

            assertEquals(key, resultKey)
            val binaryFormat = cacheClient.binaryFormat
            verify(exactly = 1) { binaryFormat.encodeToByteArray(serializer, key) }
        }

    }

    @Nested
    inner class DecodeFromByteArrayOrNull {

        @Test
        fun `should decode string from byte array`() {
            val serializer = String.serializer()
            val key = getRandomString()
            val encodedKey = cacheClient.binaryFormat.encodeToByteArray(serializer, key)

            val cacheService = object : AbstractCacheService(cacheClient, "") {
                fun function() = decodeFromByteArrayOrNull(serializer, encodedKey)
            }

            val resultKey = cacheService.function()

            assertEquals(key, resultKey)
            val binaryFormat = cacheClient.binaryFormat
            verify(exactly = 1) { binaryFormat.decodeFromByteArray(serializer, encodedKey) }
        }

        @Test
        fun `should decode uuid from byte array`() {
            val serializer = UUIDSerializer
            val key = UUID.randomUUID()
            val encodedKey = cacheClient.binaryFormat.encodeToByteArray(serializer, key)

            val cacheService = object : AbstractCacheService(cacheClient, "") {
                fun function() = decodeFromByteArrayOrNull(serializer, encodedKey)
            }

            val resultKey = cacheService.function()

            assertEquals(key, resultKey)
            val binaryFormat = cacheClient.binaryFormat
            verify(exactly = 1) { binaryFormat.decodeFromByteArray(serializer, encodedKey) }
        }

        @Test
        fun `should returns null if the value is not deserializable`() {
            val serializer = UUIDSerializer
            val encodedKey = byteArrayOf(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)

            val cacheService = object : AbstractCacheService(cacheClient, "") {
                fun function() = decodeFromByteArrayOrNull(serializer, encodedKey)
            }

            val resultKey = cacheService.function()

            assertNull(resultKey)
            val binaryFormat = cacheClient.binaryFormat
            verify(exactly = 1) { binaryFormat.decodeFromByteArray(serializer, encodedKey) }
        }

    }

    @Nested
    inner class SetWithExpiration {

        @Test
        fun `should set value with expiration`() = runTest {
            val binaryFormat = cacheClient.binaryFormat

            val key = getRandomString()
            val encodeKey = key.encodeToByteArray()

            val value = getRandomString()
            val encodeValue = binaryFormat.encodeToByteArray(String.serializer(), value)

            val expiration = 10.seconds

            val cacheService = object : AbstractCacheService(cacheClient, "", expiration) {
                suspend fun function() {
                    cacheClient.connect {
                        setWithExpiration(
                            it,
                            encodeKey,
                            encodeValue
                        )
                    }
                }
            }

            cacheService.function()

            cacheClient.connect {
                assertThat(it.keys(encodeKey).toList()).containsExactly(encodeKey)
            }
            assertEquals(10, cacheClient.getTTL(cacheService, key))
        }

        @Test
        fun `should set value without expiration`() = runTest {
            val binaryFormat = cacheClient.binaryFormat

            val key = getRandomString()
            val encodeKey = key.encodeToByteArray()

            val value = getRandomString()
            val encodeValue = binaryFormat.encodeToByteArray(String.serializer(), value)

            val cacheService = object : AbstractCacheService(cacheClient, "", null) {
                suspend fun function() {
                    cacheClient.connect {
                        setWithExpiration(
                            it,
                            encodeKey,
                            encodeValue
                        )
                    }
                }
            }

            cacheService.function()

            cacheClient.connect {
                assertThat(it.keys(encodeKey).toList()).containsExactly(encodeKey)
            }
            assertEquals(-1, cacheClient.getTTL(cacheService, key))
        }
    }
}
