package com.github.rushyverse.core.cache

import com.github.rushyverse.core.utils.getRandomString
import io.mockk.every
import io.mockk.mockk
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.protobuf.ProtoBuf
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.assertThrows
import java.util.*
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals

class AbstractCacheServiceTest {

    private lateinit var cacheClient: CacheClient

    @BeforeTest
    fun onBefore() {
        cacheClient = mockk() {
            every { binaryFormat } returns ProtoBuf {
                encodeDefaults = false
            }
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
                fun function() = encodeFormatKey(key)
            }

            val encodedKey = cacheService.function()
            val resultKey = cacheClient.binaryFormat.decodeFromByteArray(String.serializer(), encodedKey)

            assertEquals(key, resultKey)
        }

        @Test
        fun `with zero format args without template, the prefix key should not be formatted`() {
            val key = getRandomString()
            val prefixKey = getRandomString()
            val cacheService = object : AbstractCacheService(cacheClient, "$prefixKey:%s:") {
                fun function() = encodeFormatKey(key)
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
                fun function() = encodeFormatKey(key)
            }
            val expected = "$prefixKey:$key"

            val encodedKey = cacheService.function()
            val resultKey = cacheClient.binaryFormat.decodeFromByteArray(String.serializer(), encodedKey)

            assertEquals(expected, resultKey)
        }

        @Test
        fun `with one format args the prefix key should be formatted`() {
            val user = getRandomString()
            val key = getRandomString()
            val prefixKey = getRandomString()
            val cacheService = object : AbstractCacheService(cacheClient, "$prefixKey:%s:") {
                fun function() = encodeFormatKey(key, args = arrayOf(user))
            }
            val expected = "$prefixKey:$user:$key"

            val encodedKey = cacheService.function()
            val resultKey = cacheClient.binaryFormat.decodeFromByteArray(String.serializer(), encodedKey)

            assertEquals(expected, resultKey)
        }

        @Test
        fun `with several many format args the prefix key should be formatted`() {
            val user = getRandomString()
            val guild = getRandomString()
            val key = getRandomString()
            val prefixKey = getRandomString()
            val cacheService = object : AbstractCacheService(cacheClient, "$prefixKey:%s:%s:") {
                fun function() = encodeFormatKey(key, args = arrayOf(user, guild))
            }
            val expected = "$prefixKey:$user:$guild:$key"

            val encodedKey = cacheService.function()
            val resultKey = cacheClient.binaryFormat.decodeFromByteArray(String.serializer(), encodedKey)

            assertEquals(expected, resultKey)
        }

        @Test
        fun `with too many format args the prefix key should be formatted`() {
            val user = getRandomString()
            val key = getRandomString()
            val prefixKey = getRandomString()
            val cacheService = object : AbstractCacheService(cacheClient, "$prefixKey:%s:") {
                fun function() = encodeFormatKey(key, args = arrayOf(user, getRandomString(), getRandomString()))
            }
            val expected = "$prefixKey:$user:$key"

            val encodedKey = cacheService.function()
            val resultKey = cacheClient.binaryFormat.decodeFromByteArray(String.serializer(), encodedKey)

            assertEquals(expected, resultKey)
        }

    }
}