package com.github.rushyverse.core.cache

import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.SerializationStrategy
import kotlin.time.Duration

/**
 * Service to encode and decode information with cache.
 */
public abstract class AbstractCacheService(
    public val cacheClient: CacheClient,
    public val prefixKey: String,
    public val expirationKey: Duration? = null
) {

    public companion object {
        /**
         * Default prefix key for user cache.
         * When used, the key must be formatted using another string to put the user id.
         */
        public const val DEFAULT_PREFIX_KEY_USER_CACHE: String = "user:%s:"
    }

    /**
     * Use [argsFormat] to format [prefixKey] before concatenating it with [key].
     * The result will be the final key used to identify data in cache.
     * @param key The key to use.
     * @param argsFormat The arguments to use to format [prefixKey].
     * @return [ByteArray] corresponding to the key using the [prefixKey] and [key].
     */
    protected open fun encodeFormattedKeyUsingPrefix(key: String, vararg argsFormat: String): ByteArray {
        return encodeKey(prefixKey.format(*argsFormat) + key)
    }

    /**
     * Create the key from a [String] value to identify data in cache.
     * @param key Value using to create key.
     * @return [ByteArray] corresponding to the key using the [prefixKey] and [key].
     */
    protected open fun encodeKeyUsingPrefix(key: String): ByteArray {
        return encodeKey(prefixKey + key)
    }

    /**
     * Encode a [String] to a [ByteArray] to identify data in cache.
     * @param key Value using to create key.
     * @return [ByteArray] corresponding to the key.
     */
    protected open fun encodeKey(key: String): ByteArray {
        return key.encodeToByteArray()
    }

    /**
     * Transform an instance to a [ByteArray] by encoding data using [binaryFormat][CacheClient.binaryFormat].
     * @param value Value that will be serialized.
     * @return Result of the serialization of [value].
     */
    protected open fun <T> encodeToByteArray(
        serializer: SerializationStrategy<T>,
        value: T
    ): ByteArray = cacheClient.binaryFormat.encodeToByteArray(serializer, value)

    /***
     * Transform a [ByteArray] to a value by decoding data using [binaryFormat][CacheClient.binaryFormat].
     * @param valueSerial Serialization of the value.
     * @return The value from the [valueSerial] decoded.
     */
    protected open fun <T> decodeFromByteArrayOrNull(
        deserializer: DeserializationStrategy<T>,
        valueSerial: ByteArray
    ): T? =
        try {
            cacheClient.binaryFormat.decodeFromByteArray(deserializer, valueSerial)
        } catch (_: Exception) {
            null
        }

    /**
     * Set the value for the key.
     * If [expirationKey] is not null, an expiration time will be applied, otherwise the value will be stored forever.
     * @param connection Redis connection.
     * @param key Encoded key.
     * @param value Encoded value.
     */
    protected suspend fun setWithExpiration(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        key: ByteArray,
        value: ByteArray
    ) {
        if (expirationKey != null) {
            connection.psetex(key, expirationKey.inWholeMilliseconds, value)
        } else {
            connection.set(key, value)
        }
    }
}