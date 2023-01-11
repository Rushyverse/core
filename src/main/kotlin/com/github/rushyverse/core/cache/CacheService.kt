package com.github.rushyverse.core.cache

import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.builtins.serializer
import kotlin.time.Duration

/**
 * Service to encode and decode information with cache.
 * @property prefixKey Prefix key to identify the data in cache.
 */
abstract class CacheService(
    val client: CacheClient,
    val prefixKey: String,
    val expiration: Duration?
) {

    /**
     * Set the value for the key.
     * If [expiration] is not null, an expiration time will be applied, otherwise the value will be stored forever.
     * @param connection Redis connection.
     * @param key Encoded key.
     * @param value Encoded value.
     */
    protected suspend fun setWithExpiration(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        key: ByteArray,
        value: ByteArray
    ) {
        if (expiration != null) {
            connection.psetex(key, expiration.inWholeMilliseconds, value)
        } else {
            connection.set(key, value)
        }
    }

    /**
     * Create the key from a [String] value to identify data in cache.
     * @param key Value using to create key.
     * @return [ByteArray] corresponding to the key using the [prefixKey] and [key].
     */
    protected fun encodeKey(key: String): ByteArray = encodeToByteArray(
        String.serializer(),
        "$prefixKey$key"
    )

    /**
     * Transform an instance to a [ByteArray] by encoding data using [binaryFormat][CacheClient.binaryFormat].
     * @param value Value that will be serialized.
     * @return Result of the serialization of [value].
     */
    protected fun <T> encodeToByteArray(
        serializer: SerializationStrategy<T>,
        value: T
    ): ByteArray = client.binaryFormat.encodeToByteArray(serializer, value)

    /***
     * Transform a [ByteArray] to a value by decoding data using [binaryFormat][CacheClient.binaryFormat].
     * @param valueSerial Serialization of the value.
     * @return The value from the [valueSerial] decoded.
     */
    protected fun <T> decodeFromByteArrayOrNull(
        deserializer: DeserializationStrategy<T>,
        valueSerial: ByteArray
    ): T? =
        try {
            client.binaryFormat.decodeFromByteArray(deserializer, valueSerial)
        } catch (_: Exception) {
            null
        }
}