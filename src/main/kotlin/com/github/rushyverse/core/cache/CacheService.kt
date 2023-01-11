package com.github.rushyverse.core.cache

import kotlinx.serialization.BinaryFormat
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.builtins.serializer

/**
 * Service to encode and decode information with cache.
 * @property prefixKey Prefix key to identify the data in cache.
 */
abstract class CacheService(val prefixKey: String) {

    /**
     * Create the key from a [String] value to identify data in cache.
     * @param key Value using to create key.
     * @return [ByteArray] corresponding to the key using the [prefixKey] and [key].
     */
    protected fun encodeKey(binaryFormat: BinaryFormat, key: String): ByteArray = encodeToByteArray(
        binaryFormat,
        String.serializer(),
        "$prefixKey$key"
    )

    /**
     * Transform an instance to a [ByteArray] by encoding data using [binaryFormat][CacheClient.binaryFormat].
     * @param value Value that will be serialized.
     * @return Result of the serialization of [value].
     */
    protected fun <T> encodeToByteArray(
        binaryFormat: BinaryFormat,
        serializer: SerializationStrategy<T>,
        value: T
    ): ByteArray = binaryFormat.encodeToByteArray(serializer, value)

    /***
     * Transform a [ByteArray] to a value by decoding data using [binaryFormat][CacheClient.binaryFormat].
     * @param valueSerial Serialization of the value.
     * @return The value from the [valueSerial] decoded.
     */
    protected fun <T> decodeFromByteArrayOrNull(
        binaryFormat: BinaryFormat,
        deserializer: DeserializationStrategy<T>,
        valueSerial: ByteArray
    ): T? =
        try {
            binaryFormat.decodeFromByteArray(deserializer, valueSerial)
        } catch (_: Exception) {
            null
        }
}