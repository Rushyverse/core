package com.github.rushyverse.core.cache

import com.github.rushyverse.core.data.UserCacheManager
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.builtins.serializer
import kotlin.time.Duration

public abstract class AbstractUserCacheService(
    client: CacheClient,
    public val userCacheManager: UserCacheManager
) : AbstractCacheService(client) {

    protected open fun encodeUserKey(userId: String, key: String): ByteArray {
        return encodeToByteArray(
            String.serializer(),
            userCacheManager.getFormattedKey(userId) + key
        )
    }

}

public abstract class AbstractDataCacheService(
    client: CacheClient,
    public val prefixKey: String,
    public val expirationKey: Duration?,
) : AbstractCacheService(client) {

    override fun encodeKey(key: String): ByteArray {
        return encodeToByteArray(
            String.serializer(),
            prefixKey + key
        )
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

/**
 * Service to encode and decode information with cache.
 */
public abstract class AbstractCacheService(
    public val cacheClient: CacheClient,
) {

    /**
     * Create the key from a [String] value to identify data in cache.
     * @param key Value using to create key.
     * @return [ByteArray] corresponding to the key using the [prefixKey] and [key].
     */
    protected open fun encodeKey(key: String): ByteArray = encodeToByteArray(
        String.serializer(),
        key
    )

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
}