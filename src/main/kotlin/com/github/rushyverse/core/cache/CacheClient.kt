@file:OptIn(ExperimentalSerializationApi::class, ExperimentalLettuceCoroutinesApi::class)

package com.github.rushyverse.core.cache

import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisURI
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.coroutines
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.support.AsyncConnectionPoolSupport
import io.lettuce.core.support.BoundedAsyncPool
import io.lettuce.core.support.BoundedPoolConfig
import kotlinx.coroutines.future.await
import kotlinx.serialization.BinaryFormat
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.protobuf.ProtoBuf

/**
 * Wrapper of [RedisClient] using pool to manage connection.
 * @property uri URI to connect the client.
 * @property client Redis client.
 * @property binaryFormat Object to encode and decode information.
 * @property pool Pool of connection from [client].
 */
public class CacheClient(
    public val uri: RedisURI,
    public val client: RedisClient,
    public val binaryFormat: BinaryFormat,
    public val pool: BoundedAsyncPool<StatefulRedisConnection<ByteArray, ByteArray>>
) : AutoCloseable {

    public companion object {
        public suspend inline operator fun invoke(builder: Builder.() -> Unit): CacheClient =
            Builder().apply(builder).build()
    }

    public object Default {
        /**
         * @see [CacheClient.binaryFormat].
         */
        public val binaryFormat: ProtoBuf = ProtoBuf {
            encodeDefaults = false
        }

        /**
         * Codec to encode/decode keys and values.
         */
        public val codec: ByteArrayCodec get() = ByteArrayCodec.INSTANCE
    }

    /**
     * Builder class to simplify the creation of [CacheClient].
     * @property uri @see [CacheClient.uri].
     * @property client @see [CacheClient.client].
     * @property binaryFormat @see [CacheClient.binaryFormat].
     * @property codec @see Codec to encode/decode keys and values.
     * @property poolConfiguration Configuration to create the pool of connections to interact with cache.
     */
    @Suppress("MemberVisibilityCanBePrivate")
    public class Builder {
        public lateinit var uri: RedisURI
        public var client: RedisClient? = null
        public var binaryFormat: BinaryFormat = Default.binaryFormat
        public var codec: RedisCodec<ByteArray, ByteArray> = Default.codec
        public var poolConfiguration: BoundedPoolConfig? = null

        /**
         * Build the instance of [CacheClient] with the values defined in builder.
         * @return A new instance.
         */
        public suspend fun build(): CacheClient {
            val redisClient: RedisClient = client ?: RedisClient.create()
            val codec: RedisCodec<ByteArray, ByteArray> = this.codec

            return CacheClient(
                uri = uri,
                client = redisClient,
                binaryFormat = binaryFormat,
                pool = AsyncConnectionPoolSupport.createBoundedObjectPoolAsync(
                    { redisClient.connectAsync(codec, uri) },
                    poolConfiguration ?: BoundedPoolConfig.builder().maxTotal(-1).build()
                ).await()
            )
        }
    }

    /**
     * Use a connection from the [pool] to interact with the cache.
     * At the end of the method, the connection is returned to the pool.
     * @param body Function using the connection.
     * @return An instance from [body].
     */
    public suspend inline fun <T> connect(body: (RedisCoroutinesCommands<ByteArray, ByteArray>) -> T): T {
        val connection = pool.acquire().await()
        return try {
            body(connection.coroutines())
        } finally {
            pool.release(connection).await()
        }
    }

    override fun close() {
        try {
            pool.close()
        } finally {
            client.shutdown()
        }
    }

    /**
     * Requests to close this object and releases any system resources associated with it. If the object is already closed then invoking this method has no effect.
     * All connections from the [pool] will be closed.
     */
    public suspend fun closeAsync() {
        try {
            pool.closeAsync().await()
        } finally {
            client.shutdownAsync().await()
        }
    }

}