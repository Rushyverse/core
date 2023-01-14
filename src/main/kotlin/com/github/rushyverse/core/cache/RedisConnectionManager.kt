package com.github.rushyverse.core.cache

import io.lettuce.core.RedisClient
import io.lettuce.core.RedisURI
import io.lettuce.core.api.AsyncCloseable
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection
import io.lettuce.core.support.AsyncConnectionPoolSupport
import io.lettuce.core.support.BoundedAsyncPool
import io.lettuce.core.support.BoundedPoolConfig
import kotlinx.coroutines.future.await
import java.util.concurrent.CompletableFuture


/**
 * Interface to manage the connections to interact with redis.
 */
interface IRedisConnectionManager : AsyncCloseable {

    /**
     * Pool of connections to interact with the cache.
     */
    val poolStateful: BoundedAsyncPool<StatefulRedisConnection<ByteArray, ByteArray>>

    /**
     * Pool of connections to interact in pub/sub with the cache.
     */
    val poolPubSub: BoundedAsyncPool<StatefulRedisPubSubConnection<ByteArray, ByteArray>>

    /**
     * Get a connection from the [poolStateful] to interact with the cache.
     * @return A new connection.
     */
    suspend fun getStatefulConnection(): StatefulRedisConnection<ByteArray, ByteArray>

    /**
     * Get a connection from the [poolPubSub] to interact in pub/sub with the cache.
     * @return A new connection.
     */
    suspend fun getPubSubConnection(): StatefulRedisPubSubConnection<ByteArray, ByteArray>

    /**
     * Close the connection obtained by the [poolStateful].
     */
    suspend fun releaseConnection(connection: StatefulRedisConnection<ByteArray, ByteArray>)

    /**
     * Close the connection obtained by the [poolPubSub].
     */
    suspend fun releaseConnection(connection: StatefulRedisPubSubConnection<ByteArray, ByteArray>)
}

/**
 * Connection manager to interact with redis.
 * @property poolStateful Pool of connections to interact with the cache.
 * @property poolPubSub Pool of connections to interact in pub/sub with the cache.
 */
class RedisConnectionManager(
    override val poolStateful: BoundedAsyncPool<StatefulRedisConnection<ByteArray, ByteArray>>,
    override val poolPubSub: BoundedAsyncPool<StatefulRedisPubSubConnection<ByteArray, ByteArray>>
) : IRedisConnectionManager {

    companion object {
        suspend inline operator fun invoke(
            redisClient: RedisClient,
            codec: RedisCodec<ByteArray, ByteArray>,
            uri: RedisURI,
            poolConfig: BoundedPoolConfig
        ): RedisConnectionManager = RedisConnectionManager(
            AsyncConnectionPoolSupport.createBoundedObjectPoolAsync(
                { redisClient.connectAsync(codec, uri) },
                poolConfig
            ).await(),
            AsyncConnectionPoolSupport.createBoundedObjectPoolAsync(
                { redisClient.connectPubSubAsync(codec, uri) },
                poolConfig
            ).await(),
        )
    }

    override suspend fun getStatefulConnection(): StatefulRedisConnection<ByteArray, ByteArray> {
        return poolStateful.acquire().await()
    }

    override suspend fun getPubSubConnection(): StatefulRedisPubSubConnection<ByteArray, ByteArray> {
        return poolPubSub.acquire().await()
    }

    override suspend fun releaseConnection(connection: StatefulRedisConnection<ByteArray, ByteArray>) {
        poolStateful.release(connection).await()
    }

    override suspend fun releaseConnection(connection: StatefulRedisPubSubConnection<ByteArray, ByteArray>) {
        poolPubSub.release(connection).await()
    }

    override fun closeAsync(): CompletableFuture<Void> {
        return CompletableFuture.allOf(
            poolStateful.closeAsync(),
            poolPubSub.closeAsync()
        )
    }
}