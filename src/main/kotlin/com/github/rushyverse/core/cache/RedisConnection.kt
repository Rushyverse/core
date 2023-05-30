package com.github.rushyverse.core.cache

import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import kotlin.coroutines.CoroutineContext

/**
 * A coroutine element to store a Redis connection.
 * Will be used to store the connection in the coroutine context and retrieve it later.
 * @property connection Redis connection.
 */
public interface RedisConnection : CoroutineContext.Element {

    public companion object Key : CoroutineContext.Key<RedisConnection>

    public val connection: RedisCoroutinesCommands<ByteArray, ByteArray>

}

/**
 * Implementation of [RedisConnection].
 */
internal class RedisConnectionImpl(override val connection: RedisCoroutinesCommands<ByteArray, ByteArray>) : RedisConnection {

    override val key: CoroutineContext.Key<RedisConnection>
        get() = RedisConnection

}

/**
 * Create a coroutine element to store a Redis connection.
 * @param connection Cache connection.
 * @return A new instance of [RedisConnection].
 */
public fun RedisConnection(connection: RedisCoroutinesCommands<ByteArray, ByteArray>): RedisConnection =
    RedisConnectionImpl(connection)