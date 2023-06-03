package com.github.rushyverse.core.extension

import io.lettuce.core.support.BoundedAsyncPool
import kotlinx.coroutines.future.await
import kotlin.contracts.InvocationKind
import kotlin.contracts.contract

/**
 * Use a connection from the [BoundedAsyncPool] to interact with the cache.
 * At the end of the method, the connection is returned to the pool.
 * @param body Function using the connection.
 * @return An instance from [body].
 */
public suspend inline fun <T, R> BoundedAsyncPool<T>.acquire(body: (T) -> R): R {
    contract {
        callsInPlace(body, InvocationKind.EXACTLY_ONCE)
    }

    val connection = acquire().await()
    return try {
        body(connection)
    } finally {
        release(connection).await()
    }
}
