package com.github.rushyverse.core.extension

import kotlinx.coroutines.flow.Flow

/**
 * Collects the given flow and runs the given action on each element.
 * If the action throws an exception, it will be caught and ignored.
 * @see Flow.collect
 * @receiver Flow to collect.
 * @param action Action to run on each element.
 */
public suspend fun <T> Flow<T>.safeCollect(action: suspend (T) -> Unit): Unit = collect {
    runCatching {
        action(it)
    }
}