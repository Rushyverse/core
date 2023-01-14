package com.github.rushyverse.core.utils

import kotlinx.coroutines.CoroutineDispatcher
import kotlin.coroutines.ContinuationInterceptor
import kotlin.coroutines.CoroutineContext
import kotlin.test.assertEquals

fun assertCoroutineContextUseDispatcher(coroutineContext: CoroutineContext, dispatcher: CoroutineDispatcher) {
    val coroutineDispatcher = coroutineContext[ContinuationInterceptor]
    assertEquals(coroutineDispatcher, dispatcher)
}