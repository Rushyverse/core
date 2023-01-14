package com.github.rushyverse.core.utils

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.job
import kotlin.coroutines.ContinuationInterceptor
import kotlin.coroutines.CoroutineContext
import kotlin.test.assertEquals

fun assertCoroutineContextFromScope(scope: CoroutineScope, coroutineContext: CoroutineContext) {
    assertEquals(scope.coroutineContext.job.key, coroutineContext.job.key)
}

fun assertCoroutineContextUseDispatcher(coroutineContext: CoroutineContext, dispatcher: CoroutineDispatcher) {
    val coroutineDispatcher = coroutineContext[ContinuationInterceptor]
    assertEquals(coroutineDispatcher, dispatcher)
}