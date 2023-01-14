package com.github.rushyverse.core.utils

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.job
import kotlin.coroutines.CoroutineContext
import kotlin.test.assertEquals

fun assertCoroutineContextFromScope(scope: CoroutineScope, coroutineContext: CoroutineContext) {
    assertEquals(scope.coroutineContext.job.key, coroutineContext.job.key)
}