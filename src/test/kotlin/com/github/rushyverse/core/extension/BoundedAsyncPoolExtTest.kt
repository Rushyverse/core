package com.github.rushyverse.core.extension

import io.lettuce.core.support.AsyncConnectionPoolSupport
import io.lettuce.core.support.BoundedPoolConfig
import io.mockk.mockk
import kotlinx.coroutines.future.await
import kotlinx.coroutines.test.runTest
import java.util.concurrent.CompletableFuture
import kotlin.test.Test
import kotlin.test.assertEquals

class BoundedAsyncPoolExtTest {

    @Test
    fun `acquire should get instance and release it`() = runTest {
        val pool = AsyncConnectionPoolSupport.createBoundedObjectPoolAsync(
            { CompletableFuture.completedFuture(mockk()) },
            BoundedPoolConfig.create()
        ).await()

        pool.acquire { _ ->
            assertEquals(0, pool.idle)
            assertEquals(1, pool.objectCount)
        }

        assertEquals(1, pool.idle)
        assertEquals(1, pool.objectCount)
    }
}