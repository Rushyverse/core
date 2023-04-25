package com.github.rushyverse.core.extension

import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import kotlin.test.Test

class FlowExtTest {

    @Nested
    inner class SafeCollect {

        @Test
        fun `should not throw exception`() = runTest {
            val expectedList = listOf(1, 2, 3)
            val flow = expectedList.asFlow()
            val collected = mutableListOf<Int>()
            flow.safeCollect {
                collected.add(it)
                error("test")
            }
            assertThat(collected).containsExactlyInAnyOrderElementsOf(expectedList)
        }

        @Test
        fun `should do nothing if flow is empty`() = runTest {
            val flow = flowOf<Int>()
            val collected = mutableListOf<Int>()
            flow.safeCollect {
                collected.add(it)
            }
            assertThat(collected).isEmpty()
        }

    }
}