package com.github.rushyverse.core.extension

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.assertThrows
import kotlin.test.Test

class SequenceExtTest {

    @Nested
    inner class ToTypedArray {

        @Test
        fun `should transform with exactly the same size of the sequence`() {
            val list = listOf(1, 2, 3)
            val sequence = list.asSequence()
            val array = sequence.toTypedArray(3)
            assertThat(array).containsExactly(1, 2, 3)
        }

        @Test
        fun `should transform with a larger size than the sequence`() {
            val list = listOf(1, 2, 3)
            val sequence = list.asSequence()
            assertThrows<NoSuchElementException> {
                sequence.toTypedArray(list.size + 1)
            }
        }

        @Test
        fun `should transform with a smaller size than the sequence`() {
            val list = listOf(1, 2, 3)
            val sequence = list.asSequence()
            val array = sequence.toTypedArray(2)
            assertThat(array).containsExactly(1, 2)
        }

    }
}
