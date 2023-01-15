package com.github.rushyverse.core.extension

/**
 * Transform a [Sequence] into an [Array].
 * @receiver The [Sequence] to transform.
 * @param size Size of the [Array].
 * @return A new array with the elements of the sequence.
 */
public inline fun <reified T> Sequence<T>.toTypedArray(size: Int): Array<T> {
    val iter = iterator()
    return Array(size) { iter.next() }
}