package com.github.rushyverse.core.extension

public inline fun <reified T> Sequence<T>.toTypedArray(size: Int): Array<T> {
    val iter = iterator()
    return Array(size) { iter.next() }
}