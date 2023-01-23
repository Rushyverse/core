package com.github.rushyverse.core.utils

import com.github.rushyverse.core.cache.AbstractCacheService
import com.github.rushyverse.core.cache.CacheClient
import kotlinx.serialization.builtins.serializer

suspend fun CacheClient.getTTL(service: AbstractCacheService, id: String): Long? {
    return connect { it.ttl(createKey(service, id)) }
}

fun CacheClient.createKey(
    service: AbstractCacheService,
    id: String
): ByteArray {
    return binaryFormat.encodeToByteArray(String.serializer(), service.prefixKey + id)
}