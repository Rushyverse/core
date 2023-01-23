package com.github.rushyverse.core.utils

import com.github.rushyverse.core.cache.AbstractDataCacheService
import com.github.rushyverse.core.cache.CacheClient
import kotlinx.serialization.builtins.serializer

suspend fun CacheClient.getTTL(service: AbstractDataCacheService, id: String): Long? {
    return connect { it.ttl(createKey(service, id)) }
}

fun CacheClient.createKey(
    service: AbstractDataCacheService,
    id: String
): ByteArray {
    return binaryFormat.encodeToByteArray(String.serializer(), service.prefixKey + id)
}