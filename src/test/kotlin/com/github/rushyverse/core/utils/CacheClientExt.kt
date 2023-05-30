package com.github.rushyverse.core.utils

import com.github.rushyverse.core.cache.AbstractCacheService
import com.github.rushyverse.core.cache.CacheClient

suspend fun CacheClient.getTTL(service: AbstractCacheService, id: String): Long? {
    return connect { it.ttl(createKey(service, id)) }
}

fun CacheClient.createKey(
    service: AbstractCacheService,
    id: String
): ByteArray {
    return (service.prefixKey + id).encodeToByteArray()
}