package com.github.rushyverse.core.utils

import com.github.rushyverse.core.cache.AbstractCacheService
import com.github.rushyverse.core.cache.CacheClient
import kotlinx.serialization.builtins.serializer
import java.util.*

suspend fun CacheClient.getTTL(service: AbstractCacheService, uuid: UUID): Long? {
    return getTTL(service, uuid.toString())
}

suspend fun CacheClient.getTTL(service: AbstractCacheService, id: String): Long? {
    return connect { it.ttl(createKey(service, id)) }
}

fun CacheClient.createKey(
    service: AbstractCacheService,
    uuid: UUID
): ByteArray {
    return createKey(service, uuid.toString())
}

fun CacheClient.createKey(
    service: AbstractCacheService,
    id: String
): ByteArray {
    return binaryFormat.encodeToByteArray(String.serializer(), service.prefixKey + id)
}