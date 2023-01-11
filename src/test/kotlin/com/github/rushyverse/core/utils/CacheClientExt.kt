package com.github.rushyverse.core.utils

import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.cache.CacheService
import kotlinx.serialization.builtins.serializer
import java.util.*

suspend fun CacheClient.getTTL(service: CacheService, uuid: UUID): Long? {
    return getTTL(service, uuid.toString())
}

suspend fun CacheClient.getTTL(service: CacheService, id: String): Long? {
    return connect { it.ttl(createKey(service, id)) }
}

fun CacheClient.createKey(
    service: CacheService,
    uuid: UUID
): ByteArray {
    return createKey(service, uuid.toString())
}

fun CacheClient.createKey(
    service: CacheService,
    id: String
): ByteArray {
    return binaryFormat.encodeToByteArray(String.serializer(), service.prefixKey + id)
}