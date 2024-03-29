package com.github.rushyverse.core.data

import com.github.rushyverse.core.cache.AbstractCacheService
import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.mojang.api.ProfileId
import kotlinx.serialization.builtins.serializer
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours

/**
 * Service to retrieve data about profile.
 */
public fun interface IProfileIdService {

    /**
     * Get the profile of a client from his [ProfileId.name].
     * @param name Profile's name.
     */
    public suspend fun getIdByName(name: String): ProfileId?
}


/**
 * Service to manage [ProfileId] data in cache.
 */
public interface IProfileIdCacheService : IProfileIdService {

    /**
     * Save the instance into cache using the key defined by the configuration.
     * @param profile Data that will be stored.
     */
    public suspend fun save(profile: ProfileId)
}

/**
 * Cache service for [ProfileId].
 * @property cacheClient Cache client.
 * @property expirationKey Expiration time applied when a new relationship is set.
 * @property prefixKey Prefix key to identify the data in cache.
 */
public class ProfileIdCacheService(
    client: CacheClient,
    expirationKey: Duration? = 12.hours,
    prefixKey: String = "profileId:",
) : AbstractCacheService(client, prefixKey, expirationKey), IProfileIdCacheService {

    override suspend fun getIdByName(name: String): ProfileId? {
        val key = encodeKeyWithPrefix(name)
        val dataSerial = cacheClient.connect { it.get(key) } ?: return null
        val uuid = decodeFromByteArrayOrNull(String.serializer(), dataSerial) ?: return null
        return ProfileId(id = uuid, name = name)
    }

    override suspend fun save(profile: ProfileId) {
        val key = encodeKeyWithPrefix(profile.name)
        val value = encodeToByteArray(String.serializer(), profile.id)
        cacheClient.connect {
            setWithExpiration(it, key, value)
        }
    }
}
