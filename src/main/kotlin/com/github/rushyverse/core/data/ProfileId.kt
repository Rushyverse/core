@file:OptIn(ExperimentalLettuceCoroutinesApi::class)

package com.github.rushyverse.core.data

import com.github.rushyverse.core.cache.AbstractCacheService
import com.github.rushyverse.core.cache.CacheClient
import io.github.universeproject.kotlinmojangapi.ProfileId
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.api.coroutines.multi
import kotlinx.serialization.builtins.serializer
import kotlin.time.Duration

/**
 * Service to retrieve data about profile.
 */
public interface IProfileIdService {

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
    public val expirationKey: Duration? = null,
    public val prefixKey: String = "profile:",
) : AbstractCacheService(client), IProfileIdCacheService {

    override suspend fun getIdByName(name: String): ProfileId? {
        val key = encodeKey(prefixKey + name)
        val dataSerial = cacheClient.connect { it.get(key) } ?: return null
        val uuid = decodeFromByteArrayOrNull(String.serializer(), dataSerial) ?: return null
        return ProfileId(id = uuid, name = name)
    }

    override suspend fun save(profile: ProfileId) {
        val key = encodeKey(prefixKey + profile.name)
        val value = encodeToByteArray(String.serializer(), profile.id)
        cacheClient.connect {
            it.multi {
                if (set(key, value) == "OK" && expirationKey != null){
                    pexpire(key, expirationKey.inWholeMilliseconds)
                }
            }
        }
    }
}