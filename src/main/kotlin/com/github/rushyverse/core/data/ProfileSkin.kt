@file:OptIn(ExperimentalLettuceCoroutinesApi::class, ExperimentalLettuceCoroutinesApi::class)

package com.github.rushyverse.core.data

import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.cache.CacheService
import io.github.universeproject.kotlinmojangapi.ProfileSkin
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import kotlin.time.Duration

/**
 * Service to retrieve data about profile.
 */
interface IProfileSkinService {

    /**
     * Retrieve the skin data for a player.
     * A player is represented by his UUID.
     * @param uuid Player's UUID.
     * @return Information about player's skin.
     */
    suspend fun getSkinByUUID(uuid: String): ProfileSkin?
}

/**
 * Service to manage [ProfileSkin] data in cache.
 */
interface IProfileSkinCacheService : IProfileSkinService {

    /**
     * Save the instance into cache using the key defined by the configuration.
     * @param profile Data that will be stored.
     */
    suspend fun save(profile: ProfileSkin)
}

/**
 * Cache service for [ProfileSkin].
 * @property client Cache client.
 * @property expiration Expiration time applied when a new relationship is set.
 * @property prefixKey Prefix key to identify the data in cache.
 */
class ProfileSkinCacheService(
    client: CacheClient,
    expiration: Duration? = null,
    prefixKey: String = "skin:"
) : CacheService(client, prefixKey, expiration), IProfileSkinCacheService {

    override suspend fun getSkinByUUID(uuid: String): ProfileSkin? {
        val key = encodeKey(uuid)
        val dataSerial = client.connect { it.get(key) } ?: return null
        return decodeFromByteArrayOrNull(ProfileSkin.serializer(), dataSerial)
    }

    override suspend fun save(profile: ProfileSkin) {
        val key = encodeKey(profile.id)
        val value = encodeToByteArray(ProfileSkin.serializer(), profile)

        client.connect {
            if (expiration != null) {
                it.psetex(key, expiration.inWholeMilliseconds, value)
            } else {
                it.set(key, value)
            }
        }
    }
}