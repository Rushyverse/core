@file:OptIn(ExperimentalLettuceCoroutinesApi::class, ExperimentalLettuceCoroutinesApi::class)

package com.github.rushyverse.core.data

import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.cache.AbstractCacheService
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
     * @param id Player's ID.
     * @return Information about player's skin.
     */
    suspend fun getSkinById(id: String): ProfileSkin?
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
 * @property expirationKey Expiration time applied when a new relationship is set.
 * @property prefixKey Prefix key to identify the data in cache.
 */
class ProfileSkinCacheService(
    client: CacheClient,
    expirationKey: Duration? = null,
    prefixKey: String = "skin:"
) : AbstractCacheService(client, prefixKey, expirationKey), IProfileSkinCacheService {

    override suspend fun getSkinById(id: String): ProfileSkin? {
        val key = encodeKey(id)
        val dataSerial = client.connect { it.get(key) } ?: return null
        return decodeFromByteArrayOrNull(ProfileSkin.serializer(), dataSerial)
    }

    override suspend fun save(profile: ProfileSkin) {
        val key = encodeKey(profile.id)
        val value = encodeToByteArray(ProfileSkin.serializer(), profile)
        client.connect { setWithExpiration(it, key, value) }
    }
}