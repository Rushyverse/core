package com.github.rushyverse.core.data

import com.github.rushyverse.core.cache.AbstractCacheService
import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.mojang.api.ProfileSkin
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours

/**
 * Service to retrieve data about profile.
 */
public fun interface IProfileSkinService {

    /**
     * Retrieve the skin data for a player.
     * A player is represented by his UUID.
     * @param id Player's ID.
     * @return Information about player's skin.
     */
    public suspend fun getSkinById(id: String): ProfileSkin?
}

/**
 * Service to manage [ProfileSkin] data in cache.
 */
public interface IProfileSkinCacheService : IProfileSkinService {

    /**
     * Save the instance into cache using the key defined by the configuration.
     * @param profile Data that will be stored.
     */
    public suspend fun save(profile: ProfileSkin)
}

/**
 * Cache service for [ProfileSkin].
 * @property cacheClient Cache client.
 * @property expirationKey Expiration time applied when a new relationship is set.
 * @property prefixKey Prefix key to identify the data in cache.
 */
public class ProfileSkinCacheService(
    client: CacheClient,
    expirationKey: Duration? = 12.hours,
    prefixKey: String = "skin:"
) : AbstractCacheService(client, prefixKey, expirationKey), IProfileSkinCacheService {

    override suspend fun getSkinById(id: String): ProfileSkin? {
        val key = encodeKeyWithPrefix(id)
        val dataSerial = cacheClient.connect { it.get(key) } ?: return null
        return decodeFromByteArrayOrNull(ProfileSkin.serializer(), dataSerial)
    }

    override suspend fun save(profile: ProfileSkin) {
        val key = encodeKeyWithPrefix(profile.id)
        val value = encodeToByteArray(ProfileSkin.serializer(), profile)
        cacheClient.connect {
            setWithExpiration(it, key, value)
        }
    }
}
