@file:OptIn(ExperimentalLettuceCoroutinesApi::class, ExperimentalLettuceCoroutinesApi::class)

package com.github.rushyverse.core.data

import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.cache.CacheService
import com.github.rushyverse.core.supplier.http.IHttpEntitySupplier
import com.github.rushyverse.core.supplier.http.IHttpStrategizable
import io.github.universeproject.kotlinmojangapi.ProfileSkin
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import kotlin.time.Duration

interface IProfileSkinCacheService {
    /**
     * Get the instance of [ProfileSkin] linked to the [uuid] data.
     * @param uuid UUID of the user.
     * @return The instance stored if found, or null if not found.
     */
    suspend fun getByUUID(uuid: String): ProfileSkin?

    /**
     * Save the instance into cache using the key defined by the configuration.
     * @param profile Data that will be stored.
     */
    suspend fun save(profile: ProfileSkin)
}

/**
 * Cache service for [ProfileSkin].
 * @property client Cache client.
 * @property prefixKey Prefix key to identify the data in cache.
 */
class ProfileSkinCacheService(
    private val client: CacheClient,
    val expiration: Duration? = null,
    prefixKey: String = "skin:"
) : CacheService(prefixKey), IProfileSkinCacheService {

    override suspend fun getByUUID(uuid: String): ProfileSkin? {
        return client.connect {
            val binaryFormat = client.binaryFormat
            val key = encodeKey(binaryFormat, uuid)
            val dataSerial = it.get(key) ?: return null
            decodeFromByteArrayOrNull(binaryFormat, ProfileSkin.serializer(), dataSerial)
        }
    }

    override suspend fun save(profile: ProfileSkin) {
        client.connect {
            val binaryFormat = client.binaryFormat
            val key = encodeKey(binaryFormat, profile.id)
            val value = encodeToByteArray(binaryFormat, ProfileSkin.serializer(), profile)
            if (expiration != null) {
                it.psetex(key, expiration.inWholeMilliseconds, value)
            } else {
                it.set(key, value)
            }
        }
    }
}

/**
 * Service to retrieve data about profile.
 */
interface IProfileSkinService : IHttpStrategizable {

    /**
     * Get the skin information of a player from his [ProfileSkin.id].
     * @param uuid Profile's id.
     */
    suspend fun getByUUID(uuid: String): ProfileSkin?
}

/**
 * Service to retrieve data about client identity.
 * @property supplier Strategy to manage data.
 */
class ProfileSkinService(override val supplier: IHttpEntitySupplier) : IProfileSkinService {

    override suspend fun getByUUID(uuid: String): ProfileSkin? = supplier.getSkin(uuid)

    override fun withStrategy(strategy: IHttpEntitySupplier): IProfileSkinService = ProfileSkinService(strategy)
}