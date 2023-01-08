@file:OptIn(ExperimentalLettuceCoroutinesApi::class)

package com.github.rushyverse.core.data

import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.cache.CacheService
import com.github.rushyverse.core.supplier.http.IHttpEntitySupplier
import com.github.rushyverse.core.supplier.http.IHttpStrategizable
import io.github.universeproject.kotlinmojangapi.ProfileId
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import kotlinx.serialization.builtins.serializer

/**
 * Service to manage [ProfileId] data in cache.
 */
public interface IProfileIdCacheService {
    /**
     * Get the instance of [ProfileId] linked to the [name] data.
     * @param name Name of the user.
     * @return The instance stored if found, or null if not found.
     */
    public suspend fun getByName(name: String): ProfileId?

    /**
     * Save the instance into cache using the key defined by the configuration.
     * @param profile Data that will be stored.
     */
    public suspend fun save(profile: ProfileId)
}

/**
 * Cache service for [ProfileId].
 * @property client Cache client.
 * @property prefixKey Prefix key to identify the data in cache.
 */
public class ProfileIdCacheService(
    public val client: CacheClient,
    prefixKey: String = "profileId:"
) : CacheService(prefixKey), IProfileIdCacheService {

    override suspend fun getByName(name: String): ProfileId? {
        return client.connect {
            val binaryFormat = client.binaryFormat
            val key = encodeKey(binaryFormat, name)
            val dataSerial = it.get(key) ?: return null
            val uuid = decodeFromByteArrayOrNull(binaryFormat, String.serializer(), dataSerial) ?: return null
            ProfileId(id = uuid, name = name)
        }
    }

    override suspend fun save(profile: ProfileId) {
        client.connect {
            val binaryFormat = client.binaryFormat
            val key = encodeKey(binaryFormat, profile.name)
            it.set(key, encodeToByteArray(binaryFormat, String.serializer(), profile.id))
        }
    }
}

/**
 * Service to retrieve data about profile.
 */
public interface IProfileIdService : IHttpStrategizable {

    /**
     * Get the profile of a client from his [ProfileId.name].
     * @param name Profile's name.
     */
    public suspend fun getByName(name: String): ProfileId?
}

/**
 * Service to retrieve data about client identity.
 * @property supplier Strategy to manage data.
 */
public class ProfileIdService(override val supplier: IHttpEntitySupplier) : IProfileIdService {

    override suspend fun getByName(name: String): ProfileId? = supplier.getUUID(name)

    override fun withStrategy(strategy: IHttpEntitySupplier): IProfileIdService = ProfileIdService(strategy)
}