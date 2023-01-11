@file:OptIn(ExperimentalLettuceCoroutinesApi::class)

package com.github.rushyverse.core.data

import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.cache.CacheClient.Default.binaryFormat
import com.github.rushyverse.core.cache.CacheService
import com.github.rushyverse.core.supplier.http.IHttpEntitySupplier
import com.github.rushyverse.core.supplier.http.IHttpStrategizable
import io.github.universeproject.kotlinmojangapi.ProfileId
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.protobuf.ProtoBuf.Default.encodeToByteArray
import kotlin.time.Duration

/**
 * Service to retrieve data about profile.
 */
interface IProfileIdService {

    /**
     * Get the profile of a client from his [ProfileId.name].
     * @param name Profile's name.
     */
    suspend fun getUUIDByName(name: String): ProfileId?
}


/**
 * Service to manage [ProfileId] data in cache.
 */
interface IProfileIdCacheService : IProfileIdService {

    /**
     * Save the instance into cache using the key defined by the configuration.
     * @param profile Data that will be stored.
     */
    suspend fun save(profile: ProfileId)
}

/**
 * Cache service for [ProfileId].
 * @property client Cache client.
 * @property prefixKey Prefix key to identify the data in cache.
 */
class ProfileIdCacheService(
    val client: CacheClient,
    val expiration: Duration? = null,
    prefixKey: String = "profileId:",
) : CacheService(prefixKey), IProfileIdCacheService {

    override suspend fun getUUIDByName(name: String): ProfileId? {
        val binaryFormat = client.binaryFormat
        val key = encodeKey(binaryFormat, name)

        val dataSerial = client.connect { it.get(key) } ?: return null
        val uuid = decodeFromByteArrayOrNull(binaryFormat, String.serializer(), dataSerial) ?: return null
        return ProfileId(id = uuid, name = name)
    }

    override suspend fun save(profile: ProfileId) {
        val binaryFormat = client.binaryFormat
        val key = encodeKey(binaryFormat, profile.name)
        val value = encodeToByteArray(binaryFormat, String.serializer(), profile.id)

        client.connect {
            if (expiration != null) {
                it.psetex(key, expiration.inWholeMilliseconds, value)
            } else {
                it.set(key, value)
            }
        }
    }
}

/**
 * Service to retrieve data about client identity.
 * @property supplier Strategy to manage data.
 */
class ProfileIdService(override val supplier: IHttpEntitySupplier) : IProfileIdService, IHttpStrategizable {

    override suspend fun getUUIDByName(name: String): ProfileId? = supplier.getUUIDByName(name)

    override fun withStrategy(strategy: IHttpEntitySupplier): ProfileIdService = ProfileIdService(strategy)
}