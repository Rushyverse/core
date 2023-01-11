package com.github.rushyverse.core.data

import com.github.rushyverse.core.supplier.http.IHttpEntitySupplier
import com.github.rushyverse.core.supplier.http.IHttpStrategizable
import io.github.universeproject.kotlinmojangapi.MojangAPI
import io.github.universeproject.kotlinmojangapi.ProfileId
import io.github.universeproject.kotlinmojangapi.ProfileSkin

/**
 * Service to retrieve data about players.
 */
interface IMojangService : IProfileIdService, IProfileSkinService {

    /**
     * Get the skin data using the name of a player.
     * @param name Player's name.
     * @return Information about player's skin.
     */
    suspend fun getSkinByName(name: String): ProfileSkin?
}

/**
 * Service to retrieve data using Mojang api.
 * @property supplier Strategy to retrieve data.
 */
class MojangService(override val supplier: IHttpEntitySupplier) : IMojangService, IHttpStrategizable {

    override suspend fun getSkinByName(name: String): ProfileSkin? {
        return getUUIDByName(name)?.let { getSkinByUUID(it.id) }
    }

    override suspend fun getSkinByUUID(uuid: String): ProfileSkin? {
        return supplier.getSkinByUUID(uuid)
    }

    override suspend fun getUUIDByName(name: String): ProfileId? {
        return supplier.getUUIDByName(name)
    }

    override fun withStrategy(strategy: IHttpEntitySupplier): MojangService {
        return MojangService(strategy)
    }
}