package com.github.rushyverse.core.data

import com.github.rushyverse.core.supplier.http.IHttpEntitySupplier
import com.github.rushyverse.core.supplier.http.IHttpStrategizable
import io.github.universeproject.kotlinmojangapi.ProfileId
import io.github.universeproject.kotlinmojangapi.ProfileSkin

/**
 * Service to retrieve data about players.
 */
interface IMojangService : IHttpStrategizable {

    /**
     * Get the skin data using the name of a player.
     * @param name Player's name.
     * @return Information about player's skin.
     */
    suspend fun getSkinByName(name: String): ProfileSkin?

    /**
     * Retrieve the id information about a player with his name.
     * @param name Player's name.
     * @return Information about the player's id.
     */
    suspend fun getId(name: String): ProfileId?

    /**
     * Retrieve the skin data for a player.
     * A player is represented by his UUID.
     * @param uuid Player's UUID.
     * @return Information about player's skin.
     */
    suspend fun getSkin(uuid: String): ProfileSkin?
}

/**
 * Service to retrieve data using Mojang api.
 * @property supplier Strategy to retrieve data.
 */
class MojangService(override val supplier: IHttpEntitySupplier) : IMojangService {

    override suspend fun getSkinByName(name: String): ProfileSkin? {
        return supplier.getUUID(name)?.let { supplier.getSkin(it.id) }
    }

    override suspend fun getId(name: String): ProfileId? {
        return supplier.getUUID(name)
    }

    override suspend fun getSkin(uuid: String): ProfileSkin? {
        return supplier.getSkin(uuid)
    }

    override fun withStrategy(strategy: IHttpEntitySupplier): IHttpStrategizable {
        return MojangService(strategy)
    }
}