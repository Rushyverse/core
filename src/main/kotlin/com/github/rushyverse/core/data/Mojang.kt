package com.github.rushyverse.core.data

import com.github.rushyverse.core.supplier.http.IHttpEntitySupplier
import com.github.rushyverse.core.supplier.http.IHttpStrategizable
import io.github.universeproject.kotlinmojangapi.ProfileId
import io.github.universeproject.kotlinmojangapi.ProfileSkin

/**
 * Service to retrieve data about players.
 */
public interface IMojangService : IProfileIdService, IProfileSkinService {

    /**
     * Get the skin data using the name of a player.
     * @param name Player's name.
     * @return Information about player's skin.
     */
    public suspend fun getSkinByName(name: String): ProfileSkin?
}

/**
 * Service to retrieve data using Mojang api.
 * @property supplier Strategy to retrieve data.
 */
public class MojangService(override val supplier: IHttpEntitySupplier) : IMojangService, IHttpStrategizable {

    override suspend fun getSkinByName(name: String): ProfileSkin? {
        return getIdByName(name)?.let { getSkinById(it.id) }
    }

    override suspend fun getSkinById(id: String): ProfileSkin? {
        return supplier.getSkinById(id)
    }

    override suspend fun getIdByName(name: String): ProfileId? {
        return supplier.getIdByName(name)
    }

    override fun withStrategy(strategy: IHttpEntitySupplier): MojangService {
        return MojangService(strategy)
    }
}