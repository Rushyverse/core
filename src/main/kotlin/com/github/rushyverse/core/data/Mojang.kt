package com.github.rushyverse.core.data

import com.github.rushyverse.core.supplier.http.HttpSupplierConfiguration
import com.github.rushyverse.core.supplier.http.IHttpEntitySupplier
import com.github.rushyverse.core.supplier.http.IHttpStrategizable
import com.github.rushyverse.mojang.api.ProfileSkin

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
public class MojangService(
    override val supplier: IHttpEntitySupplier
) : IMojangService, IProfileSkinService by supplier, IProfileIdService by supplier, IHttpStrategizable {

    override suspend fun getSkinByName(name: String): ProfileSkin? {
        return getIdByName(name)?.let { getSkinById(it.id) }
    }

    override fun withStrategy(getStrategy: (HttpSupplierConfiguration) -> IHttpEntitySupplier): MojangService {
        val newSupplier = getStrategy(supplier.configuration)
        return MojangService(newSupplier)
    }
}
