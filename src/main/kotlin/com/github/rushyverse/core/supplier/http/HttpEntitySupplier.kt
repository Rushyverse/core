package com.github.rushyverse.core.supplier.http

import com.github.rushyverse.mojang.api.ProfileId
import com.github.rushyverse.mojang.api.ProfileSkin
import io.ktor.client.*

/**
 * [IHttpEntitySupplier] that uses a [HttpClient] to resolve entities.
 */
public class HttpEntitySupplier(override val configuration: HttpSupplierConfiguration) : IHttpEntitySupplier {

    private val mojangAPI = configuration.mojangAPI

    override suspend fun getIdByName(name: String): ProfileId? {
        return mojangAPI.getUUID(name)
    }

    override suspend fun getSkinById(id: String): ProfileSkin? {
        return mojangAPI.getSkin(id)
    }
}
