package com.github.rushyverse.core.supplier.http

import io.github.universeproject.kotlinmojangapi.MojangAPI
import io.github.universeproject.kotlinmojangapi.ProfileId
import io.github.universeproject.kotlinmojangapi.ProfileSkin
import io.ktor.client.*

/**
 * [IHttpEntitySupplier] that uses a [HttpClient] to resolve entities.
 */
public class HttpEntitySupplier(public val mojangAPI: MojangAPI) : IHttpEntitySupplier {

    override suspend fun getIdByName(name: String): ProfileId? {
        return mojangAPI.getUUID(name)
    }

    override suspend fun getSkinById(id: String): ProfileSkin? {
        return mojangAPI.getSkin(id)
    }
}
