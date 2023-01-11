package com.github.rushyverse.core.supplier.http

import io.github.universeproject.kotlinmojangapi.MojangAPI
import io.github.universeproject.kotlinmojangapi.ProfileId
import io.github.universeproject.kotlinmojangapi.ProfileSkin
import io.ktor.client.*

/**
 * [IHttpEntitySupplier] that uses a [HttpClient] to resolve entities.
 */
class HttpEntitySupplier(private val mojangAPI: MojangAPI) : IHttpEntitySupplier {

    override suspend fun getUUIDByName(name: String): ProfileId? {
        return mojangAPI.getUUID(name)
    }

    override suspend fun getSkinByUUID(uuid: String): ProfileSkin? {
        return mojangAPI.getSkin(uuid)
    }
}
