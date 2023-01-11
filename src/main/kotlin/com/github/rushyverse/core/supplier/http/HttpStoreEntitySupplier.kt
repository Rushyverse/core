package com.github.rushyverse.core.supplier.http

import io.github.universeproject.kotlinmojangapi.ProfileId
import io.github.universeproject.kotlinmojangapi.ProfileSkin

/**
 * [IHttpEntitySupplier] that delegates to another [IHttpEntitySupplier] to resolve entities.
 *
 * Resolved entities will always be stored in [cache] if it wasn't null or empty for flows.
 */
class HttpStoreEntitySupplier(
    private val cache: HttpCacheEntitySupplier,
    private val supplier: IHttpEntitySupplier
) : IHttpEntitySupplier {

    override suspend fun getUUIDByName(name: String): ProfileId? {
        return supplier.getUUIDByName(name)?.also { cache.save(it) }
    }

    override suspend fun getSkinByUUID(uuid: String): ProfileSkin? {
        return supplier.getSkinByUUID(uuid)?.also { cache.save(it) }
    }
}
