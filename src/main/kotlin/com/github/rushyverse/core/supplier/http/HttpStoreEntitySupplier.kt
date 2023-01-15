package com.github.rushyverse.core.supplier.http

import io.github.universeproject.kotlinmojangapi.ProfileId
import io.github.universeproject.kotlinmojangapi.ProfileSkin

/**
 * [IHttpEntitySupplier] that delegates to another [IHttpEntitySupplier] to resolve entities.
 *
 * Resolved entities will always be stored in [cache] if it wasn't null or empty for flows.
 */
public class HttpStoreEntitySupplier(
    public val cache: HttpCacheEntitySupplier,
    public val supplier: IHttpEntitySupplier
) : IHttpEntitySupplier {

    override suspend fun getIdByName(name: String): ProfileId? {
        return supplier.getIdByName(name)?.also { cache.save(it) }
    }

    override suspend fun getSkinById(id: String): ProfileSkin? {
        return supplier.getSkinById(id)?.also { cache.save(it) }
    }
}
