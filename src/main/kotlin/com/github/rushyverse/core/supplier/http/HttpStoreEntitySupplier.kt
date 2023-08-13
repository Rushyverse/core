package com.github.rushyverse.core.supplier.http

import com.github.rushyverse.mojang.api.ProfileId
import com.github.rushyverse.mojang.api.ProfileSkin

/**
 * [IHttpEntitySupplier] that delegates to another [IHttpEntitySupplier] to resolve entities.
 * Resolved entities will always be stored in [cache] if it wasn't null.
 * To keep consistency, it is recommended to use the same [HttpSupplierConfiguration] for both suppliers.
 * The value of [configuration] depends on one of the suppliers.
 * @property cache Supplier used to interact with the cache.
 * @property supplier Supplier used to interact with a custom way.
 */
public class HttpStoreEntitySupplier(
    public val cache: HttpCacheEntitySupplier,
    public val supplier: IHttpEntitySupplier
) : IHttpEntitySupplier {

    override val configuration: HttpSupplierConfiguration
        get() = cache.configuration

    override suspend fun getIdByName(name: String): ProfileId? {
        return supplier.getIdByName(name)?.also { cache.save(it) }
    }

    override suspend fun getSkinById(id: String): ProfileSkin? {
        return supplier.getSkinById(id)?.also { cache.save(it) }
    }
}
