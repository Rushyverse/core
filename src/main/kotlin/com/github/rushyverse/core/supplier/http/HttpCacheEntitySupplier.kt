package com.github.rushyverse.core.supplier.http

import com.github.rushyverse.core.cache.AbstractCacheService
import com.github.rushyverse.core.data.IProfileIdCacheService
import com.github.rushyverse.core.data.IProfileSkinCacheService

/**
 * [IHttpEntitySupplier] that uses [AbstractCacheService] to resolve entities.
 */
public class HttpCacheEntitySupplier(
    override val configuration: HttpSupplierConfiguration,
) : IHttpEntitySupplier,
    IProfileSkinCacheService by configuration.profileSkinCache,
    IProfileIdCacheService by configuration.profileIdCache
