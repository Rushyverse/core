package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.data.IFriendCacheService

/**
 * [IDatabaseEntitySupplier] that uses cache to manage entities.
 * @property friendCacheService Friend cache service.
 */
public class DatabaseCacheEntitySupplier(public val friendCacheService: IFriendCacheService) :
    IDatabaseEntitySupplier,
    IFriendCacheService by friendCacheService