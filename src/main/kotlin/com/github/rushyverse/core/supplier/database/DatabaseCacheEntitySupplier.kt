package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.data.IFriendCacheService

/**
 * [IDatabaseEntitySupplier] that uses cache to manage entities.
 */
public class DatabaseCacheEntitySupplier(public override val configuration: DatabaseSupplierConfiguration) :
    IDatabaseEntitySupplier,
    IFriendCacheService by configuration.friendServices.first