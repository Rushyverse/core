package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.data.IFriendDatabaseService

/**
 * [IDatabaseEntitySupplier] that uses database to manage entities.
 * @property service Friend database service.
 */
public class DatabaseEntitySupplier(public val service: IFriendDatabaseService) :
    IDatabaseEntitySupplier,
    IFriendDatabaseService by service