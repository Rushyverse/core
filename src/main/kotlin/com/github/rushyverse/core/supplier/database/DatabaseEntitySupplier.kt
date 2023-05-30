package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.data.IFriendDatabaseService
import com.github.rushyverse.core.data.IGuildDatabaseService

/**
 * [IDatabaseEntitySupplier] that uses database to manage entities.
 */
public class DatabaseEntitySupplier(public override val configuration: DatabaseSupplierConfiguration) :
    IDatabaseEntitySupplier,
    IFriendDatabaseService by configuration.friendServices.second,
    IGuildDatabaseService by configuration.guildServices.second