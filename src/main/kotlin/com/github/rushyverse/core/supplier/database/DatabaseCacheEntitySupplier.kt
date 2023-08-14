package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.data.IFriendCacheService
import com.github.rushyverse.core.data.IGuildCacheService
import com.github.rushyverse.core.data.player.IPlayerCacheService

/**
 * [IDatabaseEntitySupplier] that uses cache to manage entities.
 */
public class DatabaseCacheEntitySupplier(public override val configuration: DatabaseSupplierConfiguration) :
    IDatabaseEntitySupplier,
    IFriendCacheService by configuration.friendServices.first,
    IGuildCacheService by configuration.guildServices.first,
    IPlayerCacheService by configuration.playerServices.first
