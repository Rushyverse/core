package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.data.*

/**
 * Contains all necessary services to manage entities linked to the database and cache.
 * @property friendServices Friends services.
 * @property guildServices Guild services.
 */
public data class DatabaseSupplierConfiguration(
    val friendServices: Pair<IFriendCacheService, IFriendDatabaseService>,
    val guildServices: Pair<IGuildCacheService, IGuildDatabaseService>,
    val playerServices: Pair<IPlayerCacheService, IPlayerDatabaseService>,
)
