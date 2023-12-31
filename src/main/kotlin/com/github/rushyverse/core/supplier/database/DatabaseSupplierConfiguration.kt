package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.data.IFriendCacheService
import com.github.rushyverse.core.data.IFriendDatabaseService
import com.github.rushyverse.core.data.IGuildCacheService
import com.github.rushyverse.core.data.IGuildDatabaseService
import com.github.rushyverse.core.data.player.IPlayerCacheService
import com.github.rushyverse.core.data.player.IPlayerDatabaseService

/**
 * Contains all necessary services to manage entities linked to the database and cache.
 * @property friendServices Friends services.
 * @property guildServices Guild services.
 * @property playerServices Player services.
 */
public data class DatabaseSupplierConfiguration(
    val friendServices: Pair<IFriendCacheService, IFriendDatabaseService>,
    val guildServices: Pair<IGuildCacheService, IGuildDatabaseService>,
    val playerServices: Pair<IPlayerCacheService, IPlayerDatabaseService>,
)
