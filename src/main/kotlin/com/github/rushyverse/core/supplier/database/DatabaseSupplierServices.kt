package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.data.IFriendCacheService
import com.github.rushyverse.core.data.IFriendDatabaseService

/**
 * Contains all necessary services to manage entities linked to the database and cache.
 * @property friendServices Friends services.
 */
data class DatabaseSupplierServices(
    val friendServices: Pair<IFriendCacheService, IFriendDatabaseService>,
)