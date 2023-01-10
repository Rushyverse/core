package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.data.FriendCacheService
import com.github.rushyverse.core.data.FriendDatabaseService
import com.github.rushyverse.core.data.IFriendCacheService
import com.github.rushyverse.core.data.IFriendDatabaseService

/**
 * Contains all necessary services to manage entities linked to the database and cache.
 * @property friendServices Friends services.
 */
public data class DatabaseSupplierServices(
    public val friendServices: Pair<IFriendCacheService, IFriendDatabaseService>,
) {
    public constructor(cacheClient: CacheClient) : this(
        FriendCacheService(cacheClient) to FriendDatabaseService(),
    )
}