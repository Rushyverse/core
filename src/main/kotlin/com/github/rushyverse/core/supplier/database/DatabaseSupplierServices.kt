package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.data.FriendCacheService
import com.github.rushyverse.core.data.IFriendCacheService

public class DatabaseSupplierServices(
    public val friendCacheService: IFriendCacheService,
) {
    public constructor(cacheClient: CacheClient) : this(
        FriendCacheService(cacheClient),
    )
}