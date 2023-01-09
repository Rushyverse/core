package com.github.rushyverse.core.supplier

import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.data.*
import io.github.universeproject.kotlinmojangapi.MojangAPI

public class SupplierConfiguration(
    public val mojangAPI: MojangAPI,
    public val profileSkinCache: IProfileSkinCacheService,
    public val profileIdCache: IProfileIdCacheService,
    public val friendCache: IFriendCacheService
) {
    public constructor(mojangAPI: MojangAPI, cacheClient: CacheClient) : this(
        mojangAPI,
        ProfileSkinCacheService(cacheClient),
        ProfileIdCacheService(cacheClient),
        FriendCacheService(cacheClient)
    )
}