package com.github.rushyverse.core.supplier.http

import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.data.IProfileIdCacheService
import com.github.rushyverse.core.data.IProfileSkinCacheService
import com.github.rushyverse.core.data.ProfileIdCacheService
import com.github.rushyverse.core.data.ProfileSkinCacheService
import io.github.universeproject.kotlinmojangapi.MojangAPI

public class HttpSupplierServices(
    public val mojangAPI: MojangAPI,
    public val profileSkinCache: IProfileSkinCacheService,
    public val profileIdCache: IProfileIdCacheService,
) {
    public constructor(mojangAPI: MojangAPI, cacheClient: CacheClient) : this(
        mojangAPI,
        ProfileSkinCacheService(cacheClient),
        ProfileIdCacheService(cacheClient),
    )
}