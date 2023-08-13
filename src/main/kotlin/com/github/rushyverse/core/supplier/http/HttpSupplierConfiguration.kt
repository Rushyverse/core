package com.github.rushyverse.core.supplier.http

import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.data.IProfileIdCacheService
import com.github.rushyverse.core.data.IProfileSkinCacheService
import com.github.rushyverse.core.data.ProfileIdCacheService
import com.github.rushyverse.core.data.ProfileSkinCacheService
import com.github.rushyverse.mojang.api.MojangAPI

/**
 * Contains the configuration for a HTTP supplier.
 * @property mojangAPI Allows to request data from the Mojang API.
 * @property profileSkinCache Skin cache service.
 * @property profileIdCache Profile ID cache service.
 */
public class HttpSupplierConfiguration(
    public val mojangAPI: MojangAPI,
    public val profileSkinCache: IProfileSkinCacheService,
    public val profileIdCache: IProfileIdCacheService,
) {

    /**
     * Creates a new instance of [HttpSupplierConfiguration].
     * @param mojangAPI Allows to request data from the Mojang API.
     * @param cacheClient Cache client used to create the cache services [profileSkinCache] and [profileIdCache].
     */
    public constructor(mojangAPI: MojangAPI, cacheClient: CacheClient) : this(
        mojangAPI,
        ProfileSkinCacheService(cacheClient),
        ProfileIdCacheService(cacheClient),
    )
}
