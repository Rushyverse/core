package com.github.rushyverse.core.supplier.http

import com.github.rushyverse.core.cache.AbstractCacheService
import com.github.rushyverse.core.data.IProfileIdCacheService
import com.github.rushyverse.core.data.IProfileSkinCacheService
import io.github.universeproject.kotlinmojangapi.ProfileId
import io.github.universeproject.kotlinmojangapi.ProfileSkin

/**
 * [IHttpEntitySupplier] that uses [AbstractCacheService] to resolve entities.
 */
public class HttpCacheEntitySupplier(
    public val profileSkinCache: IProfileSkinCacheService,
    public val profileIdCache: IProfileIdCacheService
) : IHttpEntitySupplier {

    override suspend fun getSkinById(id: String): ProfileSkin? {
        return profileSkinCache.getSkinById(id)
    }

    override suspend fun getIdByName(name: String): ProfileId? {
        return profileIdCache.getIdByName(name)
    }

    public suspend fun save(profile: ProfileId) {
        profileIdCache.save(profile)
    }

    public suspend fun save(profile: ProfileSkin) {
        profileSkinCache.save(profile)
    }
}