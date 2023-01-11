package com.github.rushyverse.core.supplier.http

import com.github.rushyverse.core.cache.CacheService
import com.github.rushyverse.core.data.IProfileIdCacheService
import com.github.rushyverse.core.data.IProfileSkinCacheService
import io.github.universeproject.kotlinmojangapi.ProfileId
import io.github.universeproject.kotlinmojangapi.ProfileSkin

/**
 * [IHttpEntitySupplier] that uses [CacheService] to resolve entities.
 */
class HttpCacheEntitySupplier(
    val profileSkinCache: IProfileSkinCacheService,
    val profileIdCache: IProfileIdCacheService
) : IHttpEntitySupplier {

    override suspend fun getSkinByUUID(uuid: String): ProfileSkin? {
        return profileSkinCache.getSkinByUUID(uuid)
    }

    override suspend fun getIdByName(name: String): ProfileId? {
        return profileIdCache.getIdByName(name)
    }

    suspend fun save(profile: ProfileId) {
        profileIdCache.save(profile)
    }

    suspend fun save(profile: ProfileSkin) {
        profileSkinCache.save(profile)
    }
}