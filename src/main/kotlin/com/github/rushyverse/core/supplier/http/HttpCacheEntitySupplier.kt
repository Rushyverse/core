package com.github.rushyverse.core.supplier.http

import com.github.rushyverse.core.data.IProfileIdCacheService
import com.github.rushyverse.core.data.IProfileSkinCacheService
import io.github.universeproject.kotlinmojangapi.ProfileId
import io.github.universeproject.kotlinmojangapi.ProfileSkin
import com.github.rushyverse.core.cache.CacheService

/**
 * [IHttpEntitySupplier] that uses [CacheService] to resolve entities.
 */
class HttpCacheEntitySupplier(
    val profileSkinCache: IProfileSkinCacheService,
    val profileIdCache: IProfileIdCacheService
) : IHttpEntitySupplier {

    override suspend fun getUUID(name: String): ProfileId? = profileIdCache.getByName(name)

    override suspend fun getSkin(uuid: String): ProfileSkin? = profileSkinCache.getByUUID(uuid)

    suspend fun save(profile: ProfileId) {
        profileIdCache.save(profile)
    }

    suspend fun save(profile: ProfileSkin) {
        profileSkinCache.save(profile)
    }
}