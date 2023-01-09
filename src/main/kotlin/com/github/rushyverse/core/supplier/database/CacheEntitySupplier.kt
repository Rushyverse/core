package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.data.IFriendCacheService
import java.util.*

/**
 * [IEntitySupplier] that uses [CacheService] to resolve entities.
 */
public class CacheEntitySupplier(
    public val friendCacheService: IFriendCacheService
) : IEntitySupplier {

    override suspend fun addFriend(uuid: UUID, friend: UUID): Boolean {
        return friendCacheService.addFriend(uuid, friend)
    }

    override suspend fun removeFriend(uuid: UUID, friend: UUID): Boolean {
        return friendCacheService.removeFriend(uuid, friend)
    }

    override suspend fun getFriends(uuid: UUID): Set<UUID> {
        return friendCacheService.getFriends(uuid).toSet()
    }

    public suspend fun setFriends(uuid: UUID, friends: Set<UUID>): Boolean {
        return friendCacheService.setFriends(uuid, friends)
    }

    override suspend fun isFriend(uuid: UUID, friend: UUID): Boolean {
        return friendCacheService.isFriend(uuid, friend)
    }


}

