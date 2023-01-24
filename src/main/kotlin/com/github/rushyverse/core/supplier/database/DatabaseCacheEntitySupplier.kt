package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.data.IFriendCacheService
import kotlinx.coroutines.flow.Flow
import java.util.*

/**
 * [IDatabaseEntitySupplier] that uses cache to manage entities.
 * @property friendCacheService Friend cache service.
 */
public class DatabaseCacheEntitySupplier(public val friendCacheService: IFriendCacheService) : IDatabaseEntitySupplier, IFriendCacheService by friendCacheService {

    override suspend fun addFriend(uuid: UUID, friend: UUID): Boolean {
        return friendCacheService.addFriend(uuid, friend)
    }

    override suspend fun addPendingFriend(uuid: UUID, friend: UUID): Boolean {
        return friendCacheService.addPendingFriend(uuid, friend)
    }

    override suspend fun removeFriend(uuid: UUID, friend: UUID): Boolean {
        return friendCacheService.removeFriend(uuid, friend)
    }

    override suspend fun removePendingFriend(uuid: UUID, friend: UUID): Boolean {
        return friendCacheService.removePendingFriend(uuid, friend)
    }

    override suspend fun getFriends(uuid: UUID): Flow<UUID> {
        return friendCacheService.getFriends(uuid)
    }

    override suspend fun getPendingFriends(uuid: UUID): Flow<UUID> {
        return friendCacheService.getPendingFriends(uuid)
    }

    override suspend fun isFriend(uuid: UUID, friend: UUID): Boolean {
        return friendCacheService.isFriend(uuid, friend)
    }

    override suspend fun isPendingFriend(uuid: UUID, friend: UUID): Boolean {
        return friendCacheService.isPendingFriend(uuid, friend)
    }
}

