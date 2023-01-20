package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.data.IFriendCacheService
import kotlinx.coroutines.flow.Flow
import java.util.*

/**
 * [IDatabaseEntitySupplier] that uses cache to manage entities.
 * @property friendCacheService Friend cache service.
 */
public class DatabaseCacheEntitySupplier(public val friendCacheService: IFriendCacheService) : IDatabaseEntitySupplier {

    override suspend fun addFriend(uuid: UUID, friend: UUID): Boolean {
        return friendCacheService.addFriend(uuid, friend)
    }

    override suspend fun addFriendPendingRequest(uuid: UUID, friend: UUID): Boolean {
        return friendCacheService.addFriendPendingRequest(uuid, friend)
    }

    override suspend fun removeFriend(uuid: UUID, friend: UUID): Boolean {
        return friendCacheService.removeFriend(uuid, friend)
    }

    override suspend fun removeFriendPendingRequest(uuid: UUID, friend: UUID): Boolean {
        return friendCacheService.removeFriendPendingRequest(uuid, friend)
    }

    override suspend fun getFriends(uuid: UUID): Flow<UUID> {
        return friendCacheService.getFriends(uuid)
    }

    override suspend fun getFriendPendingRequests(uuid: UUID): Flow<UUID> {
        return friendCacheService.getFriendPendingRequests(uuid)
    }

    public suspend fun setFriends(uuid: UUID, friends: Set<UUID>): Boolean {
        return friendCacheService.setFriends(uuid, friends)
    }

    public suspend fun setFriendPendingRequests(uuid: UUID, friends: Set<UUID>): Boolean {
        return friendCacheService.setFriendPendingRequests(uuid, friends)
    }

    override suspend fun isFriend(uuid: UUID, friend: UUID): Boolean {
        return friendCacheService.isFriend(uuid, friend)
    }

    override suspend fun isFriendPendingRequest(uuid: UUID, friend: UUID): Boolean {
        return friendCacheService.isFriendPendingRequest(uuid, friend)
    }


}

