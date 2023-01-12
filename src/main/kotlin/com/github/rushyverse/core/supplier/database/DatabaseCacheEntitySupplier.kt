package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.data.IFriendCacheService
import kotlinx.coroutines.flow.Flow
import java.util.*

/**
 * [IDatabaseEntitySupplier] that uses cache to manage entities.
 * @property service Friend cache service.
 */
class DatabaseCacheEntitySupplier(val service: IFriendCacheService) : IDatabaseEntitySupplier {

    override suspend fun addFriend(uuid: UUID, friend: UUID): Boolean {
        return service.addFriend(uuid, friend)
    }

    override suspend fun removeFriend(uuid: UUID, friend: UUID): Boolean {
        return service.removeFriend(uuid, friend)
    }

    override suspend fun getFriends(uuid: UUID): Flow<UUID> {
        return service.getFriends(uuid)
    }

    suspend fun setFriends(uuid: UUID, friends: Set<UUID>): Boolean {
        return service.setFriends(uuid, friends)
    }

    override suspend fun isFriend(uuid: UUID, friend: UUID): Boolean {
        return service.isFriend(uuid, friend)
    }


}

