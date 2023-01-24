package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.data.IFriendDatabaseService
import kotlinx.coroutines.flow.Flow
import java.util.*

/**
 * [IDatabaseEntitySupplier] that uses database to manage entities.
 * @property service Friend database service.
 */
public class DatabaseEntitySupplier(public val service: IFriendDatabaseService) : IDatabaseEntitySupplier, IFriendDatabaseService by service {

    override suspend fun addFriend(uuid: UUID, friend: UUID): Boolean {
        return service.addFriend(uuid, friend)
    }

    override suspend fun addPendingFriend(uuid: UUID, friend: UUID): Boolean {
        return service.addPendingFriend(uuid, friend)
    }

    override suspend fun removeFriend(uuid: UUID, friend: UUID): Boolean {
        return service.removeFriend(uuid, friend)
    }

    override suspend fun removePendingFriend(uuid: UUID, friend: UUID): Boolean {
        return service.removePendingFriend(uuid, friend)
    }

    override suspend fun getFriends(uuid: UUID): Flow<UUID> {
        return service.getFriends(uuid)
    }

    override suspend fun getPendingFriends(uuid: UUID): Flow<UUID> {
        return service.getPendingFriends(uuid)
    }

    override suspend fun isFriend(uuid: UUID, friend: UUID): Boolean {
        return service.isFriend(uuid, friend)
    }

    override suspend fun isPendingFriend(uuid: UUID, friend: UUID): Boolean {
        return service.isPendingFriend(uuid, friend)
    }
}
