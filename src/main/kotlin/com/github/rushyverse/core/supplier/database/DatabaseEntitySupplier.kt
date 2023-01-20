package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.data.IFriendDatabaseService
import kotlinx.coroutines.flow.Flow
import java.util.*

/**
 * [IDatabaseEntitySupplier] that uses database to manage entities.
 * @property service Friend database service.
 */
public class DatabaseEntitySupplier(public val service: IFriendDatabaseService) : IDatabaseEntitySupplier {

    override suspend fun addFriend(uuid: UUID, friend: UUID): Boolean {
        return service.addFriend(uuid, friend)
    }

    override suspend fun addFriendPendingRequest(uuid: UUID, friend: UUID): Boolean {
        return service.addFriendPendingRequest(uuid, friend)
    }

    override suspend fun removeFriend(uuid: UUID, friend: UUID): Boolean {
        return service.removeFriend(uuid, friend)
    }

    override suspend fun removeFriendPendingRequest(uuid: UUID, friend: UUID): Boolean {
        return service.removeFriendPendingRequest(uuid, friend)
    }

    override suspend fun getFriends(uuid: UUID): Flow<UUID> {
        return service.getFriends(uuid)
    }

    override suspend fun getFriendPendingRequests(uuid: UUID): Flow<UUID> {
        return service.getFriendPendingRequests(uuid)
    }

    override suspend fun isFriend(uuid: UUID, friend: UUID): Boolean {
        return service.isFriend(uuid, friend)
    }

    override suspend fun isFriendPendingRequest(uuid: UUID, friend: UUID): Boolean {
        return service.isFriendPendingRequest(uuid, friend)
    }
}
