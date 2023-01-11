package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.data.IFriendDatabaseService
import kotlinx.coroutines.flow.Flow
import java.util.*

/**
 * [IEntitySupplier] that uses database to manage entities.
 * @property service Friend database service.
 */
class DatabaseEntitySupplier(val service: IFriendDatabaseService) : IEntitySupplier {

    override suspend fun addFriend(uuid: UUID, friend: UUID): Boolean {
        return service.addFriend(uuid, friend)
    }

    override suspend fun removeFriend(uuid: UUID, friend: UUID): Boolean {
        return service.removeFriend(uuid, friend)
    }

    override suspend fun getFriends(uuid: UUID): Flow<UUID> {
        return service.getFriends(uuid)
    }

    override suspend fun isFriend(uuid: UUID, friend: UUID): Boolean {
        return service.isFriend(uuid, friend)
    }
}
