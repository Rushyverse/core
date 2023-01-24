package com.github.rushyverse.core.supplier.database

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.toSet
import java.util.*

/**
 * [IDatabaseEntitySupplier] that delegates to another [IDatabaseEntitySupplier] to resolve entities.
 *
 * Resolved entities will always be stored in [cache] if it wasn't null or empty for flows.
 */
public class DatabaseStoreEntitySupplier(
    public val cache: DatabaseCacheEntitySupplier,
    public val supplier: IDatabaseEntitySupplier
) : IDatabaseEntitySupplier {

    override suspend fun addFriend(uuid: UUID, friend: UUID): Boolean {
        return if (supplier.addFriend(uuid, friend)) {
            cache.addFriend(uuid, friend)
            true
        } else {
            false
        }
    }

    override suspend fun addPendingFriend(uuid: UUID, friend: UUID): Boolean {
        return if (supplier.addPendingFriend(uuid, friend)) {
            cache.addPendingFriend(uuid, friend)
            true
        } else {
            false
        }
    }

    override suspend fun removeFriend(uuid: UUID, friend: UUID): Boolean {
        return if (supplier.removeFriend(uuid, friend)) {
            cache.removeFriend(uuid, friend)
            true
        } else {
            false
        }
    }

    override suspend fun removePendingFriend(uuid: UUID, friend: UUID): Boolean {
        return if (supplier.removePendingFriend(uuid, friend)) {
            cache.removePendingFriend(uuid, friend)
            true
        } else {
            false
        }
    }

    override suspend fun getFriends(uuid: UUID): Flow<UUID> {
        val friends = supplier.getFriends(uuid).toSet()
        cache.setFriends(uuid, friends)
        return friends.asFlow()
    }

    override suspend fun getPendingFriends(uuid: UUID): Flow<UUID> {
        val requests = supplier.getPendingFriends(uuid).toSet()
        cache.setPendingFriends(uuid, requests)
        return requests.asFlow()
    }

    override suspend fun isFriend(uuid: UUID, friend: UUID): Boolean {
        return supplier.isFriend(uuid, friend)
    }

    override suspend fun isPendingFriend(uuid: UUID, friend: UUID): Boolean {
        return supplier.isPendingFriend(uuid, friend)
    }
}
