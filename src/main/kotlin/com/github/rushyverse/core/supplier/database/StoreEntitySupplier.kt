package com.github.rushyverse.core.supplier.database

import java.util.*

/**
 * [IEntitySupplier] that delegates to another [IEntitySupplier] to resolve entities.
 *
 * Resolved entities will always be stored in [cache] if it wasn't null or empty for flows.
 */
public class StoreEntitySupplier(
    private val cache: CacheEntitySupplier,
    private val supplier: IEntitySupplier
) : IEntitySupplier {

    override suspend fun addFriend(uuid: UUID, friend: UUID): Boolean {
        val result = supplier.addFriend(uuid, friend)
        return if (result) {
            cache.addFriend(uuid, friend)
            true
        } else {
            false
        }
    }

    override suspend fun removeFriend(uuid: UUID, friend: UUID): Boolean {
        val result = supplier.removeFriend(uuid, friend)
        return if (result) {
            cache.removeFriend(uuid, friend)
            true
        } else {
            false
        }
    }

    override suspend fun getFriends(uuid: UUID): Set<UUID> {
        val friends = supplier.getFriends(uuid)
        if (friends.isNotEmpty()) {
            cache.setFriends(uuid, friends)
        }
        return friends
    }

    override suspend fun isFriend(uuid: UUID, friend: UUID): Boolean {
        return supplier.isFriend(uuid, friend)
    }
}
