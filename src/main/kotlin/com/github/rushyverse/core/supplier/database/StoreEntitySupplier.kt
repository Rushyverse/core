package com.github.rushyverse.core.supplier.database

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.toSet
import java.util.*

/**
 * [IEntitySupplier] that delegates to another [IEntitySupplier] to resolve entities.
 *
 * Resolved entities will always be stored in [cache] if it wasn't null or empty for flows.
 */
class StoreEntitySupplier(
    private val cache: CacheEntitySupplier,
    private val supplier: IEntitySupplier
) : IEntitySupplier {

    override suspend fun addFriend(uuid: UUID, friend: UUID): Boolean {
        return if (supplier.addFriend(uuid, friend)) {
            cache.addFriend(uuid, friend)
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

    override suspend fun getFriends(uuid: UUID): Flow<UUID> {
        val friends = supplier.getFriends(uuid).toSet()
        cache.setFriends(uuid, friends)
        return friends.asFlow()
    }

    override suspend fun isFriend(uuid: UUID, friend: UUID): Boolean {
        return supplier.isFriend(uuid, friend)
    }
}
