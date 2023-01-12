package com.github.rushyverse.core.supplier.database

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.*
import java.util.*

/**
 * [IEntitySupplier] that delegates to another [IEntitySupplier] to resolve entities.
 *
 * Resolved entities will always be stored in [cache] if it wasn't null or empty for flows.
 */
class StoreEntitySupplier(
    val cache: CacheEntitySupplier,
    val supplier: IEntitySupplier
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
        return supplier.getFriends(uuid)
            .onEach { cache.addFriend(uuid, it) }
            .onEmpty { cache.setFriends(uuid, emptySet()) }
            .flowOn(Dispatchers.IO)
    }

    override suspend fun isFriend(uuid: UUID, friend: UUID): Boolean {
        return supplier.isFriend(uuid, friend)
    }
}
