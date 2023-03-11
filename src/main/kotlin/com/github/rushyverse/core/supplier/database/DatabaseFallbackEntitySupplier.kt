package com.github.rushyverse.core.supplier.database

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.onEmpty
import java.util.*

/**
 * [IDatabaseEntitySupplier] that uses two suppliers.
 * Each supplier is used according to a priority.
 * [getPriority] is used first when a data is retrieved. If the data is not found, [setPriority] is used.
 * [setPriority] is used first when a data is set. If the data is set, the same information is set using [getPriority].
 * To keep consistency, it is recommended to use the same [DatabaseSupplierConfiguration] for both suppliers.
 * The value of [configuration] depends on one of the suppliers.
 * @property getPriority Priority of the supplier used when a data is retrieved.
 * @property setPriority Priority of the supplier used when a data is set.
 */
public class DatabaseFallbackEntitySupplier(
    public val getPriority: IDatabaseEntitySupplier,
    public val setPriority: IDatabaseEntitySupplier
) : IDatabaseEntitySupplier {

    override val configuration: DatabaseSupplierConfiguration
        get() = getPriority.configuration

    override suspend fun addFriend(uuid: UUID, friend: UUID): Boolean {
        return setPriority.addFriend(uuid, friend)
    }

    override suspend fun addPendingFriend(uuid: UUID, friend: UUID): Boolean {
        return setPriority.addPendingFriend(uuid, friend)
    }

    override suspend fun removeFriend(uuid: UUID, friend: UUID): Boolean {
        return setPriority.removeFriend(uuid, friend)
    }

    override suspend fun removePendingFriend(uuid: UUID, friend: UUID): Boolean {
        return setPriority.removePendingFriend(uuid, friend)
    }

    override suspend fun getFriends(uuid: UUID): Flow<UUID> {
        return getPriority.getFriends(uuid).onEmpty {
            emitAll(setPriority.getFriends(uuid))
        }
    }

    override suspend fun getPendingFriends(uuid: UUID): Flow<UUID> {
        return getPriority.getPendingFriends(uuid).onEmpty {
            emitAll(setPriority.getPendingFriends(uuid))
        }
    }

    override suspend fun isFriend(uuid: UUID, friend: UUID): Boolean {
        return getPriority.isFriend(uuid, friend) || setPriority.isFriend(uuid, friend)
    }

    override suspend fun isPendingFriend(uuid: UUID, friend: UUID): Boolean {
        return getPriority.isPendingFriend(uuid, friend) || setPriority.isPendingFriend(uuid, friend)
    }
}
