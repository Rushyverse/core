package com.github.rushyverse.core.supplier.database

import java.util.*

/**
 * [IEntitySupplier] that uses two suppliers.
 * Each supplier is used according to a priority.
 * [getPriority] is used first when a data is retrieved. If the data is not found, [setPriority] is used.
 * [setPriority] is used first when a data is set. If the data is set, the same information is set using [getPriority].
 * @property getPriority Priority of the supplier used when a data is retrieved.
 * @property setPriority Priority of the supplier used when a data is set.
 */
public class FallbackEntitySupplier(
    public val getPriority: IEntitySupplier,
    public val setPriority: IEntitySupplier
) : IEntitySupplier {

    override suspend fun addFriend(uuid: UUID, friend: UUID): Boolean {
        return if (setPriority.addFriend(uuid, friend)) {
            getPriority.addFriend(uuid, friend)
            true
        } else false
    }

    override suspend fun removeFriend(uuid: UUID, friend: UUID): Boolean {
        return if (setPriority.removeFriend(uuid, friend)) {
            getPriority.removeFriend(uuid, friend)
            true
        } else false
    }

    override suspend fun getFriends(uuid: UUID): Set<UUID> {
        return getPriority.getFriends(uuid).ifEmpty {
            setPriority.getFriends(uuid)
        }
    }

    override suspend fun isFriend(uuid: UUID, friend: UUID): Boolean {
        return getPriority.isFriend(uuid, friend) || setPriority.isFriend(uuid, friend)
    }
}
