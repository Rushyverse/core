package com.github.rushyverse.core.supplier.database

import java.util.*

/**
 * [IEntitySupplier] that uses the first supplier to retrieve a data, if the value is null, get the data through the second supplier.
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
