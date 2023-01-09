package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.data.Friends
import com.github.rushyverse.core.data.Friends.uuid1
import com.github.rushyverse.core.data.Friends.uuid2
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.transactions.experimental.newSuspendedTransaction
import java.util.*

public class DatabaseEntitySupplier : IEntitySupplier {

    override suspend fun addFriend(uuid: UUID, friend: UUID): Boolean {
        return newSuspendedTransaction {
            Friends.insert {
                it[uuid1] = uuid
                it[uuid2] = friend
            }
        }.insertedCount == 1
    }

    override suspend fun removeFriend(uuid: UUID, friend: UUID): Boolean {
        return newSuspendedTransaction {
            Friends.deleteWhere(1) {
                (uuid1.eq(uuid) and uuid2.eq(friend)) or (uuid1.eq(friend) and uuid2.eq(uuid))
            }
        } > 0
    }

    override suspend fun getFriends(uuid: UUID): Set<UUID> {
        return newSuspendedTransaction {
            Friends.select {
                (uuid1.eq(uuid) or uuid2.eq(uuid))
            }.withDistinct().mapTo(mutableSetOf()) {
                val uuid1 = it[uuid1]
                val uuid2 = it[uuid2]
                if (uuid1 == uuid) uuid2 else uuid1
            }
        }
    }

    override suspend fun isFriend(uuid: UUID, friend: UUID): Boolean {
        return newSuspendedTransaction {
            !Friends.select {
                (uuid1.eq(uuid) and uuid2.eq(friend)) or (uuid1.eq(friend) and uuid2.eq(uuid))
            }.empty()
        }
    }
}
