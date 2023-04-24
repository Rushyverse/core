package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.data.Guild
import com.github.rushyverse.core.data.GuildInvite
import com.github.rushyverse.core.data.GuildMember
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.onEmpty
import java.time.Instant
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

    override fun getFriends(uuid: UUID): Flow<UUID> {
        return getPriority.getFriends(uuid).onEmpty {
            emitAll(setPriority.getFriends(uuid))
        }
    }

    override fun getPendingFriends(uuid: UUID): Flow<UUID> {
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

    override suspend fun deleteExpiredInvitations(): Boolean {
        return setPriority.deleteExpiredInvitations()
    }

    override suspend fun createGuild(name: String, ownerId: String): Guild {
        return setPriority.createGuild(name, ownerId)
    }

    override suspend fun deleteGuild(id: Int): Boolean {
        return setPriority.deleteGuild(id)
    }

    override suspend fun getGuild(id: Int): Guild? {
        return getPriority.getGuild(id) ?: setPriority.getGuild(id)
    }

    override fun getGuild(name: String): Flow<Guild> {
        return getPriority.getGuild(name).onEmpty {
            emitAll(setPriority.getGuild(name))
        }
    }

    override suspend fun isOwner(guildId: Int, entityId: String): Boolean {
        return getPriority.isOwner(guildId, entityId) || setPriority.isOwner(guildId, entityId)
    }

    override suspend fun isMember(guildId: Int, entityId: String): Boolean {
        return getPriority.isMember(guildId, entityId) || setPriority.isMember(guildId, entityId)
    }

    override suspend fun hasInvitation(guildId: Int, entityId: String): Boolean {
        return getPriority.hasInvitation(guildId, entityId) || setPriority.hasInvitation(guildId, entityId)
    }

    override suspend fun addMember(guildId: Int, entityId: String): Boolean {
        return setPriority.addMember(guildId, entityId)
    }

    override suspend fun addInvitation(guildId: Int, entityId: String, expiredAt: Instant?): Boolean {
        return setPriority.addInvitation(guildId, entityId, expiredAt)
    }

    override suspend fun removeMember(guildId: Int, entityId: String): Boolean {
        return setPriority.removeMember(guildId, entityId)
    }

    override suspend fun removeInvitation(guildId: Int, entityId: String): Boolean {
        return setPriority.removeInvitation(guildId, entityId)
    }

    override fun getMembers(guildId: Int): Flow<GuildMember> {
        return getPriority.getMembers(guildId).onEmpty {
            emitAll(setPriority.getMembers(guildId))
        }
    }

    override fun getInvitations(guildId: Int): Flow<GuildInvite> {
        return getPriority.getInvitations(guildId).onEmpty {
            emitAll(setPriority.getInvitations(guildId))
        }
    }
}
