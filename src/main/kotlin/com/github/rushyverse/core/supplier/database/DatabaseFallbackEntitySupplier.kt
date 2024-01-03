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

    override suspend fun deleteExpiredInvitations(): Long {
        return setPriority.deleteExpiredInvitations()
    }

    override suspend fun createGuild(guild: Guild): Boolean {
        return setPriority.createGuild(guild)
    }

    override suspend fun deleteGuild(guildId: String): Boolean {
        return setPriority.deleteGuild(guildId)
    }

    override suspend fun getGuildById(guildId: String): Guild? {
        return getPriority.getGuildById(guildId) ?: setPriority.getGuildById(guildId)
    }

    override fun getGuildByName(name: String): Flow<Guild> {
        return getPriority.getGuildByName(name).onEmpty {
            emitAll(setPriority.getGuildByName(name))
        }
    }

    override suspend fun isGuildOwner(guildId: String, entityId: String): Boolean {
        return getPriority.isGuildOwner(guildId, entityId) || setPriority.isGuildOwner(guildId, entityId)
    }

    override suspend fun isGuildMember(guildId: String, entityId: String): Boolean {
        return getPriority.isGuildMember(guildId, entityId) || setPriority.isGuildMember(guildId, entityId)
    }

    override suspend fun hasGuildInvitation(guildId: String, entityId: String): Boolean {
        return getPriority.hasGuildInvitation(guildId, entityId) || setPriority.hasGuildInvitation(guildId, entityId)
    }

    override suspend fun addGuildMember(member: GuildMember): Boolean {
        return setPriority.addGuildMember(member)
    }

    override suspend fun addGuildInvitation(invite: GuildInvite): Boolean {
        return setPriority.addGuildInvitation(invite)
    }

    override suspend fun removeGuildMember(guildId: String, entityId: String): Boolean {
        return setPriority.removeGuildMember(guildId, entityId)
    }

    override suspend fun removeGuildInvitation(guildId: String, entityId: String): Boolean {
        return setPriority.removeGuildInvitation(guildId, entityId)
    }

    override fun getGuildMembers(guildId: String): Flow<GuildMember> {
        return getPriority.getGuildMembers(guildId).onEmpty {
            emitAll(setPriority.getGuildMembers(guildId))
        }
    }

    override fun getGuildInvitations(guildId: String): Flow<GuildInvite> {
        return getPriority.getGuildInvitations(guildId).onEmpty {
            emitAll(setPriority.getGuildInvitations(guildId))
        }
    }
}
