package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.data.Guild
import com.github.rushyverse.core.data.GuildInvite
import com.github.rushyverse.core.data.GuildMember
import kotlinx.coroutines.flow.*
import java.time.Instant
import java.util.*

/**
 * [IDatabaseEntitySupplier] that delegates to another [IDatabaseEntitySupplier] to resolve entities.
 * Resolved entities will always be stored in [cache] if it wasn't null.
 * To keep consistency, it is recommended to use the same [DatabaseSupplierConfiguration] for both suppliers.
 * The value of [configuration] depends on one of the suppliers.
 * @property cache Supplier used to interact with the cache.
 * @property supplier Supplier used to interact with a custom way.
 */
public class DatabaseStoreEntitySupplier(
    public val cache: DatabaseCacheEntitySupplier,
    public val supplier: IDatabaseEntitySupplier
) : IDatabaseEntitySupplier {

    override val configuration: DatabaseSupplierConfiguration
        get() = cache.configuration

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

    override fun getFriends(uuid: UUID): Flow<UUID> = flow {
        val friends = supplier.getFriends(uuid).toSet()
        cache.setFriends(uuid, friends)
        emitAll(friends.asFlow())
    }

    override fun getPendingFriends(uuid: UUID): Flow<UUID> = flow {
        val requests = supplier.getPendingFriends(uuid).toSet()
        cache.setPendingFriends(uuid, requests)
        emitAll(requests.asFlow())
    }

    override suspend fun isFriend(uuid: UUID, friend: UUID): Boolean {
        return supplier.isFriend(uuid, friend)
    }

    override suspend fun isPendingFriend(uuid: UUID, friend: UUID): Boolean {
        return supplier.isPendingFriend(uuid, friend)
    }

    override suspend fun createGuild(name: String, ownerId: String): Guild {
        return supplier.createGuild(name, ownerId).also {
            cache.importGuild(it)
        }
    }

    override suspend fun deleteGuild(id: Int): Boolean {
        return supplier.deleteGuild(id).or(cache.deleteGuild(id))
    }

    override suspend fun getGuild(id: Int): Guild? {
        return supplier.getGuild(id)?.also {
            cache.importGuild(it)
        }
    }

    override fun getGuild(name: String): Flow<Guild> {
        return supplier.getGuild(name).onEach {
            cache.importGuild(it)
        }
    }

    override suspend fun isOwner(guildId: Int, entityId: String): Boolean {
        return supplier.isOwner(guildId, entityId)
    }

    override suspend fun isMember(guildId: Int, entityId: String): Boolean {
        return supplier.isMember(guildId, entityId)
    }

    override suspend fun hasInvitation(guildId: Int, entityId: String): Boolean {
        return supplier.hasInvitation(guildId, entityId)
    }

    override suspend fun addMember(guildId: Int, entityId: String): Boolean {
        return if (supplier.addMember(guildId, entityId)) {
            cache.addMember(guildId, entityId)
            true
        } else {
            false
        }
    }

    override suspend fun addInvitation(guildId: Int, entityId: String, expiredAt: Instant?): Boolean {
        return if (supplier.addInvitation(guildId, entityId, expiredAt)) {
            cache.addInvitation(guildId, entityId, expiredAt)
            true
        } else {
            false
        }
    }

    override suspend fun removeMember(guildId: Int, entityId: String): Boolean {
        return if(supplier.removeMember(guildId, entityId)) {
            cache.removeMember(guildId, entityId)
            true
        } else {
            false
        }
    }

    override suspend fun removeInvitation(guildId: Int, entityId: String): Boolean {
        return if (supplier.removeInvitation(guildId, entityId)) {
            cache.removeInvitation(guildId, entityId)
            true
        } else {
            false
        }
    }

    override fun getMembers(guildId: Int): Flow<GuildMember> = flow {
        val requests = supplier.getMembers(guildId).toList()
        cache.importMembers(requests)
        emitAll(requests.asFlow())
    }

    override fun getInvitations(guildId: Int): Flow<GuildInvite> = flow {
        val requests = supplier.getInvitations(guildId).toList()
        cache.importInvitations(requests)
        emitAll(requests.asFlow())
    }
}
