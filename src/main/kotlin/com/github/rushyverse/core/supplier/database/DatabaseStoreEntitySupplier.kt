package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.data.Guild
import com.github.rushyverse.core.data.GuildInvite
import com.github.rushyverse.core.data.GuildMember
import com.github.rushyverse.core.data.player.Player
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.*
import mu.KotlinLogging
import java.time.Instant
import java.util.*

private val logger = KotlinLogging.logger {}

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

    override suspend fun deleteExpiredInvitations(): Long = coroutineScope {
        val supplierDeletedDeferred = async { supplier.deleteExpiredInvitations() }
        val cacheDeleted = cache.deleteExpiredInvitations()
        supplierDeletedDeferred.await().plus(cacheDeleted)
    }

    override suspend fun createGuild(name: String, ownerId: UUID): Guild {
        return supplier.createGuild(name, ownerId).also {
            importCatchFailure(it, cache::addGuild)
        }
    }

    override suspend fun deleteGuild(id: Int): Boolean = coroutineScope {
        val supplierDeletedDeferred = async { supplier.deleteGuild(id) }
        val cacheDeleted = cache.deleteGuild(id)
        supplierDeletedDeferred.await() || cacheDeleted
    }

    override suspend fun getGuild(id: Int): Guild? {
        return supplier.getGuild(id)?.also {
            importCatchFailure(it, cache::addGuild)
        }
    }

    override fun getGuild(name: String): Flow<Guild> {
        return supplier.getGuild(name).onEach {
            importCatchFailure(it, cache::addGuild)
        }
    }

    override suspend fun isOwner(guildId: Int, entityId: UUID): Boolean {
        return supplier.isOwner(guildId, entityId)
    }

    override suspend fun isMember(guildId: Int, entityId: UUID): Boolean {
        return supplier.isMember(guildId, entityId)
    }

    override suspend fun hasInvitation(guildId: Int, entityId: UUID): Boolean {
        return supplier.hasInvitation(guildId, entityId)
    }

    override suspend fun addMember(guildId: Int, entityId: UUID): Boolean {
        return supplier.addMember(guildId, entityId).also {
            if (it) {
                cache.addMember(guildId, entityId)
            }
        }
    }

    override suspend fun addInvitation(guildId: Int, entityId: UUID, expiredAt: Instant?): Boolean {
        return supplier.addInvitation(guildId, entityId, expiredAt).also {
            if (it) {
                cache.addInvitation(guildId, entityId, expiredAt)
            }
        }
    }

    override suspend fun removeMember(guildId: Int, entityId: UUID): Boolean {
        return supplier.removeMember(guildId, entityId).also {
            if (it) {
                cache.removeMember(guildId, entityId)
            }
        }
    }

    override suspend fun removeInvitation(guildId: Int, entityId: UUID): Boolean {
        return supplier.removeInvitation(guildId, entityId).also {
            if (it) {
                cache.removeInvitation(guildId, entityId)
            }
        }
    }

    override fun getMembers(guildId: Int): Flow<GuildMember> = supplier.getMembers(guildId)
        .onEach {
            importCatchFailure(it, cache::addMember)
        }

    override fun getInvitations(guildId: Int): Flow<GuildInvite> = supplier.getInvitations(guildId)
        .onEach {
            importCatchFailure(it, cache::addInvitation)
        }

    override suspend fun savePlayer(player: Player): Boolean {
        TODO("Not yet implemented")
    }

    override suspend fun getPlayer(uuid: UUID): Player? {
        TODO("Not yet implemented")
    }

    override suspend fun removePlayer(uuid: UUID): Boolean {
        TODO("Not yet implemented")
    }

    /**
     * Calls the [action] with the [value] and catches any [Exception] that may occur.
     * If an exception occurs, it will be logged.
     * @param value Value to pass to the [action] and may be used in the log message.
     * @param action Action to perform with the [value].
     */
    private suspend inline fun <T> importCatchFailure(value: T, action: suspend (T) -> Unit) {
        try {
            action(value)
        } catch (e: Exception) {
            logger.error(e) { "Error during import of the value $value into the cache." }
        }
    }
}
