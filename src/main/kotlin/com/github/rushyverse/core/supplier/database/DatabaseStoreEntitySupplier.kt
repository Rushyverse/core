package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.data.Guild
import com.github.rushyverse.core.data.GuildInvite
import com.github.rushyverse.core.data.GuildMember
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

    override suspend fun createGuild(guild: Guild): Boolean {
        return supplier.createGuild(guild).also {
            if (it) {
                cache.createGuild(guild)
            }
        }
    }

    override suspend fun deleteGuild(guildId: String): Boolean = coroutineScope {
        val supplierDeletedDeferred = async { supplier.deleteGuild(guildId) }
        val cacheDeleted = cache.deleteGuild(guildId)
        supplierDeletedDeferred.await() || cacheDeleted
    }

    override suspend fun getGuildById(guildId: String): Guild? {
        return supplier.getGuildById(guildId)?.also {
            importCatchFailure(it, cache::createGuild)
        }
    }

    override fun getGuildByName(name: String): Flow<Guild> {
        return supplier.getGuildByName(name).onEach {
            importCatchFailure(it, cache::createGuild)
        }
    }

    override suspend fun isGuildOwner(guildId: String, entityId: String): Boolean {
        return supplier.isGuildOwner(guildId, entityId)
    }

    override suspend fun isGuildMember(guildId: String, entityId: String): Boolean {
        return supplier.isGuildMember(guildId, entityId)
    }

    override suspend fun hasGuildInvitation(guildId: String, entityId: String): Boolean {
        return supplier.hasGuildInvitation(guildId, entityId)
    }

    override suspend fun addGuildMember(member: GuildMember): Boolean {
        return supplier.addGuildMember(member).also {
            if (it) {
                cache.addGuildMember(member)
            }
        }
    }

    override suspend fun addGuildInvitation(invite: GuildInvite): Boolean {
        return supplier.addGuildInvitation(invite).also {
            if (it) {
                cache.addGuildInvitation(invite)
            }
        }
    }

    override suspend fun removeGuildMember(guildId: String, entityId: String): Boolean {
        return supplier.removeGuildMember(guildId, entityId).also {
            if (it) {
                cache.removeGuildMember(guildId, entityId)
            }
        }
    }

    override suspend fun removeGuildInvitation(guildId: String, entityId: String): Boolean {
        return supplier.removeGuildInvitation(guildId, entityId).also {
            if (it) {
                cache.removeGuildInvitation(guildId, entityId)
            }
        }
    }

    override fun getGuildMembers(guildId: String): Flow<GuildMember> = supplier.getGuildMembers(guildId)
        .onEach {
            importCatchFailure(it, cache::addGuildMember)
        }

    override fun getGuildInvitations(guildId: String): Flow<GuildInvite> = supplier.getGuildInvitations(guildId)
        .onEach {
            importCatchFailure(it, cache::addGuildInvitation)
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
