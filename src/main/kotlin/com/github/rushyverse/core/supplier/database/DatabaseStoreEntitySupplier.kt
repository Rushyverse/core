package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.data.Guild
import com.github.rushyverse.core.data.GuildInvite
import com.github.rushyverse.core.data.GuildMember
import com.github.rushyverse.core.data.player.Player
import java.time.Instant
import java.util.*
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onEach
import mu.KotlinLogging

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
        return applyInCacheIfSupplierSucceed { it.addFriend(uuid, friend) }
    }

    override suspend fun addPendingFriend(uuid: UUID, friend: UUID): Boolean {
        return applyInCacheIfSupplierSucceed { it.addPendingFriend(uuid, friend) }
    }

    override suspend fun removeFriend(uuid: UUID, friend: UUID): Boolean {
        return applyInCacheIfSupplierSucceed { it.removeFriend(uuid, friend) }
    }

    override suspend fun removePendingFriend(uuid: UUID, friend: UUID): Boolean {
        return applyInCacheIfSupplierSucceed { it.removePendingFriend(uuid, friend) }
    }

    override fun getFriends(uuid: UUID): Flow<UUID> {
        // TODO Add instead of set
        return importValues(supplier.getFriends(uuid)) { cache.addFriends(uuid, it) }
    }

    override fun getPendingFriends(uuid: UUID): Flow<UUID> {
        // TODO Add instead of set
        return importValues(supplier.getPendingFriends(uuid)) { cache.addPendingFriends(uuid, it) }
    }

    override suspend fun isFriend(uuid: UUID, friend: UUID): Boolean {
        return supplier.isFriend(uuid, friend)
    }

    override suspend fun isPendingFriend(uuid: UUID, friend: UUID): Boolean {
        return supplier.isPendingFriend(uuid, friend)
    }

    override suspend fun deleteExpiredInvitations(): Long {
        return removeInSuppliers(
            { it.deleteExpiredInvitations() },
            { supplierDeleted, cacheDeleted -> supplierDeleted + cacheDeleted }
        )
    }

    override suspend fun createGuild(name: String, ownerId: UUID): Guild {
        return supplier.createGuild(name, ownerId).also { guild ->
            importWithCatchFailure(guild) {
                cache.addGuild(it)
            }
        }
    }

    override suspend fun deleteGuild(id: Int): Boolean {
        return removeInSuppliers(
            { it.deleteGuild(id) },
            { supplierDeleted, cacheDeleted -> supplierDeleted || cacheDeleted }
        )
    }

    override suspend fun getGuild(id: Int): Guild? {
        return supplier.getGuild(id)?.also { guild ->
            importWithCatchFailure(guild) {
                cache.addGuild(it)
            }
        }
    }

    override fun getGuild(name: String): Flow<Guild> {
        return supplier.getGuild(name).onEach { guild ->
            importWithCatchFailure(guild) {
                cache.addGuild(it)
            }
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
        return applyInCacheIfSupplierSucceed { it.addMember(guildId, entityId) }
    }

    override suspend fun addInvitation(guildId: Int, entityId: UUID, expiredAt: Instant?): Boolean {
        return applyInCacheIfSupplierSucceed { it.addInvitation(guildId, entityId, expiredAt) }
    }

    override suspend fun removeMember(guildId: Int, entityId: UUID): Boolean {
        return removeInSuppliers(
            { it.removeMember(guildId, entityId) },
            { supplierDeleted, cacheDeleted -> supplierDeleted || cacheDeleted }
        )
    }

    override suspend fun removeInvitation(guildId: Int, entityId: UUID): Boolean {
        return removeInSuppliers(
            { it.removeInvitation(guildId, entityId) },
            { supplierDeleted, cacheDeleted -> supplierDeleted || cacheDeleted }
        )
    }

    override fun getMembers(guildId: Int): Flow<GuildMember> {
        return importValues(supplier.getMembers(guildId)) { cache.addMembers(it) }
    }

    override fun getInvitations(guildId: Int): Flow<GuildInvite> {
        return importValues(supplier.getInvitations(guildId)) { cache.addInvitations(it) }
    }

    override suspend fun savePlayer(player: Player): Boolean {
        return applyInCacheIfSupplierSucceed { it.savePlayer(player) }
    }

    override suspend fun getPlayer(uuid: UUID): Player? {
        return supplier.getPlayer(uuid)?.also { player ->
            importWithCatchFailure(player) {
                cache.savePlayer(it)
            }
        }
    }

    override suspend fun removePlayer(uuid: UUID): Boolean {
        return removeInSuppliers(
            { it.removePlayer(uuid) },
            { supplierDeleted, cacheDeleted ->
                supplierDeleted || cacheDeleted
            }
        )
    }

    /**
     * Calls the [action] with the [value] and catches any [Exception] that may occur.
     * If an exception occurs, it will be logged.
     * @param value Value to pass to the [action] and may be used in the log message.
     * @param action Action to perform with the [value].
     */
    private inline fun <T> importWithCatchFailure(value: T, action: (T) -> Unit) {
        try {
            action(value)
        } catch (e: Exception) {
            logger.error(e) { "Error during import of the value $value into the cache." }
        }
    }

    /**
     * Imports the values of the [flow] into the cache using the [sendToCache] function.
     * The values are stored in a [Collection] and sent to the [sendToCache] function when the [flow] completes.
     * This allows sending the values in bulk instead of one by one to improve performance.
     * @param flow Flow of values to import.
     * @param sendToCache Function to send the values to the cache.
     * @return The [flow] with the same values.
     */
    private inline fun <reified T> importValues(
        flow: Flow<T>,
        crossinline sendToCache: suspend (Collection<T>) -> Unit
    ): Flow<T> {
        val values = mutableListOf<T>()
        return flow.onEach {
            values.add(it)
        }.onCompletion {
            importWithCatchFailure(values) {
                sendToCache(it)
            }
        }
    }

    /**
     * Removes the value from the [supplier] and [cache] and returns the result of the [result] function.
     * @param remove Function to remove the value from the [supplier].
     * @param result Function to return the result of the [supplier] and [cache] removal.
     * @return The result of the [result] function.
     */
    private suspend inline fun <T, R> removeInSuppliers(
        crossinline remove: suspend (IDatabaseEntitySupplier) -> T,
        crossinline result: (T, T) -> R
    ): R = coroutineScope {
        val supplierDeletedDeferred = async { remove(supplier) }
        val cacheDeleted = remove(cache)
        result(supplierDeletedDeferred.await(), cacheDeleted)
    }

    /**
     * Applies the [apply] function to the [supplier] and [cache] if the [supplier] succeeds.
     * @param apply Function to apply to the [supplier] and [cache].
     * @return True if the [supplier] succeeded, false otherwise.
     */
    private inline fun applyInCacheIfSupplierSucceed(apply: (IDatabaseEntitySupplier) -> Boolean): Boolean {
        return if (apply(supplier)) {
            apply(cache)
            true
        } else {
            false
        }
    }
}
