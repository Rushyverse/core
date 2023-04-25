package com.github.rushyverse.core.data

import com.github.rushyverse.core.cache.AbstractCacheService
import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.data._Guild.Companion.guild
import com.github.rushyverse.core.data._GuildInvite.Companion.guildInvite
import com.github.rushyverse.core.serializer.InstantSerializer
import com.github.rushyverse.core.supplier.database.DatabaseSupplierConfiguration
import com.github.rushyverse.core.supplier.database.IDatabaseEntitySupplier
import com.github.rushyverse.core.supplier.database.IDatabaseStrategizable
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import io.r2dbc.spi.R2dbcException
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.serializer
import org.komapper.annotation.*
import org.komapper.core.DryRunDatabaseConfig.id
import org.komapper.core.dsl.QueryDsl
import org.komapper.core.dsl.query.bind
import org.komapper.r2dbc.R2dbcDatabase
import java.time.Instant
import kotlin.contracts.InvocationKind
import kotlin.contracts.contract

/**
 * Exception about guilds information.
 */
public open class GuildException(reason: String?) : Exception(reason)

/**
 * Exception thrown when a guild is not found.
 */
public open class GuildNotFoundException(reason: String?) : GuildException(reason)

/**
 * Exception thrown when an entity is invited to a guild, but is already a member.
 */
public open class GuildInvitedIsAlreadyMemberException(public val guild: Int, public val entity: String) :
    GuildException("The entity $entity cannot be invited to the guild $guild because he is already a member of it")

/**
 * Exception thrown when a member is added to a guild, but it's the owner.
 */
public open class GuildMemberIsOwnerOfGuildException(public val guild: Int, public val entity: String) :
    GuildException("The entity $entity cannot be set as a member of the guild $guild because he is the owner")

public interface GuildEntityIds {
    public val guildId: Int
    public val entityId: String
}

/**
 * Data class for guilds.
 * @property id Unique identifier.
 * @property name Name.
 * @property ownerId ID of the owner.
 * @property createdAt Timestamp of when the guild was created.
 */
@KomapperEntity
@KomapperTable("guild")
@KomapperOneToMany(GuildMember::class, "members")
@Serializable
public data class Guild(
    @KomapperId
    @KomapperAutoIncrement
    val id: Int,
    val name: String,
    val ownerId: String,
    @KomapperCreatedAt
    @Serializable(with = InstantSerializer::class)
    val createdAt: Instant = Instant.EPOCH,
)

/**
 * Database definition for guild invites.
 * @property id IDs of the data.
 * @property createdAt Timestamp of when the invite was created.
 */
@KomapperEntity
@KomapperTable("guild_invite")
@KomapperManyToOne(Guild::class, "guild")
@Serializable
public data class GuildInvite(
    @KomapperId
    override val guildId: Int,
    @KomapperId
    override val entityId: String,
    @Serializable(with = InstantSerializer::class)
    val expiredAt: Instant?,
    @KomapperCreatedAt
    @Serializable(with = InstantSerializer::class)
    val createdAt: Instant = Instant.EPOCH,
) : GuildEntityIds {

    /**
     * Check if the invite is expired.
     * @return `true` if the invite is expired, `false` if it is not.
     */
    public fun isExpired(): Boolean {
        return expiredAt != null && expiredAt.isBefore(Instant.now())
    }

}

@KomapperEntity
@KomapperTable("guild_member")
@KomapperManyToOne(Guild::class, "guild")
@Serializable
public data class GuildMember(
    @KomapperId
    override val guildId: Int,
    @KomapperId
    override val entityId: String,
    @KomapperCreatedAt
    @Serializable(with = InstantSerializer::class)
    val createdAt: Instant = Instant.EPOCH,
) : GuildEntityIds

public interface IGuildService {

    /**
     * Delete all expired invitations.
     * @return The number of deleted invitations.
     */
    public suspend fun deleteExpiredInvitations(): Long

    /**
     * Create a guild with a name and define someone as the owner.
     * @param name Name of the guild.
     * @param ownerId ID of the owner.
     * @return The created guild.
     */
    public suspend fun createGuild(name: String, ownerId: String): Guild

    /**
     * Delete a guild by its ID.
     * @param id ID of the guild.
     * @return `true` if the guild was deleted, `false` if it did not exist.
     */
    public suspend fun deleteGuild(id: Int): Boolean

    /**
     * Get a guild by its ID.
     * @param id ID of the guild.
     * @return The guild, or `null` if it does not exist.
     */
    public suspend fun getGuild(id: Int): Guild?

    /**
     * Get the guilds with the name
     * @param name Name of the guild.
     * @return A flow of guilds, can be empty.
     */
    public fun getGuild(name: String): Flow<Guild>

    /**
     * Check if an entity is the owner of a guild.
     * @param guildId ID of the guild.
     * @param entityId ID of the entity.
     * @return `true` if the entity is the owner, `false` if the entity is not the owner or the guild does not exist.
     */
    public suspend fun isOwner(guildId: Int, entityId: String): Boolean

    /**
     * Check if an entity is a member of a guild.
     * @param guildId ID of the guild.
     * @param entityId ID of the entity.
     * @return `true` if an entity is a member, `false` if the entity is not a member or the guild does not exist.
     */
    public suspend fun isMember(guildId: Int, entityId: String): Boolean

    /**
     * Check if an entity has been invited to a guild.
     * @param guildId ID of the guild.
     * @param entityId ID of the entity.
     * @return `true` if the entity has been invited, `false` if the entity has not been invited or the guild does not exist.
     */
    public suspend fun hasInvitation(guildId: Int, entityId: String): Boolean

    /**
     * Add a member to a guild.
     * @param guildId ID of the guild.
     * @param entityId ID of the member.
     * @return `true` if the entity was added, `false` if they were already a member.
     * @throws GuildMemberIsOwnerOfGuildException If the entity is the owner of the guild.
     */
    @Throws(GuildMemberIsOwnerOfGuildException::class)
    public suspend fun addMember(guildId: Int, entityId: String): Boolean

    /**
     * Send an invitation to join the guild to an entity.
     * @param guildId ID of the guild.
     * @param entityId ID of the entity.
     * @param expiredAt Timestamp of when the invite expires.
     * @return `true` if the entity was invited, `false` if the entity has already been invited.
     * @throws GuildNotFoundException If the guild does not exist.
     */
    @Throws(GuildNotFoundException::class, GuildInvitedIsAlreadyMemberException::class)
    public suspend fun addInvitation(guildId: Int, entityId: String, expiredAt: Instant?): Boolean

    /**
     * Remove a member from a guild.
     * @param guildId ID of the guild.
     * @param entityId ID of the member.
     * @return `true` if the entity was removed, `false` if the entity was not a member or the guild does not exist.
     */
    public suspend fun removeMember(guildId: Int, entityId: String): Boolean

    /**
     * Remove an invitation to join a guild.
     * @param guildId ID of the guild.
     * @param entityId ID of the entity.
     * @return `true` if the invitation was removed, `false` if the entity was not invited or the guild does not exist.
     */
    public suspend fun removeInvitation(guildId: Int, entityId: String): Boolean

    /**
     * Get all members of a guild.
     * @param guildId ID of the guild.
     * @return A flow of all members, should be empty if no members exist or the guild does not exist.
     */
    public fun getMembers(guildId: Int): Flow<GuildMember>

    /**
     * Get all ids of entities that have been invited to a guild.
     * @param guildId ID of the guild.
     * @return A flow of all invited, should be empty if no members exist or the guild does not exist.
     */
    public fun getInvitations(guildId: Int): Flow<GuildInvite>
}

public interface IGuildDatabaseService : IGuildService

/**
 * Service for caching guilds.
 */
public interface IGuildCacheService : IGuildService {

    /**
     * Merge all cache guilds data into the supplier.
     * @param supplier Supplier to merge into.
     */
    public suspend fun merge(supplier: IDatabaseEntitySupplier)

    /**
     * Add a guild with all the information.
     * @param guild Guild to add.
     * @return `true` if the guild was added, `false` if it was not imported.
     */
    public suspend fun addGuild(guild: Guild): Boolean

    /**
     * Add a member to a guild with all the information.
     * @param member Member to add.
     * @return `true` if the member was added, `false` otherwise.
     * @throws GuildNotFoundException If the guild does not exist.
     * @throws GuildMemberIsOwnerOfGuildException If the entity is the owner of the guild.
     */
    public suspend fun addMember(member: GuildMember): Boolean

    /**
     * Add an invitation to join a guild with all the information.
     * @param invite Invitation to add.
     * @return `true` if the invitation was added, `false` otherwise.
     * @throws GuildNotFoundException If the guild does not exist.
     * @throws GuildInvitedIsAlreadyMemberException If the entity is already a member of the guild.
     */
    public suspend fun addInvitation(invite: GuildInvite): Boolean

}

public class GuildDatabaseService(public val database: R2dbcDatabase) : IGuildDatabaseService {

    public companion object {
        private const val FOREIGN_KEY_VIOLATION_EXCEPTION_CODE = "23503"
        private const val INVITED_IS_ALREADY_MEMBER_EXCEPTION_CODE = "P1000"
        private const val MEMBER_IS_OWNER_OF_GUILD_EXCEPTION_CODE = "P1001"
    }

    override suspend fun deleteExpiredInvitations(): Long {
        val meta = guildInvite
        val query = QueryDsl.delete(meta).where {
            meta.expiredAt lessEq Instant.now()
        }
        return database.runQuery(query)
    }

    override suspend fun createGuild(name: String, ownerId: String): Guild {
        requireGuildNameNotBlank(name)
        requireOwnerIdNotBlank(ownerId)

        val guild = Guild(0, name, ownerId)
        val query = QueryDsl.insert(_Guild.guild).single(guild)
        return database.runQuery(query)
    }

    override suspend fun deleteGuild(id: Int): Boolean {
        val meta = guild
        val query = QueryDsl.delete(meta).where {
            meta.id eq id
        }
        return database.runQuery(query) > 0
    }

    override suspend fun getGuild(id: Int): Guild? {
        val meta = guild
        val query = QueryDsl.from(meta).where {
            meta.id eq id
        }
        return database.runQuery(query).firstOrNull()
    }

    override fun getGuild(name: String): Flow<Guild> {
        requireGuildNameNotBlank(name)

        val meta = guild
        val query = QueryDsl.from(meta).where {
            meta.name eq name
        }
        return database.flowQuery(query)
    }

    override suspend fun isOwner(guildId: Int, entityId: String): Boolean {
        requireEntityIdNotBlank(entityId)

        val meta = guild
        val query = QueryDsl.from(meta).where {
            meta.id eq guildId
            meta.ownerId eq entityId
        }
        return database.runQuery(query).firstOrNull() != null
    }

    override suspend fun addMember(guildId: Int, entityId: String): Boolean {
        requireEntityIdNotBlank(entityId)

        val member = GuildMember(guildId, entityId)

        val query = QueryDsl.insert(_GuildMember.guildMember)
            .onDuplicateKeyIgnore()
            .single(member)

        return try {
            database.runQuery(query) > 0
        } catch (e: R2dbcException) {
            mapException(e, guildId, entityId)
        }
    }

    override suspend fun addInvitation(guildId: Int, entityId: String, expiredAt: Instant?): Boolean {
        requireValidInvitation(entityId, expiredAt)

        val invite = GuildInvite(
            guildId,
            entityId,
            expiredAt,
            Instant.EPOCH
        )

        val meta = _GuildInvite.guildInvite
        val query = QueryDsl.insert(meta)
            .onDuplicateKeyUpdate()
            .set {
                it.expiredAt eq expiredAt
            }
            .where {
                if (expiredAt == null) {
                    meta.expiredAt.isNotNull()
                } else {
                    meta.expiredAt.isNull()
                    or {
                        meta.expiredAt notEq expiredAt
                    }
                }
            }
            .single(invite)

        return try {
            database.runQuery(query) > 0
        } catch (e: R2dbcException) {
            mapException(e, guildId, entityId)
        }
    }

    override suspend fun isMember(guildId: Int, entityId: String): Boolean {
        requireEntityIdNotBlank(entityId)

        val query = QueryDsl.executeTemplate(
            """
                SELECT is_member(/*guild*/0, /*entity*/'');
            """.trimIndent()
        ).bind("guild", guildId).bind("entity", entityId)

        return database.runQuery(query) == 1L
    }

    override suspend fun hasInvitation(guildId: Int, entityId: String): Boolean {
        requireEntityIdNotBlank(entityId)

        val meta = _GuildInvite.guildInvite
        val query = QueryDsl.from(meta).where {
            meta.guildId eq guildId
            meta.entityId eq entityId
        }

        val invite = database.runQuery(query).firstOrNull() ?: return false
        return !invite.isExpired()
    }

    override suspend fun removeMember(guildId: Int, entityId: String): Boolean {
        requireEntityIdNotBlank(entityId)

        val meta = _GuildMember.guildMember
        val query = QueryDsl.delete(meta).where {
            meta.guildId eq guildId
            meta.entityId eq entityId
        }

        return database.runQuery(query) > 0
    }

    override suspend fun removeInvitation(guildId: Int, entityId: String): Boolean {
        requireEntityIdNotBlank(entityId)

        val meta = _GuildInvite.guildInvite
        val query = QueryDsl.delete(meta).where {
            meta.guildId eq guildId
            meta.entityId eq entityId
        }
        return database.runQuery(query) > 0
    }

    override fun getMembers(guildId: Int): Flow<GuildMember> {
        val meta = _GuildMember.guildMember.clone(table = "guild_members_with_owner")

        val query = QueryDsl.from(meta).where {
            meta.guildId eq guildId
        }

        return database.flowQuery(query).filterNotNull()
    }

    override fun getInvitations(guildId: Int): Flow<GuildInvite> {
        val meta = _GuildInvite.guildInvite
        val query = QueryDsl.from(meta).where {
            meta.guildId eq guildId
        }
        return database.flowQuery(query).filterNotNull().filter {
            !it.isExpired()
        }
    }

    /**
     * Map an exception to a more specific exception.
     * @param exception The exception to map.
     * @return Nothing, always throws an exception.
     */
    private fun mapException(exception: R2dbcException, guild: Int, entity: String): Nothing {
        throw when (exception.sqlState) {
            MEMBER_IS_OWNER_OF_GUILD_EXCEPTION_CODE -> GuildMemberIsOwnerOfGuildException(guild, entity)
            INVITED_IS_ALREADY_MEMBER_EXCEPTION_CODE -> GuildInvitedIsAlreadyMemberException(guild, entity)
            FOREIGN_KEY_VIOLATION_EXCEPTION_CODE -> GuildNotFoundException(exception.message)
            else -> exception
        }
    }

}

/**
 * Implementation of [IGuildService] that uses [CacheClient] to manage data in cache.
 * @property prefixCommonKey Prefix key for common guild data.
 * This key allows to define data without targeting specific guild.
 * Useful to store a set of guild's IDs.
 */
public class GuildCacheService(
    client: CacheClient,
    public val prefixCommonKey: String = "guild:",
    prefixKey: String = "$prefixCommonKey%s:"
) : AbstractCacheService(client, prefixKey), IGuildCacheService {

    private companion object {
        /**
         * Range of possible ID to generate new guild.
         */
        val RANGE_GUILD_ID: IntRange = Int.MIN_VALUE until 0

        /**
         * Check if the ID is in the range of possible ID generated by [GuildCacheService].
         * @param id The ID to check.
         * @return `true` if the ID is in range, `false` otherwise.
         */
        fun isCacheGuild(id: Int): Boolean = id in RANGE_GUILD_ID
    }

    public enum class Type(public val key: String) {
        /**
         * Key to store guilds created by [GuildCacheService].
         * Is not used to store guild from other services like [GuildDatabaseService].
         */
        ADD_GUILD("add"),

        /**
         * Key to mark guilds as removed.
         * Only the guilds from other services like [GuildDatabaseService] can be marked as removed.
         * The guilds created by [GuildCacheService] are removed from cache.
         */
        REMOVE_GUILD("remove"),

        /**
         * Key to store guild's members created by [GuildCacheService].
         * Is not used to store guild's members imported from other services like [GuildDatabaseService].
         */
        ADD_MEMBER("member:add"),

        /**
         * Key to remove guild's members imported from other services like [GuildDatabaseService].
         * Is not used to remove guild's members created by [GuildCacheService].
         * The guild's members created by [GuildCacheService] are removed from cache.
         */
        REMOVE_MEMBER("member:remove"),

        /**
         * Key to store guild's invitations created by [GuildCacheService].
         * Is not used to store guild's invitations imported from other services like [GuildDatabaseService].
         */
        ADD_INVITATION("invite:add"),

        /**
         * Key to remove guild's invitations imported from other services like [GuildDatabaseService].
         * Is not used to remove guild's invitations created by [GuildCacheService].
         * The guild's invitations created by [GuildCacheService] are removed from cache.
         */
        REMOVE_INVITATION("invite:remove"),
    }

    override suspend fun deleteExpiredInvitations(): Long {
        var numberOfDeletions: Long = 0
        val mutex = Mutex()

        cacheClient.connect { connection ->
            getAddedInvitations()
                .filter { it.isExpired() }
                .collect {
                    if (removeInvitation(connection, it.guildId, it.entityId)) {
                        mutex.withLock {
                            numberOfDeletions++
                        }
                    }
                }
        }

        return numberOfDeletions
    }

    override suspend fun merge(supplier: IDatabaseEntitySupplier) {
        getRemovedGuilds().collect { supplier.deleteGuild(it) }

        getAddedGuilds().collect {
            val guild = if (isCacheGuild(it.id)) {
                supplier.createGuild(it.name, it.ownerId)
            } else {
                val guildId = it.id
                getRemovedMembers(guildId).collect { entity ->
                    supplier.removeMember(guildId, entity)
                }
                getRemovedInvitation(guildId).collect { entity ->
                    supplier.removeInvitation(guildId, entity)
                }
                it
            }

            val guildId = guild.id
            getAddedMembers(guildId).collect { member ->
                supplier.addMember(member.guildId, member.entityId)
            }
            getInvitations(guildId).collect { invitation ->
                supplier.addInvitation(invitation.guildId, invitation.entityId, invitation.expiredAt)
            }
        }
    }

    override suspend fun createGuild(name: String, ownerId: String): Guild {
        requireGuildNameNotBlank(name)
        requireOwnerIdNotBlank(ownerId)

        var guild: Guild
        cacheClient.connect { connection ->
            do {
                // Negative ID to avoid conflict with database ID generator
                val id = RANGE_GUILD_ID.random()
                guild = Guild(id, name, ownerId)

                val key = addGuildKey(id.toString())
                val value = encodeToByteArray(Guild.serializer(), guild)
            } while (connection.setnx(key, value) != true)
        }

        return guild
    }

    override suspend fun addGuild(guild: Guild): Boolean {
        if (guild.id in RANGE_GUILD_ID) {
            throw IllegalArgumentException("Guild ID cannot be between ${RANGE_GUILD_ID.first} and ${RANGE_GUILD_ID.last}")
        }

        return cacheClient.connect {
            val key = addGuildKey(guild.id.toString())
            it.set(key, encodeToByteArray(Guild.serializer(), guild)) == "OK"
        }
    }

    override suspend fun deleteGuild(id: Int): Boolean {
        return cacheClient.connect {
            if (isCacheGuild(id)) {
                deleteGuildData(it, id)
            } else {
                hasGuild(it, id) && deleteGuildData(it, id).and(guildMarkAsDeleted(it, id))
            }
        }
    }

    override suspend fun getGuild(id: Int): Guild? {
        return cacheClient.connect { connection ->
            getGuild(connection, id)
        }
    }

    /**
     * Get a guild by its ID.
     * @param connection Cache connection.
     * @param id ID of the guild.
     * @return The guild, or `null` if it does not exist.
     */
    private suspend fun getGuild(connection: RedisCoroutinesCommands<ByteArray, ByteArray>, id: Int): Guild? {
        val guildIdString = id.toString()
        return connection.get(addGuildKey(guildIdString))?.let {
            decodeFromByteArrayOrNull(Guild.serializer(), it)
        }
    }

    override fun getGuild(name: String): Flow<Guild> {
        requireGuildNameNotBlank(name)
        return getAddedGuilds().filter { it.name == name }
    }

    /**
     * Get all added guilds.
     * @return Flow of all added guilds.
     */
    private fun getAddedGuilds(): Flow<Guild> =
        getAllKeyValues(wildcardAddGuildKey())
            .mapNotNull { decodeFromByteArrayOrNull(Guild.serializer(), it) }

    /**
     * Get all removed guilds.
     * @return Flow of all ids of removed guilds.
     */
    private fun getRemovedGuilds() = getAllValuesOfMap(removeGuildKey())
        .mapNotNull { decodeFromByteArrayOrNull(Int.serializer(), it) }

    /**
     * Get all keys linked to the guild.
     * Will get all keys that start with the guild:[guild]:.
     * @param guildId ID of the guild.
     * @return List of all keys linked to the guild.
     */
    private fun getAllKeysLinkedToGuild(guildId: String): Flow<ByteArray> {
        val searchPattern = wildcardGuildKey(guildId)
        return scanKeys(searchPattern) { _, keys ->
            keys.asFlow()
        }
    }

    /**
     * Check if guild exists in cache.
     * If the guild is present in [Type.ADD_GUILD] and is not marked as deleted, it exists.
     * @param connection Redis connection.
     * @param id Guild ID.
     * @return `true` if guild exists, `false` otherwise.
     */
    private suspend fun hasGuild(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        id: Int
    ): Boolean {
        val guildIdString = id.toString()
        val addKey = addGuildKey(guildIdString)
        return connection.exists(addKey) == 1L
    }

    /**
     * Mark guild as deleted.
     * Will add the guild ID to the set of deleted guild (guild:remove).
     * @param id ID of the guild to mark as deleted.
     * @return `true` if the guild was marked as deleted, `false` otherwise.
     */
    private suspend fun guildMarkAsDeleted(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        id: Int
    ): Boolean {
        val key = removeGuildKey()
        val value = encodeToByteArray(Int.serializer(), id)
        val result = connection.sadd(key, value)
        return result != null && result > 0
    }

    /**
     * Delete all data related to guild.
     * Will delete the keys based on the [Guild.id].
     * @param guildId Guild to delete.
     * @return `true` if at least one key was deleted, `false` otherwise.
     */
    private suspend fun deleteGuildData(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        guildId: Int
    ): Boolean {
        val keys = getAllKeysLinkedToGuild(guildId.toString()).toList()
        if (keys.isEmpty()) {
            return false
        }

        val result = connection.del(*keys.toTypedArray())
        return result != null && result > 0
    }

    /**
     * Check if the guild exists in cache.
     * If the guild does not exist, a [GuildNotFoundException] will be thrown.
     * @param connection Redis connection.
     * @param id Guild ID.
     */
    private suspend fun requireGuildExists(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        id: Int
    ) {
        if (!hasGuild(connection, id)) {
            throwGuildNotFoundException(id)
        }
    }

    override suspend fun isOwner(guildId: Int, entityId: String): Boolean {
        requireEntityIdNotBlank(entityId)
        return getGuild(guildId)?.ownerId == entityId
    }

    override suspend fun addMember(guildId: Int, entityId: String): Boolean {
        return setMember(GuildMember(guildId, entityId))
    }

    override suspend fun addMember(member: GuildMember): Boolean {
        val guildId = member.guildId
        requireImportedGuild(guildId)
        return setMember(member)
    }

    /**
     * Add a member to a guild.
     * If the member is deleted, it will be removed from the deleted set to be added.
     * If the entity is invited, it will be removed from the invited.
     * If the member is already added, will update the data linked to the entity.
     * @param member Member to add.
     * @return `true` if the member was added or updated, `false` otherwise.
     */
    private suspend fun setMember(member: GuildMember): Boolean {
        val guildId = member.guildId
        val entityId = member.entityId
        requireEntityIdNotBlank(entityId)

        return cacheClient.connect {
            val guild = getGuild(guildId) ?: throwGuildNotFoundException(guildId)
            if (entityId == guild.ownerId) {
                throw GuildMemberIsOwnerOfGuildException(guildId, entityId)
            }

            setEntityValueIfNotEquals(it, this::addMemberKey, member, GuildMember.serializer()).also { isAdded ->
                if (isAdded) {
                    val guildIdString = guildId.toString()
                    if (!isCacheGuild(guildId)) {
                        removeMarkAsDeleted(it, removeMemberKey(guildIdString), entityId)
                    }
                    removeEntityValue(it, addInvitationKey(guildIdString), entityId)
                }
            }
        }
    }

    override suspend fun removeMember(guildId: Int, entityId: String): Boolean {
        requireEntityIdNotBlank(entityId)
        return cacheClient.connect {
            val guildIdString = guildId.toString()
            removeEntityValue(it, addMemberKey(guildIdString), entityId).also { isRemoved ->
                if (isRemoved && !isCacheGuild(guildId)) {
                    markAsDeleted(it, removeMemberKey(guildIdString), entityId)
                }
            }
        }
    }

    /**
     * Get all removed members for entities.
     * @return Flow of entity ID of all removed members.
     */
    private fun getRemovedMembers(guildId: Int): Flow<String> {
        return getAllValuesOfMap(removeMemberKey(guildId.toString()))
            .mapNotNull { decodeFromByteArrayOrNull(String.serializer(), it) }
    }

    /**
     * Get all added members for entities.
     * @param guildId Guild ID.
     * @return Flow of all added members.
     */
    private fun getAddedMembers(guildId: Int): Flow<GuildMember> {
        return getAllValuesOfMap(addMemberKey(guildId.toString()))
            .mapNotNull { decodeFromByteArrayOrNull(GuildMember.serializer(), it) }
    }

    override suspend fun isMember(guildId: Int, entityId: String): Boolean {
        requireEntityIdNotBlank(entityId)
        return cacheClient.connect {
            isMember(it, guildId, entityId)
        }
    }

    /**
     * Check if entity is a member of the guild.
     * If the entity is the owner of the guild, it is a member.
     * If the entity is added or imported, it is a member.
     * @param connection Redis connection.
     * @param guildId Guild ID.
     * @param entityId Entity ID.
     * @return `true` if entity is a member, `false` otherwise.
     */
    private suspend inline fun isMember(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        guildId: Int,
        entityId: String
    ): Boolean {
        val guild = getGuild(connection, guildId)
        return guild?.ownerId == entityId || entityIsPresent(
            connection,
            addMemberKey(guildId.toString()),
            entityId
        )
    }

    override suspend fun addInvitation(guildId: Int, entityId: String, expiredAt: Instant?): Boolean {
        return setInvitation(GuildInvite(guildId, entityId, expiredAt))
    }

    override suspend fun addInvitation(invite: GuildInvite): Boolean {
        val guildId = invite.guildId
        requireImportedGuild(guildId)
        return setInvitation(invite)
    }

    /**
     * Add an invitation to a guild.
     * If the invitation is deleted, it will be removed from the deleted set to be added.
     * If the entity is already added, will update the invitation.
     * @param invite Invitation to add.
     * @return `true` if the invitation was added or updated, `false` otherwise.
     */
    private suspend fun setInvitation(invite: GuildInvite): Boolean {
        val entityId = invite.entityId
        requireValidInvitation(entityId, invite.expiredAt)

        val guildId = invite.guildId
        return cacheClient.connect {
            requireGuildExists(it, guildId)
            requireEntityIsNotMember(it, guildId, entityId)

            setEntityValueIfNotEquals(it, this::addInvitationKey, invite, GuildInvite.serializer()).also { isAdded ->
                if (isAdded && !isCacheGuild(guildId)) {
                    removeMarkAsDeleted(it, removeInvitationKey(guildId.toString()), invite.entityId)
                }
            }
        }
    }

    override suspend fun removeInvitation(guildId: Int, entityId: String): Boolean {
        requireEntityIdNotBlank(entityId)
        return cacheClient.connect {
            removeInvitation(it, guildId, entityId)
        }
    }

    private suspend fun removeInvitation(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        guildId: Int,
        entityId: String
    ): Boolean {
        val guildIdString = guildId.toString()
        return removeEntityValue(connection, addInvitationKey(guildIdString), entityId).also { isRemoved ->
            if (isRemoved && !isCacheGuild(guildId)) {
                markAsDeleted(connection, removeInvitationKey(guildIdString), entityId)
            }
        }
    }

    /**
     * Check if the entity is not a member of the guild.
     * If the entity is a member, a [GuildInvitedIsAlreadyMemberException] will be thrown.
     * @param connection Cache connection.
     * @param guildId Guild where the entity could be a member.
     * @param entityId ID of the entity.
     */
    private suspend fun requireEntityIsNotMember(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        guildId: Int,
        entityId: String
    ) {
        if (isMember(connection, guildId, entityId)) {
            throw GuildInvitedIsAlreadyMemberException(guildId, entityId)
        }
    }

    /**
     * Set the value of an entity in the cache.
     * If the value is not equal to the current value, the value will be set.
     * The value will be set in the map referenced by the key [addKey] with the field [GuildEntityIds.entityId].
     * @param connection Cache connection.
     * @param addKey Function to create the key of the added entity.
     * @param value Value to set.
     * @param serializer Serializer of the value.
     * @return `true` if the value was set or updated, `false` if the same value is present.
     */
    private suspend inline fun <T : GuildEntityIds> setEntityValueIfNotEquals(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        addKey: (String) -> ByteArray,
        value: T,
        serializer: KSerializer<T>
    ): Boolean {
        contract {
            callsInPlace(addKey, InvocationKind.EXACTLY_ONCE)
        }
        val guildIdString = value.guildId.toString()
        val key = addKey(guildIdString)
        val currentValue = getEntityValue(connection, key, value.entityId, serializer)
        if (currentValue == value) {
            return false
        }
        setEntityValue(connection, key, value, serializer)
        return true
    }

    /**
     * Set the value of an entity in the cache.
     * The value will be set in a map referenced by the key [key].
     * The entry where the value will be stored is referenced by the key [GuildEntityIds.entityId].
     * @param connection Cache connection.
     * @param key Key of the map.
     * @param value Value to set in the map.
     * @param serializer Serializer of the value.
     */
    private suspend fun <T : GuildEntityIds> setEntityValue(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        key: ByteArray,
        value: T,
        serializer: KSerializer<T>
    ): Boolean {
        val encodedField = encodeKey(value.entityId)
        return connection.hset(key, encodedField, encodeToByteArray(serializer, value)) == true
    }

    /**
     * Get the value of an entity in the cache.
     * @param connection Cache connection.
     * @param key Key of the map.
     * @param entityId ID of the entity.
     * @param serializer Serializer of the value.
     * @return Value of the entity, or `null` if the entity is not in the cache.
     */
    private suspend fun <T : GuildEntityIds> getEntityValue(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        key: ByteArray,
        entityId: String,
        serializer: KSerializer<T>
    ): T? {
        val encodedField = encodeKey(entityId)
        return connection.hget(key, encodedField)?.let { decodeFromByteArrayOrNull(serializer, it) }
    }

    override suspend fun hasInvitation(guildId: Int, entityId: String): Boolean {
        requireEntityIdNotBlank(entityId)
        return cacheClient.connect {
            val invite = getEntityValue(
                it,
                addInvitationKey(guildId.toString()),
                entityId,
                GuildInvite.serializer()
            )
            invite != null && !invite.isExpired()
        }
    }

    /**
     * Get all removed invitations for entities.
     * @param guildId ID of the guild.
     * @return Flow of entities that have been removed of invitations.
     */
    private fun getRemovedInvitation(guildId: Int): Flow<String> {
        return getAllValuesOfMap(removeInvitationKey(guildId.toString()))
            .mapNotNull { decodeFromByteArrayOrNull(String.serializer(), it) }
    }

    /**
     * Remove an entity of a map.
     * @param connection Redis connection.
     * @param key Key of the map.
     * @param entity Entity to remove.
     * @return `true` if the entity has been removed, `false` otherwise.
     */
    private suspend fun removeEntityValue(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        key: ByteArray,
        entity: String
    ): Boolean {
        val field = encodeKey(entity)
        return connection.hdel(key, field) == 1L
    }

    /**
     * Mark an entity as deleted.
     * This method will add the entity to a set of deleted entities.
     * @param connection Redis connection.
     * @param key Key of the set.
     * @param entity Entity to add.
     * @return `true` if the entity has been added, `false` otherwise.
     */
    private suspend fun markAsDeleted(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        key: ByteArray,
        entity: String
    ): Boolean {
        val value = encodeToByteArray(String.serializer(), entity)
        return connection.sadd(key, value) == 1L
    }

    /**
     * Remove the mark of an entity as deleted.
     * @param connection Cache connection.
     * @param key Key of the set.
     * @param entity Entity to remove.
     * @return `true` if the entity has been removed, `false` otherwise.
     */
    private suspend fun removeMarkAsDeleted(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        key: ByteArray,
        entity: String
    ): Boolean {
        val value = encodeToByteArray(String.serializer(), entity)
        return connection.srem(key, value) == 1L
    }

    override fun getInvitations(guildId: Int): Flow<GuildInvite> {
        return getAddedInvitations(guildId)
            .filter { !it.isExpired() }
    }

    /**
     * Get all invitations in the cache.
     * Includes all invitations, expired or not.
     * @return Flow of all invitations.
     */
    private fun getAddedInvitations(): Flow<GuildInvite> =
        getAddedGuilds().flatMapMerge { getAddedInvitations(it.id) }

    /**
     * Get all invitations of a guild.
     * Includes all invitations, expired or not.
     * @param guildId ID of the guild.
     * @return Flow of all invitations of the guild.
     */
    private fun getAddedInvitations(guildId: Int): Flow<GuildInvite> {
        return getAllValuesOfMap(addInvitationKey(guildId.toString()))
            .mapNotNull { decodeFromByteArrayOrNull(GuildInvite.serializer(), it) }
    }

    override fun getMembers(guildId: Int): Flow<GuildMember> {
        return flow {
            cacheClient.connect {
                val guild = getGuild(it, guildId) ?: return@flow
                emit(GuildMember(guild.id, guild.ownerId, guild.createdAt))
            }
            emitAll(getAddedMembers(guildId))
        }
    }

    /**
     * Get all values of a map.
     * @param key Key of the map.
     * @return Flow of all values of the map.
     */
    private fun getAllValuesOfMap(key: ByteArray): Flow<ByteArray> = flow {
        cacheClient.connect { connection ->
            connection.hvals(key).filterNotNull().let { emitAll(it) }
        }
    }

    /**
     * Returns all values linked to the existing keys of the given types.
     * @param searchPattern Search pattern to use.
     * @return Flow of all values.
     */
    private fun getAllKeyValues(searchPattern: String): Flow<ByteArray> {
        return scanKeys(searchPattern) { connection, keys ->
            connection.mget(*keys.toTypedArray()).mapNotNull { it.getValueOrElse(null) }
        }
    }

    /**
     * Check if the given entity is present in the map key.
     * @param connection Redis connection.
     * @param key Key of the map.
     * @param entity Entity to check.
     * @return `true` if the entity is present even if the value is `null`, `false` otherwise.
     */
    private suspend fun entityIsPresent(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        key: ByteArray,
        entity: String,
    ): Boolean {
        val field = encodeKey(entity)
        return connection.hexists(key, field) == true
    }

    /**
     * Throw [GuildNotFoundException] with a message containing the ID of the guild.
     * @param id ID of guild that was not found.
     */
    private fun throwGuildNotFoundException(id: Int): Nothing {
        throw GuildNotFoundException("Unable to find guild with ID $id in cache")
    }

    /**
     * Create the key for the add guild operation.
     * The format of the key is `guild:[guildId]:add`.
     * @param guildId Guild ID.
     * @return Encoded key.
     */
    private fun addGuildKey(
        guildId: String
    ): ByteArray = encodeFormattedKeyWithPrefix(Type.ADD_GUILD.key, guildId)

    /**
     * Create the key to find all added guilds.
     * The format of the key is `guild:*:add`.
     * @return Key.
     */
    private fun wildcardAddGuildKey(): String = formattedKeyWithPrefix(Type.ADD_GUILD.key, "*")

    /**
     * Create the key for the remove guild operation.
     * The format of the key is `guild:remove`.
     * @return Encoded key.
     */
    private fun removeGuildKey(): ByteArray = encodeKey(prefixCommonKey + Type.REMOVE_GUILD.key)

    /**
     * Create the key for the add member operation.
     * The format of the key is `guild:[guildId]:member:add`.
     * @param guildId Guild ID.
     * @return Encoded key.
     */
    private fun addMemberKey(
        guildId: String
    ): ByteArray = encodeFormattedKeyWithPrefix(Type.ADD_MEMBER.key, guildId)

    /**
     * Create the key for the remove member operation.
     * The format of the key is `guild:[guildId]:member:remove`.
     * @param guildId Guild ID.
     * @return Encoded key.
     */
    private fun removeMemberKey(
        guildId: String
    ): ByteArray = encodeFormattedKeyWithPrefix(Type.REMOVE_MEMBER.key, guildId)

    /**
     * Create the key for the add invitation operation.
     * The format of the key is `guild:[guildId]:invite:add:`.
     * @param guildId Guild ID.
     * @return Encoded key.
     */
    private fun addInvitationKey(
        guildId: String
    ): ByteArray = encodeFormattedKeyWithPrefix(Type.ADD_INVITATION.key, guildId)

    /**
     * Create the key for the remove invitation operation.
     * The format of the key is `guild:[guildId]:invite:remove`.
     * @param guildId Guild ID.
     * @return Encoded key.
     */
    private fun removeInvitationKey(
        guildId: String
    ): ByteArray = encodeFormattedKeyWithPrefix(Type.REMOVE_INVITATION.key, guildId)

    /**
     * Create the key to find all keys linked to the given guild ID.
     * The format of the key is `guild:[guildId]:*`.
     * @param guildId Guild ID.
     * @return Key.
     */
    private fun wildcardGuildKey(
        guildId: String
    ): String = formattedKeyWithPrefix("*", guildId)

    /**
     * Check if the guild is imported.
     * If the guild is created by the service, will throw an exception.
     * @param guild Guild ID.
     */
    private fun requireImportedGuild(guild: Int) {
        require(!isCacheGuild(guild)) { "Unable to interact with a guild[$guild] created in the cache" }
    }

}

/**
 * Check if the entity ID is not blank.
 * @param entityId ID of the entity.
 */
private fun requireEntityIdNotBlank(entityId: String) {
    require(entityId.isNotBlank()) { "Entity ID cannot be blank" }
}

/**
 * Check if the guild name is not blank.
 * @param guildName Name of the guild.
 */
private fun requireGuildNameNotBlank(guildName: String) {
    require(guildName.isNotBlank()) { "Guild name cannot be blank" }
}

/**
 * Check if the owner ID is not blank.
 * @param ownerId ID of the owner.
 */
private fun requireOwnerIdNotBlank(ownerId: String) {
    require(ownerId.isNotBlank()) { "Owner ID cannot be blank" }
}

/**
 * Check if the expired at is after now.
 * @param expiredAt Instant when the entity expires.
 */
private fun requireExpiredAtAfterNow(expiredAt: Instant) {
    require(expiredAt.isAfter(Instant.now())) { "Expired at must be after now" }
}

/**
 * Check all necessary requirements for a valid invitation.
 * Will check if the entity ID is not blank and if the expiration date is after now.
 * @param entityId Entity ID.
 * @param expiredAt Expiration date.
 */
private fun requireValidInvitation(entityId: String, expiredAt: Instant?) {
    requireEntityIdNotBlank(entityId)
    expiredAt?.let { requireExpiredAtAfterNow(it) }
}

/**
 * Implementation of [IGuildService] to manage guilds according to a [IDatabaseEntitySupplier].
 */
public class GuildService(override val supplier: IDatabaseEntitySupplier) : IGuildService by supplier,
    IDatabaseStrategizable {
    override fun withStrategy(getStrategy: (DatabaseSupplierConfiguration) -> IDatabaseEntitySupplier): GuildService {
        val newStrategy = getStrategy(supplier.configuration)
        return GuildService(newStrategy)
    }
}