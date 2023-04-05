package com.github.rushyverse.core.data

import com.github.rushyverse.core.cache.AbstractCacheService
import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.data._Guild.Companion.guild
import com.github.rushyverse.core.serializer.InstantSerializer
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import io.r2dbc.spi.R2dbcException
import kotlinx.coroutines.flow.*
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.serializer
import org.komapper.annotation.*
import org.komapper.core.DryRunDatabaseConfig.id
import org.komapper.core.dsl.QueryDsl
import org.komapper.core.dsl.operator.literal
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

public interface IGuildCacheService : IGuildService {

    public suspend fun importGuild(guild: Guild): Boolean

    public suspend fun importMembers(members: Collection<GuildMember>): Boolean

    public suspend fun importInvitations(invites: Collection<GuildInvite>): Boolean

}

public class GuildDatabaseService(public val database: R2dbcDatabase) : IGuildService {

    public companion object {
        private const val FOREIGN_KEY_VIOLATION_EXCEPTION_CODE = "23503"
        private const val INVITED_IS_ALREADY_MEMBER_EXCEPTION_CODE = "P1000"
        private const val MEMBER_IS_OWNER_OF_GUILD_EXCEPTION_CODE = "P1001"
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
        }.select(literal(1))
        return database.runQuery(query).firstOrNull() != null
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
        return database.flowQuery(query).filterNotNull()
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
         * Key to store guilds from other services like [GuildDatabaseService].
         * Is not used to store guild created by [GuildCacheService].
         * The guilds created by [GuildCacheService] are stored in [ADD_GUILD] key.
         */
        IMPORT_GUILD("import"),

        /**
         * Key to store guilds created by [GuildCacheService].
         * Is not used to store guild from other services like [GuildDatabaseService].
         * The guilds imported from other services are stored in [IMPORT_GUILD] key.
         */
        ADD_GUILD("add"),

        /**
         * Key to mark guilds as removed.
         * Only the guilds from other services like [GuildDatabaseService] can be marked as removed.
         * The guilds created by [GuildCacheService] are removed from cache.
         */
        REMOVE_GUILD("remove"),

        /**
         * Key to store guild's members imported from other services like [GuildDatabaseService].
         * Is not used to store guild's members created by [GuildCacheService].
         * The guild's members created by [GuildCacheService] are stored in [ADD_MEMBER] key.
         */
        IMPORT_MEMBER("member:import"),

        /**
         * Key to store guild's members created by [GuildCacheService].
         * Is not used to store guild's members imported from other services like [GuildDatabaseService].
         * The guild's members imported from other services are stored in [IMPORT_MEMBER] key.
         */
        ADD_MEMBER("member:add"),

        /**
         * Key to remove guild's members imported from other services like [GuildDatabaseService].
         * Is not used to remove guild's members created by [GuildCacheService].
         * The guild's members created by [GuildCacheService] are removed from cache.
         */
        REMOVE_MEMBER("member:remove"),

        /**
         * Key to store guild's invitations imported from other services like [GuildDatabaseService].
         * Is not used to store guild's invitations created by [GuildCacheService].
         * The guild's invitations created by [GuildCacheService] are stored in [ADD_INVITATION] key.
         */
        IMPORT_INVITATION("invite:import"),

        /**
         * Key to store guild's invitations created by [GuildCacheService].
         * Is not used to store guild's invitations imported from other services like [GuildDatabaseService].
         * The guild's invitations imported from other services are stored in [IMPORT_INVITATION] key.
         */
        ADD_INVITATION("invite:add"),

        /**
         * Key to remove guild's invitations imported from other services like [GuildDatabaseService].
         * Is not used to remove guild's invitations created by [GuildCacheService].
         * The guild's invitations created by [GuildCacheService] are removed from cache.
         */
        REMOVE_INVITATION("invite:remove"),
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

                val key = createAddGuildKey(id.toString())
                val value = encodeToByteArray(Guild.serializer(), guild)
            } while (connection.setnx(key, value) != true)
        }

        return guild
    }

    override suspend fun importGuild(guild: Guild): Boolean {
        if (guild.id in RANGE_GUILD_ID) {
            throw IllegalArgumentException("Guild ID cannot be between ${RANGE_GUILD_ID.first} and ${RANGE_GUILD_ID.last}")
        }

        return cacheClient.connect {
            if (guildIsMarkedAsDeleted(it, guild.id)) return@connect false

            val key = createImportGuildKey(guild.id.toString())
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
        return (connection.get(createImportGuildKey(guildIdString)) ?: connection.get(createAddGuildKey(guildIdString)))
            ?.let {
                decodeFromByteArrayOrNull(Guild.serializer(), it)
            }
    }

    override fun getGuild(name: String): Flow<Guild> {
        requireGuildNameNotBlank(name)
        return getAllImportedAndAddedGuilds().filter { it.name == name }
    }

    /**
     * Get all imported and added guilds.
     * @return Flow of all guilds that were imported and added.
     */
    private fun getAllImportedAndAddedGuilds(): Flow<Guild> =
        listOf(getAllImportedGuilds(), getAllAddedGuilds()).merge()

    /**
     * Get all added guilds.
     * @return Flow of all added guilds.
     */
    private fun getAllAddedGuilds(): Flow<Guild> =
        getAllKeyValues(createWildcardAddGuildKey())
            .mapNotNull { decodeFromByteArrayOrNull(Guild.serializer(), it) }

    /**
     * Get all imported guilds.
     * @return Flow of all imported guilds.
     */
    private fun getAllImportedGuilds(): Flow<Guild> =
        getAllKeyValues(createWildcardImportGuildKey())
            .mapNotNull { decodeFromByteArrayOrNull(Guild.serializer(), it) }

    /**
     * Get all keys linked to the guild.
     * Will get all keys that start with the guild:[guild]:.
     * @param guildId ID of the guild.
     * @return List of all keys linked to the guild.
     */
    private fun getAllKeysLinkedToGuild(guildId: String): Flow<ByteArray> {
        val searchPattern = createWildcardGuildKey(guildId)
        return scanKeys(searchPattern) { _, keys ->
            keys.asFlow()
        }
    }

    /**
     * Check if guild exists in cache.
     * If the guild is present in [Type.IMPORT_GUILD] or [Type.ADD_GUILD] and is not marked as deleted, it exists.
     * @param connection Redis connection.
     * @param id Guild ID.
     * @return `true` if guild exists, `false` otherwise.
     */
    private suspend fun hasGuild(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        id: Int
    ): Boolean {
        val guildIdString = id.toString()
        val addKey = createAddGuildKey(guildIdString)
        return if (isCacheGuild(id)) {
            connection.exists(addKey) == 1L
        } else {
            val importKey = createImportGuildKey(guildIdString)
            connection.exists(importKey, addKey)?.let { it > 0 } == true
        }
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
        val key = createRemoveGuildKey()
        val value = encodeToByteArray(Int.serializer(), id)
        val result = connection.sadd(key, value)
        return result != null && result > 0
    }

    /**
     * Check if guild is marked as deleted.
     * Check if the id is in the set of deleted guild (guild:remove).
     * @param connection Redis connection.
     * @param guildId Guild ID.
     * @return `true` if guild is marked as deleted, `false` otherwise.
     */
    private suspend fun guildIsMarkedAsDeleted(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        guildId: Int
    ): Boolean = connection.sismember(createRemoveGuildKey(), encodeToByteArray(Int.serializer(), guildId)) == true

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

    override suspend fun importMembers(members: Collection<GuildMember>): Boolean {
        if (members.isEmpty()) return false

        members.forEach {
            requireEntityIdNotBlank(it.entityId)
        }

        return importEntities(
            members,
            GuildMember.serializer(),
            this::createAddMemberKey,
            this::createRemoveMemberKey,
            this::createImportMemberKey
        ) { connection, guildId, entities ->
            val guild = getGuild(connection, guildId) ?: throwGuildNotFoundException(guildId)
            entities.forEach { member ->
                if (member.entityId == guild.ownerId) {
                    throw GuildMemberIsOwnerOfGuildException(guild.id, member.entityId)
                }
            }
        }
    }

    override suspend fun addMember(guildId: Int, entityId: String): Boolean {
        requireEntityIdNotBlank(entityId)

        return setEntityValueIfNotImported(
            GuildMember(guildId, entityId),
            GuildMember.serializer(),
            this::createAddMemberKey,
            this::createImportMemberKey,
        )
    }

    override suspend fun removeMember(guildId: Int, entityId: String): Boolean {
        requireEntityIdNotBlank(entityId)
        return removeEntityAndMarkAsDeletedIfImported(
            guildId,
            entityId,
            this::createAddMemberKey,
            this::createRemoveMemberKey,
            this::createImportMemberKey,
        )
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
        return guild?.ownerId == entityId || entityIsAddedOrImported(
            connection,
            guildId,
            entityId,
            this::createAddMemberKey,
            this::createImportMemberKey
        )
    }

    override suspend fun importInvitations(invites: Collection<GuildInvite>): Boolean {
        if (invites.isEmpty()) return false

        invites.forEach {
            requireValidInvitation(it.entityId, it.expiredAt)
        }

        return importEntities(
            invites,
            GuildInvite.serializer(),
            this::createAddInvitationKey,
            this::createRemoveInvitationKey,
            this::createImportInvitationKey
        ) { connection, guildId, invitations ->
            val guild = getGuild(connection, guildId) ?: throwGuildNotFoundException(guildId)
            invitations.forEach { invite ->
                val entityId = invite.entityId
                if (entityId == guild.ownerId) {
                    throw GuildInvitedIsAlreadyMemberException(guild.id, entityId)
                }
                requireEntityIsNotMember(connection, guildId, entityId)
            }
        }
    }

    /**
     * Import entities.
     * @param entities List of entities to import.
     * @param serializer Serializer of the entity type.
     * @param addKey Function to create the key to add the entity.
     * @param removeKey Function to create the key to remove the entity.
     * @param importKey Function to create the key to import the entity.
     * @return `true` if the entities were imported, `false` otherwise.
     */
    private suspend inline fun <T : GuildEntityIds> importEntities(
        entities: Collection<T>,
        serializer: KSerializer<T>,
        addKey: (String) -> ByteArray,
        removeKey: (String) -> ByteArray,
        importKey: (String) -> ByteArray,
        requirement: (RedisCoroutinesCommands<ByteArray, ByteArray>, Int, List<T>) -> Unit = { _, _, _ -> }
    ): Boolean {
        contract {
            callsInPlace(addKey, InvocationKind.AT_MOST_ONCE)
            callsInPlace(removeKey, InvocationKind.UNKNOWN)
            callsInPlace(importKey, InvocationKind.AT_MOST_ONCE)
        }

        val guildEntities = entities.groupBy { it.guildId }
        guildEntities.keys.forEach {
            requireImportedGuild(it)
        }

        val result = cacheClient.connect { connection ->
            // Before importing invitations, we check if the guilds exist
            guildEntities.forEach { (guild, entities) ->
                requirement(connection, guild, entities)
            }

            guildEntities.map { (guildId, entities) ->
                val guildIdString = guildId.toString()

                val toAdd = entities.filterNot {
                    entityIsMarkedAsDeleted(connection, removeKey(guildIdString), it.entityId)
                }

                if (toAdd.isEmpty()) return@map false

                bulkDeleteAddedEntities(connection, addKey(guildIdString), toAdd.map { it.entityId })
                toAdd.forEach {
                    val mapKey = importKey(it.guildId.toString())
                    val encodedField = encodeKey(it.entityId)
                    connection.hset(mapKey, encodedField, encodeToByteArray(serializer, it))
                }
                true
            }
        }

        return result.any { it }
    }

    /**
     * Delete all entities in the add list.
     * @param connection Cache connection.
     * @param addKey Key of the add list.
     * @param entities Entities to remove.
     * @return `true` if the at least one entity was deleted, `false` otherwise.
     */
    private suspend fun bulkDeleteAddedEntities(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        addKey: ByteArray,
        entities: Collection<String>
    ): Boolean {
        val fields = entities.map { encodeKey(it) }.toTypedArray()
        val result = connection.hdel(addKey, *fields)
        return result != null && result > 0
    }

    override suspend fun addInvitation(guildId: Int, entityId: String, expiredAt: Instant?): Boolean {
        requireValidInvitation(entityId, expiredAt)

        return setEntityValueIfNotImported(
            GuildInvite(guildId, entityId, expiredAt),
            GuildInvite.serializer(),
            this::createAddInvitationKey,
            this::createImportInvitationKey
        ) { connection ->
            requireEntityIsNotMember(connection, guildId, entityId)
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
     * If the entity is already imported, the value will not be set.
     * Otherwise, the value will be set as an added entity.
     * The entry where the value will be stored is referenced by the key [GuildEntityIds.entityId].
     * @param value Value to set.
     * @param serializer Serializer of the value.
     * @param addKey Function to create the key of the added entity.
     * @param importKey Function to create the key of the imported entity.
     * @return `true` if the value was set, `false` otherwise.
     */
    private suspend inline fun <T : GuildEntityIds> setEntityValueIfNotImported(
        value: T,
        serializer: KSerializer<T>,
        addKey: (String) -> ByteArray,
        importKey: (String) -> ByteArray,
        requirement: (RedisCoroutinesCommands<ByteArray, ByteArray>) -> Unit = { }
    ): Boolean {
        contract {
            callsInPlace(addKey, InvocationKind.AT_MOST_ONCE)
            callsInPlace(importKey, InvocationKind.AT_MOST_ONCE)
        }

        val guildId = value.guildId
        val guildIdString = guildId.toString()
        return cacheClient.connect { connection ->
            requireGuildExists(connection, guildId)
            requirement(connection)

            if (isCacheGuild(guildId)) {
                setEntityValue(connection, addKey(guildIdString), value, serializer)
            } else {
                if (connection.hexists(importKey(guildIdString), encodeKey(value.entityId)) == true) {
                    return@connect false
                }
                setEntityValue(connection, addKey(guildIdString), value, serializer)
            }
            true
        }
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

    override suspend fun removeInvitation(guildId: Int, entityId: String): Boolean {
        requireEntityIdNotBlank(entityId)
        return removeEntityAndMarkAsDeletedIfImported(
            guildId,
            entityId,
            this::createAddInvitationKey,
            this::createRemoveInvitationKey,
            this::createImportInvitationKey
        )
    }

    /**
     * Remove an entity from the cache.
     * If the entity is imported, it will be marked as deleted.
     * Otherwise, it will be completely removed.
     * @param guildId Guild ID.
     * @param entityId Entity ID.
     * @param addKey Function to create the key of the added entity.
     * @param removeKey Function to create the key of the removed entity.
     * @param importKey Function to create the key of the imported entity.
     * @return `true` if the entity was removed, `false` otherwise.
     */
    private suspend inline fun removeEntityAndMarkAsDeletedIfImported(
        guildId: Int,
        entityId: String,
        addKey: (String) -> ByteArray,
        removeKey: (String) -> ByteArray,
        importKey: (String) -> ByteArray
    ): Boolean {
        contract {
            callsInPlace(addKey, InvocationKind.AT_MOST_ONCE)
            callsInPlace(removeKey, InvocationKind.AT_MOST_ONCE)
            callsInPlace(importKey, InvocationKind.AT_MOST_ONCE)
        }

        val guildIdString = guildId.toString()
        return cacheClient.connect { connection ->
            if (isCacheGuild(guildId)) {
                removeEntityValue(connection, addKey(guildIdString), entityId)
            } else {
                // If the invitation is imported, we mark it as removed
                // Otherwise, we delete the key of the invitation added
                if (removeEntityValue(connection, importKey(guildIdString), entityId)) {
                    markEntityAsDeleted(connection, removeKey(guildIdString), entityId)
                    true
                } else {
                    removeEntityValue(connection, addKey(guildIdString), entityId)
                }
            }
        }
    }

    override suspend fun hasInvitation(guildId: Int, entityId: String): Boolean {
        requireEntityIdNotBlank(entityId)
        return entityIsAddedOrImported(
            guildId,
            entityId,
            this::createAddInvitationKey,
            this::createImportInvitationKey
        )
    }

    /**
     * Check if the entity is added or imported.
     * @param guildId Guild ID.
     * @param entityId Entity ID.
     * @param addKey Function to create the key of the added entity.
     * @param importKey Function to create the key of the imported entity.
     * @return `true` if the entity is added or imported, `false` otherwise.
     */
    private suspend inline fun entityIsAddedOrImported(
        guildId: Int,
        entityId: String,
        addKey: (String) -> ByteArray,
        importKey: (String) -> ByteArray,
    ): Boolean {
        contract {
            callsInPlace(addKey, InvocationKind.AT_MOST_ONCE)
            callsInPlace(importKey, InvocationKind.AT_MOST_ONCE)
        }

        return cacheClient.connect { connection ->
            entityIsAddedOrImported(connection, guildId, entityId, addKey, importKey)
        }
    }

    /**
     * Check if the entity is added or imported.
     * @param connection Cache connection.
     * @param guildId Guild ID.
     * @param entityId Entity ID.
     * @param addKey Function to create the key of the added entity.
     * @param importKey Function to create the key of the imported entity.
     * @return `true` if the entity is added or imported, `false` otherwise.
     */
    private suspend inline fun entityIsAddedOrImported(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        guildId: Int,
        entityId: String,
        addKey: (String) -> ByteArray,
        importKey: (String) -> ByteArray,
    ): Boolean {
        contract {
            callsInPlace(addKey, InvocationKind.AT_MOST_ONCE)
            callsInPlace(importKey, InvocationKind.AT_MOST_ONCE)
        }

        val guildIdString = guildId.toString()
        return if (isCacheGuild(guildId)) {
            // If the guild is created by GuildCacheService, we only need to check if the invitation is added
            entityIsPresent(connection, addKey(guildIdString), entityId)
        } else {
            entityIsPresent(connection, importKey(guildIdString), entityId)
                    || entityIsPresent(connection, addKey(guildIdString), entityId)
        }
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
    private suspend fun markEntityAsDeleted(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        key: ByteArray,
        entity: String
    ): Boolean {
        val value = encodeToByteArray(String.serializer(), entity)
        return connection.sadd(key, value) == 1L
    }

    /**
     * Check if an entity is marked as deleted.
     * @param connection Redis connection.
     * @param key Key of the set.
     * @param entityId ID of the entity.
     * @return `true` if the entity is marked as deleted, `false` otherwise.
     */
    private suspend fun entityIsMarkedAsDeleted(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        key: ByteArray,
        entityId: String
    ): Boolean = connection.sismember(key, encodeToByteArray(String.serializer(), entityId)) == true

    override fun getInvitations(guildId: Int): Flow<GuildInvite> {
        return getAllEntitiesValues(guildId, this::createAddInvitationKey, this::createImportInvitationKey)
            .mapNotNull { decodeFromByteArrayOrNull(GuildInvite.serializer(), it) }
            .filter { !it.isExpired() }
    }

    override fun getMembers(guildId: Int): Flow<GuildMember> {
        return getAllEntitiesValues(guildId, this::createAddMemberKey, this::createImportMemberKey)
            .mapNotNull { decodeFromByteArrayOrNull(GuildMember.serializer(), it) }
    }

    /**
     * Get all values for each entity added or imported in a guild.
     * @param guildId Guild ID.
     * @param addKey Function to create the key of the map of the entity added.
     * @param importKey Function to create the key of the map of the entity imported.
     * @return Flow of all values of the entities.
     */
    private inline fun getAllEntitiesValues(
        guildId: Int,
        addKey: (String) -> ByteArray,
        importKey: (String) -> ByteArray,
    ): Flow<ByteArray> {
        contract {
            callsInPlace(addKey, InvocationKind.EXACTLY_ONCE)
            callsInPlace(importKey, InvocationKind.AT_MOST_ONCE)
        }

        val guildIdString = guildId.toString()
        return if (isCacheGuild(guildId)) {
            getAllValuesOfMap(addKey(guildIdString))
        } else {
            listOf(
                getAllValuesOfMap(importKey(guildIdString)),
                getAllValuesOfMap(addKey(guildIdString))
            ).merge()
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
     * Create the key for the import guild operation.
     * The format of the key is `guild:[guildId]:import`.
     * @param guildId Guild ID.
     * @return Encoded key.
     */
    private fun createImportGuildKey(
        guildId: String
    ): ByteArray = encodeFormattedKeyWithPrefix(Type.IMPORT_GUILD.key, guildId)

    /**
     * Create the key to find all imported guilds.
     * The format of the key is `guild:*:import`.
     * @return Key.
     */
    private fun createWildcardImportGuildKey(): String = formattedKeyWithPrefix(Type.IMPORT_GUILD.key, "*")

    /**
     * Create the key for the add guild operation.
     * The format of the key is `guild:[guildId]:add`.
     * @param guildId Guild ID.
     * @return Encoded key.
     */
    private fun createAddGuildKey(
        guildId: String
    ): ByteArray = encodeFormattedKeyWithPrefix(Type.ADD_GUILD.key, guildId)

    /**
     * Create the key to find all added guilds.
     * The format of the key is `guild:*:add`.
     * @return Key.
     */
    private fun createWildcardAddGuildKey(): String = formattedKeyWithPrefix(Type.ADD_GUILD.key, "*")

    /**
     * Create the key for the remove guild operation.
     * The format of the key is `guild:remove`.
     * @return Encoded key.
     */
    private fun createRemoveGuildKey(): ByteArray = encodeKey(prefixCommonKey + Type.REMOVE_GUILD.key)

    /**
     * Create the key for the import member operation.
     * The format of the key is `guild:[guildId]:member:import`.
     * @param guildId Guild ID.
     * @return Encoded key.
     */
    private fun createImportMemberKey(
        guildId: String,
    ): ByteArray = encodeFormattedKeyWithPrefix(Type.IMPORT_MEMBER.key, guildId)

    /**
     * Create the key for the add member operation.
     * The format of the key is `guild:[guildId]:member:add`.
     * @param guildId Guild ID.
     * @return Encoded key.
     */
    private fun createAddMemberKey(
        guildId: String
    ): ByteArray = encodeFormattedKeyWithPrefix(Type.ADD_MEMBER.key, guildId)

    /**
     * Create the key for the remove member operation.
     * The format of the key is `guild:[guildId]:member:remove`.
     * @param guildId Guild ID.
     * @return Encoded key.
     */
    private fun createRemoveMemberKey(
        guildId: String
    ): ByteArray = encodeFormattedKeyWithPrefix(Type.REMOVE_MEMBER.key, guildId)

    /**
     * Create the key for the import invitation operation.
     * The format of the key is `guild:[guildId]:invite:import:`.
     * @param guildId Guild ID.
     * @return Encoded key.
     */
    private fun createImportInvitationKey(
        guildId: String
    ): ByteArray = encodeFormattedKeyWithPrefix(Type.IMPORT_INVITATION.key, guildId)

    /**
     * Create the key for the add invitation operation.
     * The format of the key is `guild:[guildId]:invite:add:`.
     * @param guildId Guild ID.
     * @return Encoded key.
     */
    private fun createAddInvitationKey(
        guildId: String
    ): ByteArray = encodeFormattedKeyWithPrefix(Type.ADD_INVITATION.key, guildId)

    /**
     * Create the key for the remove invitation operation.
     * The format of the key is `guild:[guildId]:invite:remove`.
     * @param guildId Guild ID.
     * @return Encoded key.
     */
    private fun createRemoveInvitationKey(
        guildId: String
    ): ByteArray = encodeFormattedKeyWithPrefix(Type.REMOVE_INVITATION.key, guildId)

    /**
     * Create the key to find all keys linked to the given guild ID.
     * The format of the key is `guild:[guildId]:*`.
     * @param guildId Guild ID.
     * @return Key.
     */
    private fun createWildcardGuildKey(
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