package com.github.rushyverse.core.data

import com.github.rushyverse.core.cache.AbstractCacheService
import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.serializer.InstantSerializer
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import io.r2dbc.spi.R2dbcException
import kotlinx.coroutines.flow.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.serializer
import org.komapper.annotation.*
import org.komapper.core.DryRunDatabaseConfig.id
import org.komapper.core.dsl.QueryDsl
import org.komapper.core.dsl.operator.literal
import org.komapper.core.dsl.query.bind
import org.komapper.r2dbc.R2dbcDatabase
import java.time.Instant

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
public open class GuildInvitedIsAlreadyMemberException(reason: String?) : GuildException(reason)

/**
 * Exception thrown when a member is added to a guild, but it's the owner.
 */
public open class GuildMemberIsOwnerOfGuildException(reason: String?) : GuildException(reason)

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
    val guildId: Int,
    @KomapperId
    val entityId: String,
    @Serializable(with = InstantSerializer::class)
    val expiredAt: Instant?,
    @KomapperCreatedAt
    @Serializable(with = InstantSerializer::class)
    val createdAt: Instant = Instant.EPOCH,
)

@KomapperEntity
@KomapperTable("guild_member")
@KomapperManyToOne(Guild::class, "guild")
@Serializable
public data class GuildMember(
    @KomapperId
    val guildId: Int,
    @KomapperId
    val entityId: String,
    @KomapperCreatedAt
    @Serializable(with = InstantSerializer::class)
    val createdAt: Instant = Instant.EPOCH,
)

/**
 * SQL View definition for guild members with the owner.
 * @property guildId ID of the guild.
 * @property memberId ID of the member.
 * @property createdAt Timestamp of when the member was added.
 */
@KomapperEntity
@KomapperTable("guild_members_with_owner")
public data class GuildMemberWithOwnerDef(
    @KomapperId
    public val guildId: Int,
    @KomapperId
    public val memberId: String,
    public val createdAt: Instant,
)

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
    public fun getMembers(guildId: Int): Flow<String>

    /**
     * Get all ids of entities that have been invited to a guild.
     * @param guildId ID of the guild.
     * @return A flow of all invited, should be empty if no members exist or the guild does not exist.
     */
    public fun getInvitations(guildId: Int): Flow<GuildInvite>
}

public interface IGuildCacheService : IGuildService {

    public suspend fun importGuild(guild: Guild): Boolean

    public suspend fun importMembers(guildId: Int, members: Collection<String>): Boolean

    public suspend fun importInvitations(guildId: Int, invites: Collection<GuildInvite>): Boolean

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
        val meta = _Guild.guild
        val query = QueryDsl.delete(meta).where {
            meta.id eq id
        }
        return database.runQuery(query) > 0
    }

    override suspend fun getGuild(id: Int): Guild? {
        val meta = _Guild.guild
        val query = QueryDsl.from(meta).where {
            meta.id eq id
        }
        return database.runQuery(query).firstOrNull()
    }

    override fun getGuild(name: String): Flow<Guild> {
        requireGuildNameNotBlank(name)

        val meta = _Guild.guild
        val query = QueryDsl.from(meta).where {
            meta.name eq name
        }
        return database.flowQuery(query)
    }

    override suspend fun isOwner(guildId: Int, entityId: String): Boolean {
        requireEntityIdNotBlank(entityId)

        val meta = _Guild.guild
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
            mapException(e)
        }
    }

    override suspend fun addInvitation(guildId: Int, entityId: String, expiredAt: Instant?): Boolean {
        requireEntityIdNotBlank(entityId)
        expiredAt?.let { requireExpiredAtAfterNow(it) }

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
            mapException(e)
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

    override fun getMembers(guildId: Int): Flow<String> {
        val meta = _GuildMemberWithOwnerDef.guildMemberWithOwnerDef

        val query = QueryDsl.from(meta).where {
            meta.guildId eq guildId
        }.select(meta.memberId)

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
    private fun mapException(exception: R2dbcException): Nothing {
        throw when (exception.sqlState) {
            MEMBER_IS_OWNER_OF_GUILD_EXCEPTION_CODE -> GuildMemberIsOwnerOfGuildException(exception.message)
            INVITED_IS_ALREADY_MEMBER_EXCEPTION_CODE -> GuildInvitedIsAlreadyMemberException(exception.message)
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
        IMPORT_MEMBER("member"),

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
        IMPORT_INVITATION("invite:%s"),

        /**
         * Key to store guild's invitations created by [GuildCacheService].
         * Is not used to store guild's invitations imported from other services like [GuildDatabaseService].
         * The guild's invitations imported from other services are stored in [IMPORT_INVITATION] key.
         */
        ADD_INVITATION("invite:add:%s"),

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

                val key = encodeFormattedKeyWithPrefix(Type.ADD_GUILD.key, id.toString())
                val value = encodeToByteArray(Guild.serializer(), guild)
            } while (connection.setnx(key, value) != true)
        }

        return guild
    }

    override suspend fun importGuild(guild: Guild): Boolean {
        if (guild.id in RANGE_GUILD_ID) {
            throw IllegalArgumentException("Guild ID cannot be between ${RANGE_GUILD_ID.first} and ${RANGE_GUILD_ID.last}")
        }

        val result = cacheClient.connect {
            val key = encodeFormattedKeyWithPrefix(Type.IMPORT_GUILD.key, guild.id.toString())
            it.set(key, encodeToByteArray(Guild.serializer(), guild))
        }

        return result == "OK"
    }

    override suspend fun deleteGuild(id: Int): Boolean {
        return if (isCacheGuild(id)) {
            deleteGuildData(id)
        } else {
            hasGuild(id) && deleteGuildData(id).and(addMarkGuildAsDeleted(id))
        }
    }

    override fun getGuild(name: String): Flow<Guild> {
        requireGuildNameNotBlank(name)

        return flow {
            val removedGuilds = cacheClient.connect { connection ->
                connection.smembers(encodeKeyUsingPrefixCommon(Type.REMOVE_GUILD))
                    .mapNotNull { decodeFromByteArrayOrNull(Int.serializer(), it) }
                    .toSet()
            }

            // If optimization is needed, we can store the guild by name too
            listOf(
                getAllKeyValues(formattedKeyWithPrefix(Type.IMPORT_GUILD.key, "*")),
                getAllKeyValues(formattedKeyWithPrefix(Type.ADD_GUILD.key, "*"))
            ).merge()
                .mapNotNull { decodeFromByteArrayOrNull(Guild.serializer(), it) }
                .filter { it.name == name && it.id !in removedGuilds }
                .distinctUntilChanged()
                .let { emitAll(it) }
        }
    }

    override suspend fun isOwner(guildId: Int, entityId: String): Boolean {
        requireEntityIdNotBlank(entityId)
        return getGuild(guildId)?.ownerId == entityId
    }

    override suspend fun isMember(guildId: Int, entityId: String): Boolean {
        requireEntityIdNotBlank(entityId)

        return if (isCacheGuild(guildId)) {
            val guildIdString = guildId.toString()
            val entityIdEncoded = encodeToByteArray(String.serializer(), entityId)
            cacheClient.connect {
                isValueOfSet(it, encodeFormattedKeyWithPrefix(Type.ADD_MEMBER.key, guildIdString), entityIdEncoded)
            }
        } else {
            isImportedOrAddedAndNotDeleted(
                guildId,
                entityId,
                Type.IMPORT_MEMBER,
                Type.ADD_MEMBER,
                Type.REMOVE_MEMBER
            )
        }
    }

    override suspend fun hasInvitation(guildId: Int, entityId: String): Boolean {
        requireEntityIdNotBlank(entityId)
        return if (isCacheGuild(guildId)) {
            val guildIdString = guildId.toString()
            val entityIdEncoded = encodeToByteArray(String.serializer(), entityId)
            cacheClient.connect {
                isValueOfSet(it, encodeFormattedKeyWithPrefix(Type.ADD_INVITATION.key, guildIdString), entityIdEncoded)
            }
        } else {
            isImportedOrAddedAndNotDeleted(
                guildId,
                entityId,
                Type.IMPORT_INVITATION,
                Type.ADD_INVITATION,
                Type.REMOVE_INVITATION
            )
        }
    }

    override suspend fun addMember(guildId: Int, entityId: String): Boolean {
        requireEntityIdNotBlank(entityId)
        checkHasGuild(guildId)
        return addValueOfSet(guildId, entityId, Type.ADD_MEMBER)
    }

    override suspend fun importMembers(guildId: Int, members: Collection<String>): Boolean {
        checkHasGuild(guildId)

        val key = encodeFormattedKeyWithPrefix(Type.IMPORT_MEMBER.key, guildId.toString())
        val values = members.map { encodeToByteArray(String.serializer(), it) }

        val result = cacheClient.connect { connection ->
            connection.sadd(key, *values.toTypedArray())
        }

        return result != null && result > 0
    }

    override suspend fun addInvitation(guildId: Int, entityId: String, expiredAt: Instant?): Boolean {
        requireEntityIdNotBlank(entityId)
        expiredAt?.let { requireExpiredAtAfterNow(it) }
        val guild = getGuild(guildId) ?: throwGuildNotFoundException(guildId)
        if (guild.ownerId == entityId || isMember(guildId, entityId)) {
            throw GuildInvitedIsAlreadyMemberException("The entity $entityId is already a member of the guild $guildId")
        }

        val invite = GuildInvite(guildId, entityId, expiredAt)
        return cacheClient.connect { connection ->
            setOrUpdateInvitation(connection, Type.ADD_INVITATION, invite)
        }
    }

    override suspend fun importInvitations(guildId: Int, invites: Collection<GuildInvite>): Boolean {
        if(invites.isEmpty()) return true
        invites.forEach { invite ->
            require(invite.guildId == guildId) { "Invitation must be for the guild $guildId" }
            invite.expiredAt?.let { requireExpiredAtAfterNow(it) }
        }

        checkHasGuild(guildId)

        val result = cacheClient.connect { connection ->
            invites.map { invite ->
                setOrUpdateInvitation(connection, Type.IMPORT_INVITATION, invite)
            }
        }

        return result.any { it }
    }

    /**
     * Set or update invitation for a guild and an entity.
     * If the invitation is different than the old one, it will be updated.
     * If the invitation does not exist, it will be created.
     * @param connection Cache connection.
     * @param type Type of invitation.
     * @param invite Guild invitation.
     * @return `true` if the invitation was created or updated, `false` otherwise.
     */
    private suspend fun setOrUpdateInvitation(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        type: Type,
        invite: GuildInvite,
    ): Boolean {
        val key = encodeFormattedKeyWithPrefix(
            type.key,
            invite.guildId.toString(),
            invite.entityId
        )

        val oldInvite = connection.get(key)?.let { decodeFromByteArrayOrNull(GuildInvite.serializer(), it) }
        return if (oldInvite == null || oldInvite != invite) {
            val newInviteEncoded = encodeToByteArray(GuildInvite.serializer(), invite)
            connection.set(key, newInviteEncoded) == "OK"
        } else false
    }

    override suspend fun removeMember(guildId: Int, entityId: String): Boolean {
        requireEntityIdNotBlank(entityId)
        return if (isCacheGuild(guildId)) {
            removeValueOfSet(guildId, entityId, Type.ADD_MEMBER)
        } else {
            // TODO remove if in ADD_MEMBER and mark as remove if in IMPORT_MEMBER
            addValueOfSet(guildId, entityId, Type.REMOVE_MEMBER)
        }
    }

    override suspend fun removeInvitation(guildId: Int, entityId: String): Boolean {
        requireEntityIdNotBlank(entityId)
        return cacheClient.connect { connection ->
            if (isCacheGuild(guildId)) {
                deleteInvitationAdded(connection, guildId, entityId)
            } else {
                val importKey = encodeFormattedKeyWithPrefix(
                    Type.IMPORT_INVITATION.key,
                    guildId.toString(),
                    entityId
                )
                if (connection.exists(importKey) == 1L) {
                    addValueOfSet(guildId, entityId, Type.REMOVE_INVITATION)
                } else {
                    deleteInvitationAdded(connection, guildId, entityId)
                }
            }
        }
    }

    /**
     * Delete the key of the invitation added.
     * @param guildId ID of the guild where the invitation was added.
     * @param entityId ID of the entity that was invited.
     * @param connection Cache connection.
     * @return `true` if the key was deleted, `false` otherwise.
     */
    private suspend fun deleteInvitationAdded(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        guildId: Int,
        entityId: String
    ): Boolean {
        val addKey = encodeFormattedKeyWithPrefix(
            Type.ADD_INVITATION.key,
            guildId.toString(),
            entityId
        )
        return connection.del(addKey) == 1L
    }

    override suspend fun getGuild(id: Int): Guild? {
        val idString = id.toString()
        return cacheClient.connect { connection ->
            val guild =
                connection.get(encodeFormattedKeyWithPrefix(Type.IMPORT_GUILD.key, idString))
                    ?: connection.get(encodeFormattedKeyWithPrefix(Type.ADD_GUILD.key, idString))
                    ?: return@connect null

            if (hasMarkGuildAsDeleted(connection, id)) null else guild
        }?.let {
            decodeFromByteArrayOrNull(Guild.serializer(), it)
        }
    }

    override fun getMembers(guildId: Int): Flow<String> {
        val idString = guildId.toString()
        return flow {
            val removedEntities = getValuesOfSet(Type.REMOVE_MEMBER, idString).toSet()

            listOf(getValuesOfSet(Type.IMPORT_MEMBER, idString), getValuesOfSet(Type.ADD_MEMBER, idString))
                .merge()
                .filter { it !in removedEntities }
                .mapNotNull { decodeFromByteArrayOrNull(String.serializer(), it) }
                .distinctUntilChanged()
                .let { emitAll(it) }
        }
    }

    override fun getInvitations(guildId: Int): Flow<GuildInvite> = flow {
        val idString = guildId.toString()
        val removedEntities = getValuesOfSet(Type.REMOVE_INVITATION, idString).mapNotNull {
            decodeFromByteArrayOrNull(String.serializer(), it)
        }.toSet()

        listOf(
            getAllKeyValues(formattedKeyWithPrefix(Type.IMPORT_INVITATION.key, guildId.toString(), "*")),
            getAllKeyValues(formattedKeyWithPrefix(Type.ADD_INVITATION.key, guildId.toString(), "*"))
        ).merge()
            .mapNotNull { decodeFromByteArrayOrNull(GuildInvite.serializer(), it) }
            .filter { it.expiredAt == null || it.expiredAt.isAfter(Instant.now()) }
            .filter { it.entityId !in removedEntities }
            .distinctUntilChanged()
            .let { emitAll(it) }
    }

    /**
     * Delete all data related to guild.
     * Will delete the keys based on the [Guild.id].
     * @param guildId Guild to delete.
     * @return `true` if at least one key was deleted, `false` otherwise.
     */
    private suspend fun deleteGuildData(
        guildId: Int
    ): Boolean {
        val searchPattern = formattedKeyWithPrefix("*", guildId.toString())
        val keys = scanKeys(searchPattern) { _, keys ->
            keys.asFlow()
        }.toList()

        if(keys.isEmpty()){
            return false
        }

        val result = cacheClient.connect {
            it.del(*keys.toTypedArray())
        }

        return result != null && result > 0
    }

    /**
     * Mark guild as deleted.
     * Will add the guild ID to the set of deleted guild (guild:remove).
     * @param id ID of the guild to mark as deleted.
     * @return `true` if the guild was marked as deleted, `false` otherwise.
     */
    private suspend fun addMarkGuildAsDeleted(id: Int): Boolean {
        val key = encodeKeyUsingPrefixCommon(Type.REMOVE_GUILD)
        val value = encodeToByteArray(Int.serializer(), id)

        val result = cacheClient.connect { connection ->
            connection.sadd(key, value)
        }

        return result != null && result > 0
    }

    /**
     * Check if guild is marked as deleted.
     * Check if the id is in the set of deleted guild (guild:remove).
     * @param connection Redis connection.
     * @param guildId Guild ID.
     * @return `true` if guild is marked as deleted, `false` otherwise.
     */
    private suspend fun hasMarkGuildAsDeleted(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        guildId: Int
    ): Boolean = isValueOfSet(
        connection,
        encodeKeyUsingPrefixCommon(Type.REMOVE_GUILD),
        encodeToByteArray(Int.serializer(), guildId)
    )

    /**
     * Check if guild exists in cache.
     * @param id ID of guild to check.
     * @return `true` if guild exists, `false` otherwise.
     */
    private suspend fun hasGuild(id: Int): Boolean {
        val guildIdString = id.toString()
        return cacheClient.connect { connection ->
            connection.exists(
                encodeFormattedKeyWithPrefix(Type.IMPORT_GUILD.key, guildIdString),
                encodeFormattedKeyWithPrefix(Type.ADD_GUILD.key, guildIdString)
            )?.let { it > 0 } == true && !hasMarkGuildAsDeleted(connection, id)
        }
    }

    /**
     * Check if an entity is present in the stored set or in the added set and not in the removed set.
     * @param guildId ID of the guild.
     * @param entityId ID of the entity.
     * @param imported Type where the entity can be stored.
     * @param added Type where the entity can be added.
     * @param removed Type where the entity can be removed.
     * @return `true` if the entity is present, `false` otherwise.
     */
    private suspend fun isImportedOrAddedAndNotDeleted(
        guildId: Int,
        entityId: String,
        imported: Type,
        added: Type,
        removed: Type
    ): Boolean {
        val guildIdString = guildId.toString()
        val entityIdEncoded = encodeToByteArray(String.serializer(), entityId)
        // TODO : Optimize with a single query using EVAL
        return cacheClient.connect {
            (isValueOfSet(it, encodeFormattedKeyWithPrefix(imported.key, guildIdString), entityIdEncoded)
                    || isValueOfSet(it, encodeFormattedKeyWithPrefix(added.key, guildIdString), entityIdEncoded))
                    && !isValueOfSet(it, encodeFormattedKeyWithPrefix(removed.key, guildIdString), entityIdEncoded)
        }
    }

    /**
     * Returns all values linked to the existing keys of the given types.
     * @param searchPattern Search pattern to use.
     * @return Flow of all values.
     */
    private fun getAllKeyValues(searchPattern: String): Flow<ByteArray> {
        return scanKeys(searchPattern) { connection, keys ->
            connection.mget(*keys.toTypedArray()).filter { it.hasValue() }.map { it.value }
        }
    }

    /**
     * Add an entity to the cache for the given guild and type.
     * The entity will be added to the set linked of the type.
     * @param guildId ID of the guild.
     * @param entityId ID of the entity.
     * @param type Category to register the entity in.
     * @return `true` if the entity was added, `false` otherwise.
     */
    private suspend fun addValueOfSet(
        guildId: Int,
        entityId: String,
        type: Type
    ): Boolean {
        val key = encodeFormattedKeyWithPrefix(type.key, guildId.toString())
        val value = encodeToByteArray(String.serializer(), entityId)

        val result = cacheClient.connect { connection ->
            connection.sadd(key, value)
        }

        return result != null && result > 0
    }

    /**
     * Remove an entity to the cache for the given guild and type.
     * The entity will be removed to the set linked of the type.
     * @param guildId ID of the guild.
     * @param entityId ID of the entity.
     * @param type Category to register the entity in.
     * @return `true` if the entity was removed, `false` otherwise.
     */
    private suspend fun removeValueOfSet(
        guildId: Int,
        entityId: String,
        type: Type
    ): Boolean {
        val key = encodeFormattedKeyWithPrefix(type.key, guildId.toString())
        val value = encodeToByteArray(String.serializer(), entityId)

        val result = cacheClient.connect { connection ->
            connection.srem(key, value)
        }

        return result != null && result > 0
    }

    /**
     * Check if the given value is present in the set linked to the given type and id.
     * @param connection Redis connection.
     * @param cacheKey Key of the set.
     * @param value Value to check.
     * @return `true` if the value is present, `false` otherwise.
     */
    private suspend fun isValueOfSet(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        cacheKey: ByteArray,
        value: ByteArray,
    ): Boolean = connection.sismember(cacheKey, value) == true

    /**
     * Returns all members of the set linked to the given type and id.
     * @param type Type of the data to get the members of.
     * @param id ID of the set.
     * @return Flow of all members of the set.
     */
    private fun getValuesOfSet(
        type: Type,
        id: String
    ): Flow<ByteArray> = flow {
        val key = encodeFormattedKeyWithPrefix(type.key, id)
        cacheClient.connect { connection ->
            emitAll(connection.smembers(key))
        }
    }

    /**
     * Create a key using [commonPrefixWith] and the type.
     * @param type Type of the data.
     * @return Key using the common prefix and the type.
     */
    private fun encodeKeyUsingPrefixCommon(type: Type): ByteArray = encodeKey(prefixCommonKey + type.key)

    /**
     * Check if guild exists in cache.
     * If the guild does not exist, throw [GuildNotFoundException].
     * @param id ID of guild to check.
     */
    private suspend fun checkHasGuild(id: Int) {
        if (!hasGuild(id)) {
            throwGuildNotFoundException(id)
        }
    }

    /**
     * Throw [GuildNotFoundException] with a message containing the ID of the guild.
     * @param id ID of guild that was not found.
     */
    private fun throwGuildNotFoundException(id: Int): Nothing {
        throw GuildNotFoundException("Unable to find guild with ID $id in cache")
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