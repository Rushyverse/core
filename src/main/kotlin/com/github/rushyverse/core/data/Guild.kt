package com.github.rushyverse.core.data

import com.github.rushyverse.core.cache.AbstractCacheService
import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.serializer.InstantSerializer
import io.lettuce.core.KeyScanArgs
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import io.r2dbc.spi.R2dbcException
import kotlinx.coroutines.flow.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.serializer
import org.komapper.annotation.*
import org.komapper.core.dsl.QueryDsl
import org.komapper.r2dbc.R2dbcDatabase
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.*
import kotlin.random.Random

/**
 * Exception thrown when an entity is invited to a guild, but is already a member.
 */
public class GuildInvitedIsAlreadyMemberException(reason: String?) : R2dbcException(reason)

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
    val createdAt: Instant,
)

/**
 * ID class for guild members.
 * @property guildId ID of the guild.
 * @property entityId ID of the member.
 */
public data class GuildMemberIds(
    public val guildId: Int,
    public val entityId: String,
)

@KomapperEntity
@KomapperTable("guild_invite")
@KomapperManyToOne(Guild::class, "guild")
public data class GuildInvite(
    @KomapperEmbeddedId
    val id: GuildMemberIds,
    @KomapperCreatedAt
    val createdAt: Instant,
    val expiredAt: Instant?,
)

/**
 * Database definition for guild members.
 * @property id ID of the guild and member.
 * @property createdAt Timestamp of when the member was added.
 */
@KomapperEntity
@KomapperTable("guild_member")
@KomapperManyToOne(Guild::class, "guild")
public data class GuildMember(
    @KomapperEmbeddedId
    val id: GuildMemberIds,
    @KomapperCreatedAt
    val createdAt: Instant,
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
     * Get a guild by its name.
     * @param name Name of the guild.
     * @return The guild, or `null` if it does not exist.
     */
    public suspend fun getGuild(name: String): Flow<Guild>

    /**
     * Check if an entity is the owner of a guild.
     * @param guildId ID of the guild.
     * @param entityId ID of the entity.
     * @return `true` if the entity is the owner, `false` otherwise.
     */
    public suspend fun isOwner(guildId: Int, entityId: String): Boolean

    /**
     * Check if an entity is a member of a guild.
     * @param guildId ID of the guild.
     * @param entityId ID of the entity.
     * @return `true` if an entity is a member, `false` otherwise.
     */
    public suspend fun isMember(guildId: Int, entityId: String): Boolean

    /**
     * Check if an entity has been invited to a guild.
     * @param guildId ID of the guild.
     * @param entityId ID of the entity.
     * @return `true` if the entity has been invited, `false` otherwise.
     */
    public suspend fun hasInvitation(guildId: Int, entityId: String): Boolean

    /**
     * Add a member to a guild.
     * @param guildId ID of the guild.
     * @param entityId ID of the member.
     * @return `true` if the entity was added, `false` if they were already a member.
     */
    public suspend fun addMember(guildId: Int, entityId: String): Boolean

    /**
     * Send an invitation to join the guild to an entity.
     * @param guildId ID of the guild.
     * @param entityId ID of the entity.
     * @param expiredAt Timestamp of when the invite expires.
     * @return `true` if the entity was invited, `false` if the entity has already been invited.
     */
    public suspend fun addInvitation(guildId: Int, entityId: String, expiredAt: Instant?): Boolean

    /**
     * Remove a member from a guild.
     * @param guildId ID of the guild.
     * @param entityId ID of the member.
     * @return `true` if the entity was removed, `false` if they were not a member.
     */
    public suspend fun removeMember(guildId: Int, entityId: String): Boolean

    /**
     * Remove an invitation to join a guild.
     * @param guildId ID of the guild.
     * @param entityId ID of the entity.
     * @return `true` if the invitation was removed, `false` if it did not exist.
     */
    public suspend fun removeInvitation(guildId: Int, entityId: String): Boolean

    /**
     * Get all members of a guild.
     * @param guildId ID of the guild.
     * @return A flow of all members.
     */
    public suspend fun getMembers(guildId: Int): Flow<String>

    /**
     * Get all ids of entities that have been invited to a guild.
     * @param guildId ID of the guild.
     * @return A flow of all ids.
     */
    public suspend fun getInvited(guildId: Int): Flow<String>
}

public class GuildDatabaseService(public val database: R2dbcDatabase) : IGuildService {

    public companion object {
        private const val INVITED_IS_ALREADY_MEMBER_EXCEPTION_CODE = "P1000"
    }

    override suspend fun createGuild(name: String, ownerId: String): Guild {
        val guild = Guild(0, name, ownerId, Instant.EPOCH)
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

    override suspend fun getGuild(name: String): Flow<Guild> {
        val meta = _Guild.guild
        val query = QueryDsl.from(meta).where {
            meta.name eq name
        }
        return database.flowQuery(query)
    }

    override suspend fun isOwner(guildId: Int, entityId: String): Boolean {
        val meta = _Guild.guild
        val query = QueryDsl.from(meta).where {
            meta.id eq guildId
            meta.ownerId eq entityId
        }
        return database.runQuery(query).firstOrNull() != null
    }

    override suspend fun addMember(guildId: Int, entityId: String): Boolean {
        val member = GuildMember(
            GuildMemberIds(guildId, entityId),
            database.config.clockProvider.now().instant()
        )

        val query = QueryDsl.insert(_GuildMember.guildMember)
            .onDuplicateKeyIgnore()
            .single(member)

        return database.runQuery(query) > 0
    }

    override suspend fun addInvitation(guildId: Int, entityId: String, expiredAt: Instant?): Boolean {
        val invite = GuildInvite(
            GuildMemberIds(guildId, entityId),
            database.config.clockProvider.now().instant(),
            expiredAt
        )

        val query = QueryDsl.insert(_GuildInvite.guildInvite)
            .onDuplicateKeyIgnore()
            .single(invite)

        return try {
            database.runQuery(query) > 0
        } catch (e: R2dbcException) {
            throw when (e.sqlState) {
                INVITED_IS_ALREADY_MEMBER_EXCEPTION_CODE -> GuildInvitedIsAlreadyMemberException(e.message)
                else -> e
            }
        }
    }

    override suspend fun isMember(guildId: Int, entityId: String): Boolean {
        val meta = _GuildMember.guildMember
        val ids = meta.id
        val query = QueryDsl.from(meta).where {
            ids.guildId eq guildId
            ids.entityId eq entityId
        }
        return database.runQuery(query).firstOrNull() != null
    }

    override suspend fun hasInvitation(guildId: Int, entityId: String): Boolean {
        val meta = _GuildInvite.guildInvite
        val ids = meta.id
        val query = QueryDsl.from(meta).where {
            ids.guildId eq guildId
            ids.entityId eq entityId
        }
        return database.runQuery(query).firstOrNull() != null
    }

    override suspend fun removeMember(guildId: Int, entityId: String): Boolean {
        val meta = _GuildMember.guildMember
        val ids = meta.id
        val query = QueryDsl.delete(meta).where {
            ids.guildId eq guildId
            ids.entityId eq entityId
        }
        return database.runQuery(query) > 0
    }

    override suspend fun removeInvitation(guildId: Int, entityId: String): Boolean {
        val meta = _GuildInvite.guildInvite
        val ids = meta.id
        val query = QueryDsl.delete(meta).where {
            ids.guildId eq guildId
            ids.entityId eq entityId
        }
        return database.runQuery(query) > 0
    }

    override suspend fun getMembers(guildId: Int): Flow<String> {
        val meta = _GuildMember.guildMember
        val ids = meta.id
        val query = QueryDsl.from(meta).where {
            ids.guildId eq guildId
        }.select(ids.entityId)
        return database.flowQuery(query).filterNotNull()
    }

    override suspend fun getInvited(guildId: Int): Flow<String> {
        val meta = _GuildInvite.guildInvite
        val ids = meta.id
        val query = QueryDsl.from(meta).where {
            ids.guildId eq guildId
        }.select(ids.entityId)
        return database.flowQuery(query).filterNotNull()
    }

}

/**
 * Implementation of [IGuildService] that uses [CacheClient] to manage data in cache.
 */
public class GuildCacheService(
    client: CacheClient,
    prefixKey: String = "guild:%s:"
) : AbstractCacheService(client, prefixKey), IGuildService {

    public enum class Type(public val key: String) {
        GUILD("store"),
        ADD_GUILD("add"),
        REMOVE_GUILD("remove"),

        MEMBERS("members"),
        ADD_MEMBER("members:add"),
        REMOVE_MEMBER("members:remove"),

        INVITATIONS("invitations"),
        ADD_INVITATION("invitations:add"),
        REMOVE_INVITATION("invitations:remove"),
    }

    override suspend fun createGuild(name: String, ownerId: String): Guild {
        require(name.isNotBlank()) { "Guild name cannot be blank" }
        require(ownerId.isNotBlank()) { "Guild owner ID cannot be blank" }

        val now = Instant.now().truncatedTo(ChronoUnit.MILLIS)
        var guild: Guild
        cacheClient.connect { connection ->
            do {
                // Negative ID to avoid conflict with database ID generator
                val id = Random.nextInt(Int.MIN_VALUE, 0)
                guild = Guild(id, name, ownerId, now)
                val key = encodeFormatKey(Type.ADD_GUILD.key, id.toString())
            } while (connection.setnx(key, encodeToByteArray(Guild.serializer(), guild)) == false)
        }

        return guild
    }

    override suspend fun deleteGuild(id: Int): Boolean {
        val key = encodeFormatKey(Type.REMOVE_GUILD.key, id.toString())

        val result = cacheClient.connect { connection ->
            connection.set(key, encodeToByteArray(Boolean.serializer(), true))
        }

        return when (result) {
            "OK" -> true
            else -> throw IllegalStateException("Unable to delete guild $id")
        }
    }

    override suspend fun getGuild(id: Int): Guild? {
        val idString = id.toString()
        return cacheClient.connect { connection ->
            val guild =
                connection.get(encodeFormatKey(Type.GUILD.key, idString))
                    ?: connection.get(encodeFormatKey(Type.ADD_GUILD.key, idString))
                    ?: return@connect null

            val resultDeleted = connection.exists(encodeFormatKey(Type.REMOVE_GUILD.key, idString))
            val hasBeenDeleted = resultDeleted != null && resultDeleted > 0
            if (hasBeenDeleted) null else guild

        }?.let { decodeFromByteArrayOrNull(Guild.serializer(), it) }
    }

    override suspend fun getGuild(name: String): Flow<Guild> {
        require(name.isNotBlank()) { "Guild name cannot be blank" }

        return mergeStoredAndAddedWithoutRemoved(
            Type.GUILD,
            Type.ADD_GUILD,
            Type.REMOVE_GUILD
        ) { connection, type ->
            getAllKeyValues(connection, type, Long.MAX_VALUE)
        }.mapNotNull { decodeFromByteArrayOrNull(Guild.serializer(), it) }.filter { it.name == name }
    }

    override suspend fun isOwner(guildId: Int, entityId: String): Boolean {
        requireEntityIdNotBlank(entityId)

        return getGuild(guildId)?.ownerId == entityId
    }

    override suspend fun isMember(guildId: Int, entityId: String): Boolean {
        requireEntityIdNotBlank(entityId)

        return cacheClient.connect { connection ->
            isStoredOrAddedAndNotDeleted(
                connection,
                guildId,
                entityId,
                Type.MEMBERS,
                Type.ADD_MEMBER,
                Type.REMOVE_MEMBER
            )
        }
    }

    override suspend fun hasInvitation(guildId: Int, entityId: String): Boolean {
        requireEntityIdNotBlank(entityId)

        return cacheClient.connect { connection ->
            isStoredOrAddedAndNotDeleted(
                connection,
                guildId,
                entityId,
                Type.INVITATIONS,
                Type.ADD_INVITATION,
                Type.REMOVE_INVITATION
            )
        }
    }

    override suspend fun addMember(guildId: Int, entityId: String): Boolean {
        requireEntityIdNotBlank(entityId)
        return addEntity(guildId, entityId, Type.ADD_MEMBER)
    }

    override suspend fun addInvitation(guildId: Int, entityId: String, expiredAt: Instant?): Boolean {
        requireEntityIdNotBlank(entityId)
        return addEntity(guildId, entityId, Type.ADD_INVITATION)
    }

    override suspend fun removeMember(guildId: Int, entityId: String): Boolean {
        requireEntityIdNotBlank(entityId)
        return addEntity(guildId, entityId, Type.REMOVE_MEMBER)
    }

    override suspend fun removeInvitation(guildId: Int, entityId: String): Boolean {
        requireEntityIdNotBlank(entityId)
        return addEntity(guildId, entityId, Type.REMOVE_INVITATION)
    }

    private fun requireEntityIdNotBlank(entityId: String) {
        require(entityId.isNotBlank()) { "Entity ID cannot be blank" }
    }

    override suspend fun getMembers(guildId: Int): Flow<String> {
        val idString = guildId.toString()
        return mergeStoredAndAddedWithoutRemoved(
            Type.MEMBERS,
            Type.ADD_MEMBER,
            Type.REMOVE_MEMBER
        ) { connection, type ->
            getAllMembers(connection, type, idString)
        }.mapNotNull { decodeFromByteArrayOrNull(String.serializer(), it) }
    }

    override suspend fun getInvited(guildId: Int): Flow<String> {
        val idString = guildId.toString()
        return mergeStoredAndAddedWithoutRemoved(
            Type.INVITATIONS,
            Type.ADD_INVITATION,
            Type.REMOVE_INVITATION,
        ) { connection, type -> getAllMembers(connection, type, idString) }
            .mapNotNull { decodeFromByteArrayOrNull(String.serializer(), it) }
    }

    /**
     * Check if an entity is present in the stored set or in the added set and not in the removed set.
     * @param connection Redis connection.
     * @param guildId ID of the guild.
     * @param entityId ID of the entity.
     * @param stored Type where the entity can be stored.
     * @param added Type where the entity can be added.
     * @param removed Type where the entity can be removed.
     * @return `true` if the entity is present, `false` otherwise.
     */
    private suspend fun isStoredOrAddedAndNotDeleted(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        guildId: Int,
        entityId: String,
        stored: Type,
        added: Type,
        removed: Type
    ): Boolean {
        val guildIdString = guildId.toString()
        val entityIdEncoded = encodeToByteArray(String.serializer(), entityId)
        // TODO : Optimize with a single query using EVAL
        return isMember(connection, guildIdString, stored, entityIdEncoded)
                || isMember(connection, guildIdString, added, entityIdEncoded)
                && !isMember(connection, guildIdString, removed, entityIdEncoded)
    }

    /**
     * Check if an entity is present in the set linked to the given type and id.
     * @param connection Redis connection.
     * @param guildId ID of the guild.
     * @param type Type of the data where the entity can be present.
     * @param entityIdEncoded Encoded ID of the entity.
     * @return `true` if the entity is present, `false` otherwise.
     */
    private suspend fun isMember(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        guildId: String,
        type: Type,
        entityIdEncoded: ByteArray
    ): Boolean {
        return connection.sismember(encodeFormatKey(type.key, guildId), entityIdEncoded) ?: false
    }

    /**
     * Returns a flow of all members of the set linked to the given type and id.
     * Will merge the stored and added values and filter out the removed values.
     * @param stored Type where the values can be stored.
     * @param added Type where the values can be added.
     * @param removed Type where the values can be removed.
     * @param getValues Function to get the values of the given type.
     * @return Flow of all data filtered and distinct.
     */
    private suspend inline fun mergeStoredAndAddedWithoutRemoved(
        stored: Type,
        added: Type,
        removed: Type,
        getValues: (RedisCoroutinesCommands<ByteArray, ByteArray>, Type) -> Flow<ByteArray>
    ): Flow<ByteArray> {
        return cacheClient.connect { connection ->
            val removedEntities = getValues(connection, removed).toSet()

            listOf(getValues(connection, stored), getValues(connection, added))
                .merge()
                .distinctUntilChanged()
                .filter { it !in removedEntities }
        }
    }

    /**
     * Returns all members of the set linked to the given type and id.
     * @param connection Redis connection.
     * @param type Type of the data to get the members of.
     * @param id ID of the set.
     * @return Flow of all members of the set.
     */
    private fun getAllMembers(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        type: Type,
        id: String
    ): Flow<ByteArray> = connection.smembers(encodeFormatKey(type.key, id))

    /**
     * Returns all keys of the given type.
     * @param connection Redis connection.
     * @param type Type of the keys.
     * @return Flow of all keys of the given type.
     */
    private suspend fun getAllKeyValues(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        type: Type,
        limit: Long
    ): Flow<ByteArray> {
        val searchKey = prefixKey.format("*") + type.key
        val scanner = connection.scan(KeyScanArgs.Builder.limit(limit).match(searchKey)) ?: return emptyFlow()
        return connection.mget(*scanner.keys.toTypedArray()).map { it.value }
    }

    /**
     * Adds an entity to the cache for the given guild and type.
     * The entity will be added to the set linked of the type.
     * @param guildId ID of the guild.
     * @param entityId ID of the entity.
     * @param type Category to register the entity in.
     * @return `true` if the entity was added, `false` otherwise.
     */
    private suspend fun addEntity(
        guildId: Int,
        entityId: String,
        type: Type
    ): Boolean {
        val key = encodeFormatKey(type.key, guildId.toString())

        val result = cacheClient.connect { connection ->
            connection.sadd(key, encodeToByteArray(String.serializer(), entityId))
        }

        return result != null && result > 0
    }

}