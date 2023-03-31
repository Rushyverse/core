package com.github.rushyverse.core.data

import com.github.rushyverse.core.cache.AbstractCacheService
import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.serializer.InstantSerializer
import io.lettuce.core.KeyScanArgs
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import io.r2dbc.spi.R2dbcException
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.isActive
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.serializer
import org.komapper.annotation.*
import org.komapper.core.dsl.QueryDsl
import org.komapper.r2dbc.R2dbcDatabase
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.random.Random
import kotlin.random.nextInt

/**
 * Exception thrown when an entity is invited to a guild, but is already a member.
 */
public class GuildInvitedIsAlreadyMemberException(reason: String?) : IllegalArgumentException(reason)

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

/**
 * Interface for guild invites.
 * @property entityId ID of the entity.
 * @property expiredAt Timestamp of when the invite expires.
 */
public interface IGuildInvite {
    public val guildId: Int
    public val entityId: String
    public val expiredAt: Instant?
}

/**
 * Database definition for guild invites.
 * @property id IDs of the data.
 * @property createdAt Timestamp of when the invite was created.
 */
@KomapperEntity
@KomapperTable("guild_invite")
@KomapperManyToOne(Guild::class, "guild")
public data class GuildInvite(
    @KomapperEmbeddedId
    val id: GuildMemberIds,
    @KomapperCreatedAt
    val createdAt: Instant,
    override val expiredAt: Instant?,
) : IGuildInvite {

    override val guildId: Int
        get() = id.guildId

    override val entityId: String
        get() = id.entityId
}

/**
 * Cache definition for guild invites.
 */
@Serializable
public data class CacheGuildInvite(
    override val guildId: Int,
    override val entityId: String,
    @Serializable(with = InstantSerializer::class)
    override val expiredAt: Instant?,
) : IGuildInvite

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
    public fun getGuild(name: String): Flow<Guild>

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
    public fun getMembers(guildId: Int): Flow<String>

    /**
     * Get all ids of entities that have been invited to a guild.
     * @param guildId ID of the guild.
     * @return A flow of all ids.
     */
    public fun getInvited(guildId: Int): Flow<String>
}

public interface IGuildCacheService : IGuildService {

    public suspend fun saveGuild(guild: Guild): Boolean

}

public class GuildDatabaseService(public val database: R2dbcDatabase) : IGuildService {

    public companion object {
        private const val INVITED_IS_ALREADY_MEMBER_EXCEPTION_CODE = "P1000"
    }

    override suspend fun createGuild(name: String, ownerId: String): Guild {
        requireGuildNameNotBlank(name)
        requireOwnerIdNotBlank(ownerId)

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

        val member = GuildMember(GuildMemberIds(guildId, entityId), Instant.EPOCH)

        val query = QueryDsl.insert(_GuildMember.guildMember)
            .onDuplicateKeyIgnore()
            .single(member)

        return database.runQuery(query) > 0
    }

    override suspend fun addInvitation(guildId: Int, entityId: String, expiredAt: Instant?): Boolean {
        requireEntityIdNotBlank(entityId)

        val invite = GuildInvite(
            GuildMemberIds(guildId, entityId),
            Instant.EPOCH,
            expiredAt
        )

        val meta = _GuildInvite.guildInvite
        val query = QueryDsl.insert(meta)
            .onDuplicateKeyUpdate()
            .set {
                it.expiredAt eq invite.expiredAt
            }
            .where {
                meta.expiredAt notEq invite.expiredAt
            }
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
        requireEntityIdNotBlank(entityId)

        val meta = _GuildMember.guildMember
        val ids = meta.id
        val query = QueryDsl.from(meta).where {
            ids.guildId eq guildId
            ids.entityId eq entityId
        }
        return database.runQuery(query).firstOrNull() != null
    }

    override suspend fun hasInvitation(guildId: Int, entityId: String): Boolean {
        requireEntityIdNotBlank(entityId)

        val meta = _GuildInvite.guildInvite
        val ids = meta.id
        val query = QueryDsl.from(meta).where {
            ids.guildId eq guildId
            ids.entityId eq entityId
        }
        return database.runQuery(query).firstOrNull() != null
    }

    override suspend fun removeMember(guildId: Int, entityId: String): Boolean {
        requireEntityIdNotBlank(entityId)

        val meta = _GuildMember.guildMember
        val ids = meta.id
        val query = QueryDsl.delete(meta).where {
            ids.guildId eq guildId
            ids.entityId eq entityId
        }
        return database.runQuery(query) > 0
    }

    override suspend fun removeInvitation(guildId: Int, entityId: String): Boolean {
        requireEntityIdNotBlank(entityId)

        val meta = _GuildInvite.guildInvite
        val ids = meta.id
        val query = QueryDsl.delete(meta).where {
            ids.guildId eq guildId
            ids.entityId eq entityId
        }
        return database.runQuery(query) > 0
    }

    override fun getMembers(guildId: Int): Flow<String> {
        val meta = _GuildMember.guildMember
        val ids = meta.id
        val query = QueryDsl.from(meta).where {
            ids.guildId eq guildId
        }.select(ids.entityId)
        return database.flowQuery(query).filterNotNull()
    }

    override fun getInvited(guildId: Int): Flow<String> {
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
    }

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

    override suspend fun saveGuild(guild: Guild): Boolean {
        val result = cacheClient.connect {
            val key = encodeFormattedKeyUsingPrefix(Type.GUILD.key, guild.id.toString())
            it.set(key, encodeToByteArray(Guild.serializer(), guild))
        }

        return result == "OK"
    }

    override suspend fun createGuild(name: String, ownerId: String): Guild {
        requireGuildNameNotBlank(name)
        requireOwnerIdNotBlank(ownerId)

        /**
         * Truncate to milliseconds to avoid precision loss with serialization.
         */
        val now = Instant.now().truncatedTo(ChronoUnit.MILLIS)
        var guild: Guild
        cacheClient.connect { connection ->
            do {
                // Negative ID to avoid conflict with database ID generator
                val id = Random.nextInt(RANGE_GUILD_ID)
                guild = Guild(id, name, ownerId, now)
                val key = encodeFormattedKeyUsingPrefix(Type.ADD_GUILD.key, id.toString())
            } while (connection.setnx(key, encodeToByteArray(Guild.serializer(), guild)) != true)
        }

        return guild
    }

    override suspend fun deleteGuild(id: Int): Boolean {
        val key = encodeKeyUsingPrefixCommon(Type.REMOVE_GUILD)

        val result = cacheClient.connect { connection ->
            connection.sadd(key, encodeToByteArray(Int.serializer(), id))
        }

        return result != null && result > 0
    }

    /**
     * Check if guild is marked as deleted.
     * @param connection Redis connection.
     * @param guildId Guild ID.
     * @return `true` if guild is marked as deleted, `false` otherwise.
     */
    private suspend fun isDeletedGuild(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        guildId: Int
    ): Boolean = isValueOfSet(connection, encodeKeyUsingPrefixCommon(Type.REMOVE_GUILD), guildId, Int.serializer())

    override suspend fun getGuild(id: Int): Guild? {
        val idString = id.toString()
        return cacheClient.connect { connection ->
            // Unable to use transaction because of https://github.com/lettuce-io/lettuce-core/issues/2371
            val guild =
                connection.get(encodeFormattedKeyUsingPrefix(Type.GUILD.key, idString))
                    ?: connection.get(encodeFormattedKeyUsingPrefix(Type.ADD_GUILD.key, idString))
                    ?: return@connect null

            if (isDeletedGuild(connection, id)) null else guild
        }?.let { decodeFromByteArrayOrNull(Guild.serializer(), it) }
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
                getAllKeyValues(Type.GUILD),
                getAllKeyValues(Type.ADD_GUILD)
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
        expiredAt?.let { requireExpiredAtAfterNow(it) }

        val key = encodeFormattedKeyUsingPrefix(Type.ADD_INVITATION.key, guildId.toString())

        val invite = CacheGuildInvite(guildId, entityId, expiredAt)
        val result = cacheClient.connect { connection ->
            connection.sadd(key, encodeToByteArray(CacheGuildInvite.serializer(), invite))
        }

        return result != null && result > 0
    }

    override suspend fun removeMember(guildId: Int, entityId: String): Boolean {
        requireEntityIdNotBlank(entityId)
        return addEntity(guildId, entityId, Type.REMOVE_MEMBER)
    }

    override suspend fun removeInvitation(guildId: Int, entityId: String): Boolean {
        requireEntityIdNotBlank(entityId)
        return addEntity(guildId, entityId, Type.REMOVE_INVITATION)
    }

    override fun getMembers(guildId: Int): Flow<String> {
        val idString = guildId.toString()
        return mergeStoredAndAddedWithoutRemoved(
            Type.MEMBERS,
            Type.ADD_MEMBER,
            Type.REMOVE_MEMBER
        ) { type -> getAllMembers(type, idString) }
            .mapNotNull { decodeFromByteArrayOrNull(String.serializer(), it) }
            .distinctUntilChanged()
    }

    override fun getInvited(guildId: Int): Flow<String> = flow {
        val idString = guildId.toString()
        val removedEntities = getAllMembers(Type.REMOVE_INVITATION, idString).mapNotNull {
            decodeFromByteArrayOrNull(String.serializer(), it)
        }.toSet()

        val now = Instant.now()
        listOf(getAllMembers(Type.INVITATIONS, idString), getAllMembers(Type.ADD_INVITATION, idString))
            .merge()
            .mapNotNull { decodeFromByteArrayOrNull(CacheGuildInvite.serializer(), it) }
            .filter { it.expiredAt == null || it.expiredAt.isAfter(now) }
            .map { it.entityId }
            .filter { it !in removedEntities }
            .distinctUntilChanged()
            .let { emitAll(it) }
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
        return isValueOfSet(connection, encodeFormattedKeyUsingPrefix(stored.key, guildIdString), entityIdEncoded)
                || isValueOfSet(connection, encodeFormattedKeyUsingPrefix(added.key, guildIdString), entityIdEncoded)
                && !isValueOfSet(connection, encodeFormattedKeyUsingPrefix(removed.key, guildIdString), entityIdEncoded)
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
    private inline fun mergeStoredAndAddedWithoutRemoved(
        stored: Type,
        added: Type,
        removed: Type,
        crossinline getValues: (Type) -> Flow<ByteArray>
    ): Flow<ByteArray> = flow {
        val removedEntities = getValues(removed).toSet()

        listOf(getValues(stored), getValues(added))
            .merge()
            .filter { it !in removedEntities }
            .let { emitAll(it) }
    }

    private suspend fun <T> isValueOfSet(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        cacheKey: ByteArray,
        entity: T,
        serializer: KSerializer<T>,
    ): Boolean = isValueOfSet(connection, cacheKey, encodeToByteArray(serializer, entity))

    private suspend fun isValueOfSet(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        cacheKey: ByteArray,
        value: ByteArray,
    ): Boolean {
        return connection.sismember(cacheKey, value) == true
    }

    /**
     * Returns all members of the set linked to the given type and id.
     * @param type Type of the data to get the members of.
     * @param id ID of the set.
     * @return Flow of all members of the set.
     */
    private fun getAllMembers(
        type: Type,
        id: String
    ): Flow<ByteArray> = flow {
        cacheClient.connect { connection ->
            emitAll(connection.smembers(encodeFormattedKeyUsingPrefix(type.key, id)))
        }
    }

    /**
     * Returns all values linked to the existing keys of the given types.
     * @param type Type of the keys to get the values of.
     * @return Flow of all values.
     */
    private fun getAllKeyValues(type: Type): Flow<ByteArray> = flow {
        val searchPattern = prefixKey.format("*") + type.key
        val scanArgs = KeyScanArgs.Builder.matches(searchPattern)

        cacheClient.connect { connection ->
            var cursor = connection.scan(scanArgs)
            while (cursor != null && currentCoroutineContext().isActive) {
                val keys = cursor.keys
                if (keys.isEmpty()) break

                emitAll(connection.mget(*keys.toTypedArray()).map { it.value })
                if (cursor.isFinished) break

                cursor = connection.scan(cursor, scanArgs)
            }
        }
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
        val key = encodeFormattedKeyUsingPrefix(type.key, guildId.toString())

        val result = cacheClient.connect { connection ->
            connection.sadd(key, encodeToByteArray(String.serializer(), entityId))
        }

        return result != null && result > 0
    }

    /**
     * Create a key using [commonPrefixWith] and the type.
     * @param type Type of the data.
     * @return Key using the common prefix and the type.
     */
    private fun encodeKeyUsingPrefixCommon(type: Type): ByteArray = encodeKey(prefixCommonKey + type.key)

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