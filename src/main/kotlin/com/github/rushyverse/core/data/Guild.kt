package com.github.rushyverse.core.data

import io.r2dbc.spi.R2dbcException
import kotlinx.coroutines.flow.*
import org.komapper.annotation.*
import org.komapper.core.dsl.QueryDsl
import org.komapper.r2dbc.R2dbcDatabase
import java.time.Instant
import java.util.*

/**
 * Exception thrown when an entity is invited to a guild, but is already a member.
 */
public class GuildAlreadyMemberException(guildId: Int, memberId: UUID) :
    R2dbcException("Guild $guildId already has member $memberId")

/**
 * Data class for guilds.
 * @property id Unique identifier.
 * @property name Name.
 * @property owner UUID of the owner.
 * @property createdAt Timestamp of when the guild was created.
 */
@KomapperEntity
@KomapperTable("guild")
@KomapperOneToMany(GuildMember::class, "members")
public data class Guild(
    @KomapperId
    @KomapperAutoIncrement
    val id: Int,
    val name: String,
    val owner: UUID,
    @KomapperCreatedAt
    val createdAt: Instant,
)

/**
 * ID class for guild members.
 * @property guildId ID of the guild.
 * @property memberId ID of the member.
 */
public data class GuildMemberIds(
    public val guildId: Int,
    public val memberId: UUID,
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
     * @param owner Identifier of the owner.
     * @return The created guild.
     */
    public suspend fun createGuild(name: String, owner: UUID): Guild

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
     * Check if a member is the owner of a guild.
     * @param guildId ID of the guild.
     * @param memberId ID of the member.
     * @return `true` if the member is the owner, `false` otherwise.
     */
    public suspend fun isOwner(guildId: Int, memberId: UUID): Boolean

    /**
     * Check if a member is a member of a guild.
     * @param guildId ID of the guild.
     * @param memberId ID of the member.
     * @return `true` if the member is a member, `false` otherwise.
     */
    public suspend fun isMember(guildId: Int, memberId: UUID): Boolean

    /**
     * Check if a member is a pending member of a guild.
     * @param guildId ID of the guild.
     * @param memberId ID of the member.
     * @return `true` if the member is a pending member, `false` otherwise.
     */
    public suspend fun isInvite(guildId: Int, memberId: UUID): Boolean

    /**
     * Add a member to a guild.
     * @param guildId ID of the guild.
     * @param memberId ID of the member.
     * @return `true` if the member was added, `false` if they were already a member.
     */
    public suspend fun addMember(guildId: Int, memberId: UUID): Boolean

    /**
     * Add a pending member to a guild.
     * A pending member is a member that has been invited to the guild, but has not accepted the invite.
     * @param guildId ID of the guild.
     * @param memberId ID of the member.
     * @param expiredAt Timestamp of when the invite expires.
     * @return `true` if the member was added as pending member, `false` the member was already a pending member or a member.
     */
    public suspend fun addInvite(guildId: Int, memberId: UUID, expiredAt: Instant?): Boolean

    /**
     * Remove a member from a guild.
     * @param guildId ID of the guild.
     * @param memberId ID of the member.
     * @return `true` if the member was removed, `false` if they were not a member.
     */
    public suspend fun removeMember(guildId: Int, memberId: UUID): Boolean

    /**
     * Remove a pending member from a guild.
     * @param guildId ID of the guild.
     * @param memberId ID of the member.
     * @return `true` if the member was removed, `false` if they were not a pending member.
     */
    public suspend fun removeInvite(guildId: Int, memberId: UUID): Boolean

    /**
     * Get all members of a guild.
     * This includes members and owner.
     * @param guildId ID of the guild.
     * @return A flow of all members.
     */
    public suspend fun getMembers(guildId: Int): Flow<UUID>

    /**
     * Get all pending members of a guild.
     * @param guildId ID of the guild.
     * @return A flow of all pending members.
     */
    public suspend fun getInvited(guildId: Int): Flow<UUID>
}

public class GuildDatabaseService(public val database: R2dbcDatabase) : IGuildService {

    override suspend fun createGuild(name: String, owner: UUID): Guild {
        val guild = Guild(0, name, owner, Instant.EPOCH)
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

    override suspend fun isOwner(guildId: Int, memberId: UUID): Boolean {
        val meta = _Guild.guild
        val query = QueryDsl.from(meta).where {
            meta.id eq guildId
            meta.owner eq memberId
        }
        return database.runQuery(query).firstOrNull() != null
    }

    override suspend fun addMember(guildId: Int, memberId: UUID): Boolean {
        val member = GuildMember(
            GuildMemberIds(guildId, memberId),
            database.config.clockProvider.now().instant()
        )

        val query = QueryDsl.insert(_GuildMember.guildMember)
            .onDuplicateKeyIgnore()
            .single(member)

        return database.runQuery(query) > 0
    }

    override suspend fun addInvite(guildId: Int, memberId: UUID, expiredAt: Instant?): Boolean {
        val invite = GuildInvite(
            GuildMemberIds(guildId, memberId),
            database.config.clockProvider.now().instant(),
            expiredAt
        )

        val query = QueryDsl.insert(_GuildInvite.guildInvite)
            .onDuplicateKeyIgnore()
            .single(invite)

        return try {
            database.runQuery(query) > 0
        } catch (e: R2dbcException) {
            when (e.message) {
                "Entity cannot be invited to guild because he is already a member of the guild" -> throw GuildAlreadyMemberException(
                    guildId,
                    memberId
                )
                else -> throw e
            }
        }
    }

    override suspend fun isMember(guildId: Int, memberId: UUID): Boolean {
        val memberMeta = _GuildMember.guildMember
        val memberIdMeta = memberMeta.id
        val query = QueryDsl.from(memberMeta).where {
            memberIdMeta.guildId eq guildId
            memberIdMeta.memberId eq memberId
        }
        return database.runQuery(query).firstOrNull() != null
    }

    override suspend fun isInvite(guildId: Int, memberId: UUID): Boolean {
        val meta = _GuildInvite.guildInvite
        val ids = meta.id
        val query = QueryDsl.from(meta).where {
            ids.guildId eq guildId
            ids.memberId eq memberId
        }
        return database.runQuery(query).firstOrNull() != null
    }

    override suspend fun removeMember(guildId: Int, memberId: UUID): Boolean {
        val meta = _GuildMember.guildMember
        val ids = meta.id
        val query = QueryDsl.delete(meta).where {
            ids.guildId eq guildId
            ids.memberId eq memberId
        }
        return database.runQuery(query) > 0
    }

    override suspend fun removeInvite(guildId: Int, memberId: UUID): Boolean {
        val meta = _GuildInvite.guildInvite
        val ids = meta.id
        val query = QueryDsl.delete(meta).where {
            ids.guildId eq guildId
            ids.memberId eq memberId
        }
        return database.runQuery(query) > 0
    }

    override suspend fun getMembers(guildId: Int): Flow<UUID> {
        val meta = _GuildMember.guildMember
        val ids = meta.id
        val query = QueryDsl.from(meta).where {
            ids.guildId eq guildId
        }.select(ids.memberId)
        return database.flowQuery(query).filterNotNull()
    }

    override suspend fun getInvited(guildId: Int): Flow<UUID> {
        val meta = _GuildInvite.guildInvite
        val ids = meta.id
        val query = QueryDsl.from(meta).where {
            ids.guildId eq guildId
        }.select(ids.memberId)
        return database.flowQuery(query).filterNotNull()
    }

}