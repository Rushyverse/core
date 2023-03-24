package com.github.rushyverse.core.data

import kotlinx.coroutines.flow.*
import org.komapper.annotation.*
import org.komapper.core.dsl.QueryDsl
import org.komapper.r2dbc.R2dbcDatabase
import java.time.Instant
import java.util.*

/**
 * Data class for guilds.
 * @property id Unique identifier.
 * @property name Name.
 * @property owner UUID of the owner.
 * @property createdAt Timestamp of when the guild was created.
 */
@KomapperEntity
@KomapperTable("guild")
@KomapperOneToMany(GuildMemberDef::class, "members")
public data class Guild(
    @KomapperId
    @KomapperAutoIncrement
    val id: Int,
    val name: String,
    val owner: UUID,
    @KomapperCreatedAt
    val createdAt: Instant,
) {
    public companion object {
        public suspend fun createTable(database: R2dbcDatabase) {
            database.runQuery(QueryDsl.create(_Guild.guild))
        }
    }
}

/**
 * ID class for guild members.
 * @property guildId ID of the guild.
 * @property memberId ID of the member.
 */
public data class GuildMemberDefId(
    public val guildId: Int,
    public val memberId: UUID,
)

/**
 * Database definition for guild members.
 * @property id ID of the guild and member.
 * @property state State of the member in the guild.
 * @property createdAt Timestamp of when the member was added.
 */
@KomapperEntity
@KomapperTable("guild_member")
@KomapperManyToOne(Guild::class, "guild")
public data class GuildMemberDef(
    @KomapperEmbeddedId
    val id: GuildMemberDefId,
    val pending: Boolean,
    @KomapperCreatedAt
    val createdAt: Instant,
) {
    public companion object {
        public suspend fun createTable(database: R2dbcDatabase) {
            val guildMeta = _Guild.guild
            val guildTableName = guildMeta.tableName()

            val meta = _GuildMemberDef.guildMemberDef
            val tableName = meta.tableName()

            database.runQuery(QueryDsl.create(_GuildMemberDef.guildMemberDef))
            database.runQuery(
                QueryDsl.executeScript(
                    """
                    ALTER TABLE $tableName
                    ADD CONSTRAINT FK_${tableName}_${guildTableName}
                    FOREIGN KEY (${meta.id.guildId.columnName})
                    REFERENCES $guildTableName (${guildMeta.id.columnName})
                    ON DELETE CASCADE
                    ON UPDATE CASCADE;
                    """.trimIndent().apply {
                        println(this)
                    }
                )
            )
        }
    }
}

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
    public suspend fun getGuild(name: String): Guild?

    /**
     * Check if a member is the owner of a guild.
     * @param guildId ID of the guild.
     * @param memberId ID of the member.
     * @return `true` if the member is the owner, `false` otherwise.
     */
    public suspend fun isOwner(guildId: Int, memberId: UUID): Boolean

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
     * @return `true` if the member was added as pending member, `false` the member was already a pending member or a member.
     */
    public suspend fun addPendingMember(guildId: Int, memberId: UUID): Boolean

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
    public suspend fun isPendingMember(guildId: Int, memberId: UUID): Boolean

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
    public suspend fun removePendingMember(guildId: Int, memberId: UUID): Boolean

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
    public suspend fun getPendingMembers(guildId: Int): Flow<UUID>
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

    override suspend fun getGuild(name: String): Guild? {
        val meta = _Guild.guild
        val query = QueryDsl.from(meta).where {
            meta.name eq name
        }
        return database.runQuery(query).firstOrNull()
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
        return addMember(guildId, memberId, false)
    }

    override suspend fun addPendingMember(guildId: Int, memberId: UUID): Boolean {
        return addMember(guildId, memberId, true)
    }

    private suspend fun addMember(guildId: Int, memberId: UUID, pending: Boolean): Boolean {
        val guildMember = GuildMemberDef(GuildMemberDefId(guildId, memberId), pending, Instant.EPOCH)
        val query = QueryDsl.insert(_GuildMemberDef.guildMemberDef).single(guildMember)
        database.runQuery(query)
        return true
    }

    override suspend fun isMember(guildId: Int, memberId: UUID): Boolean {
        val guildMeta = _Guild.guild
        val memberMeta = _GuildMemberDef.guildMemberDef
        val memberIdMeta = memberMeta.id

        val query = QueryDsl.from(memberMeta).innerJoin(guildMeta) {
            memberIdMeta.guildId eq guildMeta.id
        }.where {
            guildMeta.id eq guildId
            and {
                guildMeta.owner eq memberId
                or {
                    memberIdMeta.memberId eq memberId
                    memberMeta.pending eq false
                }
            }
        }

        return database.runQuery(query).firstOrNull() != null
    }

    override suspend fun isPendingMember(guildId: Int, memberId: UUID): Boolean {
        val meta = _GuildMemberDef.guildMemberDef
        val ids = meta.id
        val query = QueryDsl.from(meta).where {
            ids.guildId eq guildId
            ids.memberId eq memberId
            meta.pending eq true
        }
        return database.runQuery(query).firstOrNull() != null
    }

    override suspend fun removeMember(guildId: Int, memberId: UUID): Boolean {
        return removeMember(guildId, memberId, false)
    }

    override suspend fun removePendingMember(guildId: Int, memberId: UUID): Boolean {
        return removeMember(guildId, memberId, true)
    }

    private suspend fun removeMember(guildId: Int, memberId: UUID, pending: Boolean): Boolean {
        val meta = _GuildMemberDef.guildMemberDef
        val ids = meta.id
        val query = QueryDsl.delete(meta).where {
            ids.guildId eq guildId
            ids.memberId eq memberId
            meta.pending eq pending
        }
        return database.runQuery(query) > 0
    }

    override suspend fun getMembers(guildId: Int): Flow<UUID> {
        val meta = _Guild.guild
        val query = QueryDsl.from(meta).where {
            meta.id eq guildId
        }.select(meta.owner)

        return listOf(
            database.flowQuery(query).filterNotNull(),
            getAllMembers(guildId, false)
        ).merge().distinctUntilChanged()
    }

    override suspend fun getPendingMembers(guildId: Int): Flow<UUID> {
        return getAllMembers(guildId, true)
    }

    private fun getAllMembers(guildId: Int, pending: Boolean): Flow<UUID> {
        val meta = _GuildMemberDef.guildMemberDef
        val ids = meta.id
        val query = QueryDsl.from(meta).where {
            ids.guildId eq guildId
            meta.pending eq pending
        }.select(ids.memberId)
        return database.flowQuery(query).filterNotNull()
    }

}