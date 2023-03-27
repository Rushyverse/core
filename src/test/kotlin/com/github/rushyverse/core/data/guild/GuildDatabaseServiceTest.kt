package com.github.rushyverse.core.data.guild

import com.github.rushyverse.core.container.createPSQLContainer
import com.github.rushyverse.core.data.*
import com.github.rushyverse.core.data.utils.DatabaseUtils
import com.github.rushyverse.core.data.utils.DatabaseUtils.createConnectionOptions
import com.github.rushyverse.core.data.utils.MicroClockProvider
import com.github.rushyverse.core.utils.getRandomString
import io.r2dbc.spi.R2dbcDataIntegrityViolationException
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.komapper.core.dsl.QueryDsl
import org.komapper.r2dbc.R2dbcDatabase
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.MountableFile
import java.time.Clock
import java.time.Instant
import java.time.ZoneId
import java.time.temporal.ChronoUnit
import kotlin.test.*

@Testcontainers
class GuildDatabaseServiceTest {

    companion object {
        @JvmStatic
        @Container
        private val psqlContainer = createPSQLContainer()
            .withCopyToContainer(
                MountableFile.forClasspathResource("sql/guild.sql"),
                "/docker-entrypoint-initdb.d/init.sql"
            )
    }

    private lateinit var service: GuildDatabaseService
    private lateinit var database: R2dbcDatabase

    @BeforeTest
    fun onBefore() {
        database = R2dbcDatabase(createConnectionOptions(psqlContainer), clockProvider = MicroClockProvider())
        service = GuildDatabaseService(database)
    }

    @AfterEach
    fun onAfter() = runBlocking<Unit> {
        database.runQuery(
            QueryDsl.delete(_GuildInvite.guildInvite).all().options { it.copy(allowMissingWhereClause = true) })
        database.runQuery(
            QueryDsl.delete(_GuildMember.guildMember).all().options { it.copy(allowMissingWhereClause = true) })
        database.runQuery(QueryDsl.delete(_Guild.guild).all().options { it.copy(allowMissingWhereClause = true) })
    }

    @Nested
    inner class CreateGuild {

        @Test
        fun `when owner is not owner of a guild`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val guilds = getAllGuilds()
            assertEquals(1, guilds.size)
            assertEquals(guild, guilds[0])
        }

        @Test
        fun `when owner is always in a guild`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val guild2 = service.createGuild(getRandomString(), guild.ownerId)
            assertNotEquals(guild.id, guild2.id)

            val guilds = getAllGuilds()
            assertEquals(2, guilds.size)
            assertEquals(guild, guilds[0])
            assertEquals(guild2, guilds[1])
        }

        @Test
        fun `when entity is a member of another guild`() = runTest {
            val entity = getRandomString()
            val guild = service.createGuild(getRandomString(), getRandomString())
            service.addMember(guild.id, entity)

            val guild2 = service.createGuild(getRandomString(), entity)

            val guilds = getAllGuilds()
            assertEquals(2, guilds.size)
            assertEquals(guild, guilds[0])
            assertEquals(guild2, guilds[1])
        }

        @Test
        fun `with a name that is already taken`() = runTest {
            val name = getRandomString()
            val guild = service.createGuild(name, getRandomString())
            val guild2 = service.createGuild(name, getRandomString())
            assertNotEquals(guild.id, guild2.id)

            val guilds = getAllGuilds()
            assertEquals(2, guilds.size)
            assertEquals(guild, guilds[0])
            assertEquals(guild2, guilds[1])
        }

    }

    @Nested
    inner class DeleteGuild {

        @Test
        fun `when guild exists`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            assertTrue { service.deleteGuild(guild.id) }
            val guilds = getAllGuilds()
            assertEquals(0, guilds.size)
        }

        @Test
        fun `when guild does not exist`() = runTest {
            assertFalse { service.deleteGuild(0) }
            val guilds = getAllGuilds()
            assertEquals(0, guilds.size)
        }

    }

    @Nested
    inner class GetGuildById {

        @Test
        fun `when guild exists`() = runTest {
            service.createGuild(getRandomString(), getRandomString())
            val guilds = getAllGuilds()
            assertEquals(1, guilds.size)
            val guild = guilds[0]
            val retrievedGuild = service.getGuild(guild.id)
            assertEquals(guild, retrievedGuild)
        }

        @Test
        fun `when guild does not exist`() = runTest {
            val guild = service.getGuild(0)
            assertNull(guild)
        }

    }

    @Nested
    inner class GetGuildByName {

        @Test
        fun `when guild exists`() = runTest {
            val name = getRandomString()
            service.createGuild(name, getRandomString())
            val guilds = getAllGuilds()
            assertEquals(1, guilds.size)
            val guild = guilds[0]
            val (retrievedGuild) = service.getGuild(name).toList()
            assertEquals(guild, retrievedGuild)
        }

        @Test
        fun `when several guild with the same name exist`() = runTest {
            val name = getRandomString()
            service.createGuild(name, getRandomString())
            service.createGuild(name, getRandomString())

            val guilds = getAllGuilds()
            assertEquals(2, guilds.size)

            val retrievedGuilds = service.getGuild(name).toList()
            assertEquals(guilds, retrievedGuilds)
        }

        @Test
        fun `when guild does not exist`() = runTest {
            val guild = service.getGuild(getRandomString()).toList()
            assertEquals(0, guild.size)
        }

    }

    @Nested
    inner class IsOwner {

        @Test
        fun `when owner of guild`() = runTest {
            val owner = getRandomString()
            val guild = service.createGuild(getRandomString(), owner)
            assertTrue { service.isOwner(guild.id, owner) }
        }

        @Test
        fun `when not owner of guild`() = runTest {
            val owner = getRandomString()
            val guild = service.createGuild(getRandomString(), owner)
            assertFalse { service.isOwner(guild.id, getRandomString()) }
        }

        @Test
        fun `when guild does not exist`() = runTest {
            assertFalse { service.isOwner(0, getRandomString()) }
        }

    }

    @Nested
    inner class AddMember {

        @Nested
        inner class CreateDate {

            @Test
            fun `define using current time`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                val entityId = getRandomString()
                val now = Instant.now()
                assertTrue { service.addMember(guild.id, entityId) }

                val members = getAllMembers()
                assertEquals(2, members.size)

                val member = members[1]
                assertEquals(guild.id, member.id.guildId)
                assertEquals(entityId, member.id.entityId)
                assertEquals(now.truncatedTo(ChronoUnit.MINUTES), member.createdAt.truncatedTo(ChronoUnit.MINUTES))
            }

            @ParameterizedTest
            @EnumSource(value = ChronoUnit::class, names = ["DAYS", "HOURS", "MINUTES"])
            fun `should use clock provider of database`(chronoUnit: ChronoUnit) = runTest {
                database = R2dbcDatabase(createConnectionOptions(psqlContainer), clockProvider = {
                    val instant = Instant.now().truncatedTo(chronoUnit)
                    Clock.fixed(instant, ZoneId.systemDefault())
                })
                service = GuildDatabaseService(database)

                val guild = service.createGuild(getRandomString(), getRandomString())
                val entityId = getRandomString()
                val now = Instant.now()
                assertTrue { service.addMember(guild.id, entityId) }

                val members = getAllMembers()
                assertEquals(2, members.size)

                val member = members[1]
                assertEquals(guild.id, member.id.guildId)
                assertEquals(entityId, member.id.entityId)

                assertEquals(now.truncatedTo(chronoUnit), member.createdAt)
            }
        }

        @Test
        fun `when entity is not a member of the guild`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val entityId = getRandomString()
            assertTrue { service.addMember(guild.id, entityId) }

            val members = service.getMembers(guild.id).toList()
            assertEquals(2, members.size)
            assertEquals(entityId, members[1])
        }

        @Test
        fun `when entity is already in the guild`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val entityId = getRandomString()
            assertTrue { service.addMember(guild.id, entityId) }
            assertFalse { service.addMember(guild.id, entityId) }

            val members = service.getMembers(guild.id).toList()
            assertEquals(2, members.size)
            assertEquals(entityId, members[1])
        }

        @Test
        fun `when entity is owner of the guild`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            assertFalse { service.addMember(guild.id, guild.ownerId) }

            val members = service.getMembers(guild.id).toList()
            assertEquals(1, members.size)
        }

        @Test
        fun `when entity has invitation to join the guild`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val entityId = getRandomString()
            assertTrue { service.addInvitation(guild.id, entityId, null) }
            assertTrue { service.addMember(guild.id, entityId) }

            val members = service.getMembers(guild.id).toList()
            assertEquals(2, members.size)
            assertEquals(entityId, members[1])
            val pendingMembers = service.getInvited(guild.id).toList()
            assertEquals(0, pendingMembers.size)
        }

        @Test
        fun `when guild does not exist`() = runTest {
            assertThrows<R2dbcDataIntegrityViolationException> {
                service.addMember(0, getRandomString())
            }

            val guilds = getAllGuilds()
            assertEquals(0, guilds.size)
        }

    }

    @Nested
    inner class AddInvite {

        @Nested
        inner class CreateDate {

            @Test
            fun `define using current time`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                val entityId = getRandomString()
                val now = Instant.now()
                assertTrue { service.addInvitation(guild.id, entityId, null) }

                val invites = getAllInvites()
                assertEquals(1, invites.size)

                val invite = invites.single()
                assertEquals(guild.id, invite.id.guildId)
                assertEquals(entityId, invite.id.entityId)
                assertEquals(now.truncatedTo(ChronoUnit.MINUTES), invite.createdAt.truncatedTo(ChronoUnit.MINUTES))
            }

            @ParameterizedTest
            @EnumSource(value = ChronoUnit::class, names = ["DAYS", "HOURS", "MINUTES"])
            fun `should use clock provider of database`(chronoUnit: ChronoUnit) = runTest {
                database = R2dbcDatabase(createConnectionOptions(psqlContainer), clockProvider = {
                    val instant = Instant.now().truncatedTo(chronoUnit)
                    Clock.fixed(instant, ZoneId.systemDefault())
                })
                service = GuildDatabaseService(database)

                val guild = service.createGuild(getRandomString(), getRandomString())
                val entityId = getRandomString()
                val now = Instant.now()
                assertTrue { service.addInvitation(guild.id, entityId, null) }

                val invites = getAllInvites()
                assertEquals(1, invites.size)

                val invite = invites.single()
                assertEquals(guild.id, invite.id.guildId)
                assertEquals(entityId, invite.id.entityId)

                assertEquals(now.truncatedTo(chronoUnit), invite.createdAt)
            }
        }

        @Nested
        inner class ExpirationDate {

            @Test
            fun `with field`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                val entityId = getRandomString()
                val now = Instant.now()
                val expirationDate = now.plusSeconds(10)
                assertTrue { service.addInvitation(guild.id, entityId, expirationDate) }

                val invites = getAllInvites()
                assertEquals(1, invites.size)

                val invite = invites.single()
                assertEquals(guild.id, invite.id.guildId)
                assertEquals(entityId, invite.id.entityId)
                assertEquals(now.truncatedTo(ChronoUnit.MINUTES), invite.createdAt.truncatedTo(ChronoUnit.MINUTES))
                assertEquals(
                    expirationDate.truncatedTo(ChronoUnit.SECONDS),
                    invite.expiredAt!!.truncatedTo(ChronoUnit.SECONDS)
                )
            }

            @Test
            fun `without field`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                val entityId = getRandomString()
                val now = Instant.now()
                assertTrue { service.addInvitation(guild.id, entityId, null) }

                val invites = getAllInvites()
                assertEquals(1, invites.size)

                val invite = invites.single()
                assertEquals(guild.id, invite.id.guildId)
                assertEquals(entityId, invite.id.entityId)
                assertEquals(now.truncatedTo(ChronoUnit.MINUTES), invite.createdAt.truncatedTo(ChronoUnit.MINUTES))
                assertNull(invite.expiredAt)
            }

        }

        @Test
        fun `when entity is not invited in a guild`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val entityId = getRandomString()
            assertTrue { service.addInvitation(guild.id, entityId, null) }

            val invited = service.getInvited(guild.id).toList()
            assertEquals(1, invited.size)
            assertEquals(entityId, invited[0])
        }

        @Test
        fun `when entity is already a member of the guild`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val entityId = getRandomString()
            assertTrue { service.addMember(guild.id, entityId) }
            assertThrows<GuildInvitedIsAlreadyMemberException> {
                service.addInvitation(guild.id, entityId, null)
            }

            val invited = service.getInvited(guild.id).toList()
            assertEquals(0, invited.size)
        }

        @Test
        fun `when entity is invited in another guild`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val guild2 = service.createGuild(getRandomString(), getRandomString())

            val entityId = getRandomString()
            assertTrue { service.addInvitation(guild.id, entityId, null) }
            assertTrue { service.addInvitation(guild2.id, entityId, null) }

            assertEquals(entityId, service.getInvited(guild.id).toList().single())
            assertEquals(entityId, service.getInvited(guild2.id).toList().single())
        }

        @Test
        fun `when entity is owner of a guild`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            assertThrows<GuildInvitedIsAlreadyMemberException> {
                service.addInvitation(guild.id, guild.ownerId, null)
            }

            val invited = service.getInvited(guild.id).toList()
            assertEquals(0, invited.size)
        }

        @Test
        fun `when entity is already invited`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val entityId = getRandomString()
            assertTrue { service.addInvitation(guild.id, entityId, null) }
            assertFalse { service.addInvitation(guild.id, entityId, null) }

            val invited = service.getInvited(guild.id).toList()
            assertEquals(1, invited.size)
            assertEquals(entityId, invited[0])
        }

        @Test
        fun `when guild does not exist`() = runTest {
            assertThrows<R2dbcDataIntegrityViolationException> {
                service.addInvitation(0, getRandomString(), null)
            }

            val guilds = getAllGuilds()
            assertEquals(0, guilds.size)
        }
    }

    private suspend fun getAllGuilds(): List<Guild> {
        return DatabaseUtils.getAll(database, _Guild.guild)
    }

    private suspend fun getAllMembers(): List<GuildMember> {
        return DatabaseUtils.getAll(database, _GuildMember.guildMember)
    }

    private suspend fun getAllInvites(): List<GuildInvite> {
        return DatabaseUtils.getAll(database, _GuildInvite.guildInvite)
    }
}