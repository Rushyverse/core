package com.github.rushyverse.core.data.guild

import com.github.rushyverse.core.container.createPSQLContainer
import com.github.rushyverse.core.data.*
import com.github.rushyverse.core.data.utils.DatabaseUtils
import com.github.rushyverse.core.data.utils.DatabaseUtils.createConnectionOptions
import com.github.rushyverse.core.data.utils.MicroClockProvider
import com.github.rushyverse.core.utils.getRandomString
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Nested
import org.komapper.core.dsl.QueryDsl
import org.komapper.r2dbc.R2dbcDatabase
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.MountableFile
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.*
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
            val guild = service.createGuild(getRandomString(), UUID.randomUUID())
            val guilds = getAllGuilds()
            assertEquals(1, guilds.size)
            assertEquals(guild, guilds[0])
        }

        @Test
        fun `when owner is always in a guild`() = runTest {
            val guild = service.createGuild(getRandomString(), UUID.randomUUID())
            val guild2 = service.createGuild(getRandomString(), guild.owner)
            assertNotEquals(guild.id, guild2.id)

            val guilds = getAllGuilds()
            assertEquals(2, guilds.size)
            assertEquals(guild, guilds[0])
            assertEquals(guild2, guilds[1])
        }

        @Test
        fun `when member of another guild`() = runTest {
            val member = UUID.randomUUID()
            val guild = service.createGuild(getRandomString(), UUID.randomUUID())
            service.addMember(guild.id, member)

            val guild2 = service.createGuild(getRandomString(), member)

            val guilds = getAllGuilds()
            assertEquals(2, guilds.size)
            assertEquals(guild, guilds[0])
            assertEquals(guild2, guilds[1])
        }

        @Test
        fun `with a name that is already taken`() = runTest {
            val name = getRandomString()
            val guild = service.createGuild(name, UUID.randomUUID())
            val guild2 = service.createGuild(name, UUID.randomUUID())
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
            val guild = service.createGuild(getRandomString(), UUID.randomUUID())
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
            service.createGuild(getRandomString(), UUID.randomUUID())
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
            service.createGuild(name, UUID.randomUUID())
            val guilds = getAllGuilds()
            assertEquals(1, guilds.size)
            val guild = guilds[0]
            val (retrievedGuild) = service.getGuild(name).toList()
            assertEquals(guild, retrievedGuild)
        }

        @Test
        fun `when several guild with the same name exist`() = runTest {
            val name = getRandomString()
            service.createGuild(name, UUID.randomUUID())
            service.createGuild(name, UUID.randomUUID())

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
            val owner = UUID.randomUUID()
            val guild = service.createGuild(getRandomString(), owner)
            assertTrue { service.isOwner(guild.id, owner) }
        }

        @Test
        fun `when not owner of guild`() = runTest {
            val owner = UUID.randomUUID()
            val guild = service.createGuild(getRandomString(), owner)
            assertFalse { service.isOwner(guild.id, UUID.randomUUID()) }
        }

        @Test
        fun `when guild does not exist`() = runTest {
            assertFalse { service.isOwner(0, UUID.randomUUID()) }
        }

    }

    @Nested
    inner class AddMember {

        @Nested
        inner class CreateDate {

            @Test
            fun `define using current time`() = runTest {
                val guild = service.createGuild(getRandomString(), UUID.randomUUID())
                val memberId = UUID.randomUUID()
                val now = Instant.now()
                assertTrue { service.addMember(guild.id, memberId) }

                val members = getAllMembers()
                assertEquals(2, members.size)

                val member = members[1]
                assertEquals(guild.id, member.id.guildId)
                assertEquals(memberId, member.id.memberId)
                assertEquals(now.truncatedTo(ChronoUnit.MINUTES), member.createdAt.truncatedTo(ChronoUnit.MINUTES))
            }
        }

        @Test
        fun `when member is not in a guild`() = runTest {
            val guild = service.createGuild(getRandomString(), UUID.randomUUID())
            val member = UUID.randomUUID()
            assertTrue { service.addMember(guild.id, member) }

            val members = service.getMembers(guild.id).toList()
            assertEquals(2, members.size)
            assertEquals(member, members[1])
        }

        @Test
        fun `when member is already in a guild`() = runTest {
            val guild = service.createGuild(getRandomString(), UUID.randomUUID())
            val member = UUID.randomUUID()
            assertTrue { service.addMember(guild.id, member) }
            assertFalse { service.addMember(guild.id, member) }

            val members = service.getMembers(guild.id).toList()
            assertEquals(2, members.size)
            assertEquals(member, members[1])
        }

        @Test
        fun `when member is owner of a guild`() = runTest {
            val guild = service.createGuild(getRandomString(), UUID.randomUUID())
            assertFalse { service.addMember(guild.id, guild.owner) }

            val members = service.getMembers(guild.id).toList()
            assertEquals(1, members.size)
        }

        @Test
        fun `when member was a pending member`() = runTest {
            val guild = service.createGuild(getRandomString(), UUID.randomUUID())
            val member = UUID.randomUUID()
            assertTrue { service.addInvite(guild.id, member, null) }
            assertTrue { service.addMember(guild.id, member) }

            val members = service.getMembers(guild.id).toList()
            assertEquals(2, members.size)
            assertEquals(member, members[1])
            val pendingMembers = service.getInvited(guild.id).toList()
            assertEquals(0, pendingMembers.size)
        }

        @Test
        fun `when guild does not exist`() = runTest {
            assertFalse { service.addMember(0, UUID.randomUUID()) }
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
                val guild = service.createGuild(getRandomString(), UUID.randomUUID())
                val member = UUID.randomUUID()
                val now = Instant.now()
                assertTrue { service.addInvite(guild.id, member, null) }

                val invites = getAllInvites()
                assertEquals(1, invites.size)

                val invite = invites.single()
                assertEquals(guild.id, invite.id.guildId)
                assertEquals(member, invite.id.memberId)
                assertEquals(now.truncatedTo(ChronoUnit.MINUTES), invite.createdAt.truncatedTo(ChronoUnit.MINUTES))
            }
        }

        @Nested
        inner class ExpirationDate {

            @Test
            fun `with field`() = runTest {
                val guild = service.createGuild(getRandomString(), UUID.randomUUID())
                val member = UUID.randomUUID()
                val now = Instant.now()
                val expirationDate = now.plusSeconds(10)
                assertTrue { service.addInvite(guild.id, member, expirationDate) }

                val invites = getAllInvites()
                assertEquals(1, invites.size)

                val invite = invites.single()
                assertEquals(guild.id, invite.id.guildId)
                assertEquals(member, invite.id.memberId)
                assertEquals(now.truncatedTo(ChronoUnit.MINUTES), invite.createdAt.truncatedTo(ChronoUnit.MINUTES))
                assertEquals(
                    expirationDate.truncatedTo(ChronoUnit.SECONDS),
                    invite.expiredAt!!.truncatedTo(ChronoUnit.SECONDS)
                )
            }

            @Test
            fun `without field`() = runTest {
                val guild = service.createGuild(getRandomString(), UUID.randomUUID())
                val member = UUID.randomUUID()
                val now = Instant.now()
                assertTrue { service.addInvite(guild.id, member, null) }

                val invites = getAllInvites()
                assertEquals(1, invites.size)

                val invite = invites.single()
                assertEquals(guild.id, invite.id.guildId)
                assertEquals(member, invite.id.memberId)
                assertEquals(now.truncatedTo(ChronoUnit.MINUTES), invite.createdAt.truncatedTo(ChronoUnit.MINUTES))
                assertNull(invite.expiredAt)
            }

        }

        @Test
        fun `when member is not in a guild`() = runTest {
            val guild = service.createGuild(getRandomString(), UUID.randomUUID())
            val member = UUID.randomUUID()
            assertTrue { service.addInvite(guild.id, member, null) }

            val pendingMembers = service.getInvited(guild.id).toList()
            assertEquals(1, pendingMembers.size)
            assertEquals(member, pendingMembers[0])
        }

        @Test
        fun `when member is already in a guild`() = runTest {
            val guild = service.createGuild(getRandomString(), UUID.randomUUID())
            val member = UUID.randomUUID()
            assertTrue { service.addMember(guild.id, member) }
            assertFalse { service.addInvite(guild.id, member, null) }

            val pendingMembers = service.getInvited(guild.id).toList()
            assertEquals(0, pendingMembers.size)
        }

        @Test
        fun `when member is owner of a guild`() = runTest {
            val guild = service.createGuild(getRandomString(), UUID.randomUUID())
            assertFalse { service.addInvite(guild.id, guild.owner, null) }

            val pendingMembers = service.getInvited(guild.id).toList()
            assertEquals(0, pendingMembers.size)
        }

        @Test
        fun `when member was a pending member`() = runTest {
            val guild = service.createGuild(getRandomString(), UUID.randomUUID())
            val member = UUID.randomUUID()
            assertTrue { service.addInvite(guild.id, member, null) }
            assertFalse { service.addInvite(guild.id, member, null) }

            val pendingMembers = service.getInvited(guild.id).toList()
            assertEquals(1, pendingMembers.size)
            assertEquals(member, pendingMembers[0])
        }

        @Test
        fun `when guild does not exist`() = runTest {
            assertFalse { service.addInvite(0, UUID.randomUUID(), null) }
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