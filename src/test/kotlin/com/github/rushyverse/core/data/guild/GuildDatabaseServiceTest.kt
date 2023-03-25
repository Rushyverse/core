package com.github.rushyverse.core.data.guild

import com.github.rushyverse.core.container.createPSQLContainer
import com.github.rushyverse.core.data.*
import com.github.rushyverse.core.data.utils.DatabaseUtils
import com.github.rushyverse.core.data.utils.DatabaseUtils.createConnectionOptions
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
import java.util.*
import kotlin.test.*

@Testcontainers
class GuildDatabaseServiceTest {

    companion object {
        @JvmStatic
        @Container
        private val psqlContainer = createPSQLContainer()
    }

    private lateinit var service: GuildDatabaseService
    private lateinit var database: R2dbcDatabase

    @BeforeTest
    fun onBefore() {
        database = R2dbcDatabase(createConnectionOptions(psqlContainer))
        service = GuildDatabaseService(database)
        // Create tables from guild schema
    }

    @AfterEach
    fun onAfter() = runBlocking {
        database.runQuery(QueryDsl.drop(_GuildInvite.guildInvite))
        database.runQuery(QueryDsl.drop(_GuildMemberDef.guildMemberDef))
        database.runQuery(QueryDsl.drop(_Guild.guild))
    }

    @Nested
    inner class CreateGuild {

        @Test
        fun `when owner is not owner of a guild`() = runTest {
            val guild = service.createGuild(getRandomString(), UUID.randomUUID())
            val guilds = getAll()
            assertEquals(1, guilds.size)
            assertEquals(guild, guilds[0])
        }

        @Test
        fun `when owner is always in a guild`() = runTest {
            val guild = service.createGuild(getRandomString(), UUID.randomUUID())
            val guild2 = service.createGuild(getRandomString(), guild.owner)
            assertNotEquals(guild.id, guild2.id)

            val guilds = getAll()
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

            val guilds = getAll()
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

            val guilds = getAll()
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
            val guilds = getAll()
            assertEquals(0, guilds.size)
        }

        @Test
        fun `when guild does not exist`() = runTest {
            assertFalse { service.deleteGuild(0) }
            val guilds = getAll()
            assertEquals(0, guilds.size)
        }

    }

    @Nested
    inner class GetGuildById {

        @Test
        fun `when guild exists`() = runTest {
            service.createGuild(getRandomString(), UUID.randomUUID())
            val guilds = getAll()
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
            val guilds = getAll()
            assertEquals(1, guilds.size)
            val guild = guilds[0]
            val retrievedGuild = service.getGuild(name)
            assertEquals(guild, retrievedGuild)
        }

        @Test
        fun `when guild does not exist`() = runTest {
            val guild = service.getGuild(getRandomString())
            assertNull(guild)
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
            assertTrue { service.addPendingMember(guild.id, member) }
            assertTrue { service.addMember(guild.id, member) }

            val members = service.getMembers(guild.id).toList()
            assertEquals(2, members.size)
            assertEquals(member, members[1])
            val pendingMembers = service.getPendingMembers(guild.id).toList()
            assertEquals(0, pendingMembers.size)
        }

        @Test
        fun `when guild does not exist`() = runTest {
            assertFalse { service.addMember(0, UUID.randomUUID()) }
            val guilds = getAll()
            assertEquals(0, guilds.size)
        }

    }

    private suspend fun getAll(): List<Guild> {
        return DatabaseUtils.getAll(database, _Guild.guild)
    }
}