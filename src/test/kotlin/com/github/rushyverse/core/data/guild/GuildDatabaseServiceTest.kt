package com.github.rushyverse.core.data.guild

import com.github.rushyverse.core.container.createPSQLContainer
import com.github.rushyverse.core.data.*
import com.github.rushyverse.core.data.utils.DatabaseUtils
import com.github.rushyverse.core.data.utils.DatabaseUtils.createConnectionOptions
import com.github.rushyverse.core.utils.getRandomString
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Nested
import org.komapper.core.dsl.QueryDsl
import org.komapper.r2dbc.R2dbcDatabase
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.util.*
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals

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
    fun onBefore() = runBlocking {
        database = R2dbcDatabase(createConnectionOptions(psqlContainer))
        Guild.createTable(database)
        GuildMemberDef.createTable(database)
        service = GuildDatabaseService(database)
    }

    @AfterEach
    fun onAfter() = runBlocking {
        database.runQuery(QueryDsl.drop(_Guild.guild))
        database.runQuery(QueryDsl.drop(_GuildMemberDef.guildMemberDef))
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

    }

    private suspend fun getAll(): List<Guild> {
        return DatabaseUtils.getAll(database, _Guild.guild)
    }
}