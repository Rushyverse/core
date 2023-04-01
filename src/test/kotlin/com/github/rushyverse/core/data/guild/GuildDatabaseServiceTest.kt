package com.github.rushyverse.core.data.guild

import com.github.rushyverse.core.container.createPSQLContainer
import com.github.rushyverse.core.data.*
import com.github.rushyverse.core.data.utils.DatabaseUtils
import com.github.rushyverse.core.data.utils.DatabaseUtils.createConnectionOptions
import com.github.rushyverse.core.data.utils.MicroClockProvider
import com.github.rushyverse.core.utils.getRandomString
import io.r2dbc.spi.R2dbcDataIntegrityViolationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.junit.jupiter.params.provider.ValueSource
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

        @Nested
        inner class Owner {

            @Test
            fun `when owner is not owner of a guild`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                val guilds = getAllGuilds()
                assertThat(guilds).containsExactly(guild)
            }

            @Test
            fun `when owner is already in a guild`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                val guild2 = service.createGuild(getRandomString(), guild.ownerId)
                assertNotEquals(guild.id, guild2.id)

                val guilds = getAllGuilds()
                assertThat(guilds).containsExactly(guild, guild2)
            }

            @Test
            fun `when owner is a member of another guild`() = runTest {
                val entity = getRandomString()
                val guild = service.createGuild(getRandomString(), getRandomString())
                service.addMember(guild.id, entity)

                val guild2 = service.createGuild(getRandomString(), entity)

                val guilds = getAllGuilds()
                assertThat(guilds).containsExactly(guild, guild2)
            }

            @ParameterizedTest
            @ValueSource(strings = ["", " ", "  ", "   "])
            fun `when owner is blank`(owner: String) = runTest {
                assertThrows<IllegalArgumentException> {
                    service.createGuild(getRandomString(), owner)
                }
            }
        }

        @Nested
        inner class Name {

            @Test
            fun `with a name that is already taken`() = runTest {
                val name = getRandomString()
                val guild = service.createGuild(name, getRandomString())
                val guild2 = service.createGuild(name, getRandomString())
                assertNotEquals(guild.id, guild2.id)

                val guilds = getAllGuilds()
                assertThat(guilds).containsExactly(guild, guild2)
            }

            @ParameterizedTest
            @ValueSource(strings = ["", " ", "  ", "   "])
            fun `when name is blank`(name: String) = runTest {
                assertThrows<IllegalArgumentException> {
                    service.createGuild(name, getRandomString())
                }
            }
        }

    }

    @Nested
    inner class DeleteGuild {

        @Test
        fun `when guild exists`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            assertTrue { service.deleteGuild(guild.id) }
            assertThat(getAllGuilds()).isEmpty()
        }

        @Test
        fun `when guild does not exist`() = runTest {
            assertFalse { service.deleteGuild(0) }
            assertThat(getAllGuilds()).isEmpty()
        }

        @Test
        fun `when another guild is deleted`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val guild2 = service.createGuild(getRandomString(), getRandomString())

            assertTrue { service.deleteGuild(guild.id) }
            assertThat(getAllGuilds()).containsExactly(guild2)

            assertTrue { service.deleteGuild(guild2.id) }
            assertThat(getAllGuilds()).isEmpty()
        }

        @Test
        fun `when guild is already deleted`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            assertTrue { service.deleteGuild(guild.id) }
            assertFalse { service.deleteGuild(guild.id) }
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
        fun `when several guilds exist`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val guild2 = service.createGuild(getRandomString(), getRandomString())
            assertEquals(guild, service.getGuild(guild.id))
            assertEquals(guild2, service.getGuild(guild2.id))
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
            val guild = service.createGuild(name, getRandomString())
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

        @ParameterizedTest
        @ValueSource(strings = ["", " ", "  ", "   "])
        fun `when name is blank`(name: String) = runTest {
            assertThrows<IllegalArgumentException> {
                service.getGuild(name)
            }
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
        fun `when owner of several guilds`() = runTest {
            val owner = getRandomString()
            val guild = service.createGuild(getRandomString(), owner)
            val guild2 = service.createGuild(getRandomString(), owner)
            assertTrue { service.isOwner(guild.id, owner) }
            assertTrue { service.isOwner(guild2.id, owner) }
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

        @ParameterizedTest
        @ValueSource(strings = ["", " ", "  ", "   "])
        fun `when entity id is blank`(id: String) = runTest {
            assertThrows<IllegalArgumentException> {
                service.isOwner(0, id)
            }
        }

    }

    @Nested
    inner class IsMember {

        @Test
        fun `when entity is owner of the guild`() = runTest {
            val owner = getRandomString()
            val guild = service.createGuild(getRandomString(), owner)
            assertTrue { service.isMember(guild.id, owner) }
        }

        @Test
        fun `when entity is a member of the guild`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val entityId = getRandomString()
            service.addMember(guild.id, entityId)
            assertTrue { service.isMember(guild.id, entityId) }
        }

        @Test
        fun `when entity is not a member of the guild`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val entityId = getRandomString()
            assertFalse { service.isMember(guild.id, entityId) }
        }

        @Test
        fun `when entity is invited in the guild`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val entityId = getRandomString()
            service.addInvitation(guild.id, entityId, null)
            assertFalse { service.isMember(guild.id, entityId) }
        }

        @Test
        fun `when entity is member of another guild`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val guild2 = service.createGuild(getRandomString(), getRandomString())
            val entityId = getRandomString()
            service.addMember(guild.id, entityId)
            assertTrue { service.isMember(guild.id, entityId) }
            assertFalse { service.isMember(guild2.id, entityId) }
        }

        @Test
        fun `when another entity is member of the guild`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val entityId = getRandomString()
            val entity2Id = getRandomString()
            service.addMember(guild.id, entity2Id)
            assertFalse { service.isMember(guild.id, entityId) }
        }

        @Test
        fun `when guild does not exist`() = runTest {
            assertFalse { service.isMember(0, getRandomString()) }
        }

        @ParameterizedTest
        @ValueSource(strings = ["", " ", "  ", "   "])
        fun `when entity id is blank`(id: String) = runTest {
            assertThrows<IllegalArgumentException> {
                service.isMember(0, id)
            }
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
                assertEquals(1, members.size)

                val member = members.single()
                assertEquals(guild.id, member.guildId)
                assertEquals(entityId, member.entityId)
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
                assertEquals(1, members.size)

                val member = members.single()
                assertEquals(guild.id, member.guildId)
                assertEquals(entityId, member.entityId)

                assertEquals(now.truncatedTo(chronoUnit), member.createdAt)
            }
        }

        @Test
        fun `when entity is not a member of the guild`() = runTest {
            val owner = getRandomString()
            val guild = service.createGuild(getRandomString(), owner)
            val entityId = getRandomString()
            assertTrue { service.addMember(guild.id, entityId) }

            val members = service.getMembers(guild.id).toList()
            assertThat(members).containsExactlyInAnyOrder(owner, entityId)
        }

        @Test
        fun `when entity is already in the guild`() = runTest {
            val owner = getRandomString()
            val guild = service.createGuild(getRandomString(), owner)
            val entityId = getRandomString()
            assertTrue { service.addMember(guild.id, entityId) }
            assertFalse { service.addMember(guild.id, entityId) }

            val members = service.getMembers(guild.id).toList()
            assertThat(members).containsExactlyInAnyOrder(owner, entityId)
        }

        @Test
        fun `when entity is owner of the guild`() = runTest {
            val owner = getRandomString()
            val guild = service.createGuild(getRandomString(), owner)
            assertThrows<GuildMemberIsOwnerOfGuildException> {
                service.addMember(guild.id, owner)
            }

            val members = service.getMembers(guild.id).toList()
            assertThat(members).containsExactlyInAnyOrder(owner)
        }

        @Test
        fun `when entity has invitation to join the guild`() = runTest {
            val owner = getRandomString()
            val guild = service.createGuild(getRandomString(), owner)
            val entityId = getRandomString()
            assertTrue { service.addInvitation(guild.id, entityId, null) }
            assertTrue { service.addMember(guild.id, entityId) }

            val members = service.getMembers(guild.id).toList()
            assertThat(members).containsExactlyInAnyOrder(owner, entityId)

            val pendingMembers = service.getInvited(guild.id).toList()
            assertThat(pendingMembers).isEmpty()
        }

        @Test
        fun `when guild does not exist`() = runTest {
            assertThrows<GuildNotFoundException> {
                service.addMember(0, getRandomString())
            }

            val guilds = getAllGuilds()
            assertEquals(0, guilds.size)
        }

        @ParameterizedTest
        @ValueSource(strings = ["", " ", "  ", "   "])
        fun `when entity id is blank`(id: String) = runTest {
            assertThrows<IllegalArgumentException> {
                service.addMember(0, id)
            }
        }

    }

    @Nested
    inner class AddInvitation {


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
                assertEquals(guild.id, invite.guildId)
                assertEquals(entityId, invite.entityId)
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
                assertEquals(guild.id, invite.guildId)
                assertEquals(entityId, invite.entityId)

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
                assertEquals(guild.id, invite.guildId)
                assertEquals(entityId, invite.entityId)
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
                assertEquals(guild.id, invite.guildId)
                assertEquals(entityId, invite.entityId)
                assertEquals(now.truncatedTo(ChronoUnit.MINUTES), invite.createdAt.truncatedTo(ChronoUnit.MINUTES))
                assertNull(invite.expiredAt)
            }

            @Test
            fun `when date is before now`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                val entityId = getRandomString()
                val expiredAt = Instant.now().minusMillis(1)

                assertThrows<IllegalArgumentException> {
                    service.addInvitation(guild.id, entityId, expiredAt)
                }
            }
        }

        @Test
        fun `when entity is not invited in a guild`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val entityId = getRandomString()
            assertTrue { service.addInvitation(guild.id, entityId, null) }

            val invited = service.getInvited(guild.id).toList()
            assertContentEquals(listOf(entityId), invited)
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
            val expiredAt = Instant.now().plusSeconds(10)
            assertTrue { service.addInvitation(guild.id, entityId, null) }
            assertFalse { service.addInvitation(guild.id, entityId, null) }
            assertTrue { service.addInvitation(guild.id, entityId, expiredAt) }

            val invites = getAllInvites()
            val invite = invites.single()
            assertEquals(guild.id, invite.guildId)
            assertEquals(entityId, invite.entityId)
            assertEquals(expiredAt.truncatedTo(ChronoUnit.SECONDS), invite.expiredAt!!.truncatedTo(ChronoUnit.SECONDS))
        }

        @Test
        fun `when entity is already invited with another entity invited`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val entityId = getRandomString()
            val entityId2 = getRandomString()
            val expiredAt = Instant.now().plusSeconds(10)

            assertTrue { service.addInvitation(guild.id, entityId, null) }
            assertTrue { service.addInvitation(guild.id, entityId2, null) }

            assertFalse { service.addInvitation(guild.id, entityId, null) }

            assertTrue { service.addInvitation(guild.id, entityId, expiredAt) }

            val invites = getAllInvites()
            assertEquals(2, invites.size)

            val inviteForEntity1 = invites.first { it.entityId == entityId }
            assertEquals(guild.id, inviteForEntity1.guildId)
            assertEquals(entityId, inviteForEntity1.entityId)
            assertEquals(
                expiredAt.truncatedTo(ChronoUnit.SECONDS),
                inviteForEntity1.expiredAt!!.truncatedTo(ChronoUnit.SECONDS)
            )

            val inviteForEntity2 = invites.first { it.entityId == entityId2 }
            assertEquals(guild.id, inviteForEntity2.guildId)
            assertEquals(entityId2, inviteForEntity2.entityId)
            assertNull(inviteForEntity2.expiredAt)
        }

        @Test
        fun `when guild does not exist`() = runTest {
            assertThrows<GuildNotFoundException> {
                service.addInvitation(0, getRandomString(), null)
            }

            val guilds = getAllGuilds()
            assertEquals(0, guilds.size)
        }

        @ParameterizedTest
        @ValueSource(strings = ["", " ", "  ", "   "])
        fun `when entity id is blank`(id: String) = runTest {
            assertThrows<IllegalArgumentException> {
                service.addInvitation(0, id, null)
            }
        }
    }

    @Nested
    inner class HasInvitation {

        @Test
        fun `when entity is invited in the guild`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val entityId = getRandomString()
            service.addInvitation(guild.id, entityId, null)
            assertTrue { service.hasInvitation(guild.id, entityId) }
        }

        @Test
        fun `when entity is not invited in the guild`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val entityId = getRandomString()
            assertFalse { service.hasInvitation(guild.id, entityId) }
        }

        @Test
        fun `when invitation is expired`() = runBlocking {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val entityId = getRandomString()
            val entityId2 = getRandomString()

            val millis = 500L

            val expiredAt = Instant.now().plusMillis(millis)
            service.addInvitation(guild.id, entityId, expiredAt)
            delay(millis)
            // Invitation is not deleted without an update on the invitation table
            assertTrue { service.hasInvitation(guild.id, entityId) }

            service.addInvitation(guild.id, entityId2, null)
            // Invitation is deleted due to an insert on the invitation table
            assertFalse { service.hasInvitation(guild.id, entityId) }
        }

        @Test
        fun `when guild does not exist`() = runTest {
            assertFalse { service.hasInvitation(0, getRandomString()) }
        }

        @ParameterizedTest
        @ValueSource(strings = ["", " ", "  ", "   "])
        fun `when entity id is blank`(id: String) = runTest {
            assertThrows<IllegalArgumentException> {
                service.hasInvitation(0, id)
            }
        }
    }

    @Nested
    inner class RemoveMember {

        @Test
        fun `when entity is a member of the guild`() = runTest {
            val owner = getRandomString()
            val guild = service.createGuild(getRandomString(), owner)
            val entityId = getRandomString()
            service.addMember(guild.id, entityId)
            assertTrue { service.removeMember(guild.id, entityId) }

            val members = service.getMembers(guild.id).toList()
            assertContentEquals(listOf(owner), members)
        }

        @Test
        fun `when entity is not a member of the guild`() = runTest {
            val owner = getRandomString()
            val guild = service.createGuild(getRandomString(), owner)
            val entityId = getRandomString()
            assertFalse { service.removeMember(guild.id, entityId) }

            val members = service.getMembers(guild.id).toList()
            assertContentEquals(listOf(owner), members)
        }

        @Test
        fun `when another entity is a member of the guild`() = runTest {
            val owner = getRandomString()
            val guild = service.createGuild(getRandomString(), owner)
            val entityId = getRandomString()
            service.addMember(guild.id, entityId)

            val entityId2 = getRandomString()
            assertFalse { service.removeMember(guild.id, entityId2) }

            val members = service.getMembers(guild.id).toList()
            assertThat(members).containsExactlyInAnyOrder(owner, entityId)
        }

        @Test
        fun `when guild does not exist`() = runTest {
            assertFalse { service.removeMember(0, getRandomString()) }
        }

        @ParameterizedTest
        @ValueSource(strings = ["", " ", "  ", "   "])
        fun `when entity id is blank`(id: String) = runTest {
            assertThrows<IllegalArgumentException> {
                service.removeMember(0, id)
            }
        }

        @Test
        fun `when entity is owner of the guild`() = runTest {
            val owner = getRandomString()
            val guild = service.createGuild(getRandomString(), owner)
            assertFalse { service.removeMember(guild.id, owner) }

            val members = service.getMembers(guild.id).toList()
            assertContentEquals(listOf(owner), members)
        }
    }

    @Nested
    inner class RemoveInvitation {

        @Test
        fun `when entity is invited in the guild`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val entityId = getRandomString()
            service.addInvitation(guild.id, entityId, null)
            assertTrue { service.removeInvitation(guild.id, entityId) }

            val invites = service.getInvited(guild.id).toList()
            assertContentEquals(emptyList(), invites)
        }

        @Test
        fun `when entity is not invited in the guild`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val entityId = getRandomString()
            assertFalse { service.removeInvitation(guild.id, entityId) }

            val invites = service.getInvited(guild.id).toList()
            assertContentEquals(emptyList(), invites)
        }

        @Test
        fun `when another entity is invited in the guild`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val entityId = getRandomString()
            val entityId2 = getRandomString()
            service.addInvitation(guild.id, entityId, null)
            assertFalse { service.removeInvitation(guild.id, entityId2) }

            val invites = service.getInvited(guild.id).toList()
            assertContentEquals(listOf(entityId), invites)
        }

        @Test
        fun `when guild does not exist`() = runTest {
            assertFalse { service.removeInvitation(0, getRandomString()) }
        }

        @Test
        fun `when entity is owner of the guild`() = runTest {
            val owner = getRandomString()
            val guild = service.createGuild(getRandomString(), owner)
            assertFalse { service.removeInvitation(guild.id, owner) }
        }

        @ParameterizedTest
        @ValueSource(strings = ["", " ", "  ", "   "])
        fun `when entity id is blank`(id: String) = runTest {
            assertThrows<IllegalArgumentException> {
                service.removeInvitation(0, id)
            }
        }
    }

    @Nested
    inner class GetMembers {

        @Test
        fun `when guild has no members except the owner`() = runTest {
            val owner = getRandomString()
            val guild = service.createGuild(getRandomString(), owner)
            val members = service.getMembers(guild.id).toList()
            assertContentEquals(listOf(owner), members)
        }

        @Test
        fun `when guild has members`() = runTest {
            val owner = getRandomString()
            val guild = service.createGuild(getRandomString(), owner)
            val membersToAdd = listOf(getRandomString(), getRandomString())
            membersToAdd.forEach { service.addMember(guild.id, it) }

            val members = service.getMembers(guild.id).toList()
            assertThat(members).containsExactlyInAnyOrderElementsOf(membersToAdd + owner)
        }

        @Test
        fun `when an entity is invited but not member`() = runTest {
            val owner = getRandomString()
            val guild = service.createGuild(getRandomString(), owner)
            service.addInvitation(guild.id, getRandomString(), null)

            val members = service.getMembers(guild.id).toList()
            assertThat(members).containsExactlyInAnyOrder(owner)
        }

        @Test
        fun `when guild does not exist`() = runTest {
            val members = service.getMembers(0).toList()
            assertThat(members).isEmpty()
        }
    }

    @Nested
    inner class GetInvited {

        @Test
        fun `when guild has no invitations`() = runTest {
            val owner = getRandomString()
            val guild = service.createGuild(getRandomString(), owner)
            val members = service.getInvited(guild.id).toList()
            assertContentEquals(emptyList(), members)
        }

        @Test
        fun `when guild has invitations`() = runTest {
            val owner = getRandomString()
            val guild = service.createGuild(getRandomString(), owner)
            val membersToAdd = listOf(getRandomString(), getRandomString())
            membersToAdd.forEach { service.addInvitation(guild.id, it, null) }

            val members = service.getInvited(guild.id).toList()
            assertContentEquals(membersToAdd, members)
        }

        @Test
        fun `when an entity is member but not invited`() = runTest {
            val owner = getRandomString()
            val guild = service.createGuild(getRandomString(), owner)
            service.addMember(guild.id, getRandomString())

            val members = service.getInvited(guild.id).toList()
            assertContentEquals(emptyList(), members)
        }

        @Test
        fun `when guild does not exist`() = runTest {
            val members = service.getInvited(0).toList()
            assertContentEquals(emptyList(), members)
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