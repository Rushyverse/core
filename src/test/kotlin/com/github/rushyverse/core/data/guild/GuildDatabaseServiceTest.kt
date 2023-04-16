package com.github.rushyverse.core.data.guild

import com.github.rushyverse.core.container.createPSQLContainer
import com.github.rushyverse.core.data.*
import com.github.rushyverse.core.data.utils.DatabaseUtils
import com.github.rushyverse.core.data.utils.DatabaseUtils.createConnectionOptions
import com.github.rushyverse.core.data.utils.MicroClockProvider
import com.github.rushyverse.core.utils.getRandomString
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.single
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

fun GuildInvite.defaultTime() = copy(
    createdAt = Instant.EPOCH,
    expiredAt = null
)

fun GuildMember.defaultTime() = copy(
    createdAt = Instant.EPOCH
)

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

        private const val WAIT_EXPIRATION_MILLIS = 200L
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
            fun `should create a new guild`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                val guilds = getAllGuilds()
                assertThat(guilds).containsExactly(guild)
            }

            @Test
            fun `should create guild if owner is an owner of another guild`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                val guild2 = service.createGuild(getRandomString(), guild.ownerId)
                assertNotEquals(guild.id, guild2.id)

                val guilds = getAllGuilds()
                assertThat(guilds).containsExactly(guild, guild2)
            }

            @Test
            fun `should create guild if owner is a member of another guild`() = runTest {
                val entity = getRandomString()
                val guild = service.createGuild(getRandomString(), getRandomString())
                service.addMember(guild.id, entity)

                val guild2 = service.createGuild(getRandomString(), entity)

                val guilds = getAllGuilds()
                assertThat(guilds).containsExactly(guild, guild2)
            }

            @ParameterizedTest
            @ValueSource(strings = ["", " ", "  ", "   "])
            fun `should throw if owner is blank`(owner: String) = runTest {
                assertThrows<IllegalArgumentException> {
                    service.createGuild(getRandomString(), owner)
                }
            }
        }

        @Nested
        inner class Name {

            @Test
            fun `should create guilds with the same name`() = runTest {
                val name = getRandomString()
                val guild = service.createGuild(name, getRandomString())
                val guild2 = service.createGuild(name, getRandomString())
                assertNotEquals(guild.id, guild2.id)

                val guilds = getAllGuilds()
                assertThat(guilds).containsExactly(guild, guild2)
            }

            @ParameterizedTest
            @ValueSource(strings = ["", " ", "  ", "   "])
            fun `should throw if name is blank`(name: String) = runTest {
                assertThrows<IllegalArgumentException> {
                    service.createGuild(name, getRandomString())
                }
            }
        }

    }

    @Nested
    inner class DeleteGuild {

        @Test
        fun `should return true if the guild exists`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            assertTrue { service.deleteGuild(guild.id) }
            assertThat(getAllGuilds()).isEmpty()
        }

        @Test
        fun `should return false if the guild doesn't exist`() = runTest {
            assertFalse { service.deleteGuild(0) }
            assertThat(getAllGuilds()).isEmpty()
        }

        @Test
        fun `should delete only the targeted guild`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val guild2 = service.createGuild(getRandomString(), getRandomString())

            assertTrue { service.deleteGuild(guild.id) }
            assertThat(getAllGuilds()).containsExactly(guild2)

            assertTrue { service.deleteGuild(guild2.id) }
            assertThat(getAllGuilds()).isEmpty()
        }

        @Test
        fun `should return false if the guild is deleted`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            assertTrue { service.deleteGuild(guild.id) }
            assertFalse { service.deleteGuild(guild.id) }
        }

        @Test
        fun `should delete all linked data to the deleted guild`() = runTest {
            TODO()
        }

        @Test
        fun `should not delete another guild data`() = runTest {
            TODO()
        }

    }

    @Nested
    inner class GetGuildById {

        @Test
        fun `should return the guild`() = runTest {
            service.createGuild(getRandomString(), getRandomString())
            val guilds = getAllGuilds()
            assertEquals(1, guilds.size)
            val guild = guilds[0]
            val retrievedGuild = service.getGuild(guild.id)
            assertEquals(guild, retrievedGuild)
        }

        @Test
        fun `should return each guild with the corresponding id`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val guild2 = service.createGuild(getRandomString(), getRandomString())
            assertEquals(guild, service.getGuild(guild.id))
            assertEquals(guild2, service.getGuild(guild2.id))
        }

        @Test
        fun `should return null if the guild doesn't exist`() = runTest {
            val guild = service.getGuild(0)
            assertNull(guild)
        }

        @Test
        fun `should return null if the guild is deleted`() = runTest {
            TODO()
        }

    }

    @Nested
    inner class GetGuildByName {

        @Test
        fun `should return the guild with the same name`() = runTest {
            val name = getRandomString()
            val guild = service.createGuild(name, getRandomString())
            val (retrievedGuild) = service.getGuild(name).toList()
            assertEquals(guild, retrievedGuild)
        }

        @Test
        fun `should return empty flow if the guild is deleted`() = runTest {
            TODO()
        }

        @Test
        fun `should retrieve several guilds with the same name`() = runTest {
            val name = getRandomString()
            service.createGuild(name, getRandomString())
            service.createGuild(name, getRandomString())

            val guilds = getAllGuilds()
            assertEquals(2, guilds.size)

            val retrievedGuilds = service.getGuild(name).toList()
            assertEquals(guilds, retrievedGuilds)
        }

        @Test
        fun `should return empty flow when no guild has the name`() = runTest {
            val guild = service.getGuild(getRandomString()).toList()
            assertEquals(0, guild.size)
        }

        @ParameterizedTest
        @ValueSource(strings = ["", " ", "  ", "   "])
        fun `should throw if name is blank`(name: String) = runTest {
            assertThrows<IllegalArgumentException> {
                service.getGuild(name)
            }
        }

    }

    @Nested
    inner class IsOwner {

        @Test
        fun `should return true if the entity is owner of the guild`() = runTest {
            val owner = getRandomString()
            val guild = service.createGuild(getRandomString(), owner)
            assertTrue { service.isOwner(guild.id, owner) }
        }

        @Test
        fun `should return true if entity is owner of several guilds`() = runTest {
            val owner = getRandomString()
            val guild = service.createGuild(getRandomString(), owner)
            val guild2 = service.createGuild(getRandomString(), owner)
            assertTrue { service.isOwner(guild.id, owner) }
            assertTrue { service.isOwner(guild2.id, owner) }
        }

        @Test
        fun `should return false if the entity is not the owner`() = runTest {
            val owner = getRandomString()
            val guild = service.createGuild(getRandomString(), owner)
            assertFalse { service.isOwner(guild.id, getRandomString()) }
        }

        @Test
        fun `should return false if the guild doesn't exist`() = runTest {
            assertFalse { service.isOwner(0, getRandomString()) }
        }

        @ParameterizedTest
        @ValueSource(strings = ["", " ", "  ", "   "])
        fun `should throw exception if the id is blank`(id: String) = runTest {
            assertThrows<IllegalArgumentException> {
                service.isOwner(0, id)
            }
        }

    }

    @Nested
    inner class IsMember {

        @Test
        fun `should return true if the entity is owner`() = runTest {
            val owner = getRandomString()
            val guild = service.createGuild(getRandomString(), owner)
            assertTrue { service.isMember(guild.id, owner) }
        }

        @Test
        fun `should return true if entity is member`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val entityId = getRandomString()
            service.addMember(guild.id, entityId)
            assertTrue { service.isMember(guild.id, entityId) }
        }

        @Test
        fun `should return false when entity is not member`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val entityId = getRandomString()
            assertFalse { service.isMember(guild.id, entityId) }
        }

        @Test
        fun `should return false when entity is invited`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val entityId = getRandomString()
            service.addInvitation(guild.id, entityId, null)
            assertFalse { service.isMember(guild.id, entityId) }
        }

        @Test
        fun `should return false if entity is member of another guild`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val guild2 = service.createGuild(getRandomString(), getRandomString())
            val entityId = getRandomString()
            service.addMember(guild.id, entityId)
            assertTrue { service.isMember(guild.id, entityId) }
            assertFalse { service.isMember(guild2.id, entityId) }
        }

        @Test
        fun `should return false after the deletion of the member`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val entity = getRandomString()
            service.addMember(guild.id, entity)
            service.removeMember(guild.id, entity)
            assertFalse { service.isMember(guild.id, entity) }
        }

        @Test
        fun `should return false if the guild doesn't exist`() = runTest {
            assertFalse { service.isMember(0, getRandomString()) }
        }

        @ParameterizedTest
        @ValueSource(strings = ["", " ", "  ", "   "])
        fun `should throw exception if the id is blank`(id: String) = runTest {
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
            fun `should define using current time`() = runTest {
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
        fun `should return true if the entity is added as member`() = runTest {
            val owner = getRandomString()
            val guild = service.createGuild(getRandomString(), owner)
            val entityId = getRandomString()
            assertTrue { service.addMember(guild.id, entityId) }

            val members = service.getMembers(guild.id).map(GuildMember::defaultTime).toList()
            assertThat(members).containsExactlyInAnyOrder(
                GuildMember(guild.id, owner),
                GuildMember(guild.id, entityId)
            )
        }

        @Test
        fun `should return false if the entity is already a member of the guild`() = runTest {
            val owner = getRandomString()
            val guild = service.createGuild(getRandomString(), owner)
            val entityId = getRandomString()
            assertTrue { service.addMember(guild.id, entityId) }
            assertFalse { service.addMember(guild.id, entityId) }

            val members = service.getMembers(guild.id).map(GuildMember::defaultTime).toList()
            assertThat(members).containsExactlyInAnyOrder(
                GuildMember(guild.id, owner),
                GuildMember(guild.id, entityId)
            )
        }

        @Test
        fun `should throw if the entity is the owner`() = runTest {
            val owner = getRandomString()
            val guild = service.createGuild(getRandomString(), owner)
            assertThrows<GuildMemberIsOwnerOfGuildException> {
                service.addMember(guild.id, owner)
            }

            val members = service.getMembers(guild.id).map(GuildMember::defaultTime).toList()
            assertThat(members).containsExactlyInAnyOrder(
                GuildMember(guild.id, owner)
            )
        }

        @Test
        fun `should delete invitation when entity is added as member`() = runTest {
            val owner = getRandomString()
            val guild = service.createGuild(getRandomString(), owner)
            val entityId = getRandomString()
            assertTrue { service.addInvitation(guild.id, entityId, null) }
            assertTrue { service.addMember(guild.id, entityId) }

            val members = service.getMembers(guild.id).map(GuildMember::defaultTime).toList()
            assertThat(members).containsExactlyInAnyOrder(
                GuildMember(guild.id, owner),
                GuildMember(guild.id, entityId)
            )

            val pendingMembers = service.getInvitations(guild.id).toList()
            assertThat(pendingMembers).isEmpty()
        }

        @Test
        fun `should throw exception if guild doesn't exist`() = runTest {
            assertThrows<GuildNotFoundException> {
                service.addMember(0, getRandomString())
            }

            val guilds = getAllGuilds()
            assertEquals(0, guilds.size)
        }

        @Test
        fun `should support several members for one guild`() = runTest {
            TODO()
        }

        @ParameterizedTest
        @ValueSource(strings = ["", " ", "  ", "   "])
        fun `should throw exception if id is blank`(id: String) = runTest {
            assertThrows<IllegalArgumentException> {
                service.addMember(0, id)
            }
        }

    }

    @Nested
    inner class RemoveMember {

        @Test
        fun `should return true if entity is member`() = runTest {
            val owner = getRandomString()
            val guild = service.createGuild(getRandomString(), owner)
            val entityId = getRandomString()
            service.addMember(guild.id, entityId)
            assertTrue { service.removeMember(guild.id, entityId) }

            val members = service.getMembers(guild.id).map(GuildMember::defaultTime).toList()
            assertThat(members).containsExactlyInAnyOrder(
                GuildMember(guild.id, owner)
            )
        }

        @Test
        fun `should return false if entity is not member`() = runTest {
            val owner = getRandomString()
            val guild = service.createGuild(getRandomString(), owner)
            val entityId = getRandomString()
            assertFalse { service.removeMember(guild.id, entityId) }

            val members = service.getMembers(guild.id).map(GuildMember::defaultTime).toList()
            assertThat(members).containsExactlyInAnyOrder(
                GuildMember(guild.id, owner)
            )
        }

        @Test
        fun `should return false if another entity is member in the guild`() = runTest {
            val owner = getRandomString()
            val guild = service.createGuild(getRandomString(), owner)
            val entityId = getRandomString()
            service.addMember(guild.id, entityId)

            val entityId2 = getRandomString()
            assertFalse { service.removeMember(guild.id, entityId2) }

            val members = service.getMembers(guild.id).map(GuildMember::defaultTime).toList()
            assertThat(members).containsExactlyInAnyOrder(
                GuildMember(guild.id, owner),
                GuildMember(guild.id, entityId)
            )
        }

        @Test
        fun `should return false if guild doesn't exist`() = runTest {
            assertFalse { service.removeMember(0, getRandomString()) }
        }

        @ParameterizedTest
        @ValueSource(strings = ["", " ", "  ", "   "])
        fun `should throw exception if the name is blank`(id: String) = runTest {
            assertThrows<IllegalArgumentException> {
                service.removeMember(0, id)
            }
        }

        @Test
        fun `should return false if remove the member for owner`() = runTest {
            val owner = getRandomString()
            val guild = service.createGuild(getRandomString(), owner)
            assertFalse { service.removeMember(guild.id, owner) }

            val members = service.getMembers(guild.id).map(GuildMember::defaultTime).toList()
            assertThat(members).containsExactlyInAnyOrder(
                GuildMember(guild.id, owner)
            )
        }
    }

    @Nested
    inner class AddInvitation {

        @Nested
        inner class CreateDate {

            @Test
            fun `should define using current time`() = runTest {
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
            fun `should keep the expiration date integrity`() = runTest {
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
            fun `should save the invitation without expiration`() = runTest {
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
            fun `should throw exception if the expire date is before now`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                val entityId = getRandomString()
                val expiredAt = Instant.now().minusMillis(1)

                assertThrows<IllegalArgumentException> {
                    service.addInvitation(guild.id, entityId, expiredAt)
                }
            }

            @Test
            fun `should insert for already present entity with an expired invitation`() = runBlocking {
                val guild = service.createGuild(getRandomString(), getRandomString())
                val entityId = getRandomString()
                val now = Instant.now()
                assertTrue { service.addInvitation(guild.id, entityId, now.plusMillis(WAIT_EXPIRATION_MILLIS)) }
                delay(WAIT_EXPIRATION_MILLIS * 2)

                var invites = getAllInvites()
                assertEquals(1, invites.size)

                assertTrue { service.addInvitation(guild.id, entityId, null) }
                invites = getAllInvites()
                assertEquals(1, invites.size)

                val invite = invites.single()
                assertEquals(guild.id, invite.guildId)
                assertEquals(entityId, invite.entityId)
                assertNull(invite.expiredAt)
            }
        }

        @Test
        fun `should return true if the entity is invited`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val entityId = getRandomString()
            assertTrue { service.addInvitation(guild.id, entityId, null) }

            assertEquals(
                GuildInvite(guild.id, entityId, null),
                service.getInvitations(guild.id).single().defaultTime()
            )
        }

        @Test
        fun `should throw exception if the entity is already a member of the guild`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val entityId = getRandomString()
            assertTrue { service.addMember(guild.id, entityId) }
            assertThrows<GuildInvitedIsAlreadyMemberException> {
                service.addInvitation(guild.id, entityId, null)
            }

            val invited = service.getInvitations(guild.id).toList()
            assertEquals(0, invited.size)
        }

        @Test
        fun `should return true if the entity is already invited in another guild`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val guild2 = service.createGuild(getRandomString(), getRandomString())

            val entityId = getRandomString()
            assertTrue { service.addInvitation(guild.id, entityId, null) }
            assertTrue { service.addInvitation(guild2.id, entityId, null) }

            assertEquals(
                GuildInvite(guild.id, entityId, null),
                service.getInvitations(guild.id).single().defaultTime()
            )
            assertEquals(
                GuildInvite(guild2.id, entityId, null),
                service.getInvitations(guild2.id).single().defaultTime()
            )
        }

        @Test
        fun `should throw exception if the entity is the owner`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            assertThrows<GuildInvitedIsAlreadyMemberException> {
                service.addInvitation(guild.id, guild.ownerId, null)
            }

            val invited = service.getInvitations(guild.id).toList()
            assertEquals(0, invited.size)
        }

        @Test
        fun `should update the invitation`() = runTest {
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
        fun `should support several invitation for one guild`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val entityId = getRandomString()
            val entityId2 = getRandomString()

            assertTrue { service.addInvitation(guild.id, entityId, null) }
            assertTrue { service.addInvitation(guild.id, entityId2, null) }

            val invites = getAllInvites()
            assertEquals(2, invites.size)

            val inviteForEntity1 = invites.first { it.entityId == entityId }.defaultTime()
            assertEquals(inviteForEntity1, GuildInvite(guild.id, entityId, null))

            val inviteForEntity2 = invites.first { it.entityId == entityId2 }.defaultTime()
            assertEquals(inviteForEntity2, GuildInvite(guild.id, entityId2, null))
        }

        @Test
        fun `should delete invitation when add in the table`() = runBlocking<Unit> {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val entityId = getRandomString()
            val entityId2 = getRandomString()

            val expiredAt = Instant.now().plusMillis(WAIT_EXPIRATION_MILLIS)
            service.addInvitation(guild.id, entityId, expiredAt)

            delay(WAIT_EXPIRATION_MILLIS * 2)
            assertThat(getAllInvites()).hasSize(1)

            service.addInvitation(guild.id, entityId2, null)
            val invites = getAllInvites()
            val invite = invites.single()
            assertEquals(guild.id, invite.guildId)
            assertEquals(entityId2, invite.entityId)
        }

        @Test
        fun `should throw exception if guild doesn't exist`() = runTest {
            assertThrows<GuildNotFoundException> {
                service.addInvitation(0, getRandomString(), null)
            }

            val guilds = getAllGuilds()
            assertEquals(0, guilds.size)
        }

        @ParameterizedTest
        @ValueSource(strings = ["", " ", "  ", "   "])
        fun `should throw exception if id is blank`(id: String) = runTest {
            assertThrows<IllegalArgumentException> {
                service.addInvitation(0, id, null)
            }
        }
    }

    @Nested
    inner class HasInvitation {

        @Test
        fun `should return true if entity is invited`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val entityId = getRandomString()
            service.addInvitation(guild.id, entityId, null)
            assertTrue { service.hasInvitation(guild.id, entityId) }
        }

        @Test
        fun `should return false when entity is not invited`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val entityId = getRandomString()
            assertFalse { service.hasInvitation(guild.id, entityId) }
        }

        @Test
        fun `should return false if invitation is expired`() = runBlocking<Unit> {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val entityId = getRandomString()

            val expiredAt = Instant.now().plusMillis(WAIT_EXPIRATION_MILLIS)
            service.addInvitation(guild.id, entityId, expiredAt)
            delay(WAIT_EXPIRATION_MILLIS * 2)
            assertFalse { service.hasInvitation(guild.id, entityId) }

            // should not delete the invitation
            assertThat(getAllInvites()).hasSize(1)
        }

        @Test
        fun `should return false when entity is member`() = runTest {
            TODO()
        }

        @Test
        fun `should return false if entity is invited to another guild`() = runTest {
            TODO()
        }

        @Test
        fun `should return false after the deletion of the invitation`() = runTest {
            TODO()
        }

        @Test
        fun `should return false if the entity is owner`() = runTest {
            TODO()
        }

        @Test
        fun `should return false if the guild doesn't exist`() = runTest {
            assertFalse { service.hasInvitation(0, getRandomString()) }
        }

        @ParameterizedTest
        @ValueSource(strings = ["", " ", "  ", "   "])
        fun `should throw exception if the id is blank`(id: String) = runTest {
            assertThrows<IllegalArgumentException> {
                service.hasInvitation(0, id)
            }
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

            val invites = service.getInvitations(guild.id).toList()
            assertContentEquals(emptyList(), invites)
        }

        @Test
        fun `when entity is not invited in the guild`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val entityId = getRandomString()
            assertFalse { service.removeInvitation(guild.id, entityId) }

            val invites = service.getInvitations(guild.id).toList()
            assertContentEquals(emptyList(), invites)
        }

        @Test
        fun `when another entity is invited in the guild`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val entityId = getRandomString()
            val entityId2 = getRandomString()
            service.addInvitation(guild.id, entityId, null)
            assertFalse { service.removeInvitation(guild.id, entityId2) }

            assertEquals(
                GuildInvite(guild.id, entityId, null),
                service.getInvitations(guild.id).single().defaultTime()
            )
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
            val members = service.getMembers(guild.id).map(GuildMember::defaultTime).toList()
            assertThat(members).containsExactlyInAnyOrder(
                GuildMember(guild.id, owner)
            )
        }

        @Test
        fun `when guild has members`() = runTest {
            val owner = getRandomString()
            val guild = service.createGuild(getRandomString(), owner)
            val membersToAdd = listOf(getRandomString(), getRandomString())
            membersToAdd.forEach { service.addMember(guild.id, it) }

            val members = service.getMembers(guild.id).map(GuildMember::defaultTime).toList()
            assertThat(members).containsExactlyInAnyOrderElementsOf(
                membersToAdd.map { GuildMember(guild.id, it) } + GuildMember(guild.id, owner)
            )
        }

        @Test
        fun `when an entity is invited but not member`() = runTest {
            val owner = getRandomString()
            val guild = service.createGuild(getRandomString(), owner)
            service.addInvitation(guild.id, getRandomString(), null)

            val members = service.getMembers(guild.id).map(GuildMember::defaultTime).toList()
            assertThat(members).containsExactlyInAnyOrder(
                GuildMember(guild.id, owner)
            )
        }

        @Test
        fun `when guild does not exist`() = runTest {
            val members = service.getMembers(0).toList()
            assertThat(members).isEmpty()
        }
    }

    @Nested
    inner class GetInvitations {

        @Test
        fun `when guild has no invitations`() = runTest {
            val owner = getRandomString()
            val guild = service.createGuild(getRandomString(), owner)
            val invites = service.getInvitations(guild.id).toList()
            assertContentEquals(emptyList(), invites)
        }

        @Test
        fun `when guild has invitations`() = runTest {
            val owner = getRandomString()
            val guild = service.createGuild(getRandomString(), owner)
            val invitesToAdd = listOf(getRandomString(), getRandomString())
            val invites = invitesToAdd.map { GuildInvite(guild.id, it, null) }.onEach {
                service.addInvitation(it.guildId, it.entityId, it.expiredAt)
            }

            val invitesService = service.getInvitations(guild.id).map(GuildInvite::defaultTime).toList()
            assertThat(invitesService).containsExactlyInAnyOrderElementsOf(invites)
        }

        @Test
        fun `when an entity is member but not invited`() = runTest {
            val owner = getRandomString()
            val guild = service.createGuild(getRandomString(), owner)
            service.addMember(guild.id, getRandomString())

            val invites = service.getInvitations(guild.id).toList()
            assertContentEquals(emptyList(), invites)
        }

        @Test
        fun `when guild does not exist`() = runTest {
            val invites = service.getInvitations(0).toList()
            assertContentEquals(emptyList(), invites)
        }

        @Test
        fun `should filter out the expired invitations`() = runBlocking<Unit> {
            val owner = getRandomString()
            val guild = service.createGuild(getRandomString(), owner)

            val invitesNotExpired = listOf(
                GuildInvite(guild.id, getRandomString(), null),
                GuildInvite(guild.id, getRandomString(), null)
            )
            invitesNotExpired.forEach { service.addInvitation(it.guildId, it.entityId, it.expiredAt) }

            val invitesExpired = listOf(
                GuildInvite(guild.id, getRandomString(), Instant.now().plusMillis(WAIT_EXPIRATION_MILLIS)),
                GuildInvite(guild.id, getRandomString(), Instant.now().plusMillis(WAIT_EXPIRATION_MILLIS))
            )
            invitesExpired.forEach { service.addInvitation(it.guildId, it.entityId, it.expiredAt) }

            delay(WAIT_EXPIRATION_MILLIS * 2)

            val invitesService = service.getInvitations(guild.id).map(GuildInvite::defaultTime).toList()
            assertThat(invitesService).containsExactlyInAnyOrderElementsOf(invitesNotExpired)
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