package com.github.rushyverse.core.data.guild

import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.container.createRedisContainer
import com.github.rushyverse.core.data.*
import com.github.rushyverse.core.utils.getRandomString
import io.lettuce.core.FlushMode
import io.lettuce.core.KeyScanArgs
import io.lettuce.core.KeyValue
import io.lettuce.core.RedisURI
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.builtins.serializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit
import kotlin.test.*

@Timeout(10, unit = TimeUnit.SECONDS)
@Testcontainers
class GuildCacheServiceTest {

    companion object {
        @JvmStatic
        @Container
        private val redisContainer = createRedisContainer()
    }

    private lateinit var cacheClient: CacheClient
    private lateinit var service: GuildCacheService

    @BeforeTest
    fun onBefore() = runBlocking {
        cacheClient = CacheClient {
            uri = RedisURI.create(redisContainer.url)
        }
        service = GuildCacheService(cacheClient)
    }

    @AfterTest
    fun onAfter(): Unit = runBlocking {
        cacheClient.connect {
            it.flushall(FlushMode.SYNC)
        }
        cacheClient.closeAsync().await()
    }

    @Nested
    inner class DefaultParameter {

        @Test
        fun `default values`() {
            assertEquals("guild:%s:", service.prefixKey)
            assertNull(service.expirationKey)
        }

    }

    @Nested
    inner class CreateGuild {

        @Nested
        inner class Owner {

            @Test
            fun `when owner is not owner of a guild`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                assertThat(getAllAddedGuilds()).containsExactly(guild)
                assertThat(getAllImportedGuilds()).isEmpty()
                assertThat(getAllDeletedGuilds()).isEmpty()
            }

            @Test
            fun `when owner is already in a guild`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                val guild2 = service.createGuild(getRandomString(), guild.ownerId)
                assertNotEquals(guild.id, guild2.id)

                val guilds = getAllAddedGuilds()
                assertThat(guilds).containsExactlyInAnyOrder(guild, guild2)
                assertThat(getAllImportedGuilds()).isEmpty()
                assertThat(getAllDeletedGuilds()).isEmpty()
            }

            @Test
            fun `when owner is a member of another guild`() = runTest {
                val entity = getRandomString()
                val guild = service.createGuild(getRandomString(), getRandomString())
                service.addMember(guild.id, entity)

                val guild2 = service.createGuild(getRandomString(), entity)

                val guilds = getAllAddedGuilds()
                assertThat(guilds).containsExactlyInAnyOrder(guild, guild2)
                assertThat(getAllImportedGuilds()).isEmpty()
                assertThat(getAllDeletedGuilds()).isEmpty()
            }

            @ParameterizedTest
            @ValueSource(strings = ["", " ", "  ", "   "])
            fun `when owner is blank`(id: String) = runTest {
                assertThrows<IllegalArgumentException> {
                    service.createGuild(getRandomString(), id)
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

                val guilds = getAllAddedGuilds()
                assertThat(guilds).containsExactlyInAnyOrder(guild, guild2)
                assertThat(getAllImportedGuilds()).isEmpty()
                assertThat(getAllDeletedGuilds()).isEmpty()
            }

            @ParameterizedTest
            @ValueSource(strings = ["", " ", "  ", "   "])
            fun `when name is blank`(name: String) = runTest {
                assertThrows<IllegalArgumentException> {
                    service.createGuild(name, getRandomString())
                }
            }
        }

        @Test
        fun `create always an unused id`() = runTest {
            val expectedSize = 1000

            val idsCreated = List(expectedSize) {
                service.createGuild(getRandomString(), getRandomString())
            }.map { it.id }
            assertEquals(expectedSize, idsCreated.toSet().size)

            val addedIds: List<Int> = getAllAddedGuilds().map { it.id }
            assertThat(idsCreated).containsExactlyInAnyOrderElementsOf(addedIds)
            assertThat(getAllImportedGuilds()).isEmpty()
            assertThat(getAllDeletedGuilds()).isEmpty()
        }

    }

    @Nested
    inner class DeleteGuild {

        @Nested
        inner class WithCacheGuild {

            @Test
            fun `when guild exists`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                assertTrue { service.deleteGuild(guild.id) }
                assertThat(getAllAddedGuilds()).isEmpty()
                assertThat(getAllDeletedGuilds()).isEmpty()
                assertThat(getAllImportedGuilds()).isEmpty()
            }

            @Test
            fun `when another guild is deleted`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                val guild2 = service.createGuild(getRandomString(), getRandomString())

                assertTrue { service.deleteGuild(guild.id) }
                assertThat(getAllAddedGuilds()).containsExactlyInAnyOrder(guild2)
                assertThat(getAllDeletedGuilds()).isEmpty()
                assertThat(getAllImportedGuilds()).isEmpty()
            }

            @Test
            fun `when guild is already deleted`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                assertTrue { service.deleteGuild(guild.id) }
                assertFalse { service.deleteGuild(guild.id) }

                assertThat(getAllAddedGuilds()).isEmpty()
                assertThat(getAllDeletedGuilds()).isEmpty()
                assertThat(getAllImportedGuilds()).isEmpty()
            }

            @Test
            fun `delete all linked data`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                val guildId = guild.id
                val sizeData = 10
                repeat(sizeData) {
                    service.addMember(guildId, getRandomString())
                }
                service.importInvitations(guildId, List(sizeData) {
                    GuildInvite(guildId, getRandomString(), null)
                })
                repeat(sizeData) {
                    service.addInvitation(guildId, getRandomString(), null)
                }
                service.importMembers(guildId, List(sizeData) { getRandomString() })

                val guildIdString = guildId.toString()

                assertThat(getAllImportedInvites(guildIdString)).hasSize(10)
                assertThat(getAllAddInvites(guildIdString)).hasSize(10)
                assertThat(getAllImportedMembers(guildIdString)).hasSize(10)
                assertThat(getAllAddMembers(guildIdString)).hasSize(10)

                assertTrue { service.deleteGuild(guild.id) }
                assertThat(getAllImportedInvites(guildIdString)).isEmpty()
                assertThat(getAllAddInvites(guildIdString)).isEmpty()
                assertThat(getAllImportedMembers(guildIdString)).isEmpty()
                assertThat(getAllAddMembers(guildIdString)).isEmpty()
                assertThat(getAllAddedGuilds()).isEmpty()
            }

            @Test
            fun `should not delete another guild data`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                val guild2 = service.createGuild(getRandomString(), getRandomString())

                val guildId = guild2.id
                val sizeData = 10
                repeat(sizeData) {
                    service.addMember(guildId, getRandomString())
                }
                service.importMembers(guildId, List(sizeData) { getRandomString() })

                service.importInvitations(guildId, List(sizeData) {
                    GuildInvite(guildId, getRandomString(), null)
                })
                repeat(sizeData) {
                    service.addInvitation(guildId, getRandomString(), null)
                }

                val guildIdString = guildId.toString()

                assertThat(getAllImportedInvites(guildIdString)).hasSize(10)
                assertThat(getAllAddInvites(guildIdString)).hasSize(10)
                assertThat(getAllImportedMembers(guildIdString)).hasSize(10)
                assertThat(getAllAddMembers(guildIdString)).hasSize(10)

                assertTrue { service.deleteGuild(guild.id) }

                assertThat(getAllImportedInvites(guildIdString)).hasSize(10)
                assertThat(getAllAddInvites(guildIdString)).hasSize(10)
                assertThat(getAllImportedMembers(guildIdString)).hasSize(10)
                assertThat(getAllAddMembers(guildIdString)).hasSize(10)
                assertThat(getAllAddedGuilds()).containsExactly(guild2)

                assertThat(getAllDeletedGuilds()).isEmpty()
            }

        }

        @Nested
        inner class WithImportedGuild {

            @Test
            fun `when guild exists`() = runTest {
                val guild = Guild(0, getRandomString(), getRandomString())
                service.importGuild(guild)
                assertTrue { service.deleteGuild(guild.id) }
                assertThat(getAllAddedGuilds()).isEmpty()
                assertThat(getAllDeletedGuilds()).containsExactly(guild.id)
                assertThat(getAllImportedGuilds()).isEmpty()
            }

            @Test
            fun `when another guild is deleted`() = runTest {
                val guild = Guild(0, getRandomString(), getRandomString())
                service.importGuild(guild)
                val guild2 = Guild(1, getRandomString(), getRandomString())
                service.importGuild(guild2)

                assertTrue { service.deleteGuild(guild.id) }
                assertThat(getAllAddedGuilds()).isEmpty()
                assertThat(getAllDeletedGuilds()).containsExactly(guild.id)
                assertThat(getAllImportedGuilds()).containsExactlyInAnyOrder(guild2)
            }

            @Test
            fun `when guild is already deleted`() = runTest {
                val guild = Guild(0, getRandomString(), getRandomString())
                service.importGuild(guild)
                assertTrue { service.deleteGuild(guild.id) }
                assertFalse { service.deleteGuild(guild.id) }

                assertThat(getAllAddedGuilds()).isEmpty()
                assertThat(getAllDeletedGuilds()).containsExactly(guild.id)
                assertThat(getAllImportedGuilds()).isEmpty()
            }

            @Test
            fun `delete all linked data`() = runTest {
                val guild = Guild(0, getRandomString(), getRandomString())
                service.importGuild(guild)

                val guildId = guild.id
                val sizeData = 10
                repeat(sizeData) {
                    service.addMember(guildId, getRandomString())
                }
                val importedMembers = List(sizeData) { getRandomString() }
                service.importMembers(guildId, importedMembers)
                importedMembers.forEach {
                    service.removeMember(guildId, it)
                }

                val importedInvites = List(sizeData) {
                    GuildInvite(guildId, getRandomString(), null)
                }
                service.importInvitations(guildId, importedInvites)
                repeat(sizeData) {
                    service.addInvitation(guildId, getRandomString(), null)
                }
                importedInvites.forEach {
                    service.removeInvitation(guildId, it.entityId)
                }

                val guildIdString = guildId.toString()

                assertThat(getAllImportedInvites(guildIdString)).hasSize(10)
                assertThat(getAllAddInvites(guildIdString)).hasSize(10)
                assertThat(getAllRemoveInvites(guildIdString)).hasSize(10)
                assertThat(getAllImportedMembers(guildIdString)).hasSize(10)
                assertThat(getAllAddMembers(guildIdString)).hasSize(10)
                assertThat(getAllRemoveMembers(guildIdString)).hasSize(10)

                assertTrue { service.deleteGuild(guild.id) }

                assertThat(getAllImportedInvites(guildIdString)).isEmpty()
                assertThat(getAllAddInvites(guildIdString)).isEmpty()
                assertThat(getAllRemoveInvites(guildIdString)).isEmpty()
                assertThat(getAllImportedMembers(guildIdString)).isEmpty()
                assertThat(getAllAddMembers(guildIdString)).isEmpty()
                assertThat(getAllRemoveMembers(guildIdString)).isEmpty()
                assertThat(getAllImportedGuilds()).isEmpty()
                assertThat(getAllDeletedGuilds()).containsExactly(guild.id)
            }

            @Test
            fun `should not delete another guild data`() = runTest {
                val guild = Guild(0, getRandomString(), getRandomString())
                service.importGuild(guild)

                val guild2 = Guild(1, getRandomString(), getRandomString())
                service.importGuild(guild2)

                val guildId = guild2.id
                val sizeData = 10
                repeat(sizeData) {
                    service.addMember(guildId, getRandomString())
                }
                val importedMembers = List(sizeData) { getRandomString() }
                service.importMembers(guildId, importedMembers)
                importedMembers.forEach {
                    service.removeMember(guildId, it)
                }

                val importedInvites = List(sizeData) {
                    GuildInvite(guildId, getRandomString(), null)
                }
                service.importInvitations(guildId, importedInvites)
                repeat(sizeData) {
                    service.addInvitation(guildId, getRandomString(), null)
                }
                importedInvites.forEach {
                    service.removeInvitation(guildId, it.entityId)
                }

                val guildIdString = guildId.toString()

                assertThat(getAllImportedInvites(guildIdString)).hasSize(10)
                assertThat(getAllAddInvites(guildIdString)).hasSize(10)
                assertThat(getAllRemoveInvites(guildIdString)).hasSize(10)
                assertThat(getAllImportedMembers(guildIdString)).hasSize(10)
                assertThat(getAllAddMembers(guildIdString)).hasSize(10)
                assertThat(getAllRemoveMembers(guildIdString)).hasSize(10)

                assertTrue { service.deleteGuild(guild.id) }

                assertThat(getAllImportedInvites(guildIdString)).hasSize(10)
                assertThat(getAllAddInvites(guildIdString)).hasSize(10)
                assertThat(getAllRemoveInvites(guildIdString)).hasSize(10)
                assertThat(getAllImportedMembers(guildIdString)).hasSize(10)
                assertThat(getAllAddMembers(guildIdString)).hasSize(10)
                assertThat(getAllRemoveMembers(guildIdString)).hasSize(10)
                assertThat(getAllImportedGuilds()).containsExactly(guild2)

                assertThat(getAllDeletedGuilds()).containsExactly(guild.id)
            }
        }

        @ParameterizedTest
        @ValueSource(ints = [-1, 0, 1])
        fun `when guild does not exist`(id: Int) = runTest {
            assertFalse { service.deleteGuild(id) }
            assertThat(getAllAddedGuilds()).isEmpty()
            assertThat(getAllDeletedGuilds()).isEmpty()
            assertThat(getAllImportedGuilds()).isEmpty()
        }
    }

    @Nested
    inner class GetGuildById {

        @Test
        fun `when guild is not deleted`() = runTest {
            withGuildImportedAndCreated {
                assertEquals(it, service.getGuild(it.id))
            }
        }

        @Test
        fun `when guild is deleted`() = runTest {
            withGuildImportedAndCreated {
                service.deleteGuild(it.id)
                assertNull(service.getGuild(it.id))
            }
        }

        @Test
        fun `when another guild is added`() = runTest {
            withGuildImportedAndCreated {
                val guild2 = service.createGuild(getRandomString(), getRandomString())
                assertEquals(it, service.getGuild(it.id))
                assertEquals(guild2, service.getGuild(guild2.id))
            }
        }

        @Test
        fun `when guild does not exist`() = runTest {
            assertNull(service.getGuild(0))
        }
    }

    @Nested
    inner class GetGuildByName {

        @Test
        fun `when guild is not deleted`() = runTest {
            withGuildImportedAndCreated {
                assertThat(service.getGuild(it.name).toList()).containsExactly(it)
            }
        }

        @Test
        fun `when guild is deleted`() = runTest {
            withGuildImportedAndCreated {
                service.deleteGuild(it.id)
                assertThat(service.getGuild(it.name).toList()).isEmpty()
            }
        }

        @Test
        fun `when another guild is added`() = runTest {
            withGuildImportedAndCreated {
                val guild2 = service.createGuild(getRandomString(), getRandomString())
                assertThat(service.getGuild(it.name).toList()).containsExactly(it)
                assertThat(service.getGuild(guild2.name).toList()).containsExactly(guild2)
            }
        }

        @Test
        fun `when guild does not exist`() = runTest {
            assertThat(service.getGuild(getRandomString()).toList()).isEmpty()
        }

        @ParameterizedTest
        @ValueSource(strings = ["", " ", "  ", "   "])
        fun `when name is blank`(name: String) = runTest {
            assertThrows<IllegalArgumentException> {
                service.getGuild(name)
            }
        }

        @Test
        fun `with lot of guilds`() = runTest {
            val numberOfGuilds = 200
            val name = getRandomString()
            val createdGuild = List(numberOfGuilds) {
                service.createGuild(
                    name = if (it < numberOfGuilds / 2) name else getRandomString(),
                    getRandomString()
                )
            }

            val savedGuild = List(numberOfGuilds) {
                Guild(
                    it,
                    name = if (it < numberOfGuilds / 2) name else getRandomString(),
                    getRandomString()
                ).apply {
                    service.importGuild(this)
                }
            }

            val expectedGuild = createdGuild.take(numberOfGuilds / 2) + savedGuild.take(numberOfGuilds / 2)
            assertThat(service.getGuild(name).toList()).containsExactlyInAnyOrderElementsOf(expectedGuild)
        }
    }

    @Nested
    inner class IsOwner {

        @Test
        fun `when owner of guild`() = runTest {
            withGuildImportedAndCreated {
                assertTrue { service.isOwner(it.id, it.ownerId) }
            }
        }

        @Test
        fun `when owner of several guilds`() = runTest {
            withGuildImportedAndCreated {
                val ownerId = it.ownerId
                val guild2 = service.createGuild(getRandomString(), ownerId)
                assertTrue { service.isOwner(it.id, ownerId) }
                assertTrue { service.isOwner(guild2.id, ownerId) }
            }
        }

        @Test
        fun `when not owner of guild`() = runTest {
            withGuildImportedAndCreated {
                assertFalse { service.isOwner(it.id, getRandomString()) }
            }
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
        fun `when entity is a member of the guild`() = runTest {
            withGuildImportedAndCreated {
                val entityId = getRandomString()
                service.addMember(it.id, entityId)
                assertTrue { service.isMember(it.id, entityId) }
            }
        }

        @Test
        fun `when entity is not a member of the guild`() = runTest {
            withGuildImportedAndCreated {
                val entityId = getRandomString()
                assertFalse { service.isMember(it.id, entityId) }
            }
        }

        @Test
        fun `when entity is invited in the guild`() = runTest {
            withGuildImportedAndCreated {
                val entityId = getRandomString()
                service.addInvitation(it.id, entityId, null)
                assertFalse { service.isMember(it.id, entityId) }
            }
        }

        @Test
        fun `when entity is member of another guild`() = runTest {
            withGuildImportedAndCreated {
                val guild2 = service.createGuild(getRandomString(), getRandomString())
                val entityId = getRandomString()
                service.addMember(it.id, entityId)
                assertFalse { service.isMember(guild2.id, entityId) }
            }
        }

        @Test
        fun `when another entity is member of the guild`() = runTest {
            withGuildImportedAndCreated {
                val entityId = getRandomString()
                val entity2Id = getRandomString()
                service.addMember(it.id, entity2Id)
                assertFalse { service.isMember(it.id, entityId) }
            }
        }

        @Test
        fun `when entity is removed of members`() = runTest {
            withGuildImportedAndCreated {
                val entityId = getRandomString()
                service.addMember(it.id, entityId)
                assertTrue { service.isMember(it.id, entityId) }

                service.removeMember(it.id, entityId)
                assertFalse { service.isMember(it.id, entityId) }
            }
        }

        @ParameterizedTest
        @ValueSource(ints = [-1, 0, 1])
        fun `when guild does not exist`(id: Int) = runTest {
            assertFalse { service.isMember(id, getRandomString()) }
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
    inner class AddInvitation {

        @Nested
        inner class ExpirationDate {

            @Test
            fun `with field`() = runTest {
                withGuildImportedAndCreated {
                    val guildId = it.id
                    val entityId = getRandomString()
                    val now = Instant.now()
                    val expirationDate = now.plusSeconds(10)
                    assertTrue { service.addInvitation(guildId, entityId, expirationDate) }

                    val guildIdString = guildId.toString()
                    assertThat(getAllImportedInvites(guildIdString)).isEmpty()
                    assertThat(getAllAddInvites(guildIdString)).hasSize(1)
                    assertThat(getAllRemoveInvites(guildIdString)).isEmpty()
                    val inviteForGuild = getAllAddInvites(guildIdString)

                    val invite = inviteForGuild.single()
                    assertEquals(entityId, invite.entityId)
                    assertEquals(
                        expirationDate.truncatedTo(ChronoUnit.SECONDS),
                        invite.expiredAt!!.truncatedTo(ChronoUnit.SECONDS)
                    )
                }
            }

            @Test
            fun `without field`() = runTest {
                withGuildImportedAndCreated {
                    val guildId = it.id
                    val entityId = getRandomString()
                    assertTrue { service.addInvitation(guildId, entityId, null) }

                    val guildIdString = guildId.toString()
                    assertThat(getAllImportedInvites(guildIdString)).isEmpty()
                    assertThat(getAllAddInvites(guildIdString)).hasSize(1)
                    assertThat(getAllRemoveInvites(guildIdString)).isEmpty()
                    val inviteForGuild = getAllAddInvites(guildIdString)

                    val invite = inviteForGuild.single()
                    assertEquals(entityId, invite.entityId)
                    assertNull(invite.expiredAt)
                }
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
            withGuildImportedAndCreated {
                val guildId = it.id
                val entityId = getRandomString()
                assertTrue { service.addInvitation(guildId, entityId, null) }

                val invitations = service.getInvitations(guildId).toList()
                assertThat(invitations).containsExactly(
                    GuildInvite(guildId, entityId, null)
                )
            }
        }

        @Test
        fun `when entity is already a member of the guild`() = runTest {
            withGuildImportedAndCreated {
                val guildId = it.id
                val entityId = getRandomString()
                assertTrue { service.addMember(guildId, entityId) }
                assertThrows<GuildInvitedIsAlreadyMemberException> {
                    service.addInvitation(guildId, entityId, null)
                }

                assertThat(getAllAddInvites(guildId.toString())).isEmpty()
            }
        }

        @Test
        fun `when entity is invited in another guild`() = runTest {
            withGuildImportedAndCreated {
                val guildId = it.id
                val guild2 = service.createGuild(getRandomString(), getRandomString())
                val guildId2 = guild2.id

                val entityId = getRandomString()
                assertTrue { service.addInvitation(guildId, entityId, null) }
                assertTrue { service.addInvitation(guildId2, entityId, null) }

                assertThat(service.getInvitations(guildId).toList()).containsExactly(
                    GuildInvite(guildId, entityId, null)
                )
                assertThat(service.getInvitations(guild2.id).toList()).containsExactly(
                    GuildInvite(guildId2, entityId, null)
                )
            }
        }

        @Test
        fun `when entity is owner of a guild`() = runTest {
            withGuildImportedAndCreated {
                assertThrows<GuildInvitedIsAlreadyMemberException> {
                    service.addInvitation(it.id, it.ownerId, null)
                }
                assertThat(getAllAddInvites(it.id.toString())).isEmpty()
            }
        }

        @Test
        fun `when entity is already invited`() = runTest {
            withGuildImportedAndCreated {
                val guildId = it.id
                val entityId = getRandomString()
                val expiredAt = Instant.now().plusSeconds(10).truncatedTo(ChronoUnit.SECONDS)
                assertTrue { service.addInvitation(guildId, entityId, null) }
                assertFalse { service.addInvitation(guildId, entityId, null) }
                assertTrue { service.addInvitation(guildId, entityId, expiredAt) }

                assertThat(getAllAddInvites(guildId.toString())).containsExactly(
                    GuildInvite(guildId, entityId, expiredAt)
                )
            }
        }

        @ParameterizedTest
        @ValueSource(strings = ["", " ", "  ", "   "])
        fun `when entity id is blank`(id: String) = runTest {
            assertThrows<IllegalArgumentException> {
                service.addInvitation(0, id, null)
            }
        }

        @ParameterizedTest
        @ValueSource(ints = [-1, 0, 1])
        fun `when guild does not exist`(guildId: Int) = runTest {
            assertThrows<GuildNotFoundException> {
                service.addInvitation(guildId, getRandomString(), null)
            }
        }
    }

    private suspend fun getAllImportedGuilds(): List<Guild> {
        return getAllDataFromKey(GuildCacheService.Type.IMPORT_GUILD, "*").filter {
            it.key.decodeToString().endsWith(GuildCacheService.Type.IMPORT_GUILD.key)
        }.map { keyValue ->
            cacheClient.binaryFormat.decodeFromByteArray(Guild.serializer(), keyValue.value)
        }
    }

    private suspend fun getAllAddedGuilds(): List<Guild> {
        return getAllDataFromKey(GuildCacheService.Type.ADD_GUILD, "*").filter {
            it.key.decodeToString().endsWith(GuildCacheService.Type.ADD_GUILD.key)
        }.map { keyValue ->
            cacheClient.binaryFormat.decodeFromByteArray(Guild.serializer(), keyValue.value)
        }
    }

    private suspend fun getAllDeletedGuilds(): List<Int> {
        val key = (service.prefixCommonKey + GuildCacheService.Type.REMOVE_GUILD.key).encodeToByteArray()
        return cacheClient.connect {
            it.smembers(key).map {
                cacheClient.binaryFormat.decodeFromByteArray(Int.serializer(), it)
            }.toList()
        }
    }

    private suspend fun getAllImportedInvites(guildId: String): List<GuildInvite> {
        return getAllInvitesWithReplacement(GuildCacheService.Type.IMPORT_INVITATION, guildId)
    }

    private suspend fun getAllAddInvites(guildId: String): List<GuildInvite> {
        return getAllInvitesWithReplacement(GuildCacheService.Type.ADD_INVITATION, guildId)
    }

    private suspend fun getAllInvitesWithReplacement(type: GuildCacheService.Type, guildId: String): List<GuildInvite> {
        // Redis allows to use * as a wildcard, but we need to escape the : character
        // So we need to replace the * with [^:]+ and filter the keys with a regex
        val regexEnd = Regex(".*${type.key.replace("%s", "[^:]+")}\$")
        return getAllDataFromKey(type, guildId, "*")
            .filter {
                regexEnd.matches(it.key.decodeToString())
            }.map { keyValue ->
                cacheClient.binaryFormat.decodeFromByteArray(GuildInvite.serializer(), keyValue.value)
            }
    }

    private suspend fun getAllRemoveInvites(guildId: String): List<String> {
        return getAllValuesOfSet(GuildCacheService.Type.REMOVE_INVITATION, guildId).map {
            cacheClient.binaryFormat.decodeFromByteArray(String.serializer(), it)
        }
    }

    private suspend fun getAllImportedMembers(guildId: String): List<String> {
        return getAllValuesOfSet(GuildCacheService.Type.IMPORT_MEMBER, guildId).map {
            cacheClient.binaryFormat.decodeFromByteArray(String.serializer(), it)
        }
    }

    private suspend fun getAllAddMembers(guildId: String): List<String> {
        return getAllValuesOfSet(GuildCacheService.Type.ADD_MEMBER, guildId).map {
            cacheClient.binaryFormat.decodeFromByteArray(String.serializer(), it)
        }
    }

    private suspend fun getAllRemoveMembers(guildId: String): List<String> {
        return getAllValuesOfSet(GuildCacheService.Type.REMOVE_MEMBER, guildId).map {
            cacheClient.binaryFormat.decodeFromByteArray(String.serializer(), it)
        }
    }

    private suspend fun getAllValuesOfSet(type: GuildCacheService.Type, guildId: String): List<ByteArray> {
        val searchKey = service.prefixKey.format(guildId) + type.key
        return cacheClient.connect {
            it.smembers(searchKey.encodeToByteArray()).toList()
        }
    }

    private suspend fun getAllDataFromKey(
        type: GuildCacheService.Type,
        vararg format: String
    ): List<KeyValue<ByteArray, ByteArray>> {
        val searchKey = (service.prefixKey + type.key).format(*format)
        return cacheClient.connect {
            val scanner = it.scan(KeyScanArgs.Builder.limit(Long.MAX_VALUE).match(searchKey))
            if (scanner == null || scanner.keys.isEmpty()) return emptyList()

            it.mget(*scanner.keys.toTypedArray())
                .filter { keyValue -> keyValue.hasValue() }
                .toList()
        }
    }

    private suspend inline fun withGuildImportedAndCreated(
        crossinline block: suspend (Guild) -> Unit
    ) {
        var guild = Guild(0, getRandomString(), getRandomString())
        service.importGuild(guild)
        block(guild)

        cacheClient.connect {
            it.flushall(FlushMode.SYNC)
        }

        guild = service.createGuild(getRandomString(), getRandomString())
        block(guild)
    }
}