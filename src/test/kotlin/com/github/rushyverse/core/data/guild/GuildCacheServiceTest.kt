package com.github.rushyverse.core.data.guild

import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.container.createRedisContainer
import com.github.rushyverse.core.data.*
import com.github.rushyverse.core.utils.getRandomString
import io.lettuce.core.FlushMode
import io.lettuce.core.KeyScanArgs
import io.lettuce.core.KeyValue
import io.lettuce.core.RedisURI
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.builtins.serializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Disabled
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
    inner class ImportGuild {

        @ParameterizedTest
        @ValueSource(ints = [Int.MIN_VALUE, -800000, -1000, -1])
        fun `when id is negative`(id: Int) = runTest {
            assertThrows<IllegalArgumentException> {
                service.importGuild(Guild(id, getRandomString(), getRandomString()))
            }
        }

        @ParameterizedTest
        @ValueSource(ints = [0, 1, 1000, 800000, Int.MAX_VALUE])
        fun `should import guild with positive id`(id: Int) = runTest {
            val guild = Guild(id, getRandomString(), getRandomString())
            service.importGuild(guild)
            assertThat(getAllImportedGuilds()).containsExactly(guild)
        }

        @Test
        fun `should replace the guild with the same id`() = runTest {
            val guild = Guild(0, getRandomString(), getRandomString())
            service.importGuild(guild)

            val guild2 = Guild(0, getRandomString(), getRandomString())
            service.importGuild(guild2)

            assertThat(getAllImportedGuilds()).containsExactly(guild2)
        }

        @Test
        fun `should not import if guild is deleted`() = runTest {
            val guild = Guild(0, getRandomString(), getRandomString())
            service.importGuild(guild)
            service.deleteGuild(guild.id)

            assertThat(getAllImportedGuilds()).isEmpty()

            val guildSameID = Guild(0, getRandomString(), getRandomString())
            service.importGuild(guildSameID)

            assertThat(getAllImportedGuilds()).isEmpty()

            val otherGuild = Guild(1, getRandomString(), getRandomString())
            service.importGuild(otherGuild)

            assertThat(getAllImportedGuilds()).containsExactly(otherGuild)
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
            fun `delete all linked data for imported guild`() = runTest {
                val guild = Guild(0, getRandomString(), getRandomString())
                service.importGuild(guild)

                val guildId = guild.id
                val sizeData = 10
                repeat(sizeData) {
                    service.addMember(guildId, getRandomString())
                }
                service.importInvitations(List(sizeData) {
                    GuildInvite(guildId, getRandomString(), null)
                })
                repeat(sizeData) {
                    service.addInvitation(guildId, getRandomString(), null)
                }
                service.importMembers(List(sizeData) { GuildMember(guildId, getRandomString()) })

                val guildIdString = guildId.toString()

                assertThat(getAllImportedInvites(guildIdString)).hasSize(10)
                assertThat(getAllAddedInvites(guildIdString)).hasSize(10)
                assertThat(getAllImportedMembers(guildIdString)).hasSize(10)
                assertThat(getAllAddedMembers(guildIdString)).hasSize(10)

                assertTrue { service.deleteGuild(guild.id) }

                assertThat(getAllImportedInvites(guildIdString)).isEmpty()
                assertThat(getAllAddedInvites(guildIdString)).isEmpty()
                assertThat(getAllImportedMembers(guildIdString)).isEmpty()
                assertThat(getAllAddedMembers(guildIdString)).isEmpty()
                assertThat(getAllImportedGuilds()).isEmpty()
                assertThat(getAllAddedGuilds()).isEmpty()
                assertThat(getAllDeletedGuilds()).containsExactly(guild.id)
            }

            @Test
            fun `delete all linked data for cache guild`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                val guildId = guild.id
                val sizeData = 10
                repeat(sizeData) {
                    service.addMember(guildId, getRandomString())
                }
                repeat(sizeData) {
                    service.addInvitation(guildId, getRandomString(), null)
                }

                val guildIdString = guildId.toString()

                assertThat(getAllAddedInvites(guildIdString)).hasSize(10)
                assertThat(getAllAddedMembers(guildIdString)).hasSize(10)

                assertTrue { service.deleteGuild(guild.id) }

                assertThat(getAllImportedInvites(guildIdString)).isEmpty()
                assertThat(getAllAddedInvites(guildIdString)).isEmpty()
                assertThat(getAllImportedMembers(guildIdString)).isEmpty()
                assertThat(getAllAddedMembers(guildIdString)).isEmpty()
                assertThat(getAllImportedGuilds()).isEmpty()
                assertThat(getAllAddedGuilds()).isEmpty()
                assertThat(getAllDeletedGuilds()).isEmpty()
            }

            @Test
            fun `should not delete another guild data`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())

                val guild2 = Guild(1, getRandomString(), getRandomString())
                service.importGuild(guild2)

                val guildId = guild2.id
                val sizeData = 10
                repeat(sizeData) {
                    service.addMember(guildId, getRandomString())
                }
                service.importMembers(List(sizeData) { GuildMember(guildId, getRandomString()) })

                service.importInvitations(List(sizeData) {
                    GuildInvite(guildId, getRandomString(), null)
                })
                repeat(sizeData) {
                    service.addInvitation(guildId, getRandomString(), null)
                }

                val guildIdString = guildId.toString()

                assertThat(getAllImportedInvites(guildIdString)).hasSize(10)
                assertThat(getAllAddedInvites(guildIdString)).hasSize(10)
                assertThat(getAllImportedMembers(guildIdString)).hasSize(10)
                assertThat(getAllAddedMembers(guildIdString)).hasSize(10)

                assertTrue { service.deleteGuild(guild.id) }

                assertThat(getAllImportedInvites(guildIdString)).hasSize(10)
                assertThat(getAllAddedInvites(guildIdString)).hasSize(10)
                assertThat(getAllImportedMembers(guildIdString)).hasSize(10)
                assertThat(getAllAddedMembers(guildIdString)).hasSize(10)
                assertThat(getAllImportedGuilds()).containsExactly(guild2)
                assertThat(getAllAddedGuilds()).isEmpty()
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
                val importedMembers = List(sizeData) { GuildMember(guildId, getRandomString()) }
                service.importMembers(importedMembers)

                val importedInvites = List(sizeData) {
                    GuildInvite(guildId, getRandomString(), null)
                }
                service.importInvitations(importedInvites)
                repeat(sizeData) {
                    service.addInvitation(guildId, getRandomString(), null)
                }

                val guildIdString = guildId.toString()

                assertThat(getAllImportedInvites(guildIdString)).hasSize(10)
                assertThat(getAllAddedInvites(guildIdString)).hasSize(10)
                assertThat(getAllRemovedInvites(guildIdString)).isEmpty()
                assertThat(getAllImportedMembers(guildIdString)).hasSize(10)
                assertThat(getAllAddedMembers(guildIdString)).hasSize(10)
                assertThat(getAllRemovedMembers(guildIdString)).isEmpty()

                assertTrue { service.deleteGuild(guild.id) }

                assertThat(getAllImportedInvites(guildIdString)).isEmpty()
                assertThat(getAllAddedInvites(guildIdString)).isEmpty()
                assertThat(getAllRemovedInvites(guildIdString)).isEmpty()
                assertThat(getAllImportedMembers(guildIdString)).isEmpty()
                assertThat(getAllAddedMembers(guildIdString)).isEmpty()
                assertThat(getAllRemovedMembers(guildIdString)).isEmpty()
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
                val importedMembers = List(sizeData) { GuildMember(guildId, getRandomString()) }
                service.importMembers(importedMembers)

                val importedInvites = List(sizeData) {
                    GuildInvite(guildId, getRandomString(), null)
                }
                service.importInvitations(importedInvites)
                repeat(sizeData) {
                    service.addInvitation(guildId, getRandomString(), null)
                }

                val guildIdString = guildId.toString()

                assertThat(getAllImportedInvites(guildIdString)).hasSize(10)
                assertThat(getAllAddedInvites(guildIdString)).hasSize(10)
                assertThat(getAllRemovedInvites(guildIdString)).isEmpty()
                assertThat(getAllImportedMembers(guildIdString)).hasSize(10)
                assertThat(getAllAddedMembers(guildIdString)).hasSize(10)
                assertThat(getAllRemovedMembers(guildIdString)).isEmpty()

                assertTrue { service.deleteGuild(guild.id) }

                assertThat(getAllImportedInvites(guildIdString)).hasSize(10)
                assertThat(getAllAddedInvites(guildIdString)).hasSize(10)
                assertThat(getAllRemovedInvites(guildIdString)).isEmpty()
                assertThat(getAllImportedMembers(guildIdString)).hasSize(10)
                assertThat(getAllAddedMembers(guildIdString)).hasSize(10)
                assertThat(getAllRemovedMembers(guildIdString)).isEmpty()
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
    @Disabled
    inner class ImportMembers {

        @Test
        fun `should return false with empty list`() = runTest {
            assertFalse { service.importMembers(emptyList()) }
        }

        @Test
        fun `should throw exception when guild is not a imported guild`() {
            TODO()
        }

        @Test
        fun `should throw exception when guild doesn't exist`() = runTest {
            assertThrows<GuildNotFoundException> {
                service.importMembers(List(5) { GuildMember(0, getRandomString()) })
            }
        }

        @Test
        fun `should filter out members marked as deleted`() = runTest {
            val guild = Guild(0, getRandomString(), getRandomString())
            service.importGuild(guild)
            val guildId = guild.id
            val guildIdString = guildId.toString()

            val members = List(5) { GuildMember(guildId, getRandomString()) }
            assertTrue { service.importMembers(members) }
            assertThat(getAllImportedMembers(guildIdString)).containsExactlyInAnyOrderElementsOf(members)
            assertThat(getAllAddedMembers(guildIdString)).isEmpty()
            assertThat(getAllRemovedMembers(guildIdString)).isEmpty()

            val membersToDelete = members.take(3)
            val membersToKeep = members.drop(3)

            membersToDelete.forEach {
                service.removeMember(guildId, it.entityId)
            }
            assertThat(getAllImportedMembers(guildIdString)).containsExactlyInAnyOrderElementsOf(membersToKeep)
            assertThat(getAllAddedMembers(guildIdString)).isEmpty()
            assertThat(getAllRemovedMembers(guildIdString)).containsExactlyInAnyOrderElementsOf(membersToDelete.map { it.entityId })

            val memberShouldBeImported = GuildMember(guildId, getRandomString())
            val newMembers = (membersToDelete + memberShouldBeImported)
            assertTrue { service.importMembers(newMembers) }
            assertThat(getAllImportedMembers(guildIdString)).containsExactlyInAnyOrderElementsOf(membersToKeep + memberShouldBeImported)
            assertThat(getAllAddedMembers(guildIdString)).isEmpty()
            assertThat(getAllRemovedMembers(guildIdString)).containsExactlyInAnyOrderElementsOf(membersToDelete.map { it.entityId })
        }

        @Test
        fun `should return false when all imported members are marked as deleted`() = runTest {
            val guild = Guild(0, getRandomString(), getRandomString())
            service.importGuild(guild)
            val guildId = guild.id
            val guildIdString = guildId.toString()

            val members = List(1000) { GuildMember(guildId, getRandomString()) }
            assertTrue { service.importMembers(members) }
            assertThat(getAllImportedMembers(guildIdString)).containsExactlyInAnyOrderElementsOf(members)
            assertThat(getAllAddedMembers(guildIdString)).isEmpty()

            assertThat(getAllRemovedMembers(guildIdString)).isEmpty()

            val membersDeleted = members.map { it.entityId }
            membersDeleted.forEach {
                service.removeMember(guildId, it)
            }

            assertThat(getAllImportedMembers(guildIdString)).isEmpty()
            assertThat(getAllAddedMembers(guildIdString)).isEmpty()
            assertThat(getAllRemovedMembers(guildIdString)).containsExactlyInAnyOrderElementsOf(membersDeleted)

            assertFalse { service.importMembers(members) }

            assertThat(getAllImportedMembers(guildIdString)).isEmpty()
            assertThat(getAllAddedMembers(guildIdString)).isEmpty()
            assertThat(getAllRemovedMembers(guildIdString)).containsExactlyInAnyOrderElementsOf(membersDeleted)
        }

        @Test
        fun `should set as imported the added members`() = runTest {
            val guild = Guild(0, getRandomString(), getRandomString())
            service.importGuild(guild)
            val guildId = guild.id
            val guildIdString = guildId.toString()

            val members = List(5) { GuildMember(guildId, getRandomString()) }
            val membersToAdd = members.take(3)
            val membersToImport = members.drop(3)

            membersToAdd.forEach {
                assertTrue { service.addMember(guildId, it.entityId) }
            }

            assertTrue { service.importMembers(membersToImport) }

            assertThat(getAllImportedMembers(guildIdString)).containsExactlyInAnyOrderElementsOf(membersToImport)
            assertThat(getAllAddedMembers(guildIdString)).containsOnlyOnceElementsOf(membersToAdd)

            assertTrue { service.importMembers(membersToAdd) }
            assertThat(getAllImportedMembers(guildIdString)).containsExactlyInAnyOrderElementsOf(members)
            assertThat(getAllAddedMembers(guildIdString)).isEmpty()
        }

        @Test
        fun `should import members in each targeted guild`() {
            TODO()
        }
    }

    @Nested
    @Disabled
    inner class AddMember {

        @Test
        fun todo() {
            TODO()
        }
    }

    @Nested
    @Disabled
    inner class RemoveMember {

        @Test
        fun todo() {
            TODO()
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
                    assertThat(getAllAddedInvites(guildIdString)).hasSize(1)
                    assertThat(getAllRemovedInvites(guildIdString)).isEmpty()
                    val inviteForGuild = getAllAddedInvites(guildIdString)

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
                    assertThat(getAllAddedInvites(guildIdString)).hasSize(1)
                    assertThat(getAllRemovedInvites(guildIdString)).isEmpty()
                    val inviteForGuild = getAllAddedInvites(guildIdString)

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

                val invitations = getAllAddedInvites(guildId.toString())
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

                assertThat(getAllAddedInvites(guildId.toString())).isEmpty()
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

                assertThat(getAllAddedInvites(guildId.toString())).containsExactly(
                    GuildInvite(guildId, entityId, null)
                )
                assertThat(getAllAddedInvites(guildId2.toString())).containsExactly(
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
                assertThat(getAllAddedInvites(it.id.toString())).isEmpty()
            }
        }

        @Test
        fun `when entity is already invited`() = runTest {
            withGuildImportedAndCreated {
                val guildId = it.id
                val entityId = getRandomString()
                val expiredAt = Instant.now().plusSeconds(10).truncatedTo(ChronoUnit.SECONDS)
                assertTrue { service.addInvitation(guildId, entityId, null) }
                assertTrue { service.addInvitation(guildId, entityId, null) }
                assertTrue { service.addInvitation(guildId, entityId, expiredAt) }

                assertThat(getAllAddedInvites(guildId.toString())).containsExactly(
                    GuildInvite(guildId, entityId, expiredAt)
                )
            }
        }

        @Test
        fun `when entity is already invited by an imported invitation`() = runTest {
            val guild = Guild(0, getRandomString(), getRandomString())
            service.importGuild(guild)
            val guildId = guild.id

            val invite = GuildInvite(guildId, getRandomString(), null)
            service.importInvitations(listOf(invite))

            assertFalse { service.addInvitation(invite.guildId, invite.entityId, invite.expiredAt) }
            assertThat(getAllAddedInvites(guildId.toString())).isEmpty()
            assertThat(getAllImportedInvites(guildId.toString())).containsExactly(invite)
            assertThat(getAllRemovedInvites(guildId.toString())).isEmpty()
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

    @Nested
    inner class RemoveInvitation {

        @Nested
        inner class WithAddedInvitation {

            @Test
            fun `when entity is invited in the guild`() = runTest {
                withGuildImportedAndCreated {
                    val guildId = it.id
                    val guildIdString = guildId.toString()
                    val entityId = getRandomString()
                    service.addInvitation(guildId, entityId, null)

                    assertThat(getAllAddedInvites(guildIdString)).containsExactly(
                        GuildInvite(guildId, entityId, null)
                    )

                    assertTrue { service.removeInvitation(guildId, entityId) }

                    assertThat(getAllAddedInvites(guildIdString)).isEmpty()
                    assertThat(getAllRemovedInvites(guildIdString)).isEmpty()
                    assertThat(getAllImportedInvites(guildIdString)).isEmpty()
                }
            }

            @Test
            fun `when another entity is invited in the guild`() = runTest {
                withGuildImportedAndCreated {
                    val guildId = it.id
                    val guildIdString = guildId.toString()
                    val entityId = getRandomString()
                    val entityId2 = getRandomString()

                    service.addInvitation(guildId, entityId, null)
                    assertFalse { service.removeInvitation(guildId, entityId2) }

                    assertThat(getAllAddedInvites(guildIdString)).containsExactly(
                        GuildInvite(guildId, entityId, null)
                    )
                    assertThat(getAllRemovedInvites(guildIdString)).isEmpty()
                    assertThat(getAllImportedInvites(guildIdString)).isEmpty()
                }
            }

        }

        @Nested
        inner class WithImportedInvitation {

            @Test
            fun `when entity is invited in the guild`() = runTest {
                val guild = Guild(0, getRandomString(), getRandomString())
                service.importGuild(guild)

                val guildId = guild.id
                val guildIdString = guildId.toString()
                val entityId = getRandomString()

                val expectedInvite = GuildInvite(guildId, entityId, null)
                service.importInvitations(listOf(expectedInvite))

                assertThat(getAllImportedInvites(guildIdString)).containsExactly(
                    expectedInvite
                )

                assertTrue { service.removeInvitation(guildId, entityId) }

                assertThat(getAllAddedInvites(guildIdString)).isEmpty()
                assertThat(getAllRemovedInvites(guildIdString)).containsExactly(
                    expectedInvite.entityId
                )
                assertThat(getAllImportedInvites(guildIdString)).isEmpty()
            }

            @Test
            fun `when another entity is invited in the guild`() = runTest {
                val guild = Guild(0, getRandomString(), getRandomString())
                service.importGuild(guild)

                val guildId = guild.id
                val guildIdString = guildId.toString()
                val entityId = getRandomString()
                val entityId2 = getRandomString()

                val expectedInvite = GuildInvite(guildId, entityId, null)
                service.importInvitations(listOf(expectedInvite))

                assertFalse { service.removeInvitation(guildId, entityId2) }

                assertThat(getAllAddedInvites(guildIdString)).isEmpty()
                assertThat(getAllRemovedInvites(guildIdString)).isEmpty()
                assertThat(getAllImportedInvites(guildIdString)).containsExactly(expectedInvite)
            }

        }

        @Test
        fun `when entity is not invited in the guild`() = runTest {
            val guild = Guild(0, getRandomString(), getRandomString())
            service.importGuild(guild)
            val guildId = guild.id
            val guildIdString = guildId.toString()
            val entityId = getRandomString()
            assertFalse { service.removeInvitation(guildId, entityId) }

            assertThat(getAllAddedInvites(guildIdString)).isEmpty()
            assertThat(getAllRemovedInvites(guildIdString)).isEmpty()
            assertThat(getAllImportedInvites(guildIdString)).isEmpty()
        }

        @Test
        fun `when guild does not exist`() = runTest {
            assertFalse { service.removeInvitation(0, getRandomString()) }
        }

        @Test
        fun `when entity is owner of the guild`() = runTest {
            withGuildImportedAndCreated {
                assertFalse { service.removeInvitation(it.id, it.ownerId) }
            }
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
    inner class GetInvitations {

        @Test
        fun `when guild has no invitations`() = runTest {
            withGuildImportedAndCreated {
                val invitations = service.getInvitations(it.id).toList()
                assertThat(invitations).isEmpty()
            }
        }

        @Test
        fun `should ignore empty value for entities`() = runTest {
            TODO()
        }

        @Test
        fun `when an entity is member but not invited`() = runTest {
            val guild = Guild(0, getRandomString(), getRandomString())
            service.importGuild(guild)
            service.addMember(guild.id, getRandomString())
            service.importMembers(listOf(GuildMember(guild.id, getRandomString())))

            val invitations = service.getInvitations(guild.id).toList()
            assertThat(invitations).isEmpty()
        }

        @Test
        fun `when guild does not exist`() = runTest {
            val members = service.getInvitations(0).toList()
            assertContentEquals(emptyList(), members)
        }

        @ParameterizedTest
        @ValueSource(ints = [1, 2, 3, 4, 5])
        fun `with added invitation`(number: Int) = runTest {
            withGuildImportedAndCreated {
                val guildId = it.id
                val invites = List(number) { GuildInvite(guildId, getRandomString(), null) }.onEach { invite ->
                    service.addInvitation(guildId, invite.entityId, null)
                }

                val invitations = service.getInvitations(guildId).toList()
                assertThat(invitations).containsExactlyInAnyOrderElementsOf(invites)
            }
        }

        @ParameterizedTest
        @ValueSource(ints = [1, 2, 3, 4, 5])
        fun `with added invitation that are deleted`(number: Int) = runTest {
            withGuildImportedAndCreated {
                val guildId = it.id
                val invitesAdded = List(number) { GuildInvite(guildId, getRandomString(), null) }.onEach { invite ->
                    service.addInvitation(guildId, invite.entityId, null)
                }

                invitesAdded.forEach { invite ->
                    service.removeInvitation(guildId, invite.entityId)
                }

                val invitationsAfterRemoval = service.getInvitations(guildId).toList()
                assertThat(invitationsAfterRemoval).isEmpty()
            }
        }

        @ParameterizedTest
        @ValueSource(ints = [1, 2, 3, 4, 5])
        fun `with imported invitation`(number: Int) = runTest {
            val guild = Guild(0, getRandomString(), getRandomString())
            service.importGuild(guild)
            val guildId = guild.id
            val expectedInvites = List(number) { GuildInvite(guildId, getRandomString(), null) }
            service.importInvitations(expectedInvites)

            val invites = service.getInvitations(guildId).toList()
            assertThat(invites).containsExactlyInAnyOrderElementsOf(expectedInvites)
        }

        @ParameterizedTest
        @ValueSource(ints = [1, 2, 3, 4, 5])
        fun `with imported invitation that are deleted`(number: Int) = runTest {
            val guild = Guild(0, getRandomString(), getRandomString())
            service.importGuild(guild)
            val guildId = guild.id
            val invites = List(number) { GuildInvite(guildId, getRandomString(), null) }
            service.importInvitations(invites)

            invites.forEach { invite ->
                service.removeInvitation(guildId, invite.entityId)
            }

            val invitationsAfterRemoval = service.getInvitations(guildId).toList()
            assertThat(invitationsAfterRemoval).isEmpty()
        }

        @ParameterizedTest
        @ValueSource(ints = [1, 2, 3, 4, 5])
        fun `with imported and added invitation`(number: Int) = runTest {
            val guild = Guild(0, getRandomString(), getRandomString())
            service.importGuild(guild)
            val guildId = guild.id

            val importedInvites = List(number) { GuildInvite(guildId, getRandomString(), null) }
            service.importInvitations(importedInvites)

            val addedInvites = List(number) { GuildInvite(guildId, getRandomString(), null) }.onEach { invite ->
                service.addInvitation(guildId, invite.entityId, null)
            }

            val invites = service.getInvitations(guildId).toList()
            assertThat(invites).containsExactlyInAnyOrderElementsOf(importedInvites + addedInvites)
        }
    }

    @Nested
    inner class ImportInvitations {

        @Test
        fun `with empty list`() = runTest {
            assertFalse { service.importInvitations(emptyList()) }
        }

        @Test
        fun `with one invitation with a non existing guild`() = runTest {
            val invites = listOf(GuildInvite(0, getRandomString(), null))
            assertThrows<GuildNotFoundException> {
                service.importInvitations(invites)
            }
        }

        @Test
        fun `should throw exception if at least one invited entity is member`() = runTest {
            val guild = Guild(0, getRandomString(), getRandomString())
            service.importGuild(guild)
            val guildId = guild.id
            val invites = listOf(
                GuildInvite(guildId, getRandomString(), null),
                GuildInvite(guildId, getRandomString(), null)
            )

            service.addMember(guildId, invites[0].entityId)

            assertThrows<GuildInvitedIsAlreadyMemberException> {
                service.importInvitations(invites)
            }

            val guildIdString = guildId.toString()
            assertThat(getAllImportedInvites(guildIdString)).isEmpty()
            assertThat(getAllAddedInvites(guildIdString)).isEmpty()
        }

        @Test
        fun `should throw exception if at least one guild is not found`() = runTest {
            val guild = Guild(0, getRandomString(), getRandomString())
            val guildId = guild.id
            val invites = listOf(
                GuildInvite(guildId, getRandomString(), null),
                GuildInvite(guildId + 1, getRandomString(), null)
            )
            assertThrows<GuildNotFoundException> {
                service.importInvitations(invites)
            }

            val guildIdString = guildId.toString()
            assertThat(getAllImportedInvites(guildIdString)).isEmpty()
            assertThat(getAllAddedInvites(guildIdString)).isEmpty()

            val guildIdString2 = (guildId + 1).toString()
            assertThat(getAllImportedInvites(guildIdString2)).isEmpty()
            assertThat(getAllAddedInvites(guildIdString2)).isEmpty()
        }

        @Test
        fun `should throw exception if at least one invitation is expired`() = runTest {
            val guild = Guild(0, getRandomString(), getRandomString())
            val guildId = guild.id
            val invites = listOf(
                GuildInvite(guildId, getRandomString(), null),
                GuildInvite(guildId, getRandomString(), Instant.now().minusSeconds(1))
            )

            assertThrows<IllegalArgumentException> {
                service.importInvitations(invites)
            }

            val guildIdString = guildId.toString()
            assertThat(getAllImportedInvites(guildIdString)).isEmpty()
            assertThat(getAllAddedInvites(guildIdString)).isEmpty()
        }

        @Test
        fun `should filter out invitation marked as deleted`() = runTest {
            val guild = Guild(0, getRandomString(), getRandomString())
            service.importGuild(guild)
            val guildId = guild.id
            val guildIdString = guildId.toString()

            val invites = List(5) { GuildInvite(guildId, getRandomString(), null) }
            assertTrue { service.importInvitations(invites) }
            assertThat(getAllImportedInvites(guildIdString)).containsExactlyInAnyOrderElementsOf(invites)
            assertThat(getAllAddedInvites(guildIdString)).isEmpty()
            assertThat(getAllRemovedInvites(guildIdString)).isEmpty()

            val invitesToDelete = invites.take(3)
            val invitesEntitiesDeleted = invitesToDelete.map { it.entityId }
            val invitesToKeep = invites.drop(3)

            invitesToDelete.forEach { invite ->
                service.removeInvitation(guildId, invite.entityId)
            }
            assertThat(getAllRemovedInvites(guildIdString)).containsExactlyInAnyOrderElementsOf(invitesEntitiesDeleted)
            assertThat(getAllImportedInvites(guildIdString)).containsExactlyInAnyOrderElementsOf(invitesToKeep)
            assertThat(getAllAddedInvites(guildIdString)).isEmpty()

            val inviteShouldBeImported = GuildInvite(guildId, getRandomString(), null)
            val newInvites = invitesToDelete + inviteShouldBeImported
            assertTrue { service.importInvitations(newInvites) }
            assertThat(getAllImportedInvites(guildIdString)).containsExactlyInAnyOrderElementsOf(invitesToKeep + inviteShouldBeImported)
            assertThat(getAllRemovedInvites(guildIdString)).containsExactlyInAnyOrderElementsOf(invitesEntitiesDeleted)
            assertThat(getAllAddedInvites(guildIdString)).isEmpty()
        }

        @Test
        fun `should return false when all imported invitations are marked as deleted`() = runTest {
            val guild = Guild(0, getRandomString(), getRandomString())
            service.importGuild(guild)
            val guildId = guild.id
            val guildIdString = guildId.toString()

            val invites = List(5) { GuildInvite(guildId, getRandomString(), null) }
            assertTrue { service.importInvitations(invites) }
            assertThat(getAllImportedInvites(guildIdString)).containsExactlyInAnyOrderElementsOf(invites)
            assertThat(getAllAddedInvites(guildIdString)).isEmpty()
            assertThat(getAllRemovedInvites(guildIdString)).isEmpty()

            invites.forEach { invite ->
                service.removeInvitation(guildId, invite.entityId)
            }
            val invitesEntitiesDeleted = invites.map { it.entityId }
            assertThat(getAllRemovedInvites(guildIdString)).containsExactlyInAnyOrderElementsOf(invitesEntitiesDeleted)
            assertThat(getAllImportedInvites(guildIdString)).isEmpty()
            assertThat(getAllAddedInvites(guildIdString)).isEmpty()

            assertFalse { service.importInvitations(invites) }
            assertThat(getAllRemovedInvites(guildIdString)).containsExactlyInAnyOrderElementsOf(invitesEntitiesDeleted)
            assertThat(getAllImportedInvites(guildIdString)).isEmpty()
            assertThat(getAllAddedInvites(guildIdString)).isEmpty()
        }

        @Test
        fun `should set as imported the added invitations`() = runTest {
            val guild = Guild(0, getRandomString(), getRandomString())
            service.importGuild(guild)
            val guildId = guild.id
            val guildIdString = guildId.toString()

            val invites = List(5) { GuildInvite(guildId, getRandomString(), null) }
            val invitesToAdd = invites.take(3)
            val invitesToImport = invites.drop(3)

            invitesToAdd.forEach { invite ->
                assertTrue { service.addInvitation(invite.guildId, invite.entityId, invite.expiredAt) }
            }
            assertTrue { service.importInvitations(invitesToImport) }

            assertThat(getAllImportedInvites(guildIdString)).containsExactlyInAnyOrderElementsOf(invitesToImport)
            assertThat(getAllAddedInvites(guildIdString)).containsOnlyOnceElementsOf(invitesToAdd)

            assertTrue { service.importInvitations(invitesToAdd) }
            assertThat(getAllImportedInvites(guildIdString)).containsExactlyInAnyOrderElementsOf(invites)
            assertThat(getAllAddedInvites(guildIdString)).isEmpty()
        }

        @Test
        fun `should import invitations in each targeted guild`() = runTest {
            val guild1 = Guild(0, getRandomString(), getRandomString())
            val guild2 = Guild(1, getRandomString(), getRandomString())
            service.importGuild(guild1)
            service.importGuild(guild2)

            val invites = listOf(
                GuildInvite(guild1.id, getRandomString(), null),
                GuildInvite(guild1.id, getRandomString(), null),
                GuildInvite(guild2.id, getRandomString(), null),
                GuildInvite(guild2.id, getRandomString(), null),
                GuildInvite(guild2.id, getRandomString(), null)
            )

            assertTrue { service.importInvitations(invites) }
            assertThat(getAllImportedInvites(guild1.id.toString())).containsExactlyInAnyOrderElementsOf(invites.take(2))
            assertThat(getAllImportedInvites(guild2.id.toString())).containsExactlyInAnyOrderElementsOf(invites.drop(2))
            assertThat(getAllAddedInvites(guild1.id.toString())).isEmpty()
            assertThat(getAllAddedInvites(guild2.id.toString())).isEmpty()
            assertThat(getAllRemovedInvites(guild1.id.toString())).isEmpty()
            assertThat(getAllRemovedInvites(guild2.id.toString())).isEmpty()
        }

        @Test
        fun `should throw exception when guild is not a imported guild`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val guildId = guild.id

            assertThrows<IllegalArgumentException> {
                service.importInvitations(listOf(GuildInvite(guildId, getRandomString(), null)))
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
            it.smembers(key).map { value ->
                cacheClient.binaryFormat.decodeFromByteArray(Int.serializer(), value)
            }.toList()
        }
    }

    private suspend fun getAllImportedInvites(guildId: String): List<GuildInvite> {
        return getAllMapValues(GuildCacheService.Type.IMPORT_INVITATION, guildId)
            .map { keyValue ->
                cacheClient.binaryFormat.decodeFromByteArray(GuildInvite.serializer(), keyValue.value)
            }.toList()
    }

    private suspend fun getAllAddedInvites(guildId: String): List<GuildInvite> {
        return getAllMapValues(GuildCacheService.Type.ADD_INVITATION, guildId)
            .map { keyValue ->
                cacheClient.binaryFormat.decodeFromByteArray(GuildInvite.serializer(), keyValue.value)
            }.toList()
    }

    private suspend fun getAllMapValues(
        type: GuildCacheService.Type,
        guildId: String
    ): Flow<KeyValue<ByteArray, ByteArray>> {
        return cacheClient.connect {
            val searchKey = service.prefixKey.format(guildId) + type.key
            it.hgetall(searchKey.encodeToByteArray())
        }
    }

    private suspend fun getAllRemovedInvites(guildId: String): List<String> {
        return getAllValuesOfSet(GuildCacheService.Type.REMOVE_INVITATION, guildId).map {
            cacheClient.binaryFormat.decodeFromByteArray(String.serializer(), it)
        }
    }

    private suspend fun getAllImportedMembers(guildId: String): List<GuildMember> {
        return getAllMapValues(GuildCacheService.Type.IMPORT_MEMBER, guildId)
            .map { keyValue ->
                cacheClient.binaryFormat.decodeFromByteArray(GuildMember.serializer(), keyValue.value)
            }.toList()
    }

    private suspend fun getAllAddedMembers(guildId: String): List<GuildMember> {
        return getAllMapValues(GuildCacheService.Type.ADD_MEMBER, guildId)
            .map { keyValue ->
                cacheClient.binaryFormat.decodeFromByteArray(GuildMember.serializer(), keyValue.value)
            }.toList()
    }

    private suspend fun getAllRemovedMembers(guildId: String): List<String> {
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