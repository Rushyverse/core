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
import kotlinx.serialization.encodeToByteArray
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
            fun `should create and store a new guild`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                assertThat(getAllAddedGuilds()).containsExactly(guild)
                assertThat(getAllImportedGuilds()).isEmpty()
                assertThat(getAllDeletedGuilds()).isEmpty()
            }

            @Test
            fun `should create guild if owner is an owner of another guild`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                val guild2 = service.createGuild(getRandomString(), guild.ownerId)
                assertNotEquals(guild.id, guild2.id)

                val guilds = getAllAddedGuilds()
                assertThat(guilds).containsExactlyInAnyOrder(guild, guild2)
                assertThat(getAllImportedGuilds()).isEmpty()
                assertThat(getAllDeletedGuilds()).isEmpty()
            }

            @Test
            fun `should create guild if owner is a member of another guild`() = runTest {
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
            fun `should throw if id is blank`(id: String) = runTest {
                assertThrows<IllegalArgumentException> {
                    service.createGuild(getRandomString(), id)
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

                val guilds = getAllAddedGuilds()
                assertThat(guilds).containsExactlyInAnyOrder(guild, guild2)
                assertThat(getAllImportedGuilds()).isEmpty()
                assertThat(getAllDeletedGuilds()).isEmpty()
            }

            @ParameterizedTest
            @ValueSource(strings = ["", " ", "  ", "   "])
            fun `should throw if name is blank`(name: String) = runTest {
                assertThrows<IllegalArgumentException> {
                    service.createGuild(name, getRandomString())
                }
            }
        }

        @Test
        fun `should create distinct id for each guild created`() = runTest {
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
        fun `should throw if id is negative`(id: Int) = runTest {
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
            fun `should return true if the guild exists`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                assertTrue { service.deleteGuild(guild.id) }
                assertThat(getAllAddedGuilds()).isEmpty()
                assertThat(getAllDeletedGuilds()).isEmpty()
                assertThat(getAllImportedGuilds()).isEmpty()
            }

            @Test
            fun `should delete only the targeted guild`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                val guild2 = service.createGuild(getRandomString(), getRandomString())

                assertTrue { service.deleteGuild(guild.id) }
                assertThat(getAllAddedGuilds()).containsExactlyInAnyOrder(guild2)
                assertThat(getAllDeletedGuilds()).isEmpty()
                assertThat(getAllImportedGuilds()).isEmpty()
            }

            @Test
            fun `should return false if the guild is deleted`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                assertTrue { service.deleteGuild(guild.id) }
                assertFalse { service.deleteGuild(guild.id) }

                assertThat(getAllAddedGuilds()).isEmpty()
                assertThat(getAllDeletedGuilds()).isEmpty()
                assertThat(getAllImportedGuilds()).isEmpty()
            }

            @Test
            fun `should delete all linked data to the deleted guild`() = runTest {
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
                val members = List(sizeData) { GuildMember(guildId, getRandomString()) }
                members.forEach { service.importMember(it) }

                repeat(sizeData) {
                    service.addInvitation(guildId, getRandomString(), null)
                }
                val invites = List(sizeData) { GuildInvite(guildId, getRandomString(), null) }
                invites.forEach { service.importInvitation(it) }

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
            fun `should return true if the guild exists`() = runTest {
                val guild = Guild(0, getRandomString(), getRandomString())
                service.importGuild(guild)
                assertTrue { service.deleteGuild(guild.id) }
                assertThat(getAllAddedGuilds()).isEmpty()
                assertThat(getAllDeletedGuilds()).containsExactly(guild.id)
                assertThat(getAllImportedGuilds()).isEmpty()
            }

            @Test
            fun `should delete only the targeted guild`() = runTest {
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
            fun `should return false if the guild is deleted`() = runTest {
                val guild = Guild(0, getRandomString(), getRandomString())
                service.importGuild(guild)
                assertTrue { service.deleteGuild(guild.id) }
                assertFalse { service.deleteGuild(guild.id) }

                assertThat(getAllAddedGuilds()).isEmpty()
                assertThat(getAllDeletedGuilds()).containsExactly(guild.id)
                assertThat(getAllImportedGuilds()).isEmpty()
            }

            @Test
            fun `should delete all linked data to the deleted guild`() = runTest {
                val guild = Guild(0, getRandomString(), getRandomString())
                service.importGuild(guild)

                val guildId = guild.id
                val sizeData = 10
                repeat(sizeData) {
                    service.addMember(guildId, getRandomString())
                }
                val importedMembers = List(sizeData) { GuildMember(guildId, getRandomString()) }
                importedMembers.forEach { service.importMember(it) }

                val importedInvites = List(sizeData) {
                    GuildInvite(guildId, getRandomString(), null)
                }
                importedInvites.forEach { service.importInvitation(it) }
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
                importedMembers.forEach { service.importMember(it) }

                val importedInvites = List(sizeData) {
                    GuildInvite(guildId, getRandomString(), null)
                }
                importedInvites.forEach { service.importInvitation(it) }
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
        fun `should return false if the guild doesn't exist`(id: Int) = runTest {
            assertFalse { service.deleteGuild(id) }
            assertThat(getAllAddedGuilds()).isEmpty()
            assertThat(getAllDeletedGuilds()).isEmpty()
            assertThat(getAllImportedGuilds()).isEmpty()
        }
    }

    @Nested
    inner class GetGuildById {

        @Test
        fun `should return the guild`() = runTest {
            withGuildImportedAndCreated {
                assertEquals(it, service.getGuild(it.id))
            }
        }

        @Test
        fun `should return null if the guild is deleted`() = runTest {
            withGuildImportedAndCreated {
                service.deleteGuild(it.id)
                assertNull(service.getGuild(it.id))
            }
        }

        @Test
        fun `should return each guild with the corresponding id`() = runTest {
            withGuildImportedAndCreated {
                val guild2 = service.createGuild(getRandomString(), getRandomString())
                assertEquals(it, service.getGuild(it.id))
                assertEquals(guild2, service.getGuild(guild2.id))
            }
        }

        @Test
        fun `should return null if the guild doesn't exist`() = runTest {
            assertNull(service.getGuild(0))
        }
    }

    @Nested
    inner class GetGuildByName {

        @Test
        fun `should return the guild with the same name`() = runTest {
            withGuildImportedAndCreated {
                assertThat(service.getGuild(it.name).toList()).containsExactly(it)
            }
        }

        @Test
        fun `should return empty flow if the guild is deleted`() = runTest {
            withGuildImportedAndCreated {
                service.deleteGuild(it.id)
                assertThat(service.getGuild(it.name).toList()).isEmpty()
            }
        }

        @Test
        fun `should return the guild for each corresponding name`() = runTest {
            withGuildImportedAndCreated {
                val guild2 = service.createGuild(getRandomString(), getRandomString())
                assertThat(service.getGuild(it.name).toList()).containsExactly(it)
                assertThat(service.getGuild(guild2.name).toList()).containsExactly(guild2)
            }
        }

        @Test
        fun `should return empty flow when no guild has the name`() = runTest {
            assertThat(service.getGuild(getRandomString()).toList()).isEmpty()
        }

        @ParameterizedTest
        @ValueSource(strings = ["", " ", "  ", "   "])
        fun `should throw if name is blank`(name: String) = runTest {
            assertThrows<IllegalArgumentException> {
                service.getGuild(name)
            }
        }

        @Test
        fun `should retrieve several guilds with the same name`() = runTest {
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
        fun `should return true if the entity is owner of the guild`() = runTest {
            withGuildImportedAndCreated {
                assertTrue { service.isOwner(it.id, it.ownerId) }
            }
        }

        @Test
        fun `should return true if entity is owner of several guilds`() = runTest {
            withGuildImportedAndCreated {
                val ownerId = it.ownerId
                val guild2 = service.createGuild(getRandomString(), ownerId)
                assertTrue { service.isOwner(it.id, ownerId) }
                assertTrue { service.isOwner(guild2.id, ownerId) }
            }
        }

        @Test
        fun `should return false if the entity is not the owner`() = runTest {
            withGuildImportedAndCreated {
                assertFalse { service.isOwner(it.id, getRandomString()) }
            }
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
        fun `should return true if entity is member`() = runTest {
            withGuildImportedAndCreated {
                val entityId = getRandomString()
                service.addMember(it.id, entityId)
                assertTrue { service.isMember(it.id, entityId) }
            }
        }

        @Test
        fun `should return false when entity is not member`() = runTest {
            withGuildImportedAndCreated {
                val entityId = getRandomString()
                assertFalse { service.isMember(it.id, entityId) }
            }
        }

        @Test
        fun `should return false when entity is invited`() = runTest {
            withGuildImportedAndCreated {
                val entityId = getRandomString()
                service.addInvitation(it.id, entityId, null)
                assertFalse { service.isMember(it.id, entityId) }
            }
        }

        @Test
        fun `should return false if entity is member of another guild`() = runTest {
            withGuildImportedAndCreated {
                val entityId = getRandomString()
                val entity2Id = getRandomString()
                service.addMember(it.id, entity2Id)
                assertFalse { service.isMember(it.id, entityId) }
            }
        }

        @Test
        fun `should return false after the deletion of the member`() = runTest {
            withGuildImportedAndCreated {
                val entityId = getRandomString()
                service.addMember(it.id, entityId)
                assertTrue { service.isMember(it.id, entityId) }

                service.removeMember(it.id, entityId)
                assertFalse { service.isMember(it.id, entityId) }
            }
        }

        @Test
        fun `should return true if the entity is owner`() = runTest {
            withGuildImportedAndCreated {
                assertTrue { service.isMember(it.id, it.ownerId) }
            }
        }

        @ParameterizedTest
        @ValueSource(ints = [-1, 0, 1])
        fun `should return false if the guild doesn't exist`(id: Int) = runTest {
            assertFalse { service.isMember(id, getRandomString()) }
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
    inner class ImportMember {

        @Test
        fun `should keep integrity of member data`() = runTest {
            val guild = Guild(0, getRandomString(), getRandomString())
            service.importGuild(guild)
            val guildId = guild.id

            val createdAt = Instant.now().plusSeconds(10).truncatedTo(ChronoUnit.MILLIS)
            val member = GuildMember(guildId, getRandomString(), createdAt)
            service.importMember(member)

            val guildIdString = guildId.toString()
            assertThat(getAllImportedMembers(guildIdString)).containsExactly(member)
        }

        @Test
        fun `should throw exception if member is for a non existing guild`() = runTest {
            val guildId = 0

            val member = GuildMember(guildId, getRandomString())
            assertThrows<GuildNotFoundException> {
                service.importMember(member)
            }

            assertThat(getAllImportedMembers(guildId.toString())).isEmpty()
            assertThat(getAllAddedMembers(guildId.toString())).isEmpty()
        }

        @Test
        fun `should throw exception if the owner is added as member`() = runTest {
            val guild = Guild(0, getRandomString(), getRandomString())
            service.importGuild(guild)
            val guildId = guild.id
            val member = GuildMember(guildId, guild.ownerId)

            assertThrows<GuildMemberIsOwnerOfGuildException> {
                service.importMember(member)
            }

            val guildIdString = guildId.toString()
            assertThat(getAllImportedMembers(guildIdString)).isEmpty()
            assertThat(getAllAddedMembers(guildIdString)).isEmpty()
        }

        @Test
        fun `should return false when if member is marked as deleted`() = runTest {
            val guild = Guild(0, getRandomString(), getRandomString())
            service.importGuild(guild)
            val guildId = guild.id
            val guildIdString = guildId.toString()

            val member = GuildMember(guildId, getRandomString())
            assertTrue { service.importMember(member) }
            assertThat(getAllImportedMembers(guildIdString)).containsExactly(member)
            assertThat(getAllAddedMembers(guildIdString)).isEmpty()

            assertThat(getAllRemovedMembers(guildIdString)).isEmpty()

            service.removeMember(guildId, member.entityId)

            assertThat(getAllImportedMembers(guildIdString)).isEmpty()
            assertThat(getAllAddedMembers(guildIdString)).isEmpty()
            assertThat(getAllRemovedMembers(guildIdString)).containsExactly(member.entityId)

            assertFalse { service.importMember(member) }

            assertThat(getAllImportedMembers(guildIdString)).isEmpty()
            assertThat(getAllAddedMembers(guildIdString)).isEmpty()
            assertThat(getAllRemovedMembers(guildIdString)).containsExactly(member.entityId)
        }

        @Test
        fun `should set as imported the added member`() = runTest {
            val guild = Guild(0, getRandomString(), getRandomString())
            service.importGuild(guild)
            val guildId = guild.id
            val guildIdString = guildId.toString()

            val member = GuildMember(guildId, getRandomString())

            assertTrue { service.addMember(guildId, member.entityId) }

            assertTrue { service.importMember(member) }

            assertThat(getAllImportedMembers(guildIdString)).containsExactly(member)
            assertThat(getAllAddedMembers(guildIdString)).isEmpty()
        }

        @Test
        fun `should return true when member is updated`() = runTest {
            val guild = Guild(0, getRandomString(), getRandomString())
            service.importGuild(guild)
            val guildId = guild.id
            val guildIdString = guildId.toString()

            val member = GuildMember(guildId, getRandomString())
            assertTrue { service.importMember(member) }

            assertThat(getAllImportedMembers(guildIdString)).containsExactly(member)

            val newMembers = member.copy(createdAt = Instant.now().plusSeconds(1).truncatedTo(ChronoUnit.MILLIS))
            assertTrue { service.importMember(newMembers) }
            assertThat(getAllImportedMembers(guildIdString)).containsExactly(newMembers)
            assertThat(getAllAddedMembers(guildIdString)).isEmpty()
        }

        @Test
        fun `should return false when member is already imported with the same values`() = runTest {
            val guild = Guild(0, getRandomString(), getRandomString())
            service.importGuild(guild)
            val guildId = guild.id
            val guildIdString = guildId.toString()

            val member = GuildMember(guildId, getRandomString())
            assertTrue { service.importMember(member) }
            assertFalse { service.importMember(member) }
            assertThat(getAllImportedMembers(guildIdString)).containsExactly(member)
            assertThat(getAllAddedMembers(guildIdString)).isEmpty()
        }

        @Test
        fun `should throw exception when guild is not a imported guild`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val guildId = guild.id

            assertThrows<IllegalArgumentException> {
                service.importMember(GuildMember(guildId, getRandomString()))
            }
        }

    }

    @Nested
    inner class AddMember {

        @Test
        fun `should return true if the entity is member`() = runTest {
            withGuildImportedAndCreated {
                val guildId = it.id
                val entityId = getRandomString()
                assertTrue { service.addMember(guildId, entityId) }

                val members = getAllAddedMembers(guildId.toString())
                assertThat(members).containsExactly(GuildMember(guildId, entityId))
            }
        }

        @Test
        fun `should return false if the entity is already a member of the guild`() = runTest {
            withGuildImportedAndCreated {
                val guildId = it.id
                val entityId = getRandomString()
                assertTrue { service.addMember(guildId, entityId) }
                assertFalse { service.addMember(guildId, entityId) }

                assertThat(getAllImportedMembers(guildId.toString())).isEmpty()
                assertThat(getAllAddedMembers(guildId.toString())).containsExactly(GuildMember(guildId, entityId))
                assertThat(getAllRemovedMembers(guildId.toString())).isEmpty()
            }
        }

        @Test
        fun `should return true if the entity is member of another guild`() = runTest {
            withGuildImportedAndCreated {
                val guildId = it.id
                val guild2 = service.createGuild(getRandomString(), getRandomString())
                val guildId2 = guild2.id

                val entityId = getRandomString()
                assertTrue { service.addMember(guildId, entityId) }
                assertTrue { service.addMember(guildId2, entityId) }

                assertThat(getAllAddedMembers(guildId.toString())).containsExactly(
                    GuildMember(guildId, entityId)
                )
                assertThat(getAllAddedMembers(guildId2.toString())).containsExactly(
                    GuildMember(guildId2, entityId)
                )
            }
        }

        @Test
        fun `should return false if the entity is the owner`() = runTest {
            withGuildImportedAndCreated {
                assertThrows<GuildMemberIsOwnerOfGuildException> {
                    service.addMember(it.id, it.ownerId)
                }
                assertThat(getAllAddedMembers(it.id.toString())).isEmpty()
            }
        }

        @Test
        fun `should return false if entity already member by an imported member`() = runTest {
            val guild = Guild(0, getRandomString(), getRandomString())
            service.importGuild(guild)
            val guildId = guild.id

            val member = GuildMember(guildId, getRandomString())
            service.importMember(member)

            assertFalse { service.addMember(member.guildId, member.entityId) }
            assertThat(getAllAddedMembers(guildId.toString())).isEmpty()
            assertThat(getAllImportedMembers(guildId.toString())).containsExactly(member)
            assertThat(getAllRemovedMembers(guildId.toString())).isEmpty()
        }

        @ParameterizedTest
        @ValueSource(strings = ["", " ", "  ", "   "])
        fun `should throw exception if id is blank`(id: String) = runTest {
            assertThrows<IllegalArgumentException> {
                service.addMember(0, id)
            }
        }

        @ParameterizedTest
        @ValueSource(ints = [-1, 0, 1])
        fun `should throw exception if guild doesn't exist`(guildId: Int) = runTest {
            assertThrows<GuildNotFoundException> {
                service.addMember(guildId, getRandomString())
            }
        }
    }

    @Nested
    inner class RemoveMember {

        @Nested
        inner class WithAddedMember {

            @Test
            fun `should remove if guild comes from cache`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                val guildId = guild.id
                val guildIdString = guildId.toString()
                val entityId = getRandomString()
                service.addMember(guildId, entityId)

                assertThat(getAllAddedMembers(guildIdString)).containsExactly(
                    GuildMember(guildId, entityId)
                )

                assertTrue { service.removeMember(guildId, entityId) }

                assertThat(getAllAddedMembers(guildIdString)).isEmpty()
                assertThat(getAllRemovedMembers(guildIdString)).isEmpty()
                assertThat(getAllImportedMembers(guildIdString)).isEmpty()
            }

            @Test
            fun `should remove and mark as deleted if guild is imported`() = runTest {
                val guild = Guild(0, getRandomString(), getRandomString())
                service.importGuild(guild)
                val guildId = guild.id
                val guildIdString = guildId.toString()
                val entityId = getRandomString()
                service.addMember(guildId, entityId)

                assertThat(getAllAddedMembers(guildIdString)).containsExactly(
                    GuildMember(guildId, entityId)
                )

                assertTrue { service.removeMember(guildId, entityId) }

                assertThat(getAllAddedMembers(guildIdString)).isEmpty()
                assertThat(getAllRemovedMembers(guildIdString)).containsExactly(
                    entityId
                )
                assertThat(getAllImportedMembers(guildIdString)).isEmpty()
            }

            @Test
            fun `should return false if another entity is member in the guild`() = runTest {
                withGuildImportedAndCreated {
                    val guildId = it.id
                    val guildIdString = guildId.toString()
                    val entityId = getRandomString()
                    val entityId2 = getRandomString()

                    service.addMember(guildId, entityId)
                    assertFalse { service.removeMember(guildId, entityId2) }

                    assertThat(getAllAddedMembers(guildIdString)).containsExactly(
                        GuildMember(guildId, entityId)
                    )
                    assertThat(getAllRemovedMembers(guildIdString)).isEmpty()
                    assertThat(getAllImportedMembers(guildIdString)).isEmpty()
                }
            }

        }

        @Nested
        inner class WithImportedMember {

            @Test
            fun `should return true if entity is member in the guild`() = runTest {
                val guild = Guild(0, getRandomString(), getRandomString())
                service.importGuild(guild)

                val guildId = guild.id
                val guildIdString = guildId.toString()
                val entityId = getRandomString()

                val expectedMember = GuildMember(guildId, entityId)
                service.importMember(expectedMember)

                assertThat(getAllImportedMembers(guildIdString)).containsExactly(
                    expectedMember
                )

                assertTrue { service.removeMember(guildId, entityId) }

                assertThat(getAllAddedMembers(guildIdString)).isEmpty()
                assertThat(getAllRemovedMembers(guildIdString)).containsExactly(
                    expectedMember.entityId
                )
                assertThat(getAllImportedMembers(guildIdString)).isEmpty()
            }

            @Test
            fun `should return false if another entity is member in the guild`() = runTest {
                val guild = Guild(0, getRandomString(), getRandomString())
                service.importGuild(guild)

                val guildId = guild.id
                val guildIdString = guildId.toString()
                val entityId = getRandomString()
                val entityId2 = getRandomString()

                val expectedMember = GuildMember(guildId, entityId)
                service.importMember(expectedMember)

                assertFalse { service.removeMember(guildId, entityId2) }

                assertThat(getAllAddedMembers(guildIdString)).isEmpty()
                assertThat(getAllRemovedMembers(guildIdString)).isEmpty()
                assertThat(getAllImportedMembers(guildIdString)).containsExactly(expectedMember)
            }

        }

        @Test
        fun `should return false if entity is not member`() = runTest {
            val guild = Guild(0, getRandomString(), getRandomString())
            service.importGuild(guild)
            val guildId = guild.id
            val guildIdString = guildId.toString()
            val entityId = getRandomString()
            assertFalse { service.removeMember(guildId, entityId) }

            assertThat(getAllAddedMembers(guildIdString)).isEmpty()
            assertThat(getAllRemovedMembers(guildIdString)).isEmpty()
            assertThat(getAllImportedMembers(guildIdString)).isEmpty()
        }

        @Test
        fun `should return false if guild doesn't exist`() = runTest {
            assertFalse { service.removeMember(0, getRandomString()) }
        }

        @Test
        fun `should return false if remove the member for owner`() = runTest {
            withGuildImportedAndCreated {
                assertFalse { service.removeMember(it.id, it.ownerId) }
            }
        }

        @ParameterizedTest
        @ValueSource(strings = ["", " ", "  ", "   "])
        fun `should throw exception if the name is blank`(id: String) = runTest {
            assertThrows<IllegalArgumentException> {
                service.removeMember(0, id)
            }
        }
    }

    @Nested
    inner class AddInvitation {

        @Nested
        inner class ExpirationDate {

            @Test
            fun `should keep the expiration date integrity`() = runTest {
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
            fun `should save the invitation without expiration`() = runTest {
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
            fun `should throw exception if the expire date is before now`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                val entityId = getRandomString()
                val expiredAt = Instant.now().minusMillis(1)

                assertThrows<IllegalArgumentException> {
                    service.addInvitation(guild.id, entityId, expiredAt)
                }
            }
        }

        @Test
        fun `should return true if the entity is invited`() = runTest {
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
        fun `should throw exception if the entity is already a member of the guild`() = runTest {
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
        fun `should return true if the entity is invited in another guild`() = runTest {
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
        fun `should throw exception if the entity is the owner`() = runTest {
            withGuildImportedAndCreated {
                assertThrows<GuildInvitedIsAlreadyMemberException> {
                    service.addInvitation(it.id, it.ownerId, null)
                }
                assertThat(getAllAddedInvites(it.id.toString())).isEmpty()
            }
        }

        @Test
        fun `should update the added invitation`() = runTest {
            withGuildImportedAndCreated {
                val guildId = it.id
                val entityId = getRandomString()
                val expiredAt = Instant.now().plusSeconds(10).truncatedTo(ChronoUnit.SECONDS)
                assertTrue { service.addInvitation(guildId, entityId, null) }
                assertFalse { service.addInvitation(guildId, entityId, null) }
                assertTrue { service.addInvitation(guildId, entityId, expiredAt) }

                assertThat(getAllAddedInvites(guildId.toString())).containsExactly(
                    GuildInvite(guildId, entityId, expiredAt)
                )
            }
        }

        @Test
        fun `should return false if entity already invited by an imported invitation`() = runTest {
            val guild = Guild(0, getRandomString(), getRandomString())
            service.importGuild(guild)
            val guildId = guild.id

            val invite = GuildInvite(guildId, getRandomString(), null)
            service.importInvitation(invite)

            assertFalse { service.addInvitation(invite.guildId, invite.entityId, invite.expiredAt) }
            assertThat(getAllAddedInvites(guildId.toString())).isEmpty()
            assertThat(getAllImportedInvites(guildId.toString())).containsExactly(invite)
            assertThat(getAllRemovedInvites(guildId.toString())).isEmpty()
        }

        @ParameterizedTest
        @ValueSource(strings = ["", " ", "  ", "   "])
        fun `should throw exception if id is blank`(id: String) = runTest {
            assertThrows<IllegalArgumentException> {
                service.addInvitation(0, id, null)
            }
        }

        @ParameterizedTest
        @ValueSource(ints = [-1, 0, 1])
        fun `should throw exception if guild doesn't exist`(guildId: Int) = runTest {
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
            fun `should remove if guild comes from cache`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                val guildId = guild.id
                val guildIdString = guildId.toString()
                val entityId = getRandomString()
                service.addInvitation(guildId, entityId, null)

                assertThat(getAllAddedInvites(guildIdString)).containsExactly(
                    GuildInvite(guildId, entityId, null)
                )

                assertTrue { service.removeInvitation(guildId, entityId) }

                assertThat(getAllAddedMembers(guildIdString)).isEmpty()
                assertThat(getAllRemovedMembers(guildIdString)).isEmpty()
                assertThat(getAllImportedMembers(guildIdString)).isEmpty()
            }

            @Test
            fun `should remove and mark as deleted if guild is imported`() = runTest {
                val guild = Guild(0, getRandomString(), getRandomString())
                service.importGuild(guild)
                val guildId = guild.id
                val guildIdString = guildId.toString()
                val entityId = getRandomString()
                service.addMember(guildId, entityId)

                assertThat(getAllAddedMembers(guildIdString)).containsExactly(
                    GuildMember(guildId, entityId)
                )

                assertTrue { service.removeMember(guildId, entityId) }

                assertThat(getAllAddedMembers(guildIdString)).isEmpty()
                assertThat(getAllRemovedMembers(guildIdString)).containsExactly(
                    entityId
                )
                assertThat(getAllImportedMembers(guildIdString)).isEmpty()
            }

            @Test
            fun `should return false if another entity is invited in the guild`() = runTest {
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
            fun `should return true if entity is invited in the guild`() = runTest {
                val guild = Guild(0, getRandomString(), getRandomString())
                service.importGuild(guild)

                val guildId = guild.id
                val guildIdString = guildId.toString()
                val entityId = getRandomString()

                val expectedInvite = GuildInvite(guildId, entityId, null)
                service.importInvitation(expectedInvite)

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
            fun `should return false if another entity is invited in the guild`() = runTest {
                val guild = Guild(0, getRandomString(), getRandomString())
                service.importGuild(guild)

                val guildId = guild.id
                val guildIdString = guildId.toString()
                val entityId = getRandomString()
                val entityId2 = getRandomString()

                val expectedInvite = GuildInvite(guildId, entityId, null)
                service.importInvitation(expectedInvite)

                assertFalse { service.removeInvitation(guildId, entityId2) }

                assertThat(getAllAddedInvites(guildIdString)).isEmpty()
                assertThat(getAllRemovedInvites(guildIdString)).isEmpty()
                assertThat(getAllImportedInvites(guildIdString)).containsExactly(expectedInvite)
            }

        }

        @Test
        fun `should return false if entity is not invited`() = runTest {
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
        fun `should return false if guild doesn't exist`() = runTest {
            assertFalse { service.removeInvitation(0, getRandomString()) }
        }

        @Test
        fun `should return false if remove the invitation for owner`() = runTest {
            withGuildImportedAndCreated {
                assertFalse { service.removeInvitation(it.id, it.ownerId) }
            }
        }

        @ParameterizedTest
        @ValueSource(strings = ["", " ", "  ", "   "])
        fun `should throw exception if the name is blank`(id: String) = runTest {
            assertThrows<IllegalArgumentException> {
                service.removeInvitation(0, id)
            }
        }
    }

    @Nested
    inner class GetInvitations {

        @Test
        fun `should return empty list if guild doesn't have invitation`() = runTest {
            withGuildImportedAndCreated {
                val invitations = service.getInvitations(it.id).toList()
                assertThat(invitations).isEmpty()
            }
        }

        @Test
        fun `should ignore empty value for entities`() = runTest {
            getWithWrongValue("")
        }

        @Test
        fun `should ignore wrong value for entities`() = runTest {
            getWithWrongValue(getRandomString())
        }

        private suspend fun getWithWrongValue(value: String) {
            withGuildImportedAndCreated {
                val mapKey =
                    (service.prefixKey.format(it.id) + GuildCacheService.Type.ADD_INVITATION.key).encodeToByteArray()
                val entity = getRandomString().encodeToByteArray()

                val entity2 = getRandomString().encodeToByteArray()
                val guildInvite = GuildInvite(it.id, entity2.decodeToString(), null)
                val encodedGuildInvite = cacheClient.binaryFormat.encodeToByteArray(guildInvite)

                cacheClient.connect { connection ->
                    connection.hset(mapKey, entity, value.encodeToByteArray())
                    connection.hset(mapKey, entity2, encodedGuildInvite)
                }

                assertThat(service.getInvitations(it.id).toList()).containsExactly(guildInvite)
            }
        }

        @Test
        fun `should return empty list when an entity is member but not invited`() = runTest {
            val guild = Guild(0, getRandomString(), getRandomString())
            service.importGuild(guild)
            service.addMember(guild.id, getRandomString())
            service.importMember(GuildMember(guild.id, getRandomString()))

            val invitations = service.getInvitations(guild.id).toList()
            assertThat(invitations).isEmpty()
        }

        @Test
        fun `should return empty list when guild doesn't exists`() = runTest {
            val invites = service.getInvitations(0).toList()
            assertContentEquals(emptyList(), invites)
        }

        @ParameterizedTest
        @ValueSource(ints = [1, 2, 3, 4, 5])
        fun `should return added invitations`(number: Int) = runTest {
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
        fun `should return imported invitations`(number: Int) = runTest {
            val guild = Guild(0, getRandomString(), getRandomString())
            service.importGuild(guild)
            val guildId = guild.id
            val expectedInvites = List(number) { GuildInvite(guildId, getRandomString(), null) }
            expectedInvites.forEach { service.importInvitation(it) }

            val invites = service.getInvitations(guildId).toList()
            assertThat(invites).containsExactlyInAnyOrderElementsOf(expectedInvites)
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
        fun `should ignore the deleted invitations`(number: Int) = runTest {
            val guild = Guild(0, getRandomString(), getRandomString())
            service.importGuild(guild)
            val guildId = guild.id
            val invites = List(number) { GuildInvite(guildId, getRandomString(), null) }
            invites.forEach { service.importInvitation(it) }

            invites.forEach { invite ->
                service.removeInvitation(guildId, invite.entityId)
            }

            val invitationsAfterRemoval = service.getInvitations(guildId).toList()
            assertThat(invitationsAfterRemoval).isEmpty()
        }

        @ParameterizedTest
        @ValueSource(ints = [1, 2, 3, 4, 5])
        fun `should return the imported and added invitations`(number: Int) = runTest {
            val guild = Guild(0, getRandomString(), getRandomString())
            service.importGuild(guild)
            val guildId = guild.id

            val importedInvites = List(number) { GuildInvite(guildId, getRandomString(), null) }
            importedInvites.forEach { service.importInvitation(it) }

            val addedInvites = List(number) { GuildInvite(guildId, getRandomString(), null) }.onEach { invite ->
                service.addInvitation(guildId, invite.entityId, null)
            }

            val invites = service.getInvitations(guildId).toList()
            assertThat(invites).containsExactlyInAnyOrderElementsOf(importedInvites + addedInvites)
        }
    }

    @Nested
    inner class ImportInvitation {

        @Test
        fun `should keep integrity of invitation data`() = runTest {
            val guild = Guild(0, getRandomString(), getRandomString())
            service.importGuild(guild)
            val guildId = guild.id

            val now = Instant.now()
            val expiredAt = now.plusSeconds(10).truncatedTo(ChronoUnit.MILLIS)
            val createdAt = now.minusSeconds(10).truncatedTo(ChronoUnit.MILLIS)
            val invite = GuildInvite(guildId, getRandomString(), expiredAt, createdAt)
            service.importInvitation(invite)

            val guildIdString = guildId.toString()
            val importedInvites = getAllImportedInvites(guildIdString)
            assertThat(importedInvites).containsExactly(invite)
        }

        @Test
        fun `should throw exception if invitation is for a non existing guild`() = runTest {
            val guildId = 0

            val invite = GuildInvite(guildId, getRandomString(), null)
            assertThrows<GuildNotFoundException> {
                service.importInvitation(invite)
            }

            assertThat(getAllImportedInvites(guildId.toString())).isEmpty()
            assertThat(getAllAddedInvites(guildId.toString())).isEmpty()
        }

        @Test
        fun `should throw exception if entity is member`() = runTest {
            val guild = Guild(0, getRandomString(), getRandomString())
            service.importGuild(guild)
            val guildId = guild.id
            val invite = GuildInvite(guildId, getRandomString(), null)

            service.addMember(guildId, invite.entityId)

            assertThrows<GuildInvitedIsAlreadyMemberException> {
                service.importInvitation(invite)
            }

            val guildIdString = guildId.toString()
            assertThat(getAllImportedInvites(guildIdString)).isEmpty()
            assertThat(getAllAddedInvites(guildIdString)).isEmpty()
        }

        @Test
        fun `should throw exception if the owner is invited`() = runTest {
            val guild = Guild(0, getRandomString(), getRandomString())
            service.importGuild(guild)
            val guildId = guild.id
            val invite = GuildInvite(guildId, guild.ownerId, null)

            assertThrows<GuildInvitedIsAlreadyMemberException> {
                service.importInvitation(invite)
            }

            val guildIdString = guildId.toString()
            assertThat(getAllImportedInvites(guildIdString)).isEmpty()
            assertThat(getAllAddedInvites(guildIdString)).isEmpty()
        }

        @Test
        fun `should throw exception if invitation is expired`() = runTest {
            val guild = Guild(0, getRandomString(), getRandomString())
            val guildId = guild.id
            val invite = GuildInvite(guildId, getRandomString(), Instant.now().minusSeconds(1))

            assertThrows<IllegalArgumentException> {
                service.importInvitation(invite)
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

            val invite = GuildInvite(guildId, getRandomString(), null)

            service.addInvitation(guildId, invite.entityId, invite.expiredAt)
            assertTrue { service.removeInvitation(guildId, invite.entityId) }
            assertThat(getAllRemovedInvites(guildIdString)).containsExactly(invite.entityId)
            assertThat(getAllImportedInvites(guildIdString)).isEmpty()
            assertThat(getAllAddedInvites(guildIdString)).isEmpty()

            assertFalse { service.importInvitation(invite) }
            assertThat(getAllRemovedInvites(guildIdString)).containsExactly(invite.entityId)
            assertThat(getAllImportedInvites(guildIdString)).isEmpty()
            assertThat(getAllAddedInvites(guildIdString)).isEmpty()
        }

        @Test
        fun `should set as imported the added invitation`() = runTest {
            val guild = Guild(0, getRandomString(), getRandomString())
            service.importGuild(guild)
            val guildId = guild.id
            val guildIdString = guildId.toString()

            val invite = GuildInvite(guildId, getRandomString(), null)
            assertTrue { service.addInvitation(invite.guildId, invite.entityId, invite.expiredAt) }
            assertThat(getAllImportedInvites(guildIdString)).isEmpty()
            assertThat(getAllAddedInvites(guildIdString)).containsExactly(invite)

            assertTrue { service.importInvitation(invite) }

            assertThat(getAllImportedInvites(guildIdString)).containsExactly(invite)
            assertThat(getAllAddedInvites(guildIdString)).isEmpty()
        }

        @Test
        fun `should return true when invitations are updated`() = runTest {
            val guild = Guild(0, getRandomString(), getRandomString())
            service.importGuild(guild)
            val guildId = guild.id
            val guildIdString = guildId.toString()

            val invite = GuildInvite(guildId, getRandomString(), null)
            assertTrue { service.importInvitation(invite) }

            assertThat(getAllImportedInvites(guildIdString)).containsExactly(invite)

            val newInvite = invite.copy(expiredAt = Instant.now().plusSeconds(1).truncatedTo(ChronoUnit.MILLIS))
            assertTrue { service.importInvitation(newInvite) }

            assertThat(getAllImportedInvites(guildIdString)).containsExactly(newInvite)
            assertThat(getAllAddedInvites(guildIdString)).isEmpty()
        }

        @Test
        fun `should return false when invitations are already imported with the same values`() = runTest {
            val guild = Guild(0, getRandomString(), getRandomString())
            service.importGuild(guild)
            val guildId = guild.id
            val guildIdString = guildId.toString()

            val invite = GuildInvite(guildId, getRandomString(), null)
            assertTrue { service.importInvitation(invite) }
            assertFalse { service.importInvitation(invite) }
            assertThat(getAllImportedInvites(guildIdString)).containsExactly(invite)
            assertThat(getAllAddedInvites(guildIdString)).isEmpty()
        }

        @Test
        fun `should throw exception when guild is not a imported guild`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val guildId = guild.id

            assertThrows<IllegalArgumentException> {
                service.importInvitation(GuildInvite(guildId, getRandomString(), null))
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