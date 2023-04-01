package com.github.rushyverse.core.data.guild

import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.container.createRedisContainer
import com.github.rushyverse.core.data.Guild
import com.github.rushyverse.core.data.GuildCacheService
import com.github.rushyverse.core.data.GuildInvite
import com.github.rushyverse.core.utils.getRandomString
import io.lettuce.core.FlushMode
import io.lettuce.core.KeyScanArgs
import io.lettuce.core.KeyValue
import io.lettuce.core.RedisURI
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.builtins.serializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.test.*

//@Timeout(10, unit = TimeUnit.SECONDS)
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
                assertThat(getAllStoredGuilds()).isEmpty()
                assertThat(getAllDeletedGuilds()).isEmpty()
            }

            @Test
            fun `when owner is already in a guild`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                val guild2 = service.createGuild(getRandomString(), guild.ownerId)
                assertNotEquals(guild.id, guild2.id)

                val guilds = getAllAddedGuilds()
                assertThat(guilds).containsExactlyInAnyOrder(guild, guild2)
                assertThat(getAllStoredGuilds()).isEmpty()
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
                assertThat(getAllStoredGuilds()).isEmpty()
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
                assertThat(getAllStoredGuilds()).isEmpty()
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
            assertThat(getAllStoredGuilds()).isEmpty()
            assertThat(getAllDeletedGuilds()).isEmpty()
        }

    }

    @Nested
    inner class DeleteGuild {

        @Nested
        inner class GuildOnlyInGuild {

            @Test
            fun `when guild exists`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                assertTrue { service.deleteGuild(guild.id) }
                assertThat(getAllAddedGuilds()).isEmpty()
                assertThat(getAllDeletedGuilds()).isEmpty()
                assertThat(getAllStoredGuilds()).isEmpty()
            }

            @Test
            fun `when guild does not exist`() = runTest {
                assertFalse { service.deleteGuild(0) }
                assertThat(getAllAddedGuilds()).isEmpty()
                assertThat(getAllDeletedGuilds()).containsExactly(0)
                assertThat(getAllStoredGuilds()).isEmpty()
            }

            @Test
            fun `when another guild is deleted`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                val guild2 = service.createGuild(getRandomString(), getRandomString())

                assertTrue { service.deleteGuild(guild.id) }
                assertThat(getAllAddedGuilds()).containsExactlyInAnyOrder(guild, guild2)
                assertThat(getAllDeletedGuilds()).containsExactly(guild.id)
                assertThat(getAllStoredGuilds()).isEmpty()
            }

            @Test
            fun `when guild is already deleted`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                assertTrue { service.deleteGuild(guild.id) }
                assertFalse { service.deleteGuild(guild.id) }

                assertThat(getAllAddedGuilds()).isEmpty()
                assertThat(getAllDeletedGuilds()).isEmpty()
                assertThat(getAllStoredGuilds()).isEmpty()
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

                assertTrue { keyExists(GuildCacheService.Type.IMPORT_INVITATION, guildIdString) }
                assertThat(getAllInvites(guildIdString)).hasSize(10)

                assertTrue { keyExists(GuildCacheService.Type.ADD_INVITATION, guildIdString) }
                assertThat(getAllAddInvites(guildIdString)).hasSize(10)

                assertTrue { keyExists(GuildCacheService.Type.IMPORT_MEMBER, guildIdString) }
                assertThat(getAllMembers(guildIdString)).hasSize(10)

                assertTrue { keyExists(GuildCacheService.Type.ADD_MEMBER, guildIdString) }
                assertThat(getAllAddMembers(guildIdString)).hasSize(10)

                assertTrue { service.deleteGuild(guild.id) }

                assertFalse { keyExists(GuildCacheService.Type.IMPORT_INVITATION, guildIdString) }
                assertThat(getAllInvites(guildIdString)).isEmpty()

                assertFalse { keyExists(GuildCacheService.Type.ADD_INVITATION, guildIdString) }
                assertThat(getAllAddInvites(guildIdString)).isEmpty()

                assertFalse { keyExists(GuildCacheService.Type.IMPORT_MEMBER, guildIdString) }
                assertThat(getAllMembers(guildIdString)).isEmpty()

                assertFalse { keyExists(GuildCacheService.Type.ADD_MEMBER, guildIdString) }
                assertThat(getAllAddMembers(guildIdString)).isEmpty()

                assertFalse { keyExists(GuildCacheService.Type.ADD_GUILD, guildIdString) }
                assertThat(getAllAddedGuilds()).isEmpty()
            }

        }
    }

    @Nested
    inner class GetGuildById {

        @Nested
        inner class WithSavedState {

            @Test
            fun `when guild is not deleted`() = runTest {
                val guild = Guild(0, getRandomString(), getRandomString(), Instant.now().truncatedTo(ChronoUnit.MILLIS))
                service.importGuild(guild)
                assertEquals(guild, service.getGuild(guild.id))
            }

            @Test
            fun `when guild is deleted`() = runTest {
                val guild = Guild(0, getRandomString(), getRandomString(), Instant.now().truncatedTo(ChronoUnit.MILLIS))
                service.importGuild(guild)
                service.deleteGuild(guild.id)
                assertNull(service.getGuild(guild.id))
            }

            @Test
            fun `when another guild is added`() = runTest {
                val guild = Guild(0, getRandomString(), getRandomString(), Instant.now().truncatedTo(ChronoUnit.MILLIS))
                val guild2 =
                    Guild(1, getRandomString(), getRandomString(), Instant.now().truncatedTo(ChronoUnit.MILLIS))
                service.importGuild(guild)
                service.importGuild(guild2)

                assertEquals(guild, service.getGuild(guild.id))
                assertEquals(guild2, service.getGuild(guild2.id))
            }

        }

        @Nested
        inner class WithAddedState {

            @Test
            fun `when guild is not deleted`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                assertEquals(guild, service.getGuild(guild.id))
            }

            @Test
            fun `when guild is deleted`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                service.deleteGuild(guild.id)
                assertNull(service.getGuild(guild.id))
            }

            @Test
            fun `when another guild is added`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                val guild2 = service.createGuild(getRandomString(), getRandomString())

                assertEquals(guild, service.getGuild(guild.id))
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

        @Nested
        inner class WithSavedState {

            @Test
            fun `when guild is not deleted`() = runTest {
                val guild = Guild(0, getRandomString(), getRandomString(), Instant.now().truncatedTo(ChronoUnit.MILLIS))
                service.importGuild(guild)
                assertThat(service.getGuild(guild.name).toList()).containsExactly(guild)
            }

            @Test
            fun `when guild is deleted`() = runTest {
                val guild = Guild(0, getRandomString(), getRandomString(), Instant.now().truncatedTo(ChronoUnit.MILLIS))
                service.importGuild(guild)
                service.deleteGuild(guild.id)
                assertThat(service.getGuild(guild.name).toList()).isEmpty()
            }

            @Test
            fun `when another guild is added`() = runTest {
                val guild = Guild(0, getRandomString(), getRandomString(), Instant.now().truncatedTo(ChronoUnit.MILLIS))
                val guild2 =
                    Guild(1, getRandomString(), getRandomString(), Instant.now().truncatedTo(ChronoUnit.MILLIS))
                service.importGuild(guild)
                service.importGuild(guild2)

                assertThat(service.getGuild(guild.name).toList()).containsExactly(guild)
                assertThat(service.getGuild(guild2.name).toList()).containsExactly(guild2)
            }

        }

        @Nested
        inner class WithAddedState {

            @Test
            fun `when guild is not deleted`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                assertThat(service.getGuild(guild.name).toList()).containsExactly(guild)
            }

            @Test
            fun `when guild is deleted`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                service.deleteGuild(guild.id)
                assertThat(service.getGuild(guild.name).toList()).isEmpty()
            }

            @Test
            fun `when another guild is added`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                val guild2 = service.createGuild(getRandomString(), getRandomString())
                assertThat(service.getGuild(guild.name).toList()).containsExactly(guild)
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
            val numberOfGuilds = 1000
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
                    getRandomString(),
                    Instant.now().truncatedTo(ChronoUnit.MILLIS)
                ).apply {
                    service.importGuild(this)
                }
            }

            val expectedGuild = createdGuild.take(numberOfGuilds / 2) + savedGuild.take(numberOfGuilds / 2)
            assertThat(service.getGuild(name).toList()).containsExactlyInAnyOrderElementsOf(expectedGuild)
        }

        @Test
        fun `should return distinct guilds`() = runTest {
            val name = getRandomString()
            val guild = service.createGuild(name, getRandomString())
            service.importGuild(guild)
            assertThat(service.getGuild(name).toList()).containsExactlyInAnyOrder(guild)
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
    inner class AddInvitation {

        @Nested
        inner class ExpirationDate {

            @Test
            fun `with field`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                val entityId = getRandomString()
                val now = Instant.now()
                val expirationDate = now.plusSeconds(10)
                assertTrue { service.addInvitation(guild.id, entityId, expirationDate) }

                val guildIdString = guild.id.toString()
                assertThat(getAllInvites(guildIdString)).isEmpty()
                assertThat(getAllAddInvites(guildIdString)).hasSize(1)
                assertThat(getAllRemoveInvites(guildIdString)).isEmpty()
                val inviteForGuild = getAllAddInvites(guild.id.toString())

                val invite = inviteForGuild.single()
                assertEquals(entityId, invite.entityId)
                assertEquals(
                    expirationDate.truncatedTo(ChronoUnit.SECONDS),
                    invite.expiredAt!!.truncatedTo(ChronoUnit.SECONDS)
                )
            }

            @Test
            fun `without field`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                val entityId = getRandomString()
                assertTrue { service.addInvitation(guild.id, entityId, null) }

                val guildIdString = guild.id.toString()
                assertThat(getAllInvites(guildIdString)).isEmpty()
                assertThat(getAllAddInvites(guildIdString)).hasSize(1)
                assertThat(getAllRemoveInvites(guildIdString)).isEmpty()
                val inviteForGuild = getAllAddInvites(guild.id.toString())

                val invite = inviteForGuild.single()
                assertEquals(entityId, invite.entityId)
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
            assertTrue { service.addInvitation(guild.id, entityId, null) }

            assertThat(getAllAddInvites(guild.id.toString())).containsExactly(
                GuildInvite(guild.id, entityId, null)
            )
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
            assertTrue { service.addInvitation(guild.id, guild.ownerId, null) }

            val invited = service.getInvited(guild.id).toList()
            assertThat(getAllAddInvites(guild.id.toString())).containsExactly(
                GuildInvite(guild.id, guild.ownerId, null)
            )
        }

        @Test
        fun `when entity is already invited`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            val entityId = getRandomString()
            val expiredAt = Instant.now().plusSeconds(10)
            assertTrue { service.addInvitation(guild.id, entityId, null) }
            assertFalse { service.addInvitation(guild.id, entityId, null) }
            assertTrue { service.addInvitation(guild.id, entityId, expiredAt) }

            val invited = service.getInvited(guild.id).toList()
            assertContentEquals(listOf(entityId), invited)

            assertThat(getAllAddInvites(guild.id.toString())).containsExactly(
                GuildInvite(guild.id, entityId, expiredAt)
            )
        }

        @Test
        fun `when guild does not exist`() = runTest {
            assertTrue { service.addInvitation(0, getRandomString(), null) }
        }

        @ParameterizedTest
        @ValueSource(strings = ["", " ", "  ", "   "])
        fun `when entity id is blank`(id: String) = runTest {
            assertThrows<IllegalArgumentException> {
                service.addInvitation(0, id, null)
            }
        }
    }

    private suspend fun getAllStoredGuilds(): List<Guild> {
        return getAllDataFromKey(GuildCacheService.Type.IMPORT_GUILD).map { keyValue ->
            cacheClient.binaryFormat.decodeFromByteArray(Guild.serializer(), keyValue.value)
        }
    }

    private suspend fun getAllAddedGuilds(): List<Guild> {
        return getAllDataFromKey(GuildCacheService.Type.ADD_GUILD).map { keyValue ->
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

    private suspend fun keyExists(type: GuildCacheService.Type, guildId: String): Boolean {
        val key = service.prefixKey.format(guildId) + type.key
        return keyExists(key)
    }

    private suspend fun keyExists(key: String): Boolean {
        return cacheClient.connect {
            it.exists(key.encodeToByteArray())
        } == 1L
    }

    private suspend fun getAllInvites(guildId: String): List<GuildInvite> {
        return getAllValuesOfSet(GuildCacheService.Type.IMPORT_INVITATION, guildId).map {
            cacheClient.binaryFormat.decodeFromByteArray(GuildInvite.serializer(), it)
        }
    }

    private suspend fun getAllAddInvites(guildId: String): List<GuildInvite> {
        return getAllValuesOfSet(GuildCacheService.Type.ADD_INVITATION, guildId).map {
            cacheClient.binaryFormat.decodeFromByteArray(GuildInvite.serializer(), it)
        }
    }

    private suspend fun getAllRemoveInvites(guildId: String): List<String> {
        return getAllValuesOfSet(GuildCacheService.Type.REMOVE_INVITATION, guildId).map {
            cacheClient.binaryFormat.decodeFromByteArray(String.serializer(), it)
        }
    }

    private suspend fun getAllMembers(guildId: String): List<String> {
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
        type: GuildCacheService.Type
    ): List<KeyValue<ByteArray, ByteArray>> {
        val searchKey = service.prefixKey.format("*") + type.key
        return cacheClient.connect {
            val scanner = it.scan(KeyScanArgs.Builder.limit(Long.MAX_VALUE).match(searchKey))
            if (scanner == null || scanner.keys.isEmpty()) return emptyList()

            it.mget(*scanner.keys.toTypedArray()).toList()
        }
    }
}