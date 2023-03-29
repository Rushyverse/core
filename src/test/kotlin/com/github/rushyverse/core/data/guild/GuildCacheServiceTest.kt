package com.github.rushyverse.core.data.guild

import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.container.createRedisContainer
import com.github.rushyverse.core.data.Guild
import com.github.rushyverse.core.data.GuildCacheService
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

        @Test
        fun `when guild exists`() = runTest {
            val guild = service.createGuild(getRandomString(), getRandomString())
            assertTrue { service.deleteGuild(guild.id) }
            assertThat(getAllAddedGuilds()).containsExactly(guild)
            assertThat(getAllDeletedGuilds()).containsExactly(guild.id)
            assertThat(getAllStoredGuilds()).isEmpty()
        }

        @Test
        fun `when guild does not exist`() = runTest {
            assertTrue { service.deleteGuild(0) }
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

            assertTrue { service.deleteGuild(guild2.id) }
            assertThat(getAllAddedGuilds()).containsExactlyInAnyOrder(guild, guild2)
            assertThat(getAllDeletedGuilds()).containsExactlyInAnyOrder(guild.id, guild2.id)
            assertThat(getAllStoredGuilds()).isEmpty()
        }

        @Test
        fun `when guild is already deleted`() = runTest {
            assertTrue { service.deleteGuild(0) }
            assertFalse { service.deleteGuild(0) }

            assertThat(getAllAddedGuilds()).isEmpty()
            assertThat(getAllDeletedGuilds()).containsExactly(0)
            assertThat(getAllStoredGuilds()).isEmpty()
        }

    }

    @Nested
    inner class GetGuildById {

        @Nested
        inner class WithSavedState {

            @Test
            fun `when guild is not deleted`() = runTest {
                val guild = Guild(0, getRandomString(), getRandomString(), Instant.now().truncatedTo(ChronoUnit.MILLIS))
                service.saveGuild(guild)
                assertEquals(guild, service.getGuild(guild.id))
            }

            @Test
            fun `when guild is deleted`() = runTest {
                val guild = Guild(0, getRandomString(), getRandomString(), Instant.now().truncatedTo(ChronoUnit.MILLIS))
                service.saveGuild(guild)
                service.deleteGuild(guild.id)
                assertNull(service.getGuild(guild.id))
            }

            @Test
            fun `when another guild is added`() = runTest {
                val guild = Guild(0, getRandomString(), getRandomString(), Instant.now().truncatedTo(ChronoUnit.MILLIS))
                val guild2 =
                    Guild(1, getRandomString(), getRandomString(), Instant.now().truncatedTo(ChronoUnit.MILLIS))
                service.saveGuild(guild)
                service.saveGuild(guild2)

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
                service.saveGuild(guild)
                assertThat(service.getGuild(guild.name).toList()).containsExactly(guild)
            }

            @Test
            fun `when guild is deleted`() = runTest {
                val guild = Guild(0, getRandomString(), getRandomString(), Instant.now().truncatedTo(ChronoUnit.MILLIS))
                service.saveGuild(guild)
                service.deleteGuild(guild.id)
                assertThat(service.getGuild(guild.name).toList()).isEmpty()
            }

            @Test
            fun `when another guild is added`() = runTest {
                val guild = Guild(0, getRandomString(), getRandomString(), Instant.now().truncatedTo(ChronoUnit.MILLIS))
                val guild2 =
                    Guild(1, getRandomString(), getRandomString(), Instant.now().truncatedTo(ChronoUnit.MILLIS))
                service.saveGuild(guild)
                service.saveGuild(guild2)

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
                    service.saveGuild(this)
                }
            }

            val expectedGuild = createdGuild.take(numberOfGuilds / 2) + savedGuild.take(numberOfGuilds / 2)
            assertThat(service.getGuild(name).toList()).containsExactlyInAnyOrderElementsOf(expectedGuild)
        }
    }

    private suspend fun getAllStoredGuilds(): List<Guild> {
        return getAllDataFromKey(GuildCacheService.Type.GUILD).map { keyValue ->
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
            it.smembers(key)
        }.map {
            cacheClient.binaryFormat.decodeFromByteArray(Int.serializer(), it)
        }.toList()
    }

    private suspend fun getAllDataFromKey(
        type: GuildCacheService.Type
    ): List<KeyValue<ByteArray, ByteArray>> {
        val searchKey = service.prefixKey.format("*") + type.key
        return cacheClient.connect {
            val scanner = it.scan(KeyScanArgs.Builder.limit(Long.MAX_VALUE).match(searchKey))
            if (scanner == null || scanner.keys.isEmpty()) return emptyList()

            it.mget(*scanner.keys.toTypedArray())
        }.toList()
    }
}