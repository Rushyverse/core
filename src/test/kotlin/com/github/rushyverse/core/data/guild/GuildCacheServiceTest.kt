package com.github.rushyverse.core.data.guild

import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.container.createRedisContainer
import com.github.rushyverse.core.data.Guild
import com.github.rushyverse.core.data.GuildCacheService
import com.github.rushyverse.core.utils.getRandomString
import io.lettuce.core.FlushMode
import io.lettuce.core.RedisURI
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.assertThrows
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.util.concurrent.TimeUnit
import kotlin.test.*

@Timeout(5, unit = TimeUnit.SECONDS)
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
                val guilds = getAllAddedGuilds()
                println(
                    """
                    |Expected: $guild
                    |Actual: $guilds
                """.trimIndent()
                )
                assertContentEquals(listOf(guild), guilds)
            }

            @Test
            fun `when owner is already in a guild`() = runTest {
                val guild = service.createGuild(getRandomString(), getRandomString())
                val guild2 = service.createGuild(getRandomString(), guild.ownerId)
                assertNotEquals(guild.id, guild2.id)

                val guilds = getAllAddedGuilds()
                assertThat(guilds).containsExactlyInAnyOrder(guild, guild2)
            }

            @Test
            fun `when owner is a member of another guild`() = runTest {
                val entity = getRandomString()
                val guild = service.createGuild(getRandomString(), getRandomString())
                service.addMember(guild.id, entity)

                val guild2 = service.createGuild(getRandomString(), entity)

                val guilds = getAllAddedGuilds()
                assertThat(guilds).containsExactlyInAnyOrder(guild, guild2)
            }

            @Test
            fun `when owner is empty`() = runTest {
                assertThrows<IllegalArgumentException> {
                    service.createGuild(getRandomString(), "")
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
            }

            @Test
            fun `when name is empty`() = runTest {
                assertThrows<IllegalArgumentException> {
                    service.createGuild("", getRandomString())
                }
            }
        }

    }

    private suspend fun getAllStoredGuilds(): List<Guild> {
        val searchKey = (service.prefixKey.format("*") + GuildCacheService.Type.GUILD.key).encodeToByteArray()
        return cacheClient.connect {
            it.keys(searchKey).map { key ->
                val value = it.get(key)!!
                cacheClient.binaryFormat.decodeFromByteArray(Guild.serializer(), value)
            }
        }.toList()
    }

    private suspend fun getAllAddedGuilds(): List<Guild> {
        val searchKey = (service.prefixKey.format("*") + GuildCacheService.Type.ADD_GUILD.key).encodeToByteArray()
        return cacheClient.connect {
            it.keys(searchKey).map { key ->
                val value = it.get(key)!!
                cacheClient.binaryFormat.decodeFromByteArray(Guild.serializer(), value)
            }
        }.toList()
    }
}