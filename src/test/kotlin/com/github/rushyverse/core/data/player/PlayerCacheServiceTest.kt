package com.github.rushyverse.core.data.player

import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.container.createRedisContainer
import com.github.rushyverse.core.serializer.UUIDSerializer
import com.github.rushyverse.core.utils.createPlayer
import io.kotest.matchers.shouldBe
import io.lettuce.core.FlushMode
import io.lettuce.core.RedisURI
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Timeout
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test

@Timeout(10, unit = TimeUnit.SECONDS)
@Testcontainers
class PlayerCacheServiceTest {

    companion object {
        @JvmStatic
        @Container
        private val redisContainer = createRedisContainer()
    }

    private lateinit var cacheClient: CacheClient
    private lateinit var service: PlayerCacheService

    @BeforeTest
    fun onBefore() = runBlocking {
        cacheClient = CacheClient {
            uri = RedisURI.create(redisContainer.url)
        }
        service = PlayerCacheService(cacheClient)
    }

    @AfterTest
    fun onAfter() = runBlocking<Unit> {
        cacheClient.connect {
            it.flushall(FlushMode.SYNC)
        }
        cacheClient.closeAsync().await()
    }

    @Nested
    inner class DefaultParameter {

        @Test
        fun `default values`() {
            service.prefixKey shouldBe "player:"
            service.expirationKey shouldBe null
        }

    }

    @Nested
    inner class Save {

        @Test
        fun `should create a new player if ID does not exist`() = runTest {
            val player = createPlayer()
            service.savePlayer(player) shouldBe true

            getAdded() shouldBe listOf(player)
        }

        @Test
        fun `should update an existing player if ID exists`() = runTest {
            val player = createPlayer().copy(rank = Rank.PLAYER)
            val playerUpdated = player.copy(rank = Rank.ADMIN)

            service.savePlayer(player) shouldBe true
            service.savePlayer(playerUpdated) shouldBe true

            getAdded() shouldBe listOf(playerUpdated)
        }

        @Test
        fun `should delete removed key for player when created`() = runTest {
            val player = createPlayer()

            service.removePlayer(player.uuid)

            getRemoved() shouldBe listOf(player.uuid)

            service.savePlayer(player) shouldBe true

            getAdded() shouldBe listOf(player)
            getRemoved() shouldBe emptyList()
        }

        @Test
        fun `should delete removed key for player when updated`() = runTest {
            val player = createPlayer()
            service.savePlayer(player) shouldBe true

            val playerUpdated = player.copy(rank = Rank.ADMIN)

            service.removePlayer(player.uuid)

            getRemoved() shouldBe listOf(player.uuid)

            service.savePlayer(playerUpdated) shouldBe true

            getAdded() shouldBe listOf(playerUpdated)
            getRemoved() shouldBe emptyList()
        }

        @Test
        fun `should delete removed key for player nothing change`() = runTest {
            val player = createPlayer()
            service.savePlayer(player) shouldBe true

            service.removePlayer(player.uuid)

            getRemoved() shouldBe listOf(player.uuid)

            service.savePlayer(player) shouldBe true

            getAdded() shouldBe listOf(player)
            getRemoved() shouldBe emptyList()
        }


        @Test
        fun `should return false if no updated`() = runTest {
            val player = createPlayer()

            service.savePlayer(player) shouldBe true
            service.savePlayer(player) shouldBe false

            getAdded() shouldBe listOf(player)
        }

    }

    private suspend fun getAdded(): List<Player> = channelFlow {
        cacheClient.connect { connection ->
            val key = (service.prefixKey + PlayerCacheService.Type.ADD_PLAYER.key).encodeToByteArray()
            connection.hgetall(key).collect { value ->
                send(value)
            }
        }
    }.map { keyValue ->
        cacheClient.binaryFormat.decodeFromByteArray(Player.serializer(), keyValue.value)
    }.toList()

    private suspend fun getRemoved(): List<UUID> = channelFlow {
        cacheClient.connect { connection ->
            val key = (service.prefixKey + PlayerCacheService.Type.REMOVE_PLAYER.key).encodeToByteArray()
            connection.smembers(key).collect { value ->
                send(value)
            }
        }
    }.map { value ->
        cacheClient.binaryFormat.decodeFromByteArray(UUIDSerializer, value)
    }.toList()

}
