package com.github.rushyverse.core.data.player

import com.github.rushyverse.core.container.createPSQLContainer
import com.github.rushyverse.core.data.*
import com.github.rushyverse.core.data.utils.DatabaseUtils
import com.github.rushyverse.core.data.utils.DatabaseUtils.createConnectionOptions
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Nested
import org.komapper.core.dsl.QueryDsl
import org.komapper.r2dbc.R2dbcDatabase
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.MountableFile
import java.util.*
import kotlin.test.*

@Testcontainers
class PlayerDatabaseServiceTest {

    companion object {
        @JvmStatic
        @Container
        private val psqlContainer = createPSQLContainer()
            .withCopyToContainer(
                MountableFile.forClasspathResource("sql/player.sql"),
                "/docker-entrypoint-initdb.d/1.sql"
            )
    }

    private lateinit var playerService: PlayerDatabaseService
    private lateinit var database: R2dbcDatabase

    private val meta = _Player.player

    @BeforeTest
    fun onBefore() = runBlocking {
        database = R2dbcDatabase(createConnectionOptions(psqlContainer))
        playerService = PlayerDatabaseService(database)
    }

    @AfterTest
    fun onAfter() = runBlocking<Unit> {
        database.runQuery(QueryDsl.delete(meta).all().options { it.copy(allowMissingWhereClause = true) })
    }

    @Nested
    inner class Save {

        @Test
        fun `should create a new player if ID does not exist`() = runBlocking<Unit> {
            val player = Player(UUID.randomUUID(), Rank.PLAYER)
            playerService.save(player) shouldBe true
            getAll() shouldBe listOf(player)
        }

        @Test
        fun `should update an existing player if ID exists`() = runBlocking<Unit> {
            val player = Player(UUID.randomUUID(), Rank.PLAYER)
            playerService.save(player) shouldBe true

            val playerUpdated = player.copy(rank = Rank.ADMIN)
            playerService.save(playerUpdated) shouldBe true
            getAll() shouldBe listOf(playerUpdated)
        }

        @Test
        fun `should return false if no rows were affected`() = runBlocking<Unit> {
            val player = Player(UUID.randomUUID(), Rank.PLAYER)
            playerService.save(player) shouldBe true
            playerService.save(player) shouldBe false
        }

    }

    @Nested
    inner class Get {

        @Test
        fun `should return null if ID does not exist`() = runBlocking<Unit> {
            List(10) {
                Player(UUID.randomUUID(), Rank.PLAYER)
            }.forEach {
                playerService.save(it)
            }

            playerService.get(UUID.randomUUID()) shouldBe null
        }

        @Test
        fun `should return the player if ID exists`() = runBlocking<Unit> {
            val player = Player(UUID.randomUUID(), Rank.PLAYER)
            playerService.save(player) shouldBe true
            playerService.get(player.uuid) shouldBe player
        }

    }

    @Nested
    inner class Remove {

        @Test
        fun `should return false if ID does not exist`() = runBlocking<Unit> {
            List(10) {
                Player(UUID.randomUUID(), Rank.PLAYER)
            }.forEach {
                playerService.save(it)
            }

            playerService.remove(UUID.randomUUID()) shouldBe false
        }

        @Test
        fun `should return true if ID exist`() = runBlocking<Unit> {
            val players = List(10) {
                Player(UUID.randomUUID(), Rank.PLAYER)
            }

            players.forEach {
                playerService.save(it)
            }

            playerService.remove(players.first().uuid) shouldBe true

            getAll() shouldBe players.drop(1)
        }

    }

    private suspend fun getAll(): List<Player> {
        return DatabaseUtils.getAll(database, meta)
    }
}
