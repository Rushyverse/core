package com.github.rushyverse.core.data.player

import com.github.rushyverse.core.container.createPSQLContainer
import com.github.rushyverse.core.data.utils.DatabaseUtils
import com.github.rushyverse.core.data.utils.DatabaseUtils.createR2dbcDatabase
import com.github.rushyverse.core.utils.createPlayer
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Nested
import org.komapper.core.dsl.QueryDsl
import org.komapper.r2dbc.R2dbcDatabase
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.MountableFile
import java.util.*
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test


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
        database = createR2dbcDatabase(psqlContainer)
        playerService = PlayerDatabaseService(database)
    }

    @AfterTest
    fun onAfter() = runBlocking<Unit> {
        database.runQuery(QueryDsl.delete(meta).all().options { it.copy(allowMissingWhereClause = true) })
    }

    @Nested
    inner class Save {

        @Test
        fun `should create a new player if ID does not exist`() = runTest {
            val player = createPlayer()
            playerService.savePlayer(player) shouldBe true
            getAll() shouldBe listOf(player)
        }

        @Test
        fun `should update an existing player if ID exists`() = runTest {
            val player = createPlayer().copy(rank = Rank.PLAYER)
            playerService.savePlayer(player) shouldBe true

            val playerUpdated = player.copy(rank = Rank.ADMIN)
            playerService.savePlayer(playerUpdated) shouldBe true
            getAll() shouldBe listOf(playerUpdated)
        }

        @Test
        fun `should return false if no rows were affected`() = runTest {
            val player = createPlayer()
            playerService.savePlayer(player) shouldBe true
            playerService.savePlayer(player) shouldBe false
        }

    }

    @Nested
    inner class Get {

        @Test
        fun `should return null if ID does not exist`() = runTest {
            List(10) {
                createPlayer()
            }.forEach {
                playerService.savePlayer(it)
            }

            playerService.getPlayer(UUID.randomUUID()) shouldBe null
        }

        @Test
        fun `should return the player if ID exists`() = runTest {
            val player = createPlayer()
            playerService.savePlayer(player) shouldBe true
            playerService.getPlayer(player.uuid) shouldBe player
        }

    }

    @Nested
    inner class Remove {

        @Test
        fun `should return false if ID does not exist`() = runTest {
            List(10) {
                createPlayer()
            }.forEach {
                playerService.savePlayer(it)
            }

            playerService.removePlayer(UUID.randomUUID()) shouldBe false
        }

        @Test
        fun `should return true if ID exist`() = runTest {
            val players = List(10) {
                createPlayer()
            }

            players.forEach {
                playerService.savePlayer(it)
            }

            playerService.removePlayer(players.first().uuid) shouldBe true

            getAll() shouldBe players.drop(1)
        }

    }

    private suspend fun getAll(): List<Player> {
        return DatabaseUtils.getAll(database, meta)
    }
}
