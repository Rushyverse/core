package com.github.rushyverse.core.data.player

import com.github.rushyverse.core.data.Player
import com.github.rushyverse.core.data.PlayerService
import com.github.rushyverse.core.supplier.database.DatabaseSupplierConfiguration
import com.github.rushyverse.core.supplier.database.IDatabaseEntitySupplier
import com.github.rushyverse.core.utils.createPlayer
import com.github.rushyverse.core.utils.randomEntityId
import com.github.rushyverse.core.utils.randomString
import io.kotest.matchers.shouldBe
import io.mockk.*
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Nested
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.util.*
import kotlin.test.*

class PlayerServiceTest {

    private lateinit var service: PlayerService
    private val supplier get() = service.supplier

    @BeforeTest
    fun onBefore() {
        service = PlayerService(mockk(randomString()))
    }

    @Test
    fun `should create a new instance with another strategy`() {
        val configuration = mockk<DatabaseSupplierConfiguration>(randomString())
        val strategy = mockk<IDatabaseEntitySupplier>(randomString())
        every { strategy.configuration } returns configuration

        service = PlayerService(strategy)
        service.supplier shouldBe strategy

        val strategy2 = mockk<IDatabaseEntitySupplier>(randomString())
        val service2 = service.withStrategy {
            it shouldBe configuration
            strategy2
        }
        service2.supplier shouldBe strategy2

    }

    @Nested
    inner class Save {

        @Test
        fun `should save in supplier`() = runTest {
            val slotPlayer = slot<Player>()

            coEvery { supplier.savePlayer(capture(slotPlayer)) } returns true

            val player = createPlayer()
            assertTrue { service.savePlayer(player) }
            coVerify(exactly = 1) { supplier.savePlayer(any()) }

            slotPlayer.captured shouldBe player
        }

        @Test
        fun `should return false when supplier returns false`() = runTest {
            coEvery { supplier.savePlayer(any()) } returns false
            assertFalse { service.savePlayer(mockk()) }
            coVerify(exactly = 1) { supplier.savePlayer(any()) }
        }

        @ParameterizedTest
        @ValueSource(booleans = [true, false])
        fun `should return supplier result`(result: Boolean) = runTest {
            coEvery { supplier.savePlayer(any()) } returns true
            assertTrue { service.savePlayer(mockk()) }
            coVerify(exactly = 1) { supplier.savePlayer(any()) }
        }
    }

    @Nested
    inner class Get {

        @Test
        fun `should get in supplier`() = runTest {
            val slot = slot<UUID>()
            val player = mockk<Player>()

            coEvery { supplier.getPlayer(capture(slot)) } returns player

            val id = randomEntityId()
            service.getPlayer(id) shouldBe player
            coVerify(exactly = 1) { supplier.getPlayer(id) }

            slot.captured shouldBe id
        }

        @Test
        fun `should return null when supplier returns null`() = runTest {
            coEvery { supplier.getPlayer(any()) } returns null
            service.getPlayer(randomEntityId()) shouldBe null
            coVerify(exactly = 1) { supplier.getPlayer(any()) }
        }
    }

    @Nested
    inner class Remove {

        @Test
        fun `should remove in supplier`() = runTest {
            val slot = slot<UUID>()

            coEvery { supplier.removePlayer(capture(slot)) } returns true

            val id = randomEntityId()
            assertTrue { service.removePlayer(id) }
            coVerify(exactly = 1) { supplier.removePlayer(id) }

            slot.captured shouldBe id
        }

        @ParameterizedTest
        @ValueSource(booleans = [true, false])
        fun `should return supplier result`(result: Boolean) = runTest {
            coEvery { supplier.removePlayer(any()) } returns result
            service.removePlayer(randomEntityId()) shouldBe result
            coVerify(exactly = 1) { supplier.removePlayer(any()) }
        }
    }
}
