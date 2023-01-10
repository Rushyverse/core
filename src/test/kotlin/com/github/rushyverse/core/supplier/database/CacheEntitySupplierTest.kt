package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.data.IFriendCacheService
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import io.mockk.slot
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Nested
import java.util.*
import kotlin.test.*

class CacheEntitySupplierTest {

    private lateinit var cacheEntitySupplier: CacheEntitySupplier

    private lateinit var cacheService: IFriendCacheService

    @BeforeTest
    fun onBefore() = runBlocking {
        cacheService = mockk()
        cacheEntitySupplier = CacheEntitySupplier(cacheService)
    }

    @Nested
    inner class AddFriend {

        @Test
        fun `should add friend in supplier`() = runTest {
            val slotUuid1 = slot<UUID>()
            val slotUuid2 = slot<UUID>()

            coEvery { cacheService.addFriend(capture(slotUuid1), capture(slotUuid2)) } returns true

            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            assertTrue { cacheEntitySupplier.addFriend(uuid1, uuid2) }
            coVerify(exactly = 1) { cacheService.addFriend(any(), any()) }

            assertEquals(uuid1, slotUuid1.captured)
            assertEquals(uuid2, slotUuid2.captured)
        }

        @Test
        fun `should return false when supplier returns false`() = runTest {
            coEvery { cacheService.addFriend(any(), any()) } returns false
            assertFalse { cacheEntitySupplier.addFriend(mockk(), mockk()) }
            coVerify(exactly = 1) { cacheService.addFriend(any(), any()) }
        }

        @Test
        fun `should return true when supplier returns false`() = runTest {
            coEvery { cacheService.addFriend(any(), any()) } returns true
            assertTrue { cacheEntitySupplier.addFriend(mockk(), mockk()) }
            coVerify(exactly = 1) { cacheService.addFriend(any(), any()) }
        }
    }

    @Nested
    inner class RemoveFriend {

        @Test
        fun `should remove friend in supplier`() = runTest {
            val slotUuid1 = slot<UUID>()
            val slotUuid2 = slot<UUID>()

            coEvery { cacheService.removeFriend(capture(slotUuid1), capture(slotUuid2)) } returns true

            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            assertTrue { cacheEntitySupplier.removeFriend(uuid1, uuid2) }
            coVerify(exactly = 1) { cacheService.removeFriend(any(), any()) }

            assertEquals(uuid1, slotUuid1.captured)
            assertEquals(uuid2, slotUuid2.captured)
        }

        @Test
        fun `should return false when supplier returns false`() = runTest {
            coEvery { cacheService.removeFriend(any(), any()) } returns false
            assertFalse { cacheEntitySupplier.removeFriend(mockk(), mockk()) }
            coVerify(exactly = 1) { cacheService.removeFriend(any(), any()) }
        }

        @Test
        fun `should return true when supplier returns false`() = runTest {
            coEvery { cacheService.removeFriend(any(), any()) } returns true
            assertTrue { cacheEntitySupplier.removeFriend(mockk(), mockk()) }
            coVerify(exactly = 1) { cacheService.removeFriend(any(), any()) }
        }
    }

    @Nested
    inner class GetFriends {

        @Test
        fun `should get friends in supplier`() = runTest {
            val slotUuid = slot<UUID>()

            coEvery { cacheService.getFriends(capture(slotUuid)) } returns emptySet()

            val uuid1 = UUID.randomUUID()
            assertEquals(emptySet(), cacheEntitySupplier.getFriends(uuid1))
            coVerify(exactly = 1) { cacheService.getFriends(any()) }

            assertEquals(uuid1, slotUuid.captured)
        }

        @Test
        fun `should return empty collection when supplier returns empty collection`() = runTest {
            coEvery { cacheService.getFriends(any()) } returns emptySet()
            assertEquals(emptySet(), cacheEntitySupplier.getFriends(mockk()))
            coVerify(exactly = 1) { cacheService.getFriends(any()) }
        }

        @Test
        fun `should return not empty collection when supplier returns not empty collection`() = runTest {
            val expected = List(5) { UUID.randomUUID() }.toSet()
            coEvery { cacheService.getFriends(any()) } returns expected
            assertEquals(expected, cacheEntitySupplier.getFriends(mockk()))
            coVerify(exactly = 1) { cacheService.getFriends(any()) }
        }

    }

    @Nested
    inner class IsFriend {

        @Test
        fun `should is friend in supplier`() = runTest {
            val slotUuid1 = slot<UUID>()
            val slotUuid2 = slot<UUID>()

            coEvery { cacheService.isFriend(capture(slotUuid1), capture(slotUuid2)) } returns true

            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            assertTrue { cacheEntitySupplier.isFriend(uuid1, uuid2) }
            coVerify(exactly = 1) { cacheService.isFriend(any(), any()) }

            assertEquals(uuid1, slotUuid1.captured)
            assertEquals(uuid2, slotUuid2.captured)
        }

        @Test
        fun `should return false when supplier returns false`() = runTest {
            coEvery { cacheService.isFriend(any(), any()) } returns false
            assertFalse { cacheEntitySupplier.isFriend(mockk(), mockk()) }
            coVerify(exactly = 1) { cacheService.isFriend(any(), any()) }
        }

        @Test
        fun `should return true when supplier returns false`() = runTest {
            coEvery { cacheService.isFriend(any(), any()) } returns true
            assertTrue { cacheEntitySupplier.isFriend(mockk(), mockk()) }
            coVerify(exactly = 1) { cacheService.isFriend(any(), any()) }
        }
    }

}