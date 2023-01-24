package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.data.IFriendCacheService
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import io.mockk.slot
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Nested
import java.util.*
import kotlin.test.*

class DatabaseCacheEntitySupplierTest {

    private lateinit var cacheEntitySupplier: DatabaseCacheEntitySupplier

    private lateinit var cacheService: IFriendCacheService

    @BeforeTest
    fun onBefore() = runBlocking {
        cacheService = mockk()
        cacheEntitySupplier = DatabaseCacheEntitySupplier(cacheService)
    }

    @Nested
    inner class AddFriend {

        @Test
        fun `should add in supplier`() = runTest {
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
    inner class AddPendingFriend {

        @Test
        fun `should add in supplier`() = runTest {
            val slotUuid1 = slot<UUID>()
            val slotUuid2 = slot<UUID>()

            coEvery { cacheService.addPendingFriend(capture(slotUuid1), capture(slotUuid2)) } returns true

            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            assertTrue { cacheEntitySupplier.addPendingFriend(uuid1, uuid2) }
            coVerify(exactly = 1) { cacheService.addPendingFriend(any(), any()) }

            assertEquals(uuid1, slotUuid1.captured)
            assertEquals(uuid2, slotUuid2.captured)
        }

        @Test
        fun `should return false when supplier returns false`() = runTest {
            coEvery { cacheService.addPendingFriend(any(), any()) } returns false
            assertFalse { cacheEntitySupplier.addPendingFriend(mockk(), mockk()) }
            coVerify(exactly = 1) { cacheService.addPendingFriend(any(), any()) }
        }

        @Test
        fun `should return true when supplier returns false`() = runTest {
            coEvery { cacheService.addPendingFriend(any(), any()) } returns true
            assertTrue { cacheEntitySupplier.addPendingFriend(mockk(), mockk()) }
            coVerify(exactly = 1) { cacheService.addPendingFriend(any(), any()) }
        }
    }

    @Nested
    inner class RemoveFriend {

        @Test
        fun `should remove in supplier`() = runTest {
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
    inner class RemovePendingFriend {

        @Test
        fun `should remove in supplier`() = runTest {
            val slotUuid1 = slot<UUID>()
            val slotUuid2 = slot<UUID>()

            coEvery { cacheService.removePendingFriend(capture(slotUuid1), capture(slotUuid2)) } returns true

            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            assertTrue { cacheEntitySupplier.removePendingFriend(uuid1, uuid2) }
            coVerify(exactly = 1) { cacheService.removePendingFriend(any(), any()) }

            assertEquals(uuid1, slotUuid1.captured)
            assertEquals(uuid2, slotUuid2.captured)
        }

        @Test
        fun `should return false when supplier returns false`() = runTest {
            coEvery { cacheService.removePendingFriend(any(), any()) } returns false
            assertFalse { cacheEntitySupplier.removePendingFriend(mockk(), mockk()) }
            coVerify(exactly = 1) { cacheService.removePendingFriend(any(), any()) }
        }

        @Test
        fun `should return true when supplier returns false`() = runTest {
            coEvery { cacheService.removePendingFriend(any(), any()) } returns true
            assertTrue { cacheEntitySupplier.removePendingFriend(mockk(), mockk()) }
            coVerify(exactly = 1) { cacheService.removePendingFriend(any(), any()) }
        }
    }

    @Nested
    inner class GetFriends {

        @Test
        fun `should get in supplier`() = runTest {
            val slotUuid = slot<UUID>()

            coEvery { cacheService.getFriends(capture(slotUuid)) } returns emptyFlow()

            val uuid1 = UUID.randomUUID()
            assertEquals(emptyList(), cacheEntitySupplier.getFriends(uuid1).toList())
            coVerify(exactly = 1) { cacheService.getFriends(any()) }

            assertEquals(uuid1, slotUuid.captured)
        }

        @Test
        fun `should return empty collection when supplier returns empty collection`() = runTest {
            coEvery { cacheService.getFriends(any()) } returns emptyFlow()
            assertEquals(emptyList(), cacheEntitySupplier.getFriends(mockk()).toList())
            coVerify(exactly = 1) { cacheService.getFriends(any()) }
        }

        @Test
        fun `should return not empty collection when supplier returns not empty collection`() = runTest {
            val expected = List(5) { UUID.randomUUID() }
            coEvery { cacheService.getFriends(any()) } returns expected.asFlow()
            assertEquals(expected, cacheEntitySupplier.getFriends(mockk()).toList())
            coVerify(exactly = 1) { cacheService.getFriends(any()) }
        }

    }

    @Nested
    inner class GetPendingFriends {

        @Test
        fun `should get in supplier`() = runTest {
            val slotUuid = slot<UUID>()

            coEvery { cacheService.getPendingFriends(capture(slotUuid)) } returns emptyFlow()

            val uuid1 = UUID.randomUUID()
            assertEquals(emptyList(), cacheEntitySupplier.getPendingFriends(uuid1).toList())
            coVerify(exactly = 1) { cacheService.getPendingFriends(any()) }

            assertEquals(uuid1, slotUuid.captured)
        }

        @Test
        fun `should return empty collection when supplier returns empty collection`() = runTest {
            coEvery { cacheService.getPendingFriends(any()) } returns emptyFlow()
            assertEquals(emptyList(), cacheEntitySupplier.getPendingFriends(mockk()).toList())
            coVerify(exactly = 1) { cacheService.getPendingFriends(any()) }
        }

        @Test
        fun `should return not empty collection when supplier returns not empty collection`() = runTest {
            val expected = List(5) { UUID.randomUUID() }
            coEvery { cacheService.getPendingFriends(any()) } returns expected.asFlow()
            assertEquals(expected, cacheEntitySupplier.getPendingFriends(mockk()).toList())
            coVerify(exactly = 1) { cacheService.getPendingFriends(any()) }
        }

    }

    @Nested
    inner class SetFriends {

        @Test
        fun `should set in supplier`() = runTest {
            val slotUuid1 = slot<UUID>()
            val slotFriends = slot<Set<UUID>>()

            coEvery { cacheService.setFriends(capture(slotUuid1), capture(slotFriends)) } returns true

            val uuid1 = UUID.randomUUID()
            val friends = List(5) { UUID.randomUUID() }.toSet()
            assertTrue { cacheEntitySupplier.setFriends(uuid1, friends) }
            coVerify(exactly = 1) { cacheService.setFriends(any(), any()) }

            assertEquals(uuid1, slotUuid1.captured)
            assertEquals(friends, slotFriends.captured)
        }

        @Test
        fun `should return false when supplier returns false`() = runTest {
            coEvery { cacheService.setFriends(any(), any()) } returns false
            assertFalse { cacheEntitySupplier.setFriends(mockk(), mockk()) }
            coVerify(exactly = 1) { cacheService.setFriends(any(), any()) }
        }

        @Test
        fun `should return true when supplier returns false`() = runTest {
            coEvery { cacheService.setFriends(any(), any()) } returns true
            assertTrue { cacheEntitySupplier.setFriends(mockk(), mockk()) }
            coVerify(exactly = 1) { cacheService.setFriends(any(), any()) }
        }

    }

    @Nested
    inner class SetPendingFriends {

        @Test
        fun `should set in supplier`() = runTest {
            val slotUuid1 = slot<UUID>()
            val slotFriends = slot<Set<UUID>>()

            coEvery { cacheService.setPendingFriends(capture(slotUuid1), capture(slotFriends)) } returns true

            val uuid1 = UUID.randomUUID()
            val friends = List(5) { UUID.randomUUID() }.toSet()
            assertTrue { cacheEntitySupplier.setPendingFriends(uuid1, friends) }
            coVerify(exactly = 1) { cacheService.setPendingFriends(any(), any()) }

            assertEquals(uuid1, slotUuid1.captured)
            assertEquals(friends, slotFriends.captured)
        }

        @Test
        fun `should return false when supplier returns false`() = runTest {
            coEvery { cacheService.setPendingFriends(any(), any()) } returns false
            assertFalse { cacheEntitySupplier.setPendingFriends(mockk(), mockk()) }
            coVerify(exactly = 1) { cacheService.setPendingFriends(any(), any()) }
        }

        @Test
        fun `should return true when supplier returns false`() = runTest {
            coEvery { cacheService.setPendingFriends(any(), any()) } returns true
            assertTrue { cacheEntitySupplier.setPendingFriends(mockk(), mockk()) }
            coVerify(exactly = 1) { cacheService.setPendingFriends(any(), any()) }
        }

    }

    @Nested
    inner class IsFriend {

        @Test
        fun `should is in supplier`() = runTest {
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

    @Nested
    inner class IsPendingFriend {

        @Test
        fun `should is in supplier`() = runTest {
            val slotUuid1 = slot<UUID>()
            val slotUuid2 = slot<UUID>()

            coEvery { cacheService.isPendingFriend(capture(slotUuid1), capture(slotUuid2)) } returns true

            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            assertTrue { cacheEntitySupplier.isPendingFriend(uuid1, uuid2) }
            coVerify(exactly = 1) { cacheService.isPendingFriend(any(), any()) }

            assertEquals(uuid1, slotUuid1.captured)
            assertEquals(uuid2, slotUuid2.captured)
        }

        @Test
        fun `should return false when supplier returns false`() = runTest {
            coEvery { cacheService.isPendingFriend(any(), any()) } returns false
            assertFalse { cacheEntitySupplier.isPendingFriend(mockk(), mockk()) }
            coVerify(exactly = 1) { cacheService.isPendingFriend(any(), any()) }
        }

        @Test
        fun `should return true when supplier returns false`() = runTest {
            coEvery { cacheService.isPendingFriend(any(), any()) } returns true
            assertTrue { cacheEntitySupplier.isPendingFriend(mockk(), mockk()) }
            coVerify(exactly = 1) { cacheService.isPendingFriend(any(), any()) }
        }
    }

}