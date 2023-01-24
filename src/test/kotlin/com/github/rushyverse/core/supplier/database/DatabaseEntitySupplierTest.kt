package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.data.FriendDatabaseService
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import io.mockk.slot
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Nested
import java.util.*
import kotlin.test.*

class DatabaseEntitySupplierTest {

    private lateinit var service: FriendDatabaseService
    private lateinit var databaseEntitySupplier: DatabaseEntitySupplier

    @BeforeTest
    fun onBefore() {
        service = mockk()
        databaseEntitySupplier = DatabaseEntitySupplier(service)
    }

    @Nested
    inner class AddFriend {

        @Test
        fun `should add in supplier`() = runTest {
            val slotUuid1 = slot<UUID>()
            val slotUuid2 = slot<UUID>()

            coEvery { service.addFriend(capture(slotUuid1), capture(slotUuid2)) } returns true

            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            assertTrue { databaseEntitySupplier.addFriend(uuid1, uuid2) }
            coVerify(exactly = 1) { service.addFriend(any(), any()) }

            assertEquals(uuid1, slotUuid1.captured)
            assertEquals(uuid2, slotUuid2.captured)
        }

        @Test
        fun `should return false when supplier returns false`() = runTest {
            coEvery { service.addFriend(any(), any()) } returns false
            assertFalse { databaseEntitySupplier.addFriend(mockk(), mockk()) }
            coVerify(exactly = 1) { service.addFriend(any(), any()) }
        }

        @Test
        fun `should return true when supplier returns false`() = runTest {
            coEvery { service.addFriend(any(), any()) } returns true
            assertTrue { databaseEntitySupplier.addFriend(mockk(), mockk()) }
            coVerify(exactly = 1) { service.addFriend(any(), any()) }
        }
    }

    @Nested
    inner class AddPendingFriend {

        @Test
        fun `should add in supplier`() = runTest {
            val slotUuid1 = slot<UUID>()
            val slotUuid2 = slot<UUID>()

            coEvery { service.addPendingFriend(capture(slotUuid1), capture(slotUuid2)) } returns true

            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            assertTrue { databaseEntitySupplier.addPendingFriend(uuid1, uuid2) }
            coVerify(exactly = 1) { service.addPendingFriend(any(), any()) }

            assertEquals(uuid1, slotUuid1.captured)
            assertEquals(uuid2, slotUuid2.captured)
        }

        @Test
        fun `should return false when supplier returns false`() = runTest {
            coEvery { service.addPendingFriend(any(), any()) } returns false
            assertFalse { databaseEntitySupplier.addPendingFriend(mockk(), mockk()) }
            coVerify(exactly = 1) { service.addPendingFriend(any(), any()) }
        }

        @Test
        fun `should return true when supplier returns false`() = runTest {
            coEvery { service.addPendingFriend(any(), any()) } returns true
            assertTrue { databaseEntitySupplier.addPendingFriend(mockk(), mockk()) }
            coVerify(exactly = 1) { service.addPendingFriend(any(), any()) }
        }
    }

    @Nested
    inner class RemoveFriend {

        @Test
        fun `should remove friend in supplier`() = runTest {
            val slotUuid1 = slot<UUID>()
            val slotUuid2 = slot<UUID>()

            coEvery { service.removeFriend(capture(slotUuid1), capture(slotUuid2)) } returns true

            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            assertTrue { databaseEntitySupplier.removeFriend(uuid1, uuid2) }
            coVerify(exactly = 1) { service.removeFriend(any(), any()) }

            assertEquals(uuid1, slotUuid1.captured)
            assertEquals(uuid2, slotUuid2.captured)
        }

        @Test
        fun `should return false when supplier returns false`() = runTest {
            coEvery { service.removeFriend(any(), any()) } returns false
            assertFalse { databaseEntitySupplier.removeFriend(mockk(), mockk()) }
            coVerify(exactly = 1) { service.removeFriend(any(), any()) }
        }

        @Test
        fun `should return true when supplier returns false`() = runTest {
            coEvery { service.removeFriend(any(), any()) } returns true
            assertTrue { databaseEntitySupplier.removeFriend(mockk(), mockk()) }
            coVerify(exactly = 1) { service.removeFriend(any(), any()) }
        }
    }

    @Nested
    inner class RemovePendingFriend {

        @Test
        fun `should remove in supplier`() = runTest {
            val slotUuid1 = slot<UUID>()
            val slotUuid2 = slot<UUID>()

            coEvery { service.removePendingFriend(capture(slotUuid1), capture(slotUuid2)) } returns true

            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            assertTrue { databaseEntitySupplier.removePendingFriend(uuid1, uuid2) }
            coVerify(exactly = 1) { service.removePendingFriend(any(), any()) }

            assertEquals(uuid1, slotUuid1.captured)
            assertEquals(uuid2, slotUuid2.captured)
        }

        @Test
        fun `should return false when supplier returns false`() = runTest {
            coEvery { service.removePendingFriend(any(), any()) } returns false
            assertFalse { databaseEntitySupplier.removePendingFriend(mockk(), mockk()) }
            coVerify(exactly = 1) { service.removePendingFriend(any(), any()) }
        }

        @Test
        fun `should return true when supplier returns false`() = runTest {
            coEvery { service.removePendingFriend(any(), any()) } returns true
            assertTrue { databaseEntitySupplier.removePendingFriend(mockk(), mockk()) }
            coVerify(exactly = 1) { service.removePendingFriend(any(), any()) }
        }
    }

    @Nested
    inner class GetFriends {

        @Test
        fun `should get friends in supplier`() = runTest {
            val slotUuid = slot<UUID>()

            coEvery { service.getFriends(capture(slotUuid)) } returns emptyFlow()

            val uuid1 = UUID.randomUUID()
            assertEquals(emptyList(), databaseEntitySupplier.getFriends(uuid1).toList())
            coVerify(exactly = 1) { service.getFriends(any()) }

            assertEquals(uuid1, slotUuid.captured)
        }

        @Test
        fun `should return empty collection when supplier returns empty collection`() = runTest {
            coEvery { service.getFriends(any()) } returns emptyFlow()
            assertEquals(emptyList(), databaseEntitySupplier.getFriends(mockk()).toList())
            coVerify(exactly = 1) { service.getFriends(any()) }
        }

        @Test
        fun `should return not empty collection when supplier returns not empty collection`() = runTest {
            val expected = List(5) { UUID.randomUUID() }
            coEvery { service.getFriends(any()) } returns expected.asFlow()
            assertEquals(expected, databaseEntitySupplier.getFriends(mockk()).toList())
            coVerify(exactly = 1) { service.getFriends(any()) }
        }

    }

    @Nested
    inner class GetPendingFriends {

        @Test
        fun `should get in supplier`() = runTest {
            val slotUuid = slot<UUID>()

            coEvery { service.getPendingFriends(capture(slotUuid)) } returns emptyFlow()

            val uuid1 = UUID.randomUUID()
            assertEquals(emptyList(), databaseEntitySupplier.getPendingFriends(uuid1).toList())
            coVerify(exactly = 1) { service.getPendingFriends(any()) }

            assertEquals(uuid1, slotUuid.captured)
        }

        @Test
        fun `should return empty collection when supplier returns empty collection`() = runTest {
            coEvery { service.getPendingFriends(any()) } returns emptyFlow()
            assertEquals(emptyList(), databaseEntitySupplier.getPendingFriends(mockk()).toList())
            coVerify(exactly = 1) { service.getPendingFriends(any()) }
        }

        @Test
        fun `should return not empty collection when supplier returns not empty collection`() = runTest {
            val expected = List(5) { UUID.randomUUID() }
            coEvery { service.getPendingFriends(any()) } returns expected.asFlow()
            assertEquals(expected, databaseEntitySupplier.getPendingFriends(mockk()).toList())
            coVerify(exactly = 1) { service.getPendingFriends(any()) }
        }

    }

    @Nested
    inner class IsFriend {

        @Test
        fun `should is in supplier`() = runTest {
            val slotUuid1 = slot<UUID>()
            val slotUuid2 = slot<UUID>()

            coEvery { service.isFriend(capture(slotUuid1), capture(slotUuid2)) } returns true

            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            assertTrue { databaseEntitySupplier.isFriend(uuid1, uuid2) }
            coVerify(exactly = 1) { service.isFriend(any(), any()) }

            assertEquals(uuid1, slotUuid1.captured)
            assertEquals(uuid2, slotUuid2.captured)
        }

        @Test
        fun `should return false when supplier returns false`() = runTest {
            coEvery { service.isFriend(any(), any()) } returns false
            assertFalse { databaseEntitySupplier.isFriend(mockk(), mockk()) }
            coVerify(exactly = 1) { service.isFriend(any(), any()) }
        }

        @Test
        fun `should return true when supplier returns false`() = runTest {
            coEvery { service.isFriend(any(), any()) } returns true
            assertTrue { databaseEntitySupplier.isFriend(mockk(), mockk()) }
            coVerify(exactly = 1) { service.isFriend(any(), any()) }
        }
    }

    @Nested
    inner class IsPendingFriend {

        @Test
        fun `should is in supplier`() = runTest {
            val slotUuid1 = slot<UUID>()
            val slotUuid2 = slot<UUID>()

            coEvery { service.isPendingFriend(capture(slotUuid1), capture(slotUuid2)) } returns true

            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            assertTrue { databaseEntitySupplier.isPendingFriend(uuid1, uuid2) }
            coVerify(exactly = 1) { service.isPendingFriend(any(), any()) }

            assertEquals(uuid1, slotUuid1.captured)
            assertEquals(uuid2, slotUuid2.captured)
        }

        @Test
        fun `should return false when supplier returns false`() = runTest {
            coEvery { service.isPendingFriend(any(), any()) } returns false
            assertFalse { databaseEntitySupplier.isPendingFriend(mockk(), mockk()) }
            coVerify(exactly = 1) { service.isPendingFriend(any(), any()) }
        }

        @Test
        fun `should return true when supplier returns false`() = runTest {
            coEvery { service.isPendingFriend(any(), any()) } returns true
            assertTrue { databaseEntitySupplier.isPendingFriend(mockk(), mockk()) }
            coVerify(exactly = 1) { service.isPendingFriend(any(), any()) }
        }
    }

}