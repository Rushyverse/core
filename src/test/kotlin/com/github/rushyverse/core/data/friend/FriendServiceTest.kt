package com.github.rushyverse.core.data.friend

import com.github.rushyverse.core.data.FriendService
import com.github.rushyverse.core.utils.getRandomString
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

class FriendServiceTest {

    private lateinit var service: FriendService
    private val supplier get() = service.supplier

    @BeforeTest
    fun onBefore() {
        service = FriendService(mockk(getRandomString()))
    }

    @Nested
    inner class AddFriend {

        @Test
        fun `should add friend in supplier`() = runTest {
            val slotUuid1 = slot<UUID>()
            val slotUuid2 = slot<UUID>()

            coEvery { supplier.addFriend(capture(slotUuid1), capture(slotUuid2)) } returns true

            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            assertTrue { service.addFriend(uuid1, uuid2) }
            coVerify(exactly = 1) { supplier.addFriend(any(), any()) }

            assertEquals(uuid1, slotUuid1.captured)
            assertEquals(uuid2, slotUuid2.captured)
        }

        @Test
        fun `should return false when supplier returns false`() = runTest {
            coEvery { supplier.addFriend(any(), any()) } returns false
            assertFalse { service.addFriend(mockk(), mockk()) }
            coVerify(exactly = 1) { supplier.addFriend(any(), any()) }
        }

        @Test
        fun `should return true when supplier returns false`() = runTest {
            coEvery { supplier.addFriend(any(), any()) } returns true
            assertTrue { service.addFriend(mockk(), mockk()) }
            coVerify(exactly = 1) { supplier.addFriend(any(), any()) }
        }
    }

    @Nested
    inner class RemoveFriend {

        @Test
        fun `should remove friend in supplier`() = runTest {
            val slotUuid1 = slot<UUID>()
            val slotUuid2 = slot<UUID>()

            coEvery { supplier.removeFriend(capture(slotUuid1), capture(slotUuid2)) } returns true

            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            assertTrue { service.removeFriend(uuid1, uuid2) }
            coVerify(exactly = 1) { supplier.removeFriend(any(), any()) }

            assertEquals(uuid1, slotUuid1.captured)
            assertEquals(uuid2, slotUuid2.captured)
        }

        @Test
        fun `should return false when supplier returns false`() = runTest {
            coEvery { supplier.removeFriend(any(), any()) } returns false
            assertFalse { service.removeFriend(mockk(), mockk()) }
            coVerify(exactly = 1) { supplier.removeFriend(any(), any()) }
        }

        @Test
        fun `should return true when supplier returns false`() = runTest {
            coEvery { supplier.removeFriend(any(), any()) } returns true
            assertTrue { service.removeFriend(mockk(), mockk()) }
            coVerify(exactly = 1) { supplier.removeFriend(any(), any()) }
        }
    }

    @Nested
    inner class GetFriends {

        @Test
        fun `should get friends in supplier`() = runTest {
            val slotUuid = slot<UUID>()

            coEvery { supplier.getFriends(capture(slotUuid)) } returns emptyFlow()

            val uuid1 = UUID.randomUUID()
            assertEquals(emptyList(), service.getFriends(uuid1).toList())
            coVerify(exactly = 1) { supplier.getFriends(any()) }

            assertEquals(uuid1, slotUuid.captured)
        }

        @Test
        fun `should return empty collection when supplier returns empty collection`() = runTest {
            coEvery { supplier.getFriends(any()) } returns emptyFlow()
            assertEquals(emptyList(), service.getFriends(mockk()).toList())
            coVerify(exactly = 1) { supplier.getFriends(any()) }
        }

        @Test
        fun `should return not empty collection when supplier returns not empty collection`() = runTest {
            val expected = List(5) { UUID.randomUUID() }
            coEvery { supplier.getFriends(any()) } returns expected.asFlow()
            assertEquals(expected, service.getFriends(mockk()).toList())
            coVerify(exactly = 1) { supplier.getFriends(any()) }
        }

    }

    @Nested
    inner class IsFriend {

        @Test
        fun `should is friend in supplier`() = runTest {
            val slotUuid1 = slot<UUID>()
            val slotUuid2 = slot<UUID>()

            coEvery { supplier.isFriend(capture(slotUuid1), capture(slotUuid2)) } returns true

            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            assertTrue { service.isFriend(uuid1, uuid2) }
            coVerify(exactly = 1) { supplier.isFriend(any(), any()) }

            assertEquals(uuid1, slotUuid1.captured)
            assertEquals(uuid2, slotUuid2.captured)
        }

        @Test
        fun `should return false when supplier returns false`() = runTest {
            coEvery { supplier.isFriend(any(), any()) } returns false
            assertFalse { service.isFriend(mockk(), mockk()) }
            coVerify(exactly = 1) { supplier.isFriend(any(), any()) }
        }

        @Test
        fun `should return true when supplier returns false`() = runTest {
            coEvery { supplier.isFriend(any(), any()) } returns true
            assertTrue { service.isFriend(mockk(), mockk()) }
            coVerify(exactly = 1) { supplier.isFriend(any(), any()) }
        }
    }
}