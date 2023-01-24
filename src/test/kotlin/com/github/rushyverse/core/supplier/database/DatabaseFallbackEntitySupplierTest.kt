package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.utils.getRandomString
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Nested
import java.util.*
import kotlin.test.*

class DatabaseFallbackEntitySupplierTest {

    private lateinit var fallbackEntitySupplier: DatabaseFallbackEntitySupplier
    private lateinit var getPrioritySupplier: IDatabaseEntitySupplier
    private lateinit var setPrioritySupplier: IDatabaseEntitySupplier

    @BeforeTest
    fun onBefore() {
        getPrioritySupplier = mockk(getRandomString())
        setPrioritySupplier = mockk(getRandomString())
        fallbackEntitySupplier = DatabaseFallbackEntitySupplier(getPrioritySupplier, setPrioritySupplier)
    }

    @Nested
    inner class AddFriend {

        @Test
        fun `should invoke setPriority supplier first and not getPriority`() = runTest {
            val id = UUID.randomUUID()
            val friend = UUID.randomUUID()
            coEvery { setPrioritySupplier.addFriend(id, friend) } returns false
            coEvery { getPrioritySupplier.addFriend(id, friend) } returns false

            assertFalse(fallbackEntitySupplier.addFriend(id, friend))
            coVerify(exactly = 1) { setPrioritySupplier.addFriend(id, friend) }
            coVerify(exactly = 0) { getPrioritySupplier.addFriend(id, friend) }

            coEvery { setPrioritySupplier.addFriend(id, friend) } returns true
            coEvery { getPrioritySupplier.addFriend(id, friend) } returns false

            assertTrue(fallbackEntitySupplier.addFriend(id, friend))
            coVerify(exactly = 2) { setPrioritySupplier.addFriend(id, friend) }
            coVerify(exactly = 0) { getPrioritySupplier.addFriend(id, friend) }
        }

    }

    @Nested
    inner class AddPendingFriend {

        @Test
        fun `should invoke setPriority supplier first and not getPriority`() = runTest {
            val id = UUID.randomUUID()
            val friend = UUID.randomUUID()
            coEvery { setPrioritySupplier.addPendingFriend(id, friend) } returns false
            coEvery { getPrioritySupplier.addPendingFriend(id, friend) } returns false

            assertFalse(fallbackEntitySupplier.addPendingFriend(id, friend))
            coVerify(exactly = 1) { setPrioritySupplier.addPendingFriend(id, friend) }
            coVerify(exactly = 0) { getPrioritySupplier.addPendingFriend(id, friend) }

            coEvery { setPrioritySupplier.addPendingFriend(id, friend) } returns true
            coEvery { getPrioritySupplier.addPendingFriend(id, friend) } returns false

            assertTrue(fallbackEntitySupplier.addPendingFriend(id, friend))
            coVerify(exactly = 2) { setPrioritySupplier.addPendingFriend(id, friend) }
            coVerify(exactly = 0) { getPrioritySupplier.addPendingFriend(id, friend) }
        }

    }

    @Nested
    inner class RemoveFriend {

        @Test
        fun `should invoke setPriority supplier first and not getPriority`() = runTest {
            val id = UUID.randomUUID()
            val friend = UUID.randomUUID()
            coEvery { setPrioritySupplier.removeFriend(id, friend) } returns false
            coEvery { getPrioritySupplier.removeFriend(id, friend) } returns false

            assertFalse(fallbackEntitySupplier.removeFriend(id, friend))
            coVerify(exactly = 1) { setPrioritySupplier.removeFriend(id, friend) }
            coVerify(exactly = 0) { getPrioritySupplier.removeFriend(id, friend) }

            coEvery { setPrioritySupplier.removeFriend(id, friend) } returns true
            coEvery { getPrioritySupplier.removeFriend(id, friend) } returns false

            assertTrue(fallbackEntitySupplier.removeFriend(id, friend))
            coVerify(exactly = 2) { setPrioritySupplier.removeFriend(id, friend) }
            coVerify(exactly = 0) { getPrioritySupplier.removeFriend(id, friend) }
        }

    }

    @Nested
    inner class RemovePendingFriend {

        @Test
        fun `should invoke setPriority supplier first and not getPriority`() = runTest {
            val id = UUID.randomUUID()
            val friend = UUID.randomUUID()
            coEvery { setPrioritySupplier.removePendingFriend(id, friend) } returns false
            coEvery { getPrioritySupplier.removePendingFriend(id, friend) } returns false

            assertFalse(fallbackEntitySupplier.removePendingFriend(id, friend))
            coVerify(exactly = 1) { setPrioritySupplier.removePendingFriend(id, friend) }
            coVerify(exactly = 0) { getPrioritySupplier.removePendingFriend(id, friend) }

            coEvery { setPrioritySupplier.removePendingFriend(id, friend) } returns true
            coEvery { getPrioritySupplier.removePendingFriend(id, friend) } returns false

            assertTrue(fallbackEntitySupplier.removePendingFriend(id, friend))
            coVerify(exactly = 2) { setPrioritySupplier.removePendingFriend(id, friend) }
            coVerify(exactly = 0) { getPrioritySupplier.removePendingFriend(id, friend) }
        }

    }

    @Nested
    inner class GetFriends {

        @Test
        fun `should invoke getPriority supplier first and not setPriority if list is not empty`() = runTest {
            val id = UUID.randomUUID()
            val returnedList = listOf(UUID.randomUUID())
            coEvery { setPrioritySupplier.getFriends(id) } returns emptyFlow()
            coEvery { getPrioritySupplier.getFriends(id) } returns returnedList.asFlow()

            assertEquals(returnedList, fallbackEntitySupplier.getFriends(id).toList())
            coVerify(exactly = 0) { setPrioritySupplier.getFriends(id) }
            coVerify(exactly = 1) { getPrioritySupplier.getFriends(id) }
        }

        @Test
        fun `should invoke getPriority supplier first and setPriority if list is empty`() = runTest {
            val id = UUID.randomUUID()
            coEvery { setPrioritySupplier.getFriends(id) } returns emptyFlow()
            coEvery { getPrioritySupplier.getFriends(id) } returns emptyFlow()

            assertEquals(emptyList(), fallbackEntitySupplier.getFriends(id).toList())
            coVerify(exactly = 1) { setPrioritySupplier.getFriends(id) }
            coVerify(exactly = 1) { getPrioritySupplier.getFriends(id) }
        }
    }

    @Nested
    inner class GetPendingFriends {

        @Test
        fun `should invoke getPriority supplier first and not setPriority if list is not empty`() = runTest {
            val id = UUID.randomUUID()
            val returnedList = listOf(UUID.randomUUID())
            coEvery { setPrioritySupplier.getPendingFriends(id) } returns emptyFlow()
            coEvery { getPrioritySupplier.getPendingFriends(id) } returns returnedList.asFlow()

            assertEquals(returnedList, fallbackEntitySupplier.getPendingFriends(id).toList())
            coVerify(exactly = 0) { setPrioritySupplier.getPendingFriends(id) }
            coVerify(exactly = 1) { getPrioritySupplier.getPendingFriends(id) }
        }

        @Test
        fun `should invoke getPriority supplier first and setPriority if list is empty`() = runTest {
            val id = UUID.randomUUID()
            coEvery { setPrioritySupplier.getPendingFriends(id) } returns emptyFlow()
            coEvery { getPrioritySupplier.getPendingFriends(id) } returns emptyFlow()

            assertEquals(emptyList(), fallbackEntitySupplier.getPendingFriends(id).toList())
            coVerify(exactly = 1) { setPrioritySupplier.getPendingFriends(id) }
            coVerify(exactly = 1) { getPrioritySupplier.getPendingFriends(id) }
        }
    }

    @Nested
    inner class IsFriend {

        @Test
        fun `should invoke getPriority supplier first and setPriority if return false`() = runTest {
            val id = UUID.randomUUID()
            val friend = UUID.randomUUID()
            coEvery { setPrioritySupplier.isFriend(id, friend) } returns false
            coEvery { getPrioritySupplier.isFriend(id, friend) } returns false

            assertFalse(fallbackEntitySupplier.isFriend(id, friend))
            coVerify(exactly = 1) { setPrioritySupplier.isFriend(id, friend) }
            coVerify(exactly = 1) { getPrioritySupplier.isFriend(id, friend) }
        }

        @Test
        fun `should invoke getPriority supplier first and not setPriority if return true`() = runTest {
            val id = UUID.randomUUID()
            val friend = UUID.randomUUID()
            coEvery { setPrioritySupplier.isFriend(id, friend) } returns false
            coEvery { getPrioritySupplier.isFriend(id, friend) } returns true

            assertTrue(fallbackEntitySupplier.isFriend(id, friend))
            coVerify(exactly = 0) { setPrioritySupplier.isFriend(id, friend) }
            coVerify(exactly = 1) { getPrioritySupplier.isFriend(id, friend) }
        }

        @Test
        fun `should return true if one of the supplier returns true`() = runTest {
            val id = UUID.randomUUID()
            val friend = UUID.randomUUID()
            coEvery { setPrioritySupplier.isFriend(id, friend) } returns false
            coEvery { getPrioritySupplier.isFriend(id, friend) } returns true

            assertTrue(fallbackEntitySupplier.isFriend(id, friend))

            coEvery { setPrioritySupplier.isFriend(id, friend) } returns true
            coEvery { getPrioritySupplier.isFriend(id, friend) } returns false

            assertTrue(fallbackEntitySupplier.isFriend(id, friend))

            coEvery { setPrioritySupplier.isFriend(id, friend) } returns false
            coEvery { getPrioritySupplier.isFriend(id, friend) } returns false

            assertFalse(fallbackEntitySupplier.isFriend(id, friend))
        }

    }

    @Nested
    inner class IsPendingFriend {

        @Test
        fun `should invoke getPriority supplier first and setPriority if return false`() = runTest {
            val id = UUID.randomUUID()
            val friend = UUID.randomUUID()
            coEvery { setPrioritySupplier.isPendingFriend(id, friend) } returns false
            coEvery { getPrioritySupplier.isPendingFriend(id, friend) } returns false

            assertFalse(fallbackEntitySupplier.isPendingFriend(id, friend))
            coVerify(exactly = 1) { setPrioritySupplier.isPendingFriend(id, friend) }
            coVerify(exactly = 1) { getPrioritySupplier.isPendingFriend(id, friend) }
        }

        @Test
        fun `should invoke getPriority supplier first and not setPriority if return true`() = runTest {
            val id = UUID.randomUUID()
            val friend = UUID.randomUUID()
            coEvery { setPrioritySupplier.isPendingFriend(id, friend) } returns false
            coEvery { getPrioritySupplier.isPendingFriend(id, friend) } returns true

            assertTrue(fallbackEntitySupplier.isPendingFriend(id, friend))
            coVerify(exactly = 0) { setPrioritySupplier.isPendingFriend(id, friend) }
            coVerify(exactly = 1) { getPrioritySupplier.isPendingFriend(id, friend) }
        }

        @Test
        fun `should return true if one of the supplier returns true`() = runTest {
            val id = UUID.randomUUID()
            val friend = UUID.randomUUID()
            coEvery { setPrioritySupplier.isPendingFriend(id, friend) } returns false
            coEvery { getPrioritySupplier.isPendingFriend(id, friend) } returns true

            assertTrue(fallbackEntitySupplier.isPendingFriend(id, friend))

            coEvery { setPrioritySupplier.isPendingFriend(id, friend) } returns true
            coEvery { getPrioritySupplier.isPendingFriend(id, friend) } returns false

            assertTrue(fallbackEntitySupplier.isPendingFriend(id, friend))

            coEvery { setPrioritySupplier.isPendingFriend(id, friend) } returns false
            coEvery { getPrioritySupplier.isPendingFriend(id, friend) } returns false

            assertFalse(fallbackEntitySupplier.isPendingFriend(id, friend))
        }

    }

}