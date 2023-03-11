package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.utils.getRandomString
import io.mockk.*
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Nested
import java.util.*
import kotlin.test.*

class DatabaseStoreEntitySupplierTest {

    private lateinit var entitySupplier: DatabaseStoreEntitySupplier
    private lateinit var cache: DatabaseCacheEntitySupplier
    private lateinit var supplier: IDatabaseEntitySupplier

    @BeforeTest
    fun onBefore() {
        cache = mockk(getRandomString())
        supplier = mockk(getRandomString())
        entitySupplier = DatabaseStoreEntitySupplier(cache, supplier)
    }

    @Test
    fun `get configuration will get from cache supplier`() = runTest {
        val configuration = mockk<DatabaseSupplierConfiguration>(getRandomString())
        every { cache.configuration } returns configuration

        assertEquals(configuration, entitySupplier.configuration)
        verify(exactly = 1) { cache.configuration }
    }

    @Nested
    inner class AddFriend {

        @Test
        fun `should set data in cache if set in supplier`() = runTest {
            val id = UUID.randomUUID()
            val friend = UUID.randomUUID()

            coEvery { supplier.addFriend(id, friend) } returns true
            coEvery { cache.addFriend(id, friend) } returns true

            assertTrue(entitySupplier.addFriend(id, friend))
            coVerify(exactly = 1) { supplier.addFriend(id, friend) }
            coVerify(exactly = 1) { cache.addFriend(id, friend) }
        }

        @Test
        fun `should not set data in cache if not set in supplier`() = runTest {
            val id = UUID.randomUUID()
            val friend = UUID.randomUUID()

            coEvery { supplier.addFriend(id, friend) } returns false
            coEvery { cache.addFriend(id, friend) } returns false

            assertFalse(entitySupplier.addFriend(id, friend))
            coVerify(exactly = 1) { supplier.addFriend(id, friend) }
            coVerify(exactly = 0) { cache.addFriend(id, friend) }
        }

        @Test
        fun `should return true if data is set in supplier but not in cache`() = runTest {
            val id = UUID.randomUUID()
            val friend = UUID.randomUUID()

            coEvery { supplier.addFriend(id, friend) } returns true
            coEvery { cache.addFriend(id, friend) } returns false

            assertTrue(entitySupplier.addFriend(id, friend))
            coVerify(exactly = 1) { supplier.addFriend(id, friend) }
            coVerify(exactly = 1) { cache.addFriend(id, friend) }
        }

    }

    @Nested
    inner class AddPendingFriend {

        @Test
        fun `should set data in cache if set in supplier`() = runTest {
            val id = UUID.randomUUID()
            val friend = UUID.randomUUID()

            coEvery { supplier.addPendingFriend(id, friend) } returns true
            coEvery { cache.addPendingFriend(id, friend) } returns true

            assertTrue(entitySupplier.addPendingFriend(id, friend))
            coVerify(exactly = 1) { supplier.addPendingFriend(id, friend) }
            coVerify(exactly = 1) { cache.addPendingFriend(id, friend) }
        }

        @Test
        fun `should not set data in cache if not set in supplier`() = runTest {
            val id = UUID.randomUUID()
            val friend = UUID.randomUUID()

            coEvery { supplier.addPendingFriend(id, friend) } returns false
            coEvery { cache.addPendingFriend(id, friend) } returns false

            assertFalse(entitySupplier.addPendingFriend(id, friend))
            coVerify(exactly = 1) { supplier.addPendingFriend(id, friend) }
            coVerify(exactly = 0) { cache.addPendingFriend(id, friend) }
        }

        @Test
        fun `should return true if data is set in supplier but not in cache`() = runTest {
            val id = UUID.randomUUID()
            val friend = UUID.randomUUID()

            coEvery { supplier.addPendingFriend(id, friend) } returns true
            coEvery { cache.addPendingFriend(id, friend) } returns false

            assertTrue(entitySupplier.addPendingFriend(id, friend))
            coVerify(exactly = 1) { supplier.addPendingFriend(id, friend) }
            coVerify(exactly = 1) { cache.addPendingFriend(id, friend) }
        }

    }

    @Nested
    inner class RemoveFriend {

        @Test
        fun `should set data in cache if set in supplier`() = runTest {
            val id = UUID.randomUUID()
            val friend = UUID.randomUUID()

            coEvery { supplier.removeFriend(id, friend) } returns true
            coEvery { cache.removeFriend(id, friend) } returns true

            assertTrue(entitySupplier.removeFriend(id, friend))
            coVerify(exactly = 1) { supplier.removeFriend(id, friend) }
            coVerify(exactly = 1) { cache.removeFriend(id, friend) }
        }

        @Test
        fun `should not set data in cache if not set in supplier`() = runTest {
            val id = UUID.randomUUID()
            val friend = UUID.randomUUID()

            coEvery { supplier.removeFriend(id, friend) } returns false
            coEvery { cache.removeFriend(id, friend) } returns false

            assertFalse(entitySupplier.removeFriend(id, friend))
            coVerify(exactly = 1) { supplier.removeFriend(id, friend) }
            coVerify(exactly = 0) { cache.removeFriend(id, friend) }
        }

        @Test
        fun `should return true if data is set in supplier but not in cache`() = runTest {
            val id = UUID.randomUUID()
            val friend = UUID.randomUUID()

            coEvery { supplier.removeFriend(id, friend) } returns true
            coEvery { cache.removeFriend(id, friend) } returns false

            assertTrue(entitySupplier.removeFriend(id, friend))
            coVerify(exactly = 1) { supplier.removeFriend(id, friend) }
            coVerify(exactly = 1) { cache.removeFriend(id, friend) }
        }

    }

    @Nested
    inner class RemovePendingFriend {

        @Test
        fun `should set data in cache if set in supplier`() = runTest {
            val id = UUID.randomUUID()
            val friend = UUID.randomUUID()

            coEvery { supplier.removePendingFriend(id, friend) } returns true
            coEvery { cache.removePendingFriend(id, friend) } returns true

            assertTrue(entitySupplier.removePendingFriend(id, friend))
            coVerify(exactly = 1) { supplier.removePendingFriend(id, friend) }
            coVerify(exactly = 1) { cache.removePendingFriend(id, friend) }
        }

        @Test
        fun `should not set data in cache if not set in supplier`() = runTest {
            val id = UUID.randomUUID()
            val friend = UUID.randomUUID()

            coEvery { supplier.removePendingFriend(id, friend) } returns false
            coEvery { cache.removePendingFriend(id, friend) } returns false

            assertFalse(entitySupplier.removePendingFriend(id, friend))
            coVerify(exactly = 1) { supplier.removePendingFriend(id, friend) }
            coVerify(exactly = 0) { cache.removePendingFriend(id, friend) }
        }

        @Test
        fun `should return true if data is set in supplier but not in cache`() = runTest {
            val id = UUID.randomUUID()
            val friend = UUID.randomUUID()

            coEvery { supplier.removePendingFriend(id, friend) } returns true
            coEvery { cache.removePendingFriend(id, friend) } returns false

            assertTrue(entitySupplier.removePendingFriend(id, friend))
            coVerify(exactly = 1) { supplier.removePendingFriend(id, friend) }
            coVerify(exactly = 1) { cache.removePendingFriend(id, friend) }
        }

    }

    @Nested
    inner class GetFriends {

        @Test
        fun `should store empty list in cache if the flow is empty`() = runTest {
            val id = UUID.randomUUID()
            coEvery { supplier.getFriends(id) } returns emptyFlow()
            coEvery { cache.setFriends(id, any()) } returns true

            assertEquals(emptyList(), entitySupplier.getFriends(id).toList())
            coVerify(exactly = 1) { supplier.getFriends(id) }
            coVerify(exactly = 1) { cache.setFriends(id, emptySet()) }
        }

        @Test
        fun `should store list of friends in cache if the flow is not empty`() = runTest {
            val id = UUID.randomUUID()
            val friends = List(5) { UUID.randomUUID() }
            coEvery { supplier.getFriends(id) } returns friends.asFlow()
            coEvery { cache.setFriends(id, any()) } returns true

            assertEquals(friends, entitySupplier.getFriends(id).toList())
            coVerify(exactly = 1) { supplier.getFriends(id) }
            coVerify(exactly = 1) { cache.setFriends(id, friends.toSet()) }
        }
    }

    @Nested
    inner class GetPendingFriends {

        @Test
        fun `should store empty list in cache if the flow is empty`() = runTest {
            val id = UUID.randomUUID()
            coEvery { supplier.getPendingFriends(id) } returns emptyFlow()
            coEvery { cache.setPendingFriends(id, any()) } returns true

            assertEquals(emptyList(), entitySupplier.getPendingFriends(id).toList())
            coVerify(exactly = 1) { supplier.getPendingFriends(id) }
            coVerify(exactly = 1) { cache.setPendingFriends(id, emptySet()) }
        }

        @Test
        fun `should store list of friends in cache if the flow is not empty`() = runTest {
            val id = UUID.randomUUID()
            val friends = List(5) { UUID.randomUUID() }
            coEvery { supplier.getPendingFriends(id) } returns friends.asFlow()
            coEvery { cache.setPendingFriends(id, any()) } returns true

            assertEquals(friends, entitySupplier.getPendingFriends(id).toList())
            coVerify(exactly = 1) { supplier.getPendingFriends(id) }
            coVerify(exactly = 1) { cache.setPendingFriends(id, friends.toSet()) }
        }
    }

    @Nested
    inner class IsFriend {

        @Test
        fun `should return the result returned by supplier`() = runTest {
            val id = UUID.randomUUID()
            val friend = UUID.randomUUID()

            coEvery { supplier.isFriend(id, friend) } returns false
            assertFalse(entitySupplier.isFriend(id, friend))
            coVerify(exactly = 1) { supplier.isFriend(id, friend) }

            coEvery { supplier.isFriend(id, friend) } returns true
            assertTrue(entitySupplier.isFriend(id, friend))
            coVerify(exactly = 2) { supplier.isFriend(id, friend) }
        }

        @Test
        fun `should not use the cache to check`() = runTest {
            val id = UUID.randomUUID()
            val friend = UUID.randomUUID()

            coEvery { supplier.isFriend(id, friend) } returns false
            assertFalse(entitySupplier.isFriend(id, friend))

            coVerify(exactly = 0) { cache.isFriend(any(), any()) }
        }

    }

    @Nested
    inner class IsPendingFriend {

        @Test
        fun `should return the result returned by supplier`() = runTest {
            val id = UUID.randomUUID()
            val friend = UUID.randomUUID()

            coEvery { supplier.isPendingFriend(id, friend) } returns false
            assertFalse(entitySupplier.isPendingFriend(id, friend))
            coVerify(exactly = 1) { supplier.isPendingFriend(id, friend) }

            coEvery { supplier.isPendingFriend(id, friend) } returns true
            assertTrue(entitySupplier.isPendingFriend(id, friend))
            coVerify(exactly = 2) { supplier.isPendingFriend(id, friend) }
        }

        @Test
        fun `should not use the cache to check`() = runTest {
            val id = UUID.randomUUID()
            val friend = UUID.randomUUID()

            coEvery { supplier.isPendingFriend(id, friend) } returns false
            assertFalse(entitySupplier.isPendingFriend(id, friend))

            coVerify(exactly = 0) { cache.isPendingFriend(any(), any()) }
        }

    }

}