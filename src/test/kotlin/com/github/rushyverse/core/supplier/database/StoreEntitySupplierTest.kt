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

class StoreEntitySupplierTest {

    private lateinit var entitySupplier: StoreEntitySupplier
    private lateinit var cache: CacheEntitySupplier
    private lateinit var supplier: IEntitySupplier

    @BeforeTest
    fun onBefore() {
        cache = mockk(getRandomString())
        supplier = mockk(getRandomString())
        entitySupplier = StoreEntitySupplier(cache, supplier)
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

}