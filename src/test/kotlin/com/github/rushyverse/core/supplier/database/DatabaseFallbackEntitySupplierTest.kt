package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.data.Guild
import com.github.rushyverse.core.utils.getRandomString
import io.mockk.*
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.util.*
import kotlin.random.Random
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

    @Test
    fun `get configuration will get from getPriority supplier`() = runTest {
        val configuration = mockk<DatabaseSupplierConfiguration>(getRandomString())
        every { getPrioritySupplier.configuration } returns configuration

        assertEquals(configuration, fallbackEntitySupplier.configuration)
        verify(exactly = 1) { getPrioritySupplier.configuration }
    }

    @Nested
    inner class FriendTest {

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

    @Nested
    inner class GuildTest {

        @Nested
        inner class CreateGuild {

            @Test
            fun `should invoke setPriority supplier first and not getPriority`() = runTest {
                val name = getRandomString()
                val owner = getRandomString()
                val expectedGuild = mockk<Guild>()
                coEvery { setPrioritySupplier.createGuild(name, owner) } returns expectedGuild
                coEvery { getPrioritySupplier.createGuild(name, owner) } throws Exception()

                assertEquals(expectedGuild, fallbackEntitySupplier.createGuild(name, owner))
                coVerify(exactly = 1) { setPrioritySupplier.createGuild(name, owner) }
                coVerify(exactly = 0) { getPrioritySupplier.createGuild(name, owner) }
            }

        }

        @Nested
        inner class DeleteGuild {

            @ParameterizedTest
            @ValueSource(booleans = [true, false])
            fun `should invoke setPriority supplier first and not getPriority`(result: Boolean) = runTest {
                val id = Random.nextInt()
                coEvery { setPrioritySupplier.deleteGuild(id) } returns result
                coEvery { getPrioritySupplier.deleteGuild(id) } throws Exception()

                assertEquals(result, fallbackEntitySupplier.deleteGuild(id))
                coVerify(exactly = 1) { setPrioritySupplier.deleteGuild(id) }
                coVerify(exactly = 0) { getPrioritySupplier.deleteGuild(id) }
            }
        }

        @Nested
        inner class GetGuildById {

            @Test
            fun `should invoke getPriority supplier first and setPriority if return null`() = runTest {
                val id = Random.nextInt()
                val expectedGuild = mockk<Guild>()
                coEvery { setPrioritySupplier.getGuild(id) } returns expectedGuild
                coEvery { getPrioritySupplier.getGuild(id) } returns null

                assertEquals(expectedGuild, fallbackEntitySupplier.getGuild(id))
                coVerify(exactly = 1) { setPrioritySupplier.getGuild(id) }
                coVerify(exactly = 1) { getPrioritySupplier.getGuild(id) }
            }

            @Test
            fun `should invoke getPriority supplier first and not setPriority if return not null`() = runTest {
                val id = Random.nextInt()
                val expectedGuild = mockk<Guild>()
                coEvery { setPrioritySupplier.getGuild(id) } throws Exception()
                coEvery { getPrioritySupplier.getGuild(id) } returns expectedGuild

                assertEquals(expectedGuild, fallbackEntitySupplier.getGuild(id))
                coVerify(exactly = 0) { setPrioritySupplier.getGuild(id) }
                coVerify(exactly = 1) { getPrioritySupplier.getGuild(id) }
            }

            @Test
            fun `should return null if both return null`() = runTest {
                val id = Random.nextInt()
                coEvery { setPrioritySupplier.getGuild(id) } returns null
                coEvery { getPrioritySupplier.getGuild(id) } returns null

                assertNull(fallbackEntitySupplier.getGuild(id))
                coVerify(exactly = 1) { setPrioritySupplier.getGuild(id) }
                coVerify(exactly = 1) { getPrioritySupplier.getGuild(id) }
            }

        }

        @Nested
        inner class GetGuildByName {

            @Test
            fun `should invoke getPriority supplier first and setPriority if empty`() = runTest {
                val name = getRandomString()
                val expectedGuilds = flowOf(mockk<Guild>(), mockk())
                coEvery { setPrioritySupplier.getGuild(name) } returns expectedGuilds
                coEvery { getPrioritySupplier.getGuild(name) } returns emptyFlow()

                assertThat(fallbackEntitySupplier.getGuild(name).toList()).containsExactlyElementsOf(expectedGuilds.toList())
                coVerify(exactly = 1) { setPrioritySupplier.getGuild(name) }
                coVerify(exactly = 1) { getPrioritySupplier.getGuild(name) }
            }

            @Test
            fun `should invoke getPriority supplier first and not setPriority if return not empty`() = runTest {
                val name = getRandomString()
                val expectedGuilds = flowOf(mockk<Guild>(), mockk())
                coEvery { setPrioritySupplier.getGuild(name) } throws Exception()
                coEvery { getPrioritySupplier.getGuild(name) } returns expectedGuilds

                assertThat(fallbackEntitySupplier.getGuild(name).toList()).containsExactlyElementsOf(expectedGuilds.toList())
                coVerify(exactly = 0) { setPrioritySupplier.getGuild(name) }
                coVerify(exactly = 1) { getPrioritySupplier.getGuild(name) }
            }

            @Test
            fun `should return empty flow if both return empty flow`() = runTest {
                val name = getRandomString()
                coEvery { setPrioritySupplier.getGuild(name) } returns emptyFlow()
                coEvery { getPrioritySupplier.getGuild(name) } returns emptyFlow()

                assertThat(fallbackEntitySupplier.getGuild(name).toList()).isEmpty()
                coVerify(exactly = 1) { setPrioritySupplier.getGuild(name) }
                coVerify(exactly = 1) { getPrioritySupplier.getGuild(name) }
            }
        }

        @Nested
        inner class IsOwner {

        }

        @Nested
        inner class IsMember {

        }

        @Nested
        inner class HasInvitation {

        }

        @Nested
        inner class AddMember {

        }

        @Nested
        inner class AddInvitation {

        }

        @Nested
        inner class RemoveMember {

        }

        @Nested
        inner class RemoveInvitation {

        }

        @Nested
        inner class GetMembers {

        }

        @Nested
        inner class GetInvitations {

        }

    }
}