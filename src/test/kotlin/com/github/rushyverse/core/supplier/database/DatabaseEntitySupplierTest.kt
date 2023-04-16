package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.data.*
import com.github.rushyverse.core.utils.getRandomString
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import io.mockk.slot
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Nested
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.time.Instant
import java.util.*
import kotlin.random.Random
import kotlin.test.*

class DatabaseEntitySupplierTest {

    private lateinit var friendDatabaseService: FriendDatabaseService
    private lateinit var guildDatabaseService: IGuildDatabaseService
    private lateinit var databaseEntitySupplier: DatabaseEntitySupplier

    @BeforeTest
    fun onBefore() {
        friendDatabaseService = mockk()
        guildDatabaseService = mockk()
        val configuration = DatabaseSupplierConfiguration(
            mockk<IFriendCacheService>() to friendDatabaseService,
            mockk<IGuildCacheService>() to guildDatabaseService,
        )
        databaseEntitySupplier = DatabaseEntitySupplier(configuration)
    }

    @Nested
    inner class FriendTest {

        @Nested
        inner class AddFriend {

            @Test
            fun `should add in supplier`() = runTest {
                val slotUuid1 = slot<UUID>()
                val slotUuid2 = slot<UUID>()

                coEvery { friendDatabaseService.addFriend(capture(slotUuid1), capture(slotUuid2)) } returns true

                val uuid1 = UUID.randomUUID()
                val uuid2 = UUID.randomUUID()
                assertTrue { databaseEntitySupplier.addFriend(uuid1, uuid2) }
                coVerify(exactly = 1) { friendDatabaseService.addFriend(any(), any()) }

                assertEquals(uuid1, slotUuid1.captured)
                assertEquals(uuid2, slotUuid2.captured)
            }

            @Test
            fun `should return false when supplier returns false`() = runTest {
                coEvery { friendDatabaseService.addFriend(any(), any()) } returns false
                assertFalse { databaseEntitySupplier.addFriend(mockk(), mockk()) }
                coVerify(exactly = 1) { friendDatabaseService.addFriend(any(), any()) }
            }

            @Test
            fun `should return true when supplier returns false`() = runTest {
                coEvery { friendDatabaseService.addFriend(any(), any()) } returns true
                assertTrue { databaseEntitySupplier.addFriend(mockk(), mockk()) }
                coVerify(exactly = 1) { friendDatabaseService.addFriend(any(), any()) }
            }
        }

        @Nested
        inner class AddPendingFriend {

            @Test
            fun `should add in supplier`() = runTest {
                val slotUuid1 = slot<UUID>()
                val slotUuid2 = slot<UUID>()

                coEvery { friendDatabaseService.addPendingFriend(capture(slotUuid1), capture(slotUuid2)) } returns true

                val uuid1 = UUID.randomUUID()
                val uuid2 = UUID.randomUUID()
                assertTrue { databaseEntitySupplier.addPendingFriend(uuid1, uuid2) }
                coVerify(exactly = 1) { friendDatabaseService.addPendingFriend(any(), any()) }

                assertEquals(uuid1, slotUuid1.captured)
                assertEquals(uuid2, slotUuid2.captured)
            }

            @Test
            fun `should return false when supplier returns false`() = runTest {
                coEvery { friendDatabaseService.addPendingFriend(any(), any()) } returns false
                assertFalse { databaseEntitySupplier.addPendingFriend(mockk(), mockk()) }
                coVerify(exactly = 1) { friendDatabaseService.addPendingFriend(any(), any()) }
            }

            @Test
            fun `should return true when supplier returns false`() = runTest {
                coEvery { friendDatabaseService.addPendingFriend(any(), any()) } returns true
                assertTrue { databaseEntitySupplier.addPendingFriend(mockk(), mockk()) }
                coVerify(exactly = 1) { friendDatabaseService.addPendingFriend(any(), any()) }
            }
        }

        @Nested
        inner class RemoveFriend {

            @Test
            fun `should remove friend in supplier`() = runTest {
                val slotUuid1 = slot<UUID>()
                val slotUuid2 = slot<UUID>()

                coEvery { friendDatabaseService.removeFriend(capture(slotUuid1), capture(slotUuid2)) } returns true

                val uuid1 = UUID.randomUUID()
                val uuid2 = UUID.randomUUID()
                assertTrue { databaseEntitySupplier.removeFriend(uuid1, uuid2) }
                coVerify(exactly = 1) { friendDatabaseService.removeFriend(any(), any()) }

                assertEquals(uuid1, slotUuid1.captured)
                assertEquals(uuid2, slotUuid2.captured)
            }

            @Test
            fun `should return false when supplier returns false`() = runTest {
                coEvery { friendDatabaseService.removeFriend(any(), any()) } returns false
                assertFalse { databaseEntitySupplier.removeFriend(mockk(), mockk()) }
                coVerify(exactly = 1) { friendDatabaseService.removeFriend(any(), any()) }
            }

            @Test
            fun `should return true when supplier returns false`() = runTest {
                coEvery { friendDatabaseService.removeFriend(any(), any()) } returns true
                assertTrue { databaseEntitySupplier.removeFriend(mockk(), mockk()) }
                coVerify(exactly = 1) { friendDatabaseService.removeFriend(any(), any()) }
            }
        }

        @Nested
        inner class RemovePendingFriend {

            @Test
            fun `should remove in supplier`() = runTest {
                val slotUuid1 = slot<UUID>()
                val slotUuid2 = slot<UUID>()

                coEvery { friendDatabaseService.removePendingFriend(capture(slotUuid1), capture(slotUuid2)) } returns true

                val uuid1 = UUID.randomUUID()
                val uuid2 = UUID.randomUUID()
                assertTrue { databaseEntitySupplier.removePendingFriend(uuid1, uuid2) }
                coVerify(exactly = 1) { friendDatabaseService.removePendingFriend(any(), any()) }

                assertEquals(uuid1, slotUuid1.captured)
                assertEquals(uuid2, slotUuid2.captured)
            }

            @Test
            fun `should return false when supplier returns false`() = runTest {
                coEvery { friendDatabaseService.removePendingFriend(any(), any()) } returns false
                assertFalse { databaseEntitySupplier.removePendingFriend(mockk(), mockk()) }
                coVerify(exactly = 1) { friendDatabaseService.removePendingFriend(any(), any()) }
            }

            @Test
            fun `should return true when supplier returns false`() = runTest {
                coEvery { friendDatabaseService.removePendingFriend(any(), any()) } returns true
                assertTrue { databaseEntitySupplier.removePendingFriend(mockk(), mockk()) }
                coVerify(exactly = 1) { friendDatabaseService.removePendingFriend(any(), any()) }
            }
        }

        @Nested
        inner class GetFriends {

            @Test
            fun `should get friends in supplier`() = runTest {
                val slotUuid = slot<UUID>()

                coEvery { friendDatabaseService.getFriends(capture(slotUuid)) } returns emptyFlow()

                val uuid1 = UUID.randomUUID()
                assertEquals(emptyList(), databaseEntitySupplier.getFriends(uuid1).toList())
                coVerify(exactly = 1) { friendDatabaseService.getFriends(any()) }

                assertEquals(uuid1, slotUuid.captured)
            }

            @Test
            fun `should return empty collection when supplier returns empty collection`() = runTest {
                coEvery { friendDatabaseService.getFriends(any()) } returns emptyFlow()
                assertEquals(emptyList(), databaseEntitySupplier.getFriends(mockk()).toList())
                coVerify(exactly = 1) { friendDatabaseService.getFriends(any()) }
            }

            @Test
            fun `should return not empty collection when supplier returns not empty collection`() = runTest {
                val expected = List(5) { UUID.randomUUID() }
                coEvery { friendDatabaseService.getFriends(any()) } returns expected.asFlow()
                assertEquals(expected, databaseEntitySupplier.getFriends(mockk()).toList())
                coVerify(exactly = 1) { friendDatabaseService.getFriends(any()) }
            }

        }

        @Nested
        inner class GetPendingFriends {

            @Test
            fun `should get in supplier`() = runTest {
                val slotUuid = slot<UUID>()

                coEvery { friendDatabaseService.getPendingFriends(capture(slotUuid)) } returns emptyFlow()

                val uuid1 = UUID.randomUUID()
                assertEquals(emptyList(), databaseEntitySupplier.getPendingFriends(uuid1).toList())
                coVerify(exactly = 1) { friendDatabaseService.getPendingFriends(any()) }

                assertEquals(uuid1, slotUuid.captured)
            }

            @Test
            fun `should return empty collection when supplier returns empty collection`() = runTest {
                coEvery { friendDatabaseService.getPendingFriends(any()) } returns emptyFlow()
                assertEquals(emptyList(), databaseEntitySupplier.getPendingFriends(mockk()).toList())
                coVerify(exactly = 1) { friendDatabaseService.getPendingFriends(any()) }
            }

            @Test
            fun `should return not empty collection when supplier returns not empty collection`() = runTest {
                val expected = List(5) { UUID.randomUUID() }
                coEvery { friendDatabaseService.getPendingFriends(any()) } returns expected.asFlow()
                assertEquals(expected, databaseEntitySupplier.getPendingFriends(mockk()).toList())
                coVerify(exactly = 1) { friendDatabaseService.getPendingFriends(any()) }
            }

        }

        @Nested
        inner class IsFriend {

            @Test
            fun `should is in supplier`() = runTest {
                val slotUuid1 = slot<UUID>()
                val slotUuid2 = slot<UUID>()

                coEvery { friendDatabaseService.isFriend(capture(slotUuid1), capture(slotUuid2)) } returns true

                val uuid1 = UUID.randomUUID()
                val uuid2 = UUID.randomUUID()
                assertTrue { databaseEntitySupplier.isFriend(uuid1, uuid2) }
                coVerify(exactly = 1) { friendDatabaseService.isFriend(any(), any()) }

                assertEquals(uuid1, slotUuid1.captured)
                assertEquals(uuid2, slotUuid2.captured)
            }

            @Test
            fun `should return false when supplier returns false`() = runTest {
                coEvery { friendDatabaseService.isFriend(any(), any()) } returns false
                assertFalse { databaseEntitySupplier.isFriend(mockk(), mockk()) }
                coVerify(exactly = 1) { friendDatabaseService.isFriend(any(), any()) }
            }

            @Test
            fun `should return true when supplier returns false`() = runTest {
                coEvery { friendDatabaseService.isFriend(any(), any()) } returns true
                assertTrue { databaseEntitySupplier.isFriend(mockk(), mockk()) }
                coVerify(exactly = 1) { friendDatabaseService.isFriend(any(), any()) }
            }
        }

        @Nested
        inner class IsPendingFriend {

            @Test
            fun `should is in supplier`() = runTest {
                val slotUuid1 = slot<UUID>()
                val slotUuid2 = slot<UUID>()

                coEvery { friendDatabaseService.isPendingFriend(capture(slotUuid1), capture(slotUuid2)) } returns true

                val uuid1 = UUID.randomUUID()
                val uuid2 = UUID.randomUUID()
                assertTrue { databaseEntitySupplier.isPendingFriend(uuid1, uuid2) }
                coVerify(exactly = 1) { friendDatabaseService.isPendingFriend(any(), any()) }

                assertEquals(uuid1, slotUuid1.captured)
                assertEquals(uuid2, slotUuid2.captured)
            }

            @Test
            fun `should return false when supplier returns false`() = runTest {
                coEvery { friendDatabaseService.isPendingFriend(any(), any()) } returns false
                assertFalse { databaseEntitySupplier.isPendingFriend(mockk(), mockk()) }
                coVerify(exactly = 1) { friendDatabaseService.isPendingFriend(any(), any()) }
            }

            @Test
            fun `should return true when supplier returns false`() = runTest {
                coEvery { friendDatabaseService.isPendingFriend(any(), any()) } returns true
                assertTrue { databaseEntitySupplier.isPendingFriend(mockk(), mockk()) }
                coVerify(exactly = 1) { friendDatabaseService.isPendingFriend(any(), any()) }
            }
        }

    }

    @Nested
    inner class GuildTest {

        @Nested
        inner class CreateGuild {

            @Test
            fun `should create in supplier`() = runTest {
                val slot1 = slot<String>()
                val slot2 = slot<String>()

                val guild = mockk<Guild>()
                coEvery { guildDatabaseService.createGuild(capture(slot1), capture(slot2)) } returns guild

                val value1 = getRandomString()
                val value2 = getRandomString()
                databaseEntitySupplier.createGuild(value1, value2)
                coVerify(exactly = 1) { guildDatabaseService.createGuild(any(), any()) }

                assertEquals(value1, slot1.captured)
                assertEquals(value2, slot2.captured)
            }

            @Test
            fun `should return the supplier result`() = runTest {
                val guild = mockk<Guild>()
                coEvery { guildDatabaseService.createGuild(any(), any()) } returns guild
                val supplierGuild = databaseEntitySupplier.createGuild(getRandomString(), getRandomString())
                assertEquals(guild, supplierGuild)
                coVerify(exactly = 1) { guildDatabaseService.createGuild(any(), any()) }
            }
        }

        @Nested
        inner class DeleteGuild {

            @Test
            fun `should delete in supplier`() = runTest {
                val slot = slot<Int>()

                coEvery { guildDatabaseService.deleteGuild(capture(slot)) } returns true

                val id = Random.nextInt()
                assertTrue { databaseEntitySupplier.deleteGuild(id) }
                coVerify(exactly = 1) { guildDatabaseService.deleteGuild(any()) }

                assertEquals(id, slot.captured)
            }

            @ParameterizedTest
            @ValueSource(booleans = [true, false])
            fun `should return supplier result`(result: Boolean) = runTest {
                coEvery { guildDatabaseService.deleteGuild(any()) } returns result
                assertEquals(result, databaseEntitySupplier.deleteGuild(Random.nextInt()))
                coVerify(exactly = 1) { guildDatabaseService.deleteGuild(any()) }
            }

        }

        @Nested
        inner class GetGuildById {

            @Test
            fun `should get in supplier`() = runTest {
                val slot = slot<Int>()

                val guild = mockk<Guild>()
                coEvery { guildDatabaseService.getGuild(capture(slot)) } returns guild

                val id = Random.nextInt()
                val supplierGuild = databaseEntitySupplier.getGuild(id)
                assertEquals(guild, supplierGuild)
                coVerify(exactly = 1) { guildDatabaseService.getGuild(any<Int>()) }

                assertEquals(id, slot.captured)
            }

            @Test
            fun `should return null when supplier returns null`() = runTest {
                coEvery { guildDatabaseService.getGuild(any<Int>()) } returns null
                assertNull(databaseEntitySupplier.getGuild(Random.nextInt()))
                coVerify(exactly = 1) { guildDatabaseService.getGuild(any<Int>()) }
            }
        }

        @Nested
        inner class GetGuildByName {

            @Test
            fun `should get in supplier`() = runTest {
                val slot = slot<String>()

                val guilds = flowOf(mockk<Guild>(), mockk<Guild>())
                coEvery { guildDatabaseService.getGuild(capture(slot)) } returns guilds

                val id = getRandomString()
                val supplierGuild = databaseEntitySupplier.getGuild(id)
                assertEquals(guilds, supplierGuild)
                coVerify(exactly = 1) { guildDatabaseService.getGuild(any<String>()) }

                assertEquals(id, slot.captured)
            }
        }

        @Nested
        inner class IsOwner {

            @Test
            fun `should get in supplier`() = runTest {
                val slot1 = slot<Int>()
                val slot2 = slot<String>()

                coEvery { guildDatabaseService.isOwner(capture(slot1), capture(slot2)) } returns true

                val guildId = Random.nextInt()
                val entityId = getRandomString()
                assertTrue { databaseEntitySupplier.isOwner(guildId, entityId) }
                coVerify(exactly = 1) { guildDatabaseService.isOwner(any(), any()) }

                assertEquals(guildId, slot1.captured)
                assertEquals(entityId, slot2.captured)
            }

            @ParameterizedTest
            @ValueSource(booleans = [true, false])
            fun `should return supplier result`(result: Boolean) = runTest {
                coEvery { guildDatabaseService.isOwner(any(), any()) } returns result
                assertEquals(result, databaseEntitySupplier.isOwner(Random.nextInt(), getRandomString()))
                coVerify(exactly = 1) { guildDatabaseService.isOwner(any(), any()) }
            }

        }

        @Nested
        inner class IsMember {

            @Test
            fun `should get in supplier`() = runTest {
                val slot1 = slot<Int>()
                val slot2 = slot<String>()

                coEvery { guildDatabaseService.isMember(capture(slot1), capture(slot2)) } returns true

                val guildId = Random.nextInt()
                val entityId = getRandomString()
                assertTrue { databaseEntitySupplier.isMember(guildId, entityId) }
                coVerify(exactly = 1) { guildDatabaseService.isMember(any(), any()) }

                assertEquals(guildId, slot1.captured)
                assertEquals(entityId, slot2.captured)
            }

            @ParameterizedTest
            @ValueSource(booleans = [true, false])
            fun `should return supplier result`(result: Boolean) = runTest {
                coEvery { guildDatabaseService.isMember(any(), any()) } returns result
                assertEquals(result, databaseEntitySupplier.isMember(Random.nextInt(), getRandomString()))
                coVerify(exactly = 1) { guildDatabaseService.isMember(any(), any()) }
            }
        }

        @Nested
        inner class HasInvitation {

            @Test
            fun `should get in supplier`() = runTest {
                val slot1 = slot<Int>()
                val slot2 = slot<String>()

                coEvery { guildDatabaseService.hasInvitation(capture(slot1), capture(slot2)) } returns true

                val guildId = Random.nextInt()
                val entityId = getRandomString()
                assertTrue { databaseEntitySupplier.hasInvitation(guildId, entityId) }
                coVerify(exactly = 1) { guildDatabaseService.hasInvitation(any(), any()) }

                assertEquals(guildId, slot1.captured)
                assertEquals(entityId, slot2.captured)
            }

            @ParameterizedTest
            @ValueSource(booleans = [true, false])
            fun `should return supplier result`(result: Boolean) = runTest {
                coEvery { guildDatabaseService.hasInvitation(any(), any()) } returns result
                assertEquals(result, databaseEntitySupplier.hasInvitation(Random.nextInt(), getRandomString()))
                coVerify(exactly = 1) { guildDatabaseService.hasInvitation(any(), any()) }
            }
        }

        @Nested
        inner class AddMember {

            @Test
            fun `should add in supplier`() = runTest {
                val slot1 = slot<Int>()
                val slot2 = slot<String>()

                coEvery { guildDatabaseService.addMember(capture(slot1), capture(slot2)) } returns true

                val guildId = Random.nextInt()
                val entityId = getRandomString()
                assertTrue { databaseEntitySupplier.addMember(guildId, entityId) }
                coVerify(exactly = 1) { guildDatabaseService.addMember(any(), any()) }

                assertEquals(guildId, slot1.captured)
                assertEquals(entityId, slot2.captured)
            }

            @ParameterizedTest
            @ValueSource(booleans = [true, false])
            fun `should return supplier result`(result: Boolean) = runTest {
                coEvery { guildDatabaseService.addMember(any(), any()) } returns result
                assertEquals(result, databaseEntitySupplier.addMember(Random.nextInt(), getRandomString()))
                coVerify(exactly = 1) { guildDatabaseService.addMember(any(), any()) }
            }

        }

        @Nested
        inner class AddInvitation {

            @Test
            fun `should add in supplier`() = runTest {
                val slot1 = slot<Int>()
                val slot2 = slot<String>()
                val slot3 = slot<Instant>()

                coEvery { guildDatabaseService.addInvitation(capture(slot1), capture(slot2), capture(slot3)) } returns true

                val guildId = Random.nextInt()
                val entityId = getRandomString()
                val instant = Instant.now()
                assertTrue { databaseEntitySupplier.addInvitation(guildId, entityId, instant) }
                coVerify(exactly = 1) { guildDatabaseService.addInvitation(any(), any(), any()) }

                assertEquals(guildId, slot1.captured)
                assertEquals(entityId, slot2.captured)
                assertEquals(instant, slot3.captured)
            }

            @ParameterizedTest
            @ValueSource(booleans = [true, false])
            fun `should return supplier result`(result: Boolean) = runTest {
                coEvery { guildDatabaseService.addInvitation(any(), any(), any()) } returns result
                assertEquals(result, databaseEntitySupplier.addInvitation(Random.nextInt(), getRandomString(), mockk()))
                coVerify(exactly = 1) { guildDatabaseService.addInvitation(any(), any(), any()) }
            }

        }

        @Nested
        inner class RemoveMember {

            @Test
            fun `should remove in supplier`() = runTest {
                val slot1 = slot<Int>()
                val slot2 = slot<String>()

                coEvery { guildDatabaseService.removeMember(capture(slot1), capture(slot2)) } returns true

                val guildId = Random.nextInt()
                val entityId = getRandomString()
                assertTrue { databaseEntitySupplier.removeMember(guildId, entityId) }
                coVerify(exactly = 1) { guildDatabaseService.removeMember(any(), any()) }

                assertEquals(guildId, slot1.captured)
                assertEquals(entityId, slot2.captured)
            }

            @ParameterizedTest
            @ValueSource(booleans = [true, false])
            fun `should return supplier result`(result: Boolean) = runTest {
                coEvery { guildDatabaseService.removeMember(any(), any()) } returns result
                assertEquals(result, databaseEntitySupplier.removeMember(Random.nextInt(), getRandomString()))
                coVerify(exactly = 1) { guildDatabaseService.removeMember(any(), any()) }
            }

        }

        @Nested
        inner class RemoveInvitation {

            @Test
            fun `should remove in supplier`() = runTest {
                val slot1 = slot<Int>()
                val slot2 = slot<String>()

                coEvery { guildDatabaseService.removeInvitation(capture(slot1), capture(slot2)) } returns true

                val guildId = Random.nextInt()
                val entityId = getRandomString()
                assertTrue { databaseEntitySupplier.removeInvitation(guildId, entityId) }
                coVerify(exactly = 1) { guildDatabaseService.removeInvitation(any(), any()) }

                assertEquals(guildId, slot1.captured)
                assertEquals(entityId, slot2.captured)
            }

            @ParameterizedTest
            @ValueSource(booleans = [true, false])
            fun `should return supplier result`(result: Boolean) = runTest {
                coEvery { guildDatabaseService.removeInvitation(any(), any()) } returns result
                assertEquals(result, databaseEntitySupplier.removeInvitation(Random.nextInt(), getRandomString()))
                coVerify(exactly = 1) { guildDatabaseService.removeInvitation(any(), any()) }
            }

        }

        @Nested
        inner class GetMembers {

            @Test
            fun `should get in supplier`() = runTest {
                val slot = slot<Int>()

                coEvery { guildDatabaseService.getMembers(capture(slot)) } returns emptyFlow()

                val id = Random.nextInt()
                assertEquals(emptyList(), databaseEntitySupplier.getMembers(id).toList())
                coVerify(exactly = 1) { guildDatabaseService.getMembers(any()) }

                assertEquals(id, slot.captured)
            }

            @Test
            fun `should return empty collection when supplier returns empty collection`() = runTest {
                coEvery { guildDatabaseService.getMembers(any()) } returns emptyFlow()
                assertEquals(emptyList(), databaseEntitySupplier.getMembers(Random.nextInt()).toList())
                coVerify(exactly = 1) { guildDatabaseService.getMembers(any()) }
            }

            @Test
            fun `should return not empty collection when supplier returns not empty collection`() = runTest {
                val expected = List(5) { GuildMember(Random.nextInt(), getRandomString(), mockk()) }
                coEvery { guildDatabaseService.getMembers(any()) } returns expected.asFlow()
                assertEquals(expected, databaseEntitySupplier.getMembers(Random.nextInt()).toList())
                coVerify(exactly = 1) { guildDatabaseService.getMembers(any()) }
            }

        }

        @Nested
        inner class GetInvitations {

            @Test
            fun `should get in supplier`() = runTest {
                val slot = slot<Int>()

                coEvery { guildDatabaseService.getInvitations(capture(slot)) } returns emptyFlow()

                val id = Random.nextInt()
                assertEquals(emptyList(), databaseEntitySupplier.getInvitations(id).toList())
                coVerify(exactly = 1) { guildDatabaseService.getInvitations(any()) }

                assertEquals(id, slot.captured)
            }

            @Test
            fun `should return empty collection when supplier returns empty collection`() = runTest {
                coEvery { guildDatabaseService.getInvitations(any()) } returns emptyFlow()
                assertEquals(emptyList(), databaseEntitySupplier.getInvitations(Random.nextInt()).toList())
                coVerify(exactly = 1) { guildDatabaseService.getInvitations(any()) }
            }

            @Test
            fun `should return not empty collection when supplier returns not empty collection`() = runTest {
                val expected = List(5) { GuildInvite(Random.nextInt(), getRandomString(), mockk(), mockk()) }
                coEvery { guildDatabaseService.getInvitations(any()) } returns expected.asFlow()
                assertEquals(expected, databaseEntitySupplier.getInvitations(Random.nextInt()).toList())
                coVerify(exactly = 1) { guildDatabaseService.getInvitations(any()) }
            }

        }

    }
}