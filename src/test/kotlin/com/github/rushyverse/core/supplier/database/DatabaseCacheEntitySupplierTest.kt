package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.data.*
import com.github.rushyverse.core.utils.randomEntityId
import com.github.rushyverse.core.utils.randomString
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import io.mockk.slot
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Nested
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.time.Instant
import java.util.*
import kotlin.random.Random
import kotlin.test.*

class DatabaseCacheEntitySupplierTest {

    private lateinit var cacheEntitySupplier: DatabaseCacheEntitySupplier

    private lateinit var friendCacheService: IFriendCacheService
    private lateinit var guildCacheService: IGuildCacheService
    private lateinit var playerCacheService: IPlayerCacheService

    @BeforeTest
    fun onBefore() = runBlocking {
        friendCacheService = mockk()
        guildCacheService = mockk()
        playerCacheService = mockk()
        val configuration = DatabaseSupplierConfiguration(
            friendCacheService to mockk(),
            guildCacheService to mockk(),
            playerCacheService to mockk()
        )
        cacheEntitySupplier = DatabaseCacheEntitySupplier(configuration)
    }

    @Nested
    inner class FriendTest {

        @Nested
        inner class AddFriend {

            @Test
            fun `should add in supplier`() = runTest {
                val slotUuid1 = slot<UUID>()
                val slotUuid2 = slot<UUID>()

                coEvery { friendCacheService.addFriend(capture(slotUuid1), capture(slotUuid2)) } returns true

                val uuid1 = UUID.randomUUID()
                val uuid2 = UUID.randomUUID()
                assertTrue { cacheEntitySupplier.addFriend(uuid1, uuid2) }
                coVerify(exactly = 1) { friendCacheService.addFriend(any(), any()) }

                assertEquals(uuid1, slotUuid1.captured)
                assertEquals(uuid2, slotUuid2.captured)
            }

            @Test
            fun `should return false when supplier returns false`() = runTest {
                coEvery { friendCacheService.addFriend(any(), any()) } returns false
                assertFalse { cacheEntitySupplier.addFriend(mockk(), mockk()) }
                coVerify(exactly = 1) { friendCacheService.addFriend(any(), any()) }
            }

            @Test
            fun `should return true when supplier returns true`() = runTest {
                coEvery { friendCacheService.addFriend(any(), any()) } returns true
                assertTrue { cacheEntitySupplier.addFriend(mockk(), mockk()) }
                coVerify(exactly = 1) { friendCacheService.addFriend(any(), any()) }
            }
        }

        @Nested
        inner class AddPendingFriend {

            @Test
            fun `should add in supplier`() = runTest {
                val slotUuid1 = slot<UUID>()
                val slotUuid2 = slot<UUID>()

                coEvery { friendCacheService.addPendingFriend(capture(slotUuid1), capture(slotUuid2)) } returns true

                val uuid1 = UUID.randomUUID()
                val uuid2 = UUID.randomUUID()
                assertTrue { cacheEntitySupplier.addPendingFriend(uuid1, uuid2) }
                coVerify(exactly = 1) { friendCacheService.addPendingFriend(any(), any()) }

                assertEquals(uuid1, slotUuid1.captured)
                assertEquals(uuid2, slotUuid2.captured)
            }

            @Test
            fun `should return false when supplier returns false`() = runTest {
                coEvery { friendCacheService.addPendingFriend(any(), any()) } returns false
                assertFalse { cacheEntitySupplier.addPendingFriend(mockk(), mockk()) }
                coVerify(exactly = 1) { friendCacheService.addPendingFriend(any(), any()) }
            }

            @Test
            fun `should return true when supplier returns true`() = runTest {
                coEvery { friendCacheService.addPendingFriend(any(), any()) } returns true
                assertTrue { cacheEntitySupplier.addPendingFriend(mockk(), mockk()) }
                coVerify(exactly = 1) { friendCacheService.addPendingFriend(any(), any()) }
            }
        }

        @Nested
        inner class RemoveFriend {

            @Test
            fun `should remove in supplier`() = runTest {
                val slotUuid1 = slot<UUID>()
                val slotUuid2 = slot<UUID>()

                coEvery { friendCacheService.removeFriend(capture(slotUuid1), capture(slotUuid2)) } returns true

                val uuid1 = UUID.randomUUID()
                val uuid2 = UUID.randomUUID()
                assertTrue { cacheEntitySupplier.removeFriend(uuid1, uuid2) }
                coVerify(exactly = 1) { friendCacheService.removeFriend(any(), any()) }

                assertEquals(uuid1, slotUuid1.captured)
                assertEquals(uuid2, slotUuid2.captured)
            }

            @Test
            fun `should return false when supplier returns false`() = runTest {
                coEvery { friendCacheService.removeFriend(any(), any()) } returns false
                assertFalse { cacheEntitySupplier.removeFriend(mockk(), mockk()) }
                coVerify(exactly = 1) { friendCacheService.removeFriend(any(), any()) }
            }

            @Test
            fun `should return true when supplier returns true`() = runTest {
                coEvery { friendCacheService.removeFriend(any(), any()) } returns true
                assertTrue { cacheEntitySupplier.removeFriend(mockk(), mockk()) }
                coVerify(exactly = 1) { friendCacheService.removeFriend(any(), any()) }
            }
        }

        @Nested
        inner class RemovePendingFriend {

            @Test
            fun `should remove in supplier`() = runTest {
                val slotUuid1 = slot<UUID>()
                val slotUuid2 = slot<UUID>()

                coEvery { friendCacheService.removePendingFriend(capture(slotUuid1), capture(slotUuid2)) } returns true

                val uuid1 = UUID.randomUUID()
                val uuid2 = UUID.randomUUID()
                assertTrue { cacheEntitySupplier.removePendingFriend(uuid1, uuid2) }
                coVerify(exactly = 1) { friendCacheService.removePendingFriend(any(), any()) }

                assertEquals(uuid1, slotUuid1.captured)
                assertEquals(uuid2, slotUuid2.captured)
            }

            @Test
            fun `should return false when supplier returns false`() = runTest {
                coEvery { friendCacheService.removePendingFriend(any(), any()) } returns false
                assertFalse { cacheEntitySupplier.removePendingFriend(mockk(), mockk()) }
                coVerify(exactly = 1) { friendCacheService.removePendingFriend(any(), any()) }
            }

            @Test
            fun `should return true when supplier returns true`() = runTest {
                coEvery { friendCacheService.removePendingFriend(any(), any()) } returns true
                assertTrue { cacheEntitySupplier.removePendingFriend(mockk(), mockk()) }
                coVerify(exactly = 1) { friendCacheService.removePendingFriend(any(), any()) }
            }
        }

        @Nested
        inner class GetFriends {

            @Test
            fun `should get in supplier`() = runTest {
                val slotUuid = slot<UUID>()

                coEvery { friendCacheService.getFriends(capture(slotUuid)) } returns emptyFlow()

                val uuid1 = UUID.randomUUID()
                assertEquals(emptyList(), cacheEntitySupplier.getFriends(uuid1).toList())
                coVerify(exactly = 1) { friendCacheService.getFriends(any()) }

                assertEquals(uuid1, slotUuid.captured)
            }

            @Test
            fun `should return empty collection when supplier returns empty collection`() = runTest {
                coEvery { friendCacheService.getFriends(any()) } returns emptyFlow()
                assertEquals(emptyList(), cacheEntitySupplier.getFriends(mockk()).toList())
                coVerify(exactly = 1) { friendCacheService.getFriends(any()) }
            }

            @Test
            fun `should return not empty collection when supplier returns not empty collection`() = runTest {
                val expected = List(5) { UUID.randomUUID() }
                coEvery { friendCacheService.getFriends(any()) } returns expected.asFlow()
                assertEquals(expected, cacheEntitySupplier.getFriends(mockk()).toList())
                coVerify(exactly = 1) { friendCacheService.getFriends(any()) }
            }

        }

        @Nested
        inner class GetPendingFriends {

            @Test
            fun `should get in supplier`() = runTest {
                val slotUuid = slot<UUID>()

                coEvery { friendCacheService.getPendingFriends(capture(slotUuid)) } returns emptyFlow()

                val uuid1 = UUID.randomUUID()
                assertEquals(emptyList(), cacheEntitySupplier.getPendingFriends(uuid1).toList())
                coVerify(exactly = 1) { friendCacheService.getPendingFriends(any()) }

                assertEquals(uuid1, slotUuid.captured)
            }

            @Test
            fun `should return empty collection when supplier returns empty collection`() = runTest {
                coEvery { friendCacheService.getPendingFriends(any()) } returns emptyFlow()
                assertEquals(emptyList(), cacheEntitySupplier.getPendingFriends(mockk()).toList())
                coVerify(exactly = 1) { friendCacheService.getPendingFriends(any()) }
            }

            @Test
            fun `should return not empty collection when supplier returns not empty collection`() = runTest {
                val expected = List(5) { UUID.randomUUID() }
                coEvery { friendCacheService.getPendingFriends(any()) } returns expected.asFlow()
                assertEquals(expected, cacheEntitySupplier.getPendingFriends(mockk()).toList())
                coVerify(exactly = 1) { friendCacheService.getPendingFriends(any()) }
            }

        }

        @Nested
        inner class SetFriends {

            @Test
            fun `should set in supplier`() = runTest {
                val slotUuid1 = slot<UUID>()
                val slotFriends = slot<Set<UUID>>()

                coEvery { friendCacheService.setFriends(capture(slotUuid1), capture(slotFriends)) } returns true

                val uuid1 = UUID.randomUUID()
                val friends = List(5) { UUID.randomUUID() }.toSet()
                assertTrue { cacheEntitySupplier.setFriends(uuid1, friends) }
                coVerify(exactly = 1) { friendCacheService.setFriends(any(), any()) }

                assertEquals(uuid1, slotUuid1.captured)
                assertEquals(friends, slotFriends.captured)
            }

            @Test
            fun `should return false when supplier returns false`() = runTest {
                coEvery { friendCacheService.setFriends(any(), any()) } returns false
                assertFalse { cacheEntitySupplier.setFriends(mockk(), mockk()) }
                coVerify(exactly = 1) { friendCacheService.setFriends(any(), any()) }
            }

            @Test
            fun `should return true when supplier returns true`() = runTest {
                coEvery { friendCacheService.setFriends(any(), any()) } returns true
                assertTrue { cacheEntitySupplier.setFriends(mockk(), mockk()) }
                coVerify(exactly = 1) { friendCacheService.setFriends(any(), any()) }
            }

        }

        @Nested
        inner class SetPendingFriends {

            @Test
            fun `should set in supplier`() = runTest {
                val slotUuid1 = slot<UUID>()
                val slotFriends = slot<Set<UUID>>()

                coEvery { friendCacheService.setPendingFriends(capture(slotUuid1), capture(slotFriends)) } returns true

                val uuid1 = UUID.randomUUID()
                val friends = List(5) { UUID.randomUUID() }.toSet()
                assertTrue { cacheEntitySupplier.setPendingFriends(uuid1, friends) }
                coVerify(exactly = 1) { friendCacheService.setPendingFriends(any(), any()) }

                assertEquals(uuid1, slotUuid1.captured)
                assertEquals(friends, slotFriends.captured)
            }

            @Test
            fun `should return false when supplier returns false`() = runTest {
                coEvery { friendCacheService.setPendingFriends(any(), any()) } returns false
                assertFalse { cacheEntitySupplier.setPendingFriends(mockk(), mockk()) }
                coVerify(exactly = 1) { friendCacheService.setPendingFriends(any(), any()) }
            }

            @Test
            fun `should return true when supplier returns true`() = runTest {
                coEvery { friendCacheService.setPendingFriends(any(), any()) } returns true
                assertTrue { cacheEntitySupplier.setPendingFriends(mockk(), mockk()) }
                coVerify(exactly = 1) { friendCacheService.setPendingFriends(any(), any()) }
            }

        }

        @Nested
        inner class IsFriend {

            @Test
            fun `should is in supplier`() = runTest {
                val slotUuid1 = slot<UUID>()
                val slotUuid2 = slot<UUID>()

                coEvery { friendCacheService.isFriend(capture(slotUuid1), capture(slotUuid2)) } returns true

                val uuid1 = UUID.randomUUID()
                val uuid2 = UUID.randomUUID()
                assertTrue { cacheEntitySupplier.isFriend(uuid1, uuid2) }
                coVerify(exactly = 1) { friendCacheService.isFriend(any(), any()) }

                assertEquals(uuid1, slotUuid1.captured)
                assertEquals(uuid2, slotUuid2.captured)
            }

            @Test
            fun `should return false when supplier returns false`() = runTest {
                coEvery { friendCacheService.isFriend(any(), any()) } returns false
                assertFalse { cacheEntitySupplier.isFriend(mockk(), mockk()) }
                coVerify(exactly = 1) { friendCacheService.isFriend(any(), any()) }
            }

            @Test
            fun `should return true when supplier returns true`() = runTest {
                coEvery { friendCacheService.isFriend(any(), any()) } returns true
                assertTrue { cacheEntitySupplier.isFriend(mockk(), mockk()) }
                coVerify(exactly = 1) { friendCacheService.isFriend(any(), any()) }
            }
        }

        @Nested
        inner class IsPendingFriend {

            @Test
            fun `should is in supplier`() = runTest {
                val slotUuid1 = slot<UUID>()
                val slotUuid2 = slot<UUID>()

                coEvery { friendCacheService.isPendingFriend(capture(slotUuid1), capture(slotUuid2)) } returns true

                val uuid1 = UUID.randomUUID()
                val uuid2 = UUID.randomUUID()
                assertTrue { cacheEntitySupplier.isPendingFriend(uuid1, uuid2) }
                coVerify(exactly = 1) { friendCacheService.isPendingFriend(any(), any()) }

                assertEquals(uuid1, slotUuid1.captured)
                assertEquals(uuid2, slotUuid2.captured)
            }

            @Test
            fun `should return false when supplier returns false`() = runTest {
                coEvery { friendCacheService.isPendingFriend(any(), any()) } returns false
                assertFalse { cacheEntitySupplier.isPendingFriend(mockk(), mockk()) }
                coVerify(exactly = 1) { friendCacheService.isPendingFriend(any(), any()) }
            }

            @Test
            fun `should return true when supplier returns true`() = runTest {
                coEvery { friendCacheService.isPendingFriend(any(), any()) } returns true
                assertTrue { cacheEntitySupplier.isPendingFriend(mockk(), mockk()) }
                coVerify(exactly = 1) { friendCacheService.isPendingFriend(any(), any()) }
            }
        }

    }

    @Nested
    inner class GuildTest {

        @Nested
        inner class DeleteExpiredInvitations {

            @Test
            fun `should delete in supplier`() = runTest {
                coEvery { guildCacheService.deleteExpiredInvitations() } returns 0
                cacheEntitySupplier.deleteExpiredInvitations()
                coVerify(exactly = 1) { guildCacheService.deleteExpiredInvitations() }
            }

            @ParameterizedTest
            @ValueSource(longs = [Long.MIN_VALUE, -10, 0, 1, 5, 42, Long.MAX_VALUE])
            fun `should return the supplier result`(result: Long) = runTest {
                coEvery { guildCacheService.deleteExpiredInvitations() } returns result
                assertEquals(result, cacheEntitySupplier.deleteExpiredInvitations())
            }

        }

        @Nested
        inner class CreateGuild {

            @Test
            fun `should create in supplier`() = runTest {
                val slot1 = slot<String>()
                val slot2 = slot<UUID>()

                val guild = mockk<Guild>()
                coEvery { guildCacheService.createGuild(capture(slot1), capture(slot2)) } returns guild

                val value1 = randomString()
                val value2 = randomEntityId()
                cacheEntitySupplier.createGuild(value1, value2)
                coVerify(exactly = 1) { guildCacheService.createGuild(any(), any()) }

                assertEquals(value1, slot1.captured)
                assertEquals(value2, slot2.captured)
            }

            @Test
            fun `should return the supplier result`() = runTest {
                val guild = mockk<Guild>()
                coEvery { guildCacheService.createGuild(any(), any()) } returns guild
                val supplierGuild = cacheEntitySupplier.createGuild(randomString(), randomEntityId())
                assertEquals(guild, supplierGuild)
                coVerify(exactly = 1) { guildCacheService.createGuild(any(), any()) }
            }
        }

        @Nested
        inner class DeleteGuild {

            @Test
            fun `should delete in supplier`() = runTest {
                val slot = slot<Int>()

                coEvery { guildCacheService.deleteGuild(capture(slot)) } returns true

                val id = Random.nextInt()
                assertTrue { cacheEntitySupplier.deleteGuild(id) }
                coVerify(exactly = 1) { guildCacheService.deleteGuild(any()) }

                assertEquals(id, slot.captured)
            }

            @ParameterizedTest
            @ValueSource(booleans = [true, false])
            fun `should return supplier result`(result: Boolean) = runTest {
                coEvery { guildCacheService.deleteGuild(any()) } returns result
                assertEquals(result, cacheEntitySupplier.deleteGuild(Random.nextInt()))
                coVerify(exactly = 1) { guildCacheService.deleteGuild(any()) }
            }

        }

        @Nested
        inner class GetGuildById {

            @Test
            fun `should get in supplier`() = runTest {
                val slot = slot<Int>()

                val guild = mockk<Guild>()
                coEvery { guildCacheService.getGuild(capture(slot)) } returns guild

                val id = Random.nextInt()
                val supplierGuild = cacheEntitySupplier.getGuild(id)
                assertEquals(guild, supplierGuild)
                coVerify(exactly = 1) { guildCacheService.getGuild(any<Int>()) }

                assertEquals(id, slot.captured)
            }

            @Test
            fun `should return null when supplier returns null`() = runTest {
                coEvery { guildCacheService.getGuild(any<Int>()) } returns null
                assertNull(cacheEntitySupplier.getGuild(Random.nextInt()))
                coVerify(exactly = 1) { guildCacheService.getGuild(any<Int>()) }
            }
        }

        @Nested
        inner class GetGuildByName {

            @Test
            fun `should get in supplier`() = runTest {
                val slot = slot<String>()

                val guilds = flowOf(mockk<Guild>(), mockk<Guild>())
                coEvery { guildCacheService.getGuild(capture(slot)) } returns guilds

                val id = randomString()
                val supplierGuild = cacheEntitySupplier.getGuild(id)
                assertEquals(guilds, supplierGuild)
                coVerify(exactly = 1) { guildCacheService.getGuild(any<String>()) }

                assertEquals(id, slot.captured)
            }
        }

        @Nested
        inner class IsOwner {

            @Test
            fun `should get in supplier`() = runTest {
                val slot1 = slot<Int>()
                val slot2 = slot<UUID>()

                coEvery { guildCacheService.isOwner(capture(slot1), capture(slot2)) } returns true

                val guildId = Random.nextInt()
                val entityId = randomEntityId()
                assertTrue { cacheEntitySupplier.isOwner(guildId, entityId) }
                coVerify(exactly = 1) { guildCacheService.isOwner(any(), any()) }

                assertEquals(guildId, slot1.captured)
                assertEquals(entityId, slot2.captured)
            }

            @ParameterizedTest
            @ValueSource(booleans = [true, false])
            fun `should return supplier result`(result: Boolean) = runTest {
                coEvery { guildCacheService.isOwner(any(), any()) } returns result
                assertEquals(result, cacheEntitySupplier.isOwner(Random.nextInt(), randomEntityId()))
                coVerify(exactly = 1) { guildCacheService.isOwner(any(), any()) }
            }

        }

        @Nested
        inner class IsMember {

            @Test
            fun `should get in supplier`() = runTest {
                val slot1 = slot<Int>()
                val slot2 = slot<UUID>()

                coEvery { guildCacheService.isMember(capture(slot1), capture(slot2)) } returns true

                val guildId = Random.nextInt()
                val entityId = randomEntityId()
                assertTrue { cacheEntitySupplier.isMember(guildId, entityId) }
                coVerify(exactly = 1) { guildCacheService.isMember(any(), any()) }

                assertEquals(guildId, slot1.captured)
                assertEquals(entityId, slot2.captured)
            }

            @ParameterizedTest
            @ValueSource(booleans = [true, false])
            fun `should return supplier result`(result: Boolean) = runTest {
                coEvery { guildCacheService.isMember(any(), any()) } returns result
                assertEquals(result, cacheEntitySupplier.isMember(Random.nextInt(), randomEntityId()))
                coVerify(exactly = 1) { guildCacheService.isMember(any(), any()) }
            }
        }

        @Nested
        inner class HasInvitation {

            @Test
            fun `should get in supplier`() = runTest {
                val slot1 = slot<Int>()
                val slot2 = slot<UUID>()

                coEvery { guildCacheService.hasInvitation(capture(slot1), capture(slot2)) } returns true

                val guildId = Random.nextInt()
                val entityId = randomEntityId()
                assertTrue { cacheEntitySupplier.hasInvitation(guildId, entityId) }
                coVerify(exactly = 1) { guildCacheService.hasInvitation(any(), any()) }

                assertEquals(guildId, slot1.captured)
                assertEquals(entityId, slot2.captured)
            }

            @ParameterizedTest
            @ValueSource(booleans = [true, false])
            fun `should return supplier result`(result: Boolean) = runTest {
                coEvery { guildCacheService.hasInvitation(any(), any()) } returns result
                assertEquals(result, cacheEntitySupplier.hasInvitation(Random.nextInt(), randomEntityId()))
                coVerify(exactly = 1) { guildCacheService.hasInvitation(any(), any()) }
            }
        }

        @Nested
        inner class AddMember {

            @Test
            fun `should add in supplier`() = runTest {
                val slot1 = slot<Int>()
                val slot2 = slot<UUID>()

                coEvery { guildCacheService.addMember(capture(slot1), capture(slot2)) } returns true

                val guildId = Random.nextInt()
                val entityId = randomEntityId()
                assertTrue { cacheEntitySupplier.addMember(guildId, entityId) }
                coVerify(exactly = 1) { guildCacheService.addMember(any(), any()) }

                assertEquals(guildId, slot1.captured)
                assertEquals(entityId, slot2.captured)
            }

            @ParameterizedTest
            @ValueSource(booleans = [true, false])
            fun `should return supplier result`(result: Boolean) = runTest {
                coEvery { guildCacheService.addMember(any(), any()) } returns result
                assertEquals(result, cacheEntitySupplier.addMember(Random.nextInt(), randomEntityId()))
                coVerify(exactly = 1) { guildCacheService.addMember(any(), any()) }
            }

        }

        @Nested
        inner class AddInvitation {

            @Test
            fun `should add in supplier`() = runTest {
                val slot1 = slot<Int>()
                val slot2 = slot<UUID>()
                val slot3 = slot<Instant>()

                coEvery { guildCacheService.addInvitation(capture(slot1), capture(slot2), capture(slot3)) } returns true

                val guildId = Random.nextInt()
                val entityId = randomEntityId()
                val instant = Instant.now()
                assertTrue { cacheEntitySupplier.addInvitation(guildId, entityId, instant) }
                coVerify(exactly = 1) { guildCacheService.addInvitation(any(), any(), any()) }

                assertEquals(guildId, slot1.captured)
                assertEquals(entityId, slot2.captured)
                assertEquals(instant, slot3.captured)
            }

            @ParameterizedTest
            @ValueSource(booleans = [true, false])
            fun `should return supplier result`(result: Boolean) = runTest {
                coEvery { guildCacheService.addInvitation(any(), any(), any()) } returns result
                assertEquals(result, cacheEntitySupplier.addInvitation(Random.nextInt(), randomEntityId(), mockk()))
                coVerify(exactly = 1) { guildCacheService.addInvitation(any(), any(), any()) }
            }

        }

        @Nested
        inner class RemoveMember {

            @Test
            fun `should remove in supplier`() = runTest {
                val slot1 = slot<Int>()
                val slot2 = slot<UUID>()

                coEvery { guildCacheService.removeMember(capture(slot1), capture(slot2)) } returns true

                val guildId = Random.nextInt()
                val entityId = randomEntityId()
                assertTrue { cacheEntitySupplier.removeMember(guildId, entityId) }
                coVerify(exactly = 1) { guildCacheService.removeMember(any(), any()) }

                assertEquals(guildId, slot1.captured)
                assertEquals(entityId, slot2.captured)
            }

            @ParameterizedTest
            @ValueSource(booleans = [true, false])
            fun `should return supplier result`(result: Boolean) = runTest {
                coEvery { guildCacheService.removeMember(any(), any()) } returns result
                assertEquals(result, cacheEntitySupplier.removeMember(Random.nextInt(), randomEntityId()))
                coVerify(exactly = 1) { guildCacheService.removeMember(any(), any()) }
            }

        }

        @Nested
        inner class RemoveInvitation {

            @Test
            fun `should remove in supplier`() = runTest {
                val slot1 = slot<Int>()
                val slot2 = slot<UUID>()

                coEvery { guildCacheService.removeInvitation(capture(slot1), capture(slot2)) } returns true

                val guildId = Random.nextInt()
                val entityId = randomEntityId()
                assertTrue { cacheEntitySupplier.removeInvitation(guildId, entityId) }
                coVerify(exactly = 1) { guildCacheService.removeInvitation(any(), any()) }

                assertEquals(guildId, slot1.captured)
                assertEquals(entityId, slot2.captured)
            }

            @ParameterizedTest
            @ValueSource(booleans = [true, false])
            fun `should return supplier result`(result: Boolean) = runTest {
                coEvery { guildCacheService.removeInvitation(any(), any()) } returns result
                assertEquals(result, cacheEntitySupplier.removeInvitation(Random.nextInt(), randomEntityId()))
                coVerify(exactly = 1) { guildCacheService.removeInvitation(any(), any()) }
            }

        }

        @Nested
        inner class GetMembers {

            @Test
            fun `should get in supplier`() = runTest {
                val slot = slot<Int>()

                coEvery { guildCacheService.getMembers(capture(slot)) } returns emptyFlow()

                val id = Random.nextInt()
                assertEquals(emptyList(), cacheEntitySupplier.getMembers(id).toList())
                coVerify(exactly = 1) { guildCacheService.getMembers(any()) }

                assertEquals(id, slot.captured)
            }

            @Test
            fun `should return empty collection when supplier returns empty collection`() = runTest {
                coEvery { guildCacheService.getMembers(any()) } returns emptyFlow()
                assertEquals(emptyList(), cacheEntitySupplier.getMembers(Random.nextInt()).toList())
                coVerify(exactly = 1) { guildCacheService.getMembers(any()) }
            }

            @Test
            fun `should return not empty collection when supplier returns not empty collection`() = runTest {
                val expected = List(5) { GuildMember(Random.nextInt(), randomEntityId(), mockk()) }
                coEvery { guildCacheService.getMembers(any()) } returns expected.asFlow()
                assertEquals(expected, cacheEntitySupplier.getMembers(Random.nextInt()).toList())
                coVerify(exactly = 1) { guildCacheService.getMembers(any()) }
            }

        }

        @Nested
        inner class GetInvitations {

            @Test
            fun `should get in supplier`() = runTest {
                val slot = slot<Int>()

                coEvery { guildCacheService.getInvitations(capture(slot)) } returns emptyFlow()

                val id = Random.nextInt()
                assertEquals(emptyList(), cacheEntitySupplier.getInvitations(id).toList())
                coVerify(exactly = 1) { guildCacheService.getInvitations(any()) }

                assertEquals(id, slot.captured)
            }

            @Test
            fun `should return empty collection when supplier returns empty collection`() = runTest {
                coEvery { guildCacheService.getInvitations(any()) } returns emptyFlow()
                assertEquals(emptyList(), cacheEntitySupplier.getInvitations(Random.nextInt()).toList())
                coVerify(exactly = 1) { guildCacheService.getInvitations(any()) }
            }

            @Test
            fun `should return not empty collection when supplier returns not empty collection`() = runTest {
                val expected = List(5) { GuildInvite(Random.nextInt(), randomEntityId(), mockk(), mockk()) }
                coEvery { guildCacheService.getInvitations(any()) } returns expected.asFlow()
                assertEquals(expected, cacheEntitySupplier.getInvitations(Random.nextInt()).toList())
                coVerify(exactly = 1) { guildCacheService.getInvitations(any()) }
            }

        }

    }
}
