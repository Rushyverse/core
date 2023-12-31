package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.data.Guild
import com.github.rushyverse.core.data.GuildInvite
import com.github.rushyverse.core.data.GuildMember
import com.github.rushyverse.core.utils.randomEntityId
import com.github.rushyverse.core.utils.randomString
import io.mockk.*
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.time.Instant
import java.util.*
import kotlin.random.Random
import kotlin.test.*

class DatabaseStoreEntitySupplierTest {

    private lateinit var entitySupplier: DatabaseStoreEntitySupplier
    private lateinit var cache: DatabaseCacheEntitySupplier
    private lateinit var supplier: IDatabaseEntitySupplier

    @BeforeTest
    fun onBefore() {
        cache = mockk(randomString())
        supplier = mockk(randomString())
        entitySupplier = DatabaseStoreEntitySupplier(cache, supplier)
    }

    @Test
    fun `get configuration will get from cache supplier`() = runTest {
        val configuration = mockk<DatabaseSupplierConfiguration>(randomString())
        every { cache.configuration } returns configuration

        assertEquals(configuration, entitySupplier.configuration)
        verify(exactly = 1) { cache.configuration }
    }

    @Nested
    inner class FriendTest {

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
                coEvery { cache.addFriends(id, any()) } returns true

                assertEquals(emptyList(), entitySupplier.getFriends(id).toList())
                coVerify(exactly = 1) { supplier.getFriends(id) }
                coVerify(exactly = 1) { cache.addFriends(id, emptySet()) }
            }

            @Test
            fun `should store list of friends in cache if the flow is not empty`() = runTest {
                val id = UUID.randomUUID()
                val friends = List(5) { UUID.randomUUID() }
                coEvery { supplier.getFriends(id) } returns friends.asFlow()
                coEvery { cache.addFriends(id, any()) } returns true

                assertEquals(friends, entitySupplier.getFriends(id).toList())
                coVerify(exactly = 1) { supplier.getFriends(id) }
                coVerify(exactly = 1) { cache.addFriends(id, friends.toSet()) }
            }
        }

        @Nested
        inner class GetPendingFriends {

            @Test
            fun `should store empty list in cache if the flow is empty`() = runTest {
                val id = UUID.randomUUID()
                coEvery { supplier.getPendingFriends(id) } returns emptyFlow()
                coEvery { cache.addPendingFriends(id, any()) } returns true

                assertEquals(emptyList(), entitySupplier.getPendingFriends(id).toList())
                coVerify(exactly = 1) { supplier.getPendingFriends(id) }
                coVerify(exactly = 1) { cache.addPendingFriends(id, emptySet()) }
            }

            @Test
            fun `should store list of friends in cache if the flow is not empty`() = runTest {
                val id = UUID.randomUUID()
                val friends = List(5) { UUID.randomUUID() }
                coEvery { supplier.getPendingFriends(id) } returns friends.asFlow()
                coEvery { cache.addPendingFriends(id, any()) } returns true

                assertEquals(friends, entitySupplier.getPendingFriends(id).toList())
                coVerify(exactly = 1) { supplier.getPendingFriends(id) }
                coVerify(exactly = 1) { cache.addPendingFriends(id, friends.toSet()) }
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

    @Nested
    inner class GuildTest {

        @Nested
        inner class DeleteExpiredInvitations {

            @Test
            fun `should delete in supplier and cache`() = runTest {
                coEvery { cache.deleteExpiredInvitations() } returns 0
                coEvery { supplier.deleteExpiredInvitations() } returns 0
                entitySupplier.deleteExpiredInvitations()
                coVerify(exactly = 1) { supplier.deleteExpiredInvitations() }
                coVerify(exactly = 1) { cache.deleteExpiredInvitations() }
            }

            @Test
            fun `should return addition of both`() = runTest {
                val cacheResult = Random.nextLong()
                val supplierResult = Random.nextLong()
                coEvery { cache.deleteExpiredInvitations() } returns cacheResult
                coEvery { supplier.deleteExpiredInvitations() } returns supplierResult
                assertEquals(cacheResult + supplierResult, entitySupplier.deleteExpiredInvitations())
            }
        }

        @Nested
        inner class CreateGuild {

            @ParameterizedTest
            @ValueSource(booleans = [true, false])
            fun `should set data in cache if set in supplier`(cacheResult: Boolean) = runTest {
                val name = randomString()
                val owner = randomEntityId()
                val guild = mockk<Guild>()

                coEvery { supplier.createGuild(name, owner) } returns guild
                coEvery { cache.addGuild(any()) } returns cacheResult

                assertEquals(guild, entitySupplier.createGuild(name, owner))
                coVerify(exactly = 1) { supplier.createGuild(name, owner) }
                coVerify(exactly = 1) { cache.addGuild(guild) }
                coVerify(exactly = 0) { cache.createGuild(any(), any()) }
            }
        }

        @Nested
        inner class DeleteGuild {

            @ParameterizedTest
            @ValueSource(booleans = [true, false])
            fun `should return supplier result when cache returns false`(supplierResult: Boolean) = runTest {
                val id = Random.nextInt()

                coEvery { supplier.deleteGuild(id) } returns supplierResult
                coEvery { cache.deleteGuild(id) } returns false

                assertEquals(supplierResult, entitySupplier.deleteGuild(id))
                coVerify(exactly = 1) { supplier.deleteGuild(id) }
                coVerify(exactly = 1) { cache.deleteGuild(id) }
            }

            @ParameterizedTest
            @ValueSource(booleans = [true, false])
            fun `should return cache result when supplier returns false`(cacheResult: Boolean) = runTest {
                val id = Random.nextInt()

                coEvery { supplier.deleteGuild(id) } returns false
                coEvery { cache.deleteGuild(id) } returns cacheResult

                assertEquals(cacheResult, entitySupplier.deleteGuild(id))
                coVerify(exactly = 1) { supplier.deleteGuild(id) }
                coVerify(exactly = 1) { cache.deleteGuild(id) }
            }

            @ParameterizedTest
            @ValueSource(booleans = [true, false])
            fun `should return result when both return same result`(result: Boolean) = runTest {
                val id = Random.nextInt()

                coEvery { supplier.deleteGuild(id) } returns result
                coEvery { cache.deleteGuild(id) } returns result

                assertEquals(result, entitySupplier.deleteGuild(id))
                coVerify(exactly = 1) { supplier.deleteGuild(id) }
                coVerify(exactly = 1) { cache.deleteGuild(id) }
            }

        }

        @Nested
        inner class GetGuildById {

            @Test
            fun `should set data in cache if supplier result not null`() = runTest {
                val id = Random.nextInt()
                val guild = mockk<Guild>()

                coEvery { supplier.getGuild(id) } returns guild
                coEvery { cache.addGuild(guild) } returns true

                assertEquals(guild, entitySupplier.getGuild(id))
                coVerify(exactly = 1) { supplier.getGuild(id) }
                coVerify(exactly = 1) { cache.addGuild(guild) }
            }

            @Test
            fun `should not set data in cache if supplier result null`() = runTest {
                val id = Random.nextInt()

                coEvery { supplier.getGuild(id) } returns null

                assertNull(entitySupplier.getGuild(id))
                coVerify(exactly = 1) { supplier.getGuild(id) }
                coVerify(exactly = 0) { cache.addGuild(any()) }
            }
        }

        @Nested
        inner class GetGuildByName {

            @Test
            fun `should not import guild if flow is empty`() = runTest {
                val name = randomString()
                coEvery { supplier.getGuild(name) } returns emptyFlow()

                assertThat(entitySupplier.getGuild(name).toList()).isEmpty()
                coVerify(exactly = 1) { supplier.getGuild(name) }
                coVerify(exactly = 0) { cache.addGuild(any()) }
            }

            @Test
            fun `should not throw exception if import throws exception`() = runTest {
                val name = randomString()
                val guilds = List(2) { mockk<Guild>() }
                coEvery { supplier.getGuild(name) } returns guilds.asFlow()
                coEvery { cache.addGuild(guilds[0]) } throws Exception()

                assertThat(entitySupplier.getGuild(name).toList()).containsExactlyElementsOf(guilds)
                coVerify(exactly = 1) { supplier.getGuild(name) }
                coVerify(exactly = guilds.size) { cache.addGuild(any()) }
            }

            @Test
            fun `should import all guilds if flow is entire collected`() = runTest {
                val name = randomString()
                val guilds = List(10) { Guild(Random.nextInt(), name, randomEntityId()) }
                coEvery { supplier.getGuild(name) } returns guilds.asFlow()

                assertThat(entitySupplier.getGuild(name).toList()).containsExactlyElementsOf(guilds)
                coVerify(exactly = 1) { supplier.getGuild(name) }
                coVerify(exactly = guilds.size) { cache.addGuild(any()) }
                guilds.forEach { guild ->
                    coVerify(exactly = 1) { cache.addGuild(guild) }
                }
            }

            @Test
            fun `should import partial guilds if flow is partially collected`() = runTest {
                val name = randomString()
                val guilds = List(10) { Guild(Random.nextInt(), name, randomEntityId()) }
                val guildsToImport = guilds.take(5)
                coEvery { supplier.getGuild(name) } returns guilds.asFlow()

                assertThat(entitySupplier.getGuild(name).take(guildsToImport.size).toList())
                    .containsExactlyElementsOf(guildsToImport)

                coVerify(exactly = 1) { supplier.getGuild(name) }
                coVerify(exactly = guildsToImport.size) { cache.addGuild(any()) }
                guildsToImport.forEach { guild ->
                    coVerify(exactly = 1) { cache.addGuild(guild) }
                }
                guilds.drop(guildsToImport.size).forEach { guild ->
                    coVerify(exactly = 0) { cache.addGuild(guild) }
                }
            }

        }

        @Nested
        inner class IsOwner {

            @ParameterizedTest
            @ValueSource(booleans = [true, false])
            fun `should return the result returned by supplier without using cache`(result: Boolean) = runTest {
                val guildId = Random.nextInt()
                val entity = randomEntityId()

                coEvery { supplier.isOwner(guildId, entity) } returns result
                assertEquals(result, entitySupplier.isOwner(guildId, entity))

                coVerify(exactly = 1) { supplier.isOwner(guildId, entity) }
                coVerify(exactly = 0) { cache.isOwner(any(), any()) }
            }

        }

        @Nested
        inner class IsMember {

            @ParameterizedTest
            @ValueSource(booleans = [true, false])
            fun `should return the result returned by supplier without using cache`(result: Boolean) = runTest {
                val guildId = Random.nextInt()
                val entity = randomEntityId()

                coEvery { supplier.isMember(guildId, entity) } returns result
                assertEquals(result, entitySupplier.isMember(guildId, entity))

                coVerify(exactly = 1) { supplier.isMember(guildId, entity) }
                coVerify(exactly = 0) { cache.isMember(any(), any()) }
            }

        }

        @Nested
        inner class HasInvitation {

            @ParameterizedTest
            @ValueSource(booleans = [true, false])
            fun `should return the result returned by supplier without using cache`(result: Boolean) = runTest {
                val guildId = Random.nextInt()
                val entity = randomEntityId()

                coEvery { supplier.hasInvitation(guildId, entity) } returns result
                assertEquals(result, entitySupplier.hasInvitation(guildId, entity))

                coVerify(exactly = 1) { supplier.hasInvitation(guildId, entity) }
                coVerify(exactly = 0) { cache.hasInvitation(any(), any()) }
            }

        }

        @Nested
        inner class AddMember {

            @Test
            fun `should set data in cache if set in supplier`() = runTest {
                val guildId = Random.nextInt()
                val entity = randomEntityId()

                coEvery { supplier.addMember(guildId, entity) } returns true
                coEvery { cache.addMember(guildId, entity) } returns true

                assertTrue(entitySupplier.addMember(guildId, entity))
                coVerify(exactly = 1) { supplier.addMember(guildId, entity) }
                coVerify(exactly = 1) { cache.addMember(guildId, entity) }
            }

            @Test
            fun `should not set data in cache if not set in supplier`() = runTest {
                val guildId = Random.nextInt()
                val entity = randomEntityId()

                coEvery { supplier.addMember(guildId, entity) } returns false
                coEvery { cache.addMember(guildId, entity) } returns false

                assertFalse(entitySupplier.addMember(guildId, entity))
                coVerify(exactly = 1) { supplier.addMember(guildId, entity) }
                coVerify(exactly = 0) { cache.addMember(guildId, entity) }
            }

            @Test
            fun `should return true if data is set in supplier but not in cache`() = runTest {
                val guildId = Random.nextInt()
                val entity = randomEntityId()

                coEvery { supplier.addMember(guildId, entity) } returns true
                coEvery { cache.addMember(guildId, entity) } returns false

                assertTrue(entitySupplier.addMember(guildId, entity))
                coVerify(exactly = 1) { supplier.addMember(guildId, entity) }
                coVerify(exactly = 1) { cache.addMember(guildId, entity) }
            }

        }

        @Nested
        inner class AddInvitation {

            @Test
            fun `should set data in cache if set in supplier`() = runTest {
                val guildId = Random.nextInt()
                val entity = randomEntityId()
                val expiredAt = mockk<Instant>()

                coEvery { supplier.addInvitation(guildId, entity, expiredAt) } returns true
                coEvery { cache.addInvitation(guildId, entity, expiredAt) } returns true

                assertTrue(entitySupplier.addInvitation(guildId, entity, expiredAt))
                coVerify(exactly = 1) { supplier.addInvitation(guildId, entity, expiredAt) }
                coVerify(exactly = 1) { cache.addInvitation(guildId, entity, expiredAt) }
            }

            @Test
            fun `should not set data in cache if not set in supplier`() = runTest {
                val guildId = Random.nextInt()
                val entity = randomEntityId()
                val expiredAt = mockk<Instant>()

                coEvery { supplier.addInvitation(guildId, entity, expiredAt) } returns false
                coEvery { cache.addInvitation(guildId, entity, expiredAt) } returns false

                assertFalse(entitySupplier.addInvitation(guildId, entity, expiredAt))
                coVerify(exactly = 1) { supplier.addInvitation(guildId, entity, expiredAt) }
                coVerify(exactly = 0) { cache.addInvitation(guildId, entity, expiredAt) }
            }

            @Test
            fun `should return true if data is set in supplier but not in cache`() = runTest {
                val guildId = Random.nextInt()
                val entity = randomEntityId()
                val expiredAt = mockk<Instant>()

                coEvery { supplier.addInvitation(guildId, entity, expiredAt) } returns true
                coEvery { cache.addInvitation(guildId, entity, expiredAt) } returns false

                assertTrue(entitySupplier.addInvitation(guildId, entity, expiredAt))
                coVerify(exactly = 1) { supplier.addInvitation(guildId, entity, expiredAt) }
                coVerify(exactly = 1) { cache.addInvitation(guildId, entity, expiredAt) }
            }

        }

        @Nested
        inner class RemoveMember {

            @Test
            fun `should set data in cache if set in supplier`() = runTest {
                val guildId = Random.nextInt()
                val entity = randomEntityId()

                coEvery { supplier.removeMember(guildId, entity) } returns true
                coEvery { cache.removeMember(guildId, entity) } returns true

                assertTrue(entitySupplier.removeMember(guildId, entity))
                coVerify(exactly = 1) { supplier.removeMember(guildId, entity) }
                coVerify(exactly = 1) { cache.removeMember(guildId, entity) }
            }

            @Test
            fun `should not set data in cache if not set in supplier`() = runTest {
                val guildId = Random.nextInt()
                val entity = randomEntityId()

                coEvery { supplier.removeMember(guildId, entity) } returns false
                coEvery { cache.removeMember(guildId, entity) } returns false

                assertFalse(entitySupplier.removeMember(guildId, entity))
                coVerify(exactly = 1) { supplier.removeMember(guildId, entity) }
                coVerify(exactly = 0) { cache.removeMember(guildId, entity) }
            }

            @Test
            fun `should return true if data is set in supplier but not in cache`() = runTest {
                val guildId = Random.nextInt()
                val entity = randomEntityId()

                coEvery { supplier.removeMember(guildId, entity) } returns true
                coEvery { cache.removeMember(guildId, entity) } returns false

                assertTrue(entitySupplier.removeMember(guildId, entity))
                coVerify(exactly = 1) { supplier.removeMember(guildId, entity) }
                coVerify(exactly = 1) { cache.removeMember(guildId, entity) }
            }

        }

        @Nested
        inner class RemoveInvitation {

            @Test
            fun `should set data in cache if set in supplier`() = runTest {
                val guildId = Random.nextInt()
                val entity = randomEntityId()

                coEvery { supplier.removeInvitation(guildId, entity) } returns true
                coEvery { cache.removeInvitation(guildId, entity) } returns true

                assertTrue(entitySupplier.removeInvitation(guildId, entity))
                coVerify(exactly = 1) { supplier.removeInvitation(guildId, entity) }
                coVerify(exactly = 1) { cache.removeInvitation(guildId, entity) }
            }

            @Test
            fun `should not set data in cache if not set in supplier`() = runTest {
                val guildId = Random.nextInt()
                val entity = randomEntityId()

                coEvery { supplier.removeInvitation(guildId, entity) } returns false
                coEvery { cache.removeInvitation(guildId, entity) } returns false

                assertFalse(entitySupplier.removeInvitation(guildId, entity))
                coVerify(exactly = 1) { supplier.removeInvitation(guildId, entity) }
                coVerify(exactly = 0) { cache.removeInvitation(guildId, entity) }
            }

            @Test
            fun `should return true if data is set in supplier but not in cache`() = runTest {
                val guildId = Random.nextInt()
                val entity = randomEntityId()

                coEvery { supplier.removeInvitation(guildId, entity) } returns true
                coEvery { cache.removeInvitation(guildId, entity) } returns false

                assertTrue(entitySupplier.removeInvitation(guildId, entity))
                coVerify(exactly = 1) { supplier.removeInvitation(guildId, entity) }
                coVerify(exactly = 1) { cache.removeInvitation(guildId, entity) }
            }
        }

        @Nested
        inner class GetMembers {

            @Test
            fun `should not import member if flow is empty`() = runTest {
                val id = Random.nextInt()
                coEvery { supplier.getMembers(id) } returns emptyFlow()

                assertThat(entitySupplier.getMembers(id).toList()).isEmpty()
                coVerify(exactly = 1) { supplier.getMembers(id) }
                coVerify(exactly = 0) { cache.addMember(any()) }
            }

            @Test
            fun `should import all members if flow is entire collected`() = runTest {
                val id = Random.nextInt()
                val members = List(10) { GuildMember(id, randomEntityId()) }
                coEvery { supplier.getMembers(id) } returns members.asFlow()

                assertThat(entitySupplier.getMembers(id).toList()).containsExactlyElementsOf(members)
                coVerify(exactly = 1) { supplier.getMembers(id) }
                coVerify(exactly = members.size) { cache.addMember(any()) }
                members.forEach {
                    coVerify(exactly = 1) { cache.addMember(it) }
                }
            }

            @Test
            fun `should not throw exception if import throws exception`() = runTest {
                val id = Random.nextInt()
                val members = List(2) { GuildMember(id, randomEntityId()) }
                coEvery { supplier.getMembers(id) } returns members.asFlow()
                coEvery { cache.addMember(members[0]) } throws Exception()

                assertThat(entitySupplier.getMembers(id).toList()).containsExactlyElementsOf(members)
                coVerify(exactly = 1) { supplier.getMembers(id) }
                coVerify(exactly = members.size) { cache.addMember(any()) }
            }

            @Test
            fun `should import partial members if flow is partially collected`() = runTest {
                val id = Random.nextInt()
                val members = List(10) { GuildMember(id, randomEntityId()) }
                val toImport = members.take(5)
                coEvery { supplier.getMembers(id) } returns members.asFlow()

                assertThat(entitySupplier.getMembers(id).take(toImport.size).toList())
                    .containsExactlyElementsOf(toImport)

                coVerify(exactly = 1) { supplier.getMembers(id) }
                coVerify(exactly = toImport.size) { cache.addMember(any()) }
                toImport.forEach {
                    coVerify(exactly = 1) { cache.addMember(it) }
                }
                members.drop(toImport.size).forEach {
                    coVerify(exactly = 0) { cache.addMember(it) }
                }
            }
        }

        @Nested
        inner class GetInvitations {

            @Test
            fun `should not import member if flow is empty`() = runTest {
                val id = Random.nextInt()
                coEvery { supplier.getInvitations(id) } returns emptyFlow()

                assertThat(entitySupplier.getInvitations(id).toList()).isEmpty()
                coVerify(exactly = 1) { supplier.getInvitations(id) }
                coVerify(exactly = 0) { cache.addInvitation(any()) }
            }

            @Test
            fun `should not throw exception if import throws exception`() = runTest {
                val id = Random.nextInt()
                val invites = List(2) { mockk<GuildInvite>() }
                coEvery { supplier.getInvitations(id) } returns invites.asFlow()
                coEvery { cache.addInvitation(invites[0]) } throws Exception()

                assertThat(entitySupplier.getInvitations(id).toList()).containsExactlyElementsOf(invites)
                coVerify(exactly = 1) { supplier.getInvitations(id) }
                coVerify(exactly = invites.size) { cache.addInvitation(any()) }
            }

            @Test
            fun `should import all members if flow is entire collected`() = runTest {
                val id = Random.nextInt()
                val invites = List(10) { GuildInvite(id, randomEntityId(), mockk()) }
                coEvery { supplier.getInvitations(id) } returns invites.asFlow()

                assertThat(entitySupplier.getInvitations(id).toList()).containsExactlyElementsOf(invites)
                coVerify(exactly = 1) { supplier.getInvitations(id) }
                coVerify(exactly = invites.size) { cache.addInvitation(any()) }
                invites.forEach {
                    coVerify(exactly = 1) { cache.addInvitation(it) }
                }
            }

            @Test
            fun `should import partial members if flow is partially collected`() = runTest {
                val id = Random.nextInt()
                val invites = List(10) { GuildInvite(id, randomEntityId(), mockk()) }
                val toImport = invites.take(5)
                coEvery { supplier.getInvitations(id) } returns invites.asFlow()

                assertThat(entitySupplier.getInvitations(id).take(toImport.size).toList())
                    .containsExactlyElementsOf(toImport)

                coVerify(exactly = 1) { supplier.getInvitations(id) }
                coVerify(exactly = toImport.size) { cache.addInvitation(any()) }
                toImport.forEach {
                    coVerify(exactly = 1) { cache.addInvitation(it) }
                }
                invites.drop(toImport.size).forEach {
                    coVerify(exactly = 0) { cache.addInvitation(it) }
                }
            }

        }

    }

}
