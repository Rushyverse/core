package com.github.rushyverse.core.data.friend

import com.github.rushyverse.core.cache.AbstractCacheService
import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.container.createRedisContainer
import com.github.rushyverse.core.data.FriendCacheService
import com.github.rushyverse.core.data.IFriendCacheService
import com.github.rushyverse.core.data.IFriendDatabaseService
import com.github.rushyverse.core.data.IGuildCacheService
import com.github.rushyverse.core.data.player.IPlayerCacheService
import com.github.rushyverse.core.serializer.UUIDSerializer
import com.github.rushyverse.core.supplier.database.DatabaseSupplierConfiguration
import io.lettuce.core.RedisURI
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.flow.toSet
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Timeout
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Timeout(10, unit = TimeUnit.SECONDS)
@Testcontainers
class FriendCacheServiceTest {

    companion object {
        @JvmStatic
        @Container
        private val redisContainer = createRedisContainer()
    }

    private lateinit var cacheClient: CacheClient
    private lateinit var cacheService: FriendCacheService

    @BeforeTest
    fun onBefore() = runBlocking {
        cacheClient = CacheClient {
            uri = RedisURI.create(redisContainer.url)
        }
        cacheService = FriendCacheService(cacheClient)
    }

    @AfterTest
    fun onAfter() = runBlocking<Unit> {
        cacheClient.closeAsync().await()
    }

    @Nested
    inner class DefaultParameter {

        @Test
        fun `default values`() {
            assertEquals(AbstractCacheService.DEFAULT_PREFIX_KEY_USER_CACHE, cacheService.prefixKey)
            assertNull(cacheService.expirationKey)
        }

    }

    @Nested
    inner class UserCacheToDatabase {

        private lateinit var configuration: DatabaseSupplierConfiguration
        private lateinit var cacheServiceMock: IFriendCacheService
        private lateinit var databaseService: IFriendDatabaseService

        @BeforeTest
        fun onBefore() {
            cacheServiceMock = mockk()
            databaseService = mockk()

            configuration = DatabaseSupplierConfiguration(
                cacheServiceMock to databaseService,
                mockk<IGuildCacheService>() to mockk(),
                mockk<IPlayerCacheService>() to mockk()
            )
        }

        @Test
        fun `should returns false when no data is present in cache`() = runTest {
            val uuid = UUID.randomUUID()

            coEvery { cacheServiceMock.getAll(uuid, FriendCacheService.Type.ADD_PENDING_FRIEND) } returns emptyFlow()
            coEvery { cacheServiceMock.getAll(uuid, FriendCacheService.Type.REMOVE_PENDING_FRIEND) } returns emptyFlow()
            coEvery { cacheServiceMock.getAll(uuid, FriendCacheService.Type.ADD_FRIEND) } returns emptyFlow()
            coEvery { cacheServiceMock.getAll(uuid, FriendCacheService.Type.REMOVE_FRIEND) } returns emptyFlow()

            assertFalse(FriendCacheService.cacheToDatabase(uuid, configuration))
        }

        @Test
        fun `should returns result of adding database when add pending friend is not empty`() = runTest {
            val uuid = UUID.randomUUID()
            val friends = List(10) { UUID.randomUUID() }

            coEvery {
                cacheServiceMock.getAll(
                    uuid,
                    FriendCacheService.Type.ADD_PENDING_FRIEND
                )
            } returns friends.asFlow()
            coEvery { cacheServiceMock.getAll(uuid, FriendCacheService.Type.REMOVE_PENDING_FRIEND) } returns emptyFlow()
            coEvery { cacheServiceMock.getAll(uuid, FriendCacheService.Type.ADD_FRIEND) } returns emptyFlow()
            coEvery { cacheServiceMock.getAll(uuid, FriendCacheService.Type.REMOVE_FRIEND) } returns emptyFlow()

            coEvery { databaseService.addPendingFriends(uuid, friends) } returns true
            assertTrue(FriendCacheService.cacheToDatabase(uuid, configuration))
            coVerify(exactly = 1) { databaseService.addPendingFriends(uuid, friends) }

            coEvery { databaseService.addPendingFriends(uuid, friends) } returns false
            assertFalse(FriendCacheService.cacheToDatabase(uuid, configuration))
            coVerify(exactly = 2) { databaseService.addPendingFriends(uuid, friends) }
        }

        @Test
        fun `should returns result of adding database when remove pending friend is not empty`() = runTest {
            val uuid = UUID.randomUUID()
            val friends = List(10) { UUID.randomUUID() }

            coEvery { cacheServiceMock.getAll(uuid, FriendCacheService.Type.ADD_PENDING_FRIEND) } returns emptyFlow()
            coEvery {
                cacheServiceMock.getAll(
                    uuid,
                    FriendCacheService.Type.REMOVE_PENDING_FRIEND
                )
            } returns friends.asFlow()
            coEvery { cacheServiceMock.getAll(uuid, FriendCacheService.Type.ADD_FRIEND) } returns emptyFlow()
            coEvery { cacheServiceMock.getAll(uuid, FriendCacheService.Type.REMOVE_FRIEND) } returns emptyFlow()

            coEvery { databaseService.removePendingFriends(uuid, friends) } returns true
            assertTrue(FriendCacheService.cacheToDatabase(uuid, configuration))
            coVerify(exactly = 1) { databaseService.removePendingFriends(uuid, friends) }

            coEvery { databaseService.removePendingFriends(uuid, friends) } returns false
            assertFalse(FriendCacheService.cacheToDatabase(uuid, configuration))
            coVerify(exactly = 2) { databaseService.removePendingFriends(uuid, friends) }
        }

        @Test
        fun `should returns result of adding database when add friend is not empty`() = runTest {
            val uuid = UUID.randomUUID()
            val friends = List(10) { UUID.randomUUID() }

            coEvery { cacheServiceMock.getAll(uuid, FriendCacheService.Type.ADD_PENDING_FRIEND) } returns emptyFlow()
            coEvery { cacheServiceMock.getAll(uuid, FriendCacheService.Type.REMOVE_PENDING_FRIEND) } returns emptyFlow()
            coEvery { cacheServiceMock.getAll(uuid, FriendCacheService.Type.ADD_FRIEND) } returns friends.asFlow()
            coEvery { cacheServiceMock.getAll(uuid, FriendCacheService.Type.REMOVE_FRIEND) } returns emptyFlow()

            coEvery { databaseService.addFriends(uuid, friends) } returns true
            assertTrue(FriendCacheService.cacheToDatabase(uuid, configuration))
            coVerify(exactly = 1) { databaseService.addFriends(uuid, friends) }

            coEvery { databaseService.addFriends(uuid, friends) } returns false
            assertFalse(FriendCacheService.cacheToDatabase(uuid, configuration))
            coVerify(exactly = 2) { databaseService.addFriends(uuid, friends) }
        }

        @Test
        fun `should returns result of adding database when remove friend is not empty`() = runTest {
            val uuid = UUID.randomUUID()
            val friends = List(10) { UUID.randomUUID() }

            coEvery { cacheServiceMock.getAll(uuid, FriendCacheService.Type.ADD_PENDING_FRIEND) } returns emptyFlow()
            coEvery { cacheServiceMock.getAll(uuid, FriendCacheService.Type.REMOVE_PENDING_FRIEND) } returns emptyFlow()
            coEvery { cacheServiceMock.getAll(uuid, FriendCacheService.Type.ADD_FRIEND) } returns emptyFlow()
            coEvery { cacheServiceMock.getAll(uuid, FriendCacheService.Type.REMOVE_FRIEND) } returns friends.asFlow()

            coEvery { databaseService.removeFriends(uuid, friends) } returns true
            assertTrue(FriendCacheService.cacheToDatabase(uuid, configuration))
            coVerify(exactly = 1) { databaseService.removeFriends(uuid, friends) }

            coEvery { databaseService.removeFriends(uuid, friends) } returns false
            assertFalse(FriendCacheService.cacheToDatabase(uuid, configuration))
            coVerify(exactly = 2) { databaseService.removeFriends(uuid, friends) }
        }

        @Test
        fun `should returns true when all data in cache is defined`() = runTest {
            val uuid = UUID.randomUUID()
            val add = List(10) { UUID.randomUUID() }
            val remove = List(10) { UUID.randomUUID() }
            val addPending = List(10) { UUID.randomUUID() }
            val removePending = List(10) { UUID.randomUUID() }

            coEvery {
                cacheServiceMock.getAll(
                    uuid,
                    FriendCacheService.Type.ADD_PENDING_FRIEND
                )
            } returns addPending.asFlow()
            coEvery {
                cacheServiceMock.getAll(
                    uuid,
                    FriendCacheService.Type.REMOVE_PENDING_FRIEND
                )
            } returns removePending.asFlow()
            coEvery { cacheServiceMock.getAll(uuid, FriendCacheService.Type.ADD_FRIEND) } returns add.asFlow()
            coEvery { cacheServiceMock.getAll(uuid, FriendCacheService.Type.REMOVE_FRIEND) } returns remove.asFlow()

            coEvery { databaseService.addFriends(uuid, add) } returns true
            coEvery { databaseService.removeFriends(uuid, remove) } returns true
            coEvery { databaseService.removePendingFriends(uuid, removePending) } returns true
            coEvery { databaseService.addPendingFriends(uuid, addPending) } returns true

            assertTrue(FriendCacheService.cacheToDatabase(uuid, configuration))
            coVerify(exactly = 1) { databaseService.addFriends(uuid, add) }
            coVerify(exactly = 1) { databaseService.removeFriends(uuid, remove) }
            coVerify(exactly = 1) { databaseService.removePendingFriends(uuid, removePending) }
            coVerify(exactly = 1) { databaseService.addPendingFriends(uuid, addPending) }

            coEvery { databaseService.addFriends(uuid, add) } returns false
            coEvery { databaseService.removeFriends(uuid, remove) } returns false
            coEvery { databaseService.removePendingFriends(uuid, removePending) } returns false
            coEvery { databaseService.addPendingFriends(uuid, addPending) } returns false

            assertFalse(FriendCacheService.cacheToDatabase(uuid, configuration))
            coVerify(exactly = 2) { databaseService.addFriends(uuid, add) }
            coVerify(exactly = 2) { databaseService.removeFriends(uuid, remove) }
            coVerify(exactly = 2) { databaseService.removePendingFriends(uuid, removePending) }
            coVerify(exactly = 2) { databaseService.addPendingFriends(uuid, addPending) }
        }
    }

    @Nested
    inner class AddFriend {

        private lateinit var uuid1: UUID
        private lateinit var uuid2: UUID

        @BeforeTest
        fun onBefore() {
            uuid1 = UUID.randomUUID()
            uuid2 = UUID.randomUUID()
        }

        @Test
        fun `should define the key`() = runTest {
            assertTrue { cacheService.addFriend(uuid1, uuid2) }

            cacheClient.connect {
                val expectedKey = "user:${uuid1}:friends:add".encodeToByteArray()
                assertThat(it.keys(expectedKey).toList()).hasSize(1)
            }
        }

        @Test
        fun `should add several relations`() = runTest {
            suspend fun assertHasRelations(vararg friends: UUID) {
                cacheClient.connect {
                    assertThat(
                        getAll(
                            it,
                            uuid1,
                            FriendCacheService.Type.ADD_FRIEND
                        )
                    ).containsExactlyInAnyOrder(*friends)
                    assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                    assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).isEmpty()
                    assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()

                    FriendCacheService.Type.entries.forEach { type ->
                        assertThat(getAll(it, uuid2, type)).isEmpty()
                    }
                }
            }

            assertTrue { cacheService.addFriend(uuid1, uuid2) }
            assertHasRelations(uuid2)

            val uuid3 = UUID.randomUUID()
            assertTrue { cacheService.addFriend(uuid1, uuid3) }
            assertHasRelations(uuid2, uuid3)

            val uuid4 = UUID.randomUUID()
            assertTrue { cacheService.addFriend(uuid1, uuid4) }
            assertHasRelations(uuid2, uuid3, uuid4)
        }

        @Test
        fun `should returns false if relation exists`() = runTest {
            cacheService.addFriend(uuid1, uuid2)
            assertFalse { cacheService.addFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()

                FriendCacheService.Type.entries.forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }

        @Test
        fun `should returns true if relation doesn't exist in friends`() = runTest {
            assertTrue { cacheService.addFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()

                FriendCacheService.Type.entries.forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }

        @Test
        fun `will delete relation in remove relationship`() = runTest {
            cacheService.removeFriend(uuid1, uuid2)
            assertTrue { cacheService.addFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()

                FriendCacheService.Type.entries.forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }

        @Test
        fun `will not change relation in pending requests`() = runTest {
            val uuid3 = UUID.randomUUID()
            cacheService.addPendingFriend(uuid1, uuid2)
            cacheService.removePendingFriend(uuid1, uuid3)

            assertTrue { cacheService.addFriend(uuid1, uuid2) }
            assertTrue { cacheService.addFriend(uuid1, uuid3) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND))
                    .containsExactlyInAnyOrder(uuid2, uuid3)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).containsExactlyInAnyOrder(
                    uuid2
                )
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).containsExactlyInAnyOrder(
                    uuid3
                )

                FriendCacheService.Type.entries.forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }

        @Test
        fun `will not add if already present but will remove in delete relation`() = runTest {
            cacheService.removeFriend(uuid1, uuid2)
            assertTrue { cacheService.addFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()

                FriendCacheService.Type.entries.forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }
    }

    @Nested
    inner class AddPendingFriend {

        private lateinit var uuid1: UUID
        private lateinit var uuid2: UUID

        @BeforeTest
        fun onBefore() {
            uuid1 = UUID.randomUUID()
            uuid2 = UUID.randomUUID()
        }

        @Test
        fun `should define the key`() = runTest {
            assertTrue { cacheService.addPendingFriend(uuid1, uuid2) }

            cacheClient.connect {
                val expectedKey = "user:${uuid1}:friends:pending:add".encodeToByteArray()
                assertThat(it.keys(expectedKey).toList()).hasSize(1)
            }
        }

        @Test
        fun `should add several relations`() = runTest {
            suspend fun assertHasRelations(vararg friends: UUID) {
                cacheClient.connect {
                    assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).isEmpty()
                    assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                    assertThat(
                        getAll(
                            it,
                            uuid1,
                            FriendCacheService.Type.ADD_PENDING_FRIEND
                        )
                    ).containsExactlyInAnyOrder(*friends)
                    assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()

                    FriendCacheService.Type.entries.forEach { type ->
                        assertThat(getAll(it, uuid2, type)).isEmpty()
                    }
                }
            }

            assertTrue { cacheService.addPendingFriend(uuid1, uuid2) }
            assertHasRelations(uuid2)

            val uuid3 = UUID.randomUUID()
            assertTrue { cacheService.addPendingFriend(uuid1, uuid3) }
            assertHasRelations(uuid2, uuid3)

            val uuid4 = UUID.randomUUID()
            assertTrue { cacheService.addPendingFriend(uuid1, uuid4) }
            assertHasRelations(uuid2, uuid3, uuid4)
        }

        @Test
        fun `should returns false if relation exists in pending requests`() = runTest {
            cacheService.addPendingFriend(uuid1, uuid2)
            assertFalse { cacheService.addPendingFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).containsExactlyInAnyOrder(
                    uuid2
                )
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()

                FriendCacheService.Type.entries.forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }

        @Test
        fun `should returns true if relation doesn't exist in pending requests`() = runTest {
            assertTrue { cacheService.addPendingFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).containsExactlyInAnyOrder(
                    uuid2
                )
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()

                FriendCacheService.Type.entries.forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }

        @Test
        fun `will delete relation in remove relationship`() = runTest {
            cacheService.removePendingFriend(uuid1, uuid2)
            assertTrue { cacheService.addPendingFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).containsExactlyInAnyOrder(
                    uuid2
                )
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()

                FriendCacheService.Type.entries.forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }

        @Test
        fun `will not change relation in friend`() = runTest {
            val uuid3 = UUID.randomUUID()
            cacheService.addFriend(uuid1, uuid2)
            cacheService.removeFriend(uuid1, uuid3)

            assertTrue { cacheService.addPendingFriend(uuid1, uuid2) }
            assertTrue { cacheService.addPendingFriend(uuid1, uuid3) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).containsExactlyInAnyOrder(uuid3)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).containsExactlyInAnyOrder(
                    uuid2, uuid3
                )
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()

                FriendCacheService.Type.entries.forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }

        @Test
        fun `should returns false if relation is not added or removed`() = runTest {
            cacheService.addPendingFriend(uuid1, uuid2)
            assertFalse { cacheService.addPendingFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).containsExactlyInAnyOrder(
                    uuid2
                )
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()

                FriendCacheService.Type.entries.forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }
    }

    @Nested
    inner class RemoveFriend {

        private lateinit var uuid1: UUID
        private lateinit var uuid2: UUID

        @BeforeTest
        fun onBefore() {
            uuid1 = UUID.randomUUID()
            uuid2 = UUID.randomUUID()
        }

        @Test
        fun `should define the key`() = runTest {
            assertTrue { cacheService.removeFriend(uuid1, uuid2) }

            cacheClient.connect {
                val expectedKey = "user:${uuid1}:friends:remove".encodeToByteArray()
                assertThat(it.keys(expectedKey).toList()).hasSize(1)
            }
        }

        @Test
        fun `should add several relations`() = runTest {
            suspend fun assertHasRelations(vararg friends: UUID) {
                cacheClient.connect {
                    assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).isEmpty()
                    assertThat(
                        getAll(
                            it,
                            uuid1,
                            FriendCacheService.Type.REMOVE_FRIEND
                        )
                    ).containsExactlyInAnyOrder(*friends)
                    assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).isEmpty()
                    assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()

                    FriendCacheService.Type.entries.forEach { type ->
                        assertThat(getAll(it, uuid2, type)).isEmpty()
                    }
                }
            }

            assertTrue { cacheService.removeFriend(uuid1, uuid2) }
            assertHasRelations(uuid2)

            val uuid3 = UUID.randomUUID()
            assertTrue { cacheService.removeFriend(uuid1, uuid3) }
            assertHasRelations(uuid2, uuid3)

            val uuid4 = UUID.randomUUID()
            assertTrue { cacheService.removeFriend(uuid1, uuid4) }
            assertHasRelations(uuid2, uuid3, uuid4)
        }

        @Test
        fun `should returns true if relation doesn't exist in friends`() = runTest {
            assertTrue { cacheService.removeFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()

                FriendCacheService.Type.entries.forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }

        @Test
        fun `will delete relation in add relationship`() = runTest {
            cacheService.addFriend(uuid1, uuid2)
            assertTrue { cacheService.removeFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()

                FriendCacheService.Type.entries.forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }

        @Test
        fun `will not change relation in pending requests`() = runTest {
            val uuid3 = UUID.randomUUID()
            cacheService.addPendingFriend(uuid1, uuid2)
            cacheService.removePendingFriend(uuid1, uuid3)

            assertTrue { cacheService.removeFriend(uuid1, uuid2) }
            assertTrue { cacheService.removeFriend(uuid1, uuid3) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND))
                    .containsExactlyInAnyOrder(uuid2, uuid3)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).containsExactlyInAnyOrder(
                    uuid2
                )
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).containsExactlyInAnyOrder(
                    uuid3
                )

                FriendCacheService.Type.entries.forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }

        @Test
        fun `should returns false if relation is not added or removed`() = runTest {
            cacheService.removeFriend(uuid1, uuid2)

            assertFalse { cacheService.removeFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()

                FriendCacheService.Type.entries.forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }
    }

    @Nested
    inner class RemovePendingFriend {

        private lateinit var uuid1: UUID
        private lateinit var uuid2: UUID

        @BeforeTest
        fun onBefore() {
            uuid1 = UUID.randomUUID()
            uuid2 = UUID.randomUUID()
        }

        @Test
        fun `should define the key`() = runTest {
            assertTrue { cacheService.removePendingFriend(uuid1, uuid2) }

            cacheClient.connect {
                val expectedKey = "user:${uuid1}:friends:pending:remove".encodeToByteArray()
                assertThat(it.keys(expectedKey).toList()).hasSize(1)
            }
        }

        @Test
        fun `should add several relations`() = runTest {
            suspend fun assertHasRelations(vararg friends: UUID) {
                cacheClient.connect {
                    assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).isEmpty()
                    assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                    assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).isEmpty()
                    assertThat(
                        getAll(
                            it,
                            uuid1,
                            FriendCacheService.Type.REMOVE_PENDING_FRIEND
                        )
                    ).containsExactlyInAnyOrder(*friends)

                    FriendCacheService.Type.entries.forEach { type ->
                        assertThat(getAll(it, uuid2, type)).isEmpty()
                    }
                }
            }

            assertTrue { cacheService.removePendingFriend(uuid1, uuid2) }
            assertHasRelations(uuid2)

            val uuid3 = UUID.randomUUID()
            assertTrue { cacheService.removePendingFriend(uuid1, uuid3) }
            assertHasRelations(uuid2, uuid3)

            val uuid4 = UUID.randomUUID()
            assertTrue { cacheService.removePendingFriend(uuid1, uuid4) }
            assertHasRelations(uuid2, uuid3, uuid4)
        }

        @Test
        fun `should returns true if relation doesn't exist in pending requests`() = runTest {
            assertTrue { cacheService.removePendingFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).containsExactlyInAnyOrder(
                    uuid2
                )

                FriendCacheService.Type.entries.forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }

        @Test
        fun `will delete relation in remove relationship`() = runTest {
            cacheService.addPendingFriend(uuid1, uuid2)
            assertTrue { cacheService.removePendingFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).containsExactlyInAnyOrder(
                    uuid2
                )

                FriendCacheService.Type.entries.forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }

        @Test
        fun `will not change relation in friend`() = runTest {
            val uuid3 = UUID.randomUUID()
            cacheService.addFriend(uuid1, uuid2)
            cacheService.removeFriend(uuid1, uuid3)

            assertTrue { cacheService.removePendingFriend(uuid1, uuid2) }
            assertTrue { cacheService.removePendingFriend(uuid1, uuid3) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).containsExactlyInAnyOrder(uuid3)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).containsExactlyInAnyOrder(
                    uuid2, uuid3
                )

                FriendCacheService.Type.entries.forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }

        @Test
        fun `should returns false if relation is not added or removed`() = runTest {
            cacheService.removePendingFriend(uuid1, uuid2)
            assertFalse { cacheService.removePendingFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).containsExactlyInAnyOrder(
                    uuid2
                )

                FriendCacheService.Type.entries.forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }
    }

    @Nested
    inner class GetFriends {

        private lateinit var uuid1: UUID
        private lateinit var uuid2: UUID

        @BeforeTest
        fun onBefore() {
            uuid1 = UUID.randomUUID()
            uuid2 = UUID.randomUUID()
        }

        @Test
        fun `should returns empty flow when no relation`() = runTest {
            assertThat(cacheService.getFriends(uuid1).toList()).isEmpty()
        }

        @Test
        fun `should returns elements from relation`() = runTest {
            val uuid3 = UUID.randomUUID()
            cacheService.addFriends(uuid1, listOf(uuid2, uuid3))

            assertThat(cacheService.getFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2, uuid3)

            val uuid4 = UUID.randomUUID()
            cacheService.addFriend(uuid1, uuid4)
            assertThat(cacheService.getFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2, uuid3, uuid4)
        }

        @Test
        fun `should not returns duplicate from relations`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_FRIEND)
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_FRIEND)
            }

            assertThat(cacheService.getFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2)
        }

        @Test
        fun `should remove elements present in remove relation`() = runTest {
            val uuid3 = UUID.randomUUID()
            val uuid4 = UUID.randomUUID()

            cacheService.addFriends(uuid1, listOf(uuid2, uuid3, uuid4))

            cacheService.removeFriend(uuid1, uuid3)
            assertThat(cacheService.getFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2, uuid4)

            cacheService.removeFriend(uuid1, uuid2)
            assertThat(cacheService.getFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid4)

            cacheService.removeFriend(uuid1, uuid4)
            assertThat(cacheService.getFriends(uuid1).toList()).isEmpty()
        }

        @Test
        fun `should not use pending relation`() = runTest {
            cacheService.addPendingFriend(uuid1, uuid2)
            assertThat(cacheService.getFriends(uuid1).toList()).isEmpty()
        }
    }

    @Nested
    inner class GetPendingFriends {

        private lateinit var uuid1: UUID
        private lateinit var uuid2: UUID

        @BeforeTest
        fun onBefore() {
            uuid1 = UUID.randomUUID()
            uuid2 = UUID.randomUUID()
        }

        @Test
        fun `should returns empty flow when no relation`() = runTest {
            assertThat(cacheService.getPendingFriends(uuid1).toList()).isEmpty()
        }

        @Test
        fun `should not returns duplicate from relations`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_PENDING_FRIEND)
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_PENDING_FRIEND)
            }

            assertThat(cacheService.getPendingFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2)
        }

        @Test
        fun `should returns elements from relation`() = runTest {
            val uuid3 = UUID.randomUUID()
            cacheService.addPendingFriends(uuid1, listOf(uuid2, uuid3))

            assertThat(cacheService.getPendingFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2, uuid3)

            val uuid4 = UUID.randomUUID()
            cacheService.addPendingFriend(uuid1, uuid4)
            assertThat(cacheService.getPendingFriends(uuid1).toList()).containsExactlyInAnyOrder(
                uuid2,
                uuid3,
                uuid4
            )
        }

        @Test
        fun `should remove elements present in remove relation`() = runTest {
            val uuid3 = UUID.randomUUID()
            val uuid4 = UUID.randomUUID()

            cacheService.addPendingFriends(uuid1, listOf(uuid2, uuid3, uuid4))

            cacheService.removePendingFriend(uuid1, uuid3)
            assertThat(cacheService.getPendingFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2, uuid4)

            cacheService.removePendingFriend(uuid1, uuid2)

            assertThat(cacheService.getPendingFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid4)

            cacheService.removePendingFriend(uuid1, uuid4)
            assertThat(cacheService.getPendingFriends(uuid1).toList()).isEmpty()
        }

        @Test
        fun `should not use friend relation`() = runTest {
            cacheService.addFriend(uuid1, uuid2)
            assertThat(cacheService.getPendingFriends(uuid1).toList()).isEmpty()
        }
    }

    @Nested
    inner class AddFriends {

        private lateinit var uuid1: UUID
        private lateinit var uuid2: UUID

        @BeforeTest
        fun onBefore() {
            uuid1 = UUID.randomUUID()
            uuid2 = UUID.randomUUID()
        }

        @Test
        fun `should define the key`() = runTest {
            assertTrue { cacheService.addFriends(uuid1, setOf(uuid2)) }

            cacheClient.connect {
                val expectedKey = "user:${uuid1}:friends:add".encodeToByteArray()
                assertThat(it.keys(expectedKey).toList()).hasSize(1)
            }
        }

        @Test
        fun `should define elements to relation`() = runTest {
            val uuid3 = UUID.randomUUID()
            val uuid4 = UUID.randomUUID()

            cacheService.addFriends(uuid1, setOf(uuid2, uuid3, uuid4))

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).containsExactlyInAnyOrder(
                    uuid2,
                    uuid3,
                    uuid4
                )
            }
        }

        @Test
        fun `should not overwrite elements to relation`() = runTest {
            val uuid3 = UUID.randomUUID()
            val uuid4 = UUID.randomUUID()

            cacheService.addFriend(uuid1, uuid2)
            cacheService.addFriends(uuid1, setOf(uuid3, uuid4))

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).containsExactlyInAnyOrder(
                    uuid2,
                    uuid3,
                    uuid4
                )
            }
        }

        @Test
        fun `should not use pending request relation`() = runTest {
            val uuid3 = UUID.randomUUID()
            val uuid4 = UUID.randomUUID()

            cacheService.addPendingFriend(uuid1, uuid2)
            cacheService.addFriends(uuid1, setOf(uuid3, uuid4))

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND))
                    .containsExactlyInAnyOrder(uuid3, uuid4)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).containsExactlyInAnyOrder(
                    uuid2
                )
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()
            }
        }
    }

    @Nested
    inner class AddPendingFriends {

        private lateinit var uuid1: UUID
        private lateinit var uuid2: UUID

        @BeforeTest
        fun onBefore() {
            uuid1 = UUID.randomUUID()
            uuid2 = UUID.randomUUID()
        }

        @Test
        fun `should define the key`() = runTest {
            assertTrue { cacheService.addPendingFriends(uuid1, setOf(uuid2)) }

            cacheClient.connect {
                val expectedKey = "user:${uuid1}:friends:pending:add".encodeToByteArray()
                assertThat(it.keys(expectedKey).toList()).hasSize(1)
            }
        }

        @Test
        fun `should define elements to relation`() = runTest {
            val uuid3 = UUID.randomUUID()
            val uuid4 = UUID.randomUUID()

            cacheService.addPendingFriends(uuid1, setOf(uuid2, uuid3, uuid4))
            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).containsExactlyInAnyOrder(
                    uuid2,
                    uuid3,
                    uuid4
                )
            }
        }

        @Test
        fun `should not overwrite elements to relation`() = runTest {
            val uuid3 = UUID.randomUUID()
            val uuid4 = UUID.randomUUID()

            cacheService.addPendingFriend(uuid1, uuid2)
            cacheService.addPendingFriends(uuid1, setOf(uuid3, uuid4))
            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).containsExactlyInAnyOrder(
                    uuid2,
                    uuid3,
                    uuid4
                )
            }
        }

        @Test
        fun `should not use friend relation`() = runTest {
            val uuid3 = UUID.randomUUID()
            val uuid4 = UUID.randomUUID()

            cacheService.addFriend(uuid1, uuid2)
            cacheService.addPendingFriends(uuid1, setOf(uuid3, uuid4))

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND))
                    .containsExactlyInAnyOrder(uuid3, uuid4)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()
            }
        }
    }

    @Nested
    inner class IsFriend {

        private lateinit var uuid1: UUID
        private lateinit var uuid2: UUID

        @BeforeTest
        fun onBefore() {
            uuid1 = UUID.randomUUID()
            uuid2 = UUID.randomUUID()
        }

        @Test
        fun `should returns false if no relation exists`() = runTest {
            assertFalse { cacheService.isFriend(uuid1, uuid2) }
        }

        @Test
        fun `should returns true if the relation exists in added`() = runTest {
            cacheService.addFriend(uuid1, uuid2)
            assertTrue { cacheService.isFriend(uuid1, uuid2) }
        }

        @Test
        fun `should returns false if the relation exists in remove`() = runTest {
            cacheService.addFriend(uuid1, uuid2)
            cacheService.removeFriend(uuid1, uuid2)

            assertFalse { cacheService.isFriend(uuid1, uuid2) }
        }

        @Test
        fun `should returns false if the relation exists in pending`() = runTest {
            cacheService.addPendingFriend(uuid1, uuid2)
            assertFalse { cacheService.isFriend(uuid1, uuid2) }
        }

        @Test
        fun `should returns true if the relation exists between other relationship in list`() = runTest {
            cacheService.addFriend(uuid1, uuid2)
            cacheService.addFriend(uuid1, UUID.randomUUID())
            assertTrue { cacheService.isFriend(uuid1, uuid2) }
        }
    }

    @Nested
    inner class IsPendingFriend {

        private lateinit var uuid1: UUID
        private lateinit var uuid2: UUID

        @BeforeTest
        fun onBefore() {
            uuid1 = UUID.randomUUID()
            uuid2 = UUID.randomUUID()
        }

        @Test
        fun `should returns false if no relation exists`() = runTest {
            assertFalse { cacheService.isPendingFriend(uuid1, uuid2) }
        }

        @Test
        fun `should returns true if the relation exists in added`() = runTest {
            cacheService.addPendingFriend(uuid1, uuid2)
            assertTrue { cacheService.isPendingFriend(uuid1, uuid2) }
        }

        @Test
        fun `should returns false if the relation exists in remove`() = runTest {
            cacheService.addPendingFriend(uuid1, uuid2)
            cacheService.removePendingFriend(uuid1, uuid2)
            assertFalse { cacheService.isPendingFriend(uuid1, uuid2) }
        }

        @Test
        fun `should returns false if the relation exists in friend`() = runTest {
            cacheService.addFriend(uuid1, uuid2)
            assertFalse { cacheService.isPendingFriend(uuid1, uuid2) }
        }

        @Test
        fun `should returns true if the relation exists between other relationship in add`() = runTest {
            cacheService.addPendingFriend(uuid1, uuid2)
            cacheService.addPendingFriend(uuid1, UUID.randomUUID())
            assertTrue { cacheService.isPendingFriend(uuid1, uuid2) }
        }
    }

    private suspend fun getAll(
        it: RedisCoroutinesCommands<ByteArray, ByteArray>,
        uuid1: UUID,
        type: FriendCacheService.Type
    ) =
        it.smembers(encodeKeyWithType(uuid1, type))
            .map { cacheClient.binaryFormat.decodeFromByteArray(UUIDSerializer, it) }.toSet()

    private suspend fun add(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        uuid: UUID,
        friend: UUID,
        type: FriendCacheService.Type
    ) {
        val key = encodeKeyWithType(uuid, type)
        val friends = cacheClient.binaryFormat.encodeToByteArray(UUIDSerializer, friend)
        connection.sadd(key, friends)
    }

    private fun encodeKeyWithType(
        uuid: UUID,
        type: FriendCacheService.Type
    ) = (cacheService.prefixKey.format(uuid) + type.key).encodeToByteArray()
}
