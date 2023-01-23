package com.github.rushyverse.core.data.friend

import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.cache.DEFAULT_PREFIX_KEY_USER_CACHE
import com.github.rushyverse.core.container.createRedisContainer
import com.github.rushyverse.core.data.FriendCacheService
import com.github.rushyverse.core.data.ProfileIdCacheService
import com.github.rushyverse.core.serializer.UUIDSerializer
import io.lettuce.core.RedisURI
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.flow.toSet
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.builtins.serializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Timeout
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.test.*

@Timeout(5, unit = TimeUnit.SECONDS)
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
    fun onAfter(): Unit = runBlocking {
        cacheClient.closeAsync().await()
    }

    @Nested
    inner class DefaultParameter {

        @Test
        fun `default values`() {
            assertEquals(DEFAULT_PREFIX_KEY_USER_CACHE, cacheService.prefixKey)
            assertNull(cacheService.expirationKey)
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
                val expectedKey = cacheClient.binaryFormat.encodeToByteArray(String.serializer(), "user:${uuid1}:friends:add")
                assertThat(it.keys(expectedKey).toList()).hasSize(1)
            }
        }

        @Test
        fun `should add several relations`() = runTest {
            suspend fun assertHasRelations(vararg friends: UUID) {
                cacheClient.connect {
                    assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).isEmpty()
                    assertThat(
                        getAll(
                            it,
                            uuid1,
                            FriendCacheService.Type.ADD_FRIEND
                        )
                    ).containsExactlyInAnyOrder(*friends)
                    assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                    assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_FRIENDS)).isEmpty()
                    assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).isEmpty()
                    assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()

                    FriendCacheService.Type.values().forEach { type ->
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
        fun `should returns true if relation exists in friends`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.FRIENDS)
            }

            assertTrue { cacheService.addFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()

                FriendCacheService.Type.values().forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }

        @Test
        fun `should returns true if relation doesn't exist in friends`() = runTest {
            assertTrue { cacheService.addFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()

                FriendCacheService.Type.values().forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }

        @Test
        fun `will delete relation in remove relationship`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.REMOVE_FRIEND)
            }

            assertTrue { cacheService.addFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()

                FriendCacheService.Type.values().forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }

        @Test
        fun `will not change relation in pending requests`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.PENDING_FRIENDS)
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_PENDING_FRIEND)
                add(it, uuid1, uuid2, FriendCacheService.Type.REMOVE_PENDING_FRIEND)
            }

            assertTrue { cacheService.addFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_FRIENDS)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).containsExactlyInAnyOrder(
                    uuid2
                )
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).containsExactlyInAnyOrder(
                    uuid2
                )

                FriendCacheService.Type.values().forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }

        @Test
        fun `will not change relation for other`() = runTest {
            cacheClient.connect {
                FriendCacheService.Type.values().forEach { type ->
                    add(it, uuid2, uuid1, type)
                }
            }

            assertTrue { cacheService.addFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()

                FriendCacheService.Type.values().forEach { type ->
                    assertThat(getAll(it, uuid2, type)).containsExactlyInAnyOrder(uuid1)
                }
            }
        }

        @Test
        fun `will not add if already present but will remove in delete relation`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_FRIEND)
                add(it, uuid1, uuid2, FriendCacheService.Type.REMOVE_FRIEND)
            }

            assertTrue { cacheService.addFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()

                FriendCacheService.Type.values().forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }

        @Test
        fun `should returns false if relation is not added or removed`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_FRIEND)
            }

            assertFalse { cacheService.addFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()

                FriendCacheService.Type.values().forEach { type ->
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
                val expectedKey = cacheClient.binaryFormat.encodeToByteArray(String.serializer(), "user:${uuid1}:friends:pending:add")
                assertThat(it.keys(expectedKey).toList()).hasSize(1)
            }
        }

        @Test
        fun `should add several relations`() = runTest {
            suspend fun assertHasRelations(vararg friends: UUID) {
                cacheClient.connect {
                    assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).isEmpty()
                    assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).isEmpty()
                    assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                    assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_FRIENDS)).isEmpty()
                    assertThat(
                        getAll(
                            it,
                            uuid1,
                            FriendCacheService.Type.ADD_PENDING_FRIEND
                        )
                    ).containsExactlyInAnyOrder(*friends)
                    assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()

                    FriendCacheService.Type.values().forEach { type ->
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
        fun `should returns true if relation exists in pending requests`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.PENDING_FRIENDS)
            }

            assertTrue { cacheService.addPendingFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_FRIENDS)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).containsExactlyInAnyOrder(
                    uuid2
                )
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()

                FriendCacheService.Type.values().forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }

        @Test
        fun `should returns true if relation doesn't exist in pending requests`() = runTest {
            assertTrue { cacheService.addPendingFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).containsExactlyInAnyOrder(
                    uuid2
                )
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()

                FriendCacheService.Type.values().forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }

        @Test
        fun `will delete relation in remove relationship`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.REMOVE_PENDING_FRIEND)
            }

            assertTrue { cacheService.addPendingFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).containsExactlyInAnyOrder(
                    uuid2
                )
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()

                FriendCacheService.Type.values().forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }

        @Test
        fun `will not change relation in friend`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.FRIENDS)
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_FRIEND)
                add(it, uuid1, uuid2, FriendCacheService.Type.REMOVE_FRIEND)
            }

            assertTrue { cacheService.addPendingFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).containsExactlyInAnyOrder(
                    uuid2
                )
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()

                FriendCacheService.Type.values().forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }

        @Test
        fun `will not change relation for other`() = runTest {
            cacheClient.connect {
                FriendCacheService.Type.values().forEach { type ->
                    add(it, uuid2, uuid1, type)
                }
            }

            assertTrue { cacheService.addPendingFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).containsExactlyInAnyOrder(
                    uuid2
                )
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()

                FriendCacheService.Type.values().forEach { type ->
                    assertThat(getAll(it, uuid2, type)).containsExactlyInAnyOrder(uuid1)
                }
            }
        }

        @Test
        fun `will not add if already present but will remove in delete relation`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_PENDING_FRIEND)
                add(it, uuid1, uuid2, FriendCacheService.Type.REMOVE_PENDING_FRIEND)
            }

            assertTrue { cacheService.addPendingFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).containsExactlyInAnyOrder(
                    uuid2
                )
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()

                FriendCacheService.Type.values().forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }

        @Test
        fun `should returns false if relation is not added or removed`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_PENDING_FRIEND)
            }

            assertFalse { cacheService.addPendingFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).containsExactlyInAnyOrder(
                    uuid2
                )
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()

                FriendCacheService.Type.values().forEach { type ->
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
                val expectedKey = cacheClient.binaryFormat.encodeToByteArray(String.serializer(), "user:${uuid1}:friends:remove")
                assertThat(it.keys(expectedKey).toList()).hasSize(1)
            }
        }

        @Test
        fun `should add several relations`() = runTest {
            suspend fun assertHasRelations(vararg friends: UUID) {
                cacheClient.connect {
                    assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).isEmpty()
                    assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).isEmpty()
                    assertThat(
                        getAll(
                            it,
                            uuid1,
                            FriendCacheService.Type.REMOVE_FRIEND
                        )
                    ).containsExactlyInAnyOrder(*friends)
                    assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_FRIENDS)).isEmpty()
                    assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).isEmpty()
                    assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()

                    FriendCacheService.Type.values().forEach { type ->
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
        fun `should returns true if relation exists in friends`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.FRIENDS)
            }

            assertTrue { cacheService.removeFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()

                FriendCacheService.Type.values().forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }

        @Test
        fun `should returns true if relation doesn't exist in friends`() = runTest {
            assertTrue { cacheService.removeFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()

                FriendCacheService.Type.values().forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }

        @Test
        fun `will delete relation in add relationship`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_FRIEND)
            }

            assertTrue { cacheService.removeFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()

                FriendCacheService.Type.values().forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }

        @Test
        fun `will not change relation in pending requests`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.PENDING_FRIENDS)
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_PENDING_FRIEND)
                add(it, uuid1, uuid2, FriendCacheService.Type.REMOVE_PENDING_FRIEND)
            }

            assertTrue { cacheService.removeFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_FRIENDS)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).containsExactlyInAnyOrder(
                    uuid2
                )
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).containsExactlyInAnyOrder(
                    uuid2
                )

                FriendCacheService.Type.values().forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }

        @Test
        fun `will not change relation for other`() = runTest {
            cacheClient.connect {
                FriendCacheService.Type.values().forEach { type ->
                    add(it, uuid2, uuid1, type)
                }
            }

            assertTrue { cacheService.removeFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()

                FriendCacheService.Type.values().forEach { type ->
                    assertThat(getAll(it, uuid2, type)).containsExactlyInAnyOrder(uuid1)
                }
            }
        }

        @Test
        fun `will not add if already present but will remove in delete relation`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_FRIEND)
                add(it, uuid1, uuid2, FriendCacheService.Type.REMOVE_FRIEND)
            }

            assertTrue { cacheService.removeFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()

                FriendCacheService.Type.values().forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }

        @Test
        fun `should returns false if relation is not added or removed`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.REMOVE_FRIEND)
            }

            assertFalse { cacheService.removeFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).isEmpty()

                FriendCacheService.Type.values().forEach { type ->
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
                val expectedKey = cacheClient.binaryFormat.encodeToByteArray(String.serializer(), "user:${uuid1}:friends:pending:remove")
                assertThat(it.keys(expectedKey).toList()).hasSize(1)
            }
        }

        @Test
        fun `should add several relations`() = runTest {
            suspend fun assertHasRelations(vararg friends: UUID) {
                cacheClient.connect {
                    assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).isEmpty()
                    assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).isEmpty()
                    assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                    assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_FRIENDS)).isEmpty()
                    assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).isEmpty()
                    assertThat(
                        getAll(
                            it,
                            uuid1,
                            FriendCacheService.Type.REMOVE_PENDING_FRIEND
                        )
                    ).containsExactlyInAnyOrder(*friends)

                    FriendCacheService.Type.values().forEach { type ->
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
        fun `should returns true if relation exists in pending requests`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.PENDING_FRIENDS)
            }

            assertTrue { cacheService.removePendingFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_FRIENDS)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).containsExactlyInAnyOrder(
                    uuid2
                )

                FriendCacheService.Type.values().forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }

        @Test
        fun `should returns true if relation doesn't exist in pending requests`() = runTest {
            assertTrue { cacheService.removePendingFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).containsExactlyInAnyOrder(
                    uuid2
                )

                FriendCacheService.Type.values().forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }

        @Test
        fun `will delete relation in remove relationship`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_PENDING_FRIEND)
            }

            assertTrue { cacheService.removePendingFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).containsExactlyInAnyOrder(
                    uuid2
                )

                FriendCacheService.Type.values().forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }

        @Test
        fun `will not change relation in friend`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.FRIENDS)
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_FRIEND)
                add(it, uuid1, uuid2, FriendCacheService.Type.REMOVE_FRIEND)
            }

            assertTrue { cacheService.removePendingFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).containsExactlyInAnyOrder(
                    uuid2
                )

                FriendCacheService.Type.values().forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }

        @Test
        fun `will not change relation for other`() = runTest {
            cacheClient.connect {
                FriendCacheService.Type.values().forEach { type ->
                    add(it, uuid2, uuid1, type)
                }
            }

            assertTrue { cacheService.removePendingFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).containsExactlyInAnyOrder(
                    uuid2
                )

                FriendCacheService.Type.values().forEach { type ->
                    assertThat(getAll(it, uuid2, type)).containsExactlyInAnyOrder(uuid1)
                }
            }
        }

        @Test
        fun `will not add if already present but will remove in delete relation`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_PENDING_FRIEND)
                add(it, uuid1, uuid2, FriendCacheService.Type.REMOVE_PENDING_FRIEND)
            }

            assertTrue { cacheService.removePendingFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).containsExactlyInAnyOrder(
                    uuid2
                )

                FriendCacheService.Type.values().forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }

        @Test
        fun `should returns false if relation is not added or removed`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.REMOVE_PENDING_FRIEND)
            }

            assertFalse { cacheService.removePendingFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_PENDING_FRIEND)).containsExactlyInAnyOrder(
                    uuid2
                )

                FriendCacheService.Type.values().forEach { type ->
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

            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.FRIENDS)
                add(it, uuid1, uuid3, FriendCacheService.Type.FRIENDS)
            }

            assertThat(cacheService.getFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2, uuid3)

            val uuid4 = UUID.randomUUID()
            cacheClient.connect {
                add(it, uuid1, uuid4, FriendCacheService.Type.FRIENDS)
            }

            assertThat(cacheService.getFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2, uuid3, uuid4)
        }

        @Test
        fun `should returns elements from relation and added relation`() = runTest {
            val uuid3 = UUID.randomUUID()

            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.FRIENDS)
                add(it, uuid1, uuid3, FriendCacheService.Type.ADD_FRIEND)
            }

            assertThat(cacheService.getFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2, uuid3)
        }

        @Test
        fun `should returns elements from empty relation and not empty added relation`() = runTest {
            val uuid3 = UUID.randomUUID()

            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_FRIEND)
                add(it, uuid1, uuid3, FriendCacheService.Type.ADD_FRIEND)
            }

            assertThat(cacheService.getFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2, uuid3)
        }

        @Test
        fun `should returns elements from relation and empty added relation`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.FRIENDS)
            }

            assertThat(cacheService.getFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2)
        }

        @Test
        fun `should not returns duplicate from relations`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.FRIENDS)
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_FRIEND)
            }

            assertThat(cacheService.getFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2)
        }

        @Test
        fun `should remove elements present in remove relation`() = runTest {
            val uuid3 = UUID.randomUUID()
            val uuid4 = UUID.randomUUID()

            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.FRIENDS)
                add(it, uuid1, uuid3, FriendCacheService.Type.FRIENDS)
                add(it, uuid1, uuid4, FriendCacheService.Type.ADD_FRIEND)
                add(it, uuid1, uuid3, FriendCacheService.Type.REMOVE_FRIEND)
            }

            assertThat(cacheService.getFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2, uuid4)

            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.REMOVE_FRIEND)
            }

            assertThat(cacheService.getFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid4)

            cacheClient.connect {
                add(it, uuid1, uuid4, FriendCacheService.Type.REMOVE_FRIEND)
            }

            assertThat(cacheService.getFriends(uuid1).toList()).isEmpty()
        }

        @Test
        fun `should not use pending relation`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.FRIENDS)
                add(it, uuid1, uuid2, FriendCacheService.Type.PENDING_FRIENDS)
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_PENDING_FRIEND)
                add(it, uuid1, uuid2, FriendCacheService.Type.REMOVE_PENDING_FRIEND)
                add(it, uuid1, UUID.randomUUID(), FriendCacheService.Type.PENDING_FRIENDS)
                add(it, uuid1, UUID.randomUUID(), FriendCacheService.Type.ADD_PENDING_FRIEND)
                add(it, uuid1, UUID.randomUUID(), FriendCacheService.Type.REMOVE_PENDING_FRIEND)
            }

            assertThat(cacheService.getFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2)
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
        fun `should returns elements from relation`() = runTest {
            val uuid3 = UUID.randomUUID()

            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.PENDING_FRIENDS)
                add(it, uuid1, uuid3, FriendCacheService.Type.PENDING_FRIENDS)
            }

            assertThat(cacheService.getPendingFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2, uuid3)

            val uuid4 = UUID.randomUUID()
            cacheClient.connect {
                add(it, uuid1, uuid4, FriendCacheService.Type.PENDING_FRIENDS)
            }

            assertThat(cacheService.getPendingFriends(uuid1).toList()).containsExactlyInAnyOrder(
                uuid2,
                uuid3,
                uuid4
            )
        }

        @Test
        fun `should returns elements from relation and added relation`() = runTest {
            val uuid3 = UUID.randomUUID()

            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.PENDING_FRIENDS)
                add(it, uuid1, uuid3, FriendCacheService.Type.ADD_PENDING_FRIEND)
            }

            assertThat(cacheService.getPendingFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2, uuid3)
        }

        @Test
        fun `should returns elements from empty relation and not empty added relation`() = runTest {
            val uuid3 = UUID.randomUUID()

            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_PENDING_FRIEND)
                add(it, uuid1, uuid3, FriendCacheService.Type.ADD_PENDING_FRIEND)
            }

            assertThat(cacheService.getPendingFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2, uuid3)
        }

        @Test
        fun `should returns elements from relation and empty added relation`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.PENDING_FRIENDS)
            }

            assertThat(cacheService.getPendingFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2)
        }

        @Test
        fun `should not returns duplicate from relations`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.PENDING_FRIENDS)
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_PENDING_FRIEND)
            }

            assertThat(cacheService.getPendingFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2)
        }

        @Test
        fun `should remove elements present in remove relation`() = runTest {
            val uuid3 = UUID.randomUUID()
            val uuid4 = UUID.randomUUID()

            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.PENDING_FRIENDS)
                add(it, uuid1, uuid3, FriendCacheService.Type.PENDING_FRIENDS)
                add(it, uuid1, uuid4, FriendCacheService.Type.ADD_PENDING_FRIEND)
                add(it, uuid1, uuid3, FriendCacheService.Type.REMOVE_PENDING_FRIEND)
            }

            assertThat(cacheService.getPendingFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2, uuid4)

            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.REMOVE_PENDING_FRIEND)
            }

            assertThat(cacheService.getPendingFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid4)

            cacheClient.connect {
                add(it, uuid1, uuid4, FriendCacheService.Type.REMOVE_PENDING_FRIEND)
            }

            assertThat(cacheService.getPendingFriends(uuid1).toList()).isEmpty()
        }

        @Test
        fun `should not use friend relation`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.PENDING_FRIENDS)
                add(it, uuid1, uuid2, FriendCacheService.Type.FRIENDS)
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_FRIEND)
                add(it, uuid1, uuid2, FriendCacheService.Type.REMOVE_FRIEND)
                add(it, uuid1, UUID.randomUUID(), FriendCacheService.Type.FRIENDS)
                add(it, uuid1, UUID.randomUUID(), FriendCacheService.Type.ADD_FRIEND)
                add(it, uuid1, UUID.randomUUID(), FriendCacheService.Type.REMOVE_FRIEND)
            }

            assertThat(cacheService.getPendingFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2)
        }
    }

    @Nested
    inner class SetFriends {

        private lateinit var uuid1: UUID
        private lateinit var uuid2: UUID

        @BeforeTest
        fun onBefore() {
            uuid1 = UUID.randomUUID()
            uuid2 = UUID.randomUUID()
        }

        @Test
        fun `should define the key`() = runTest {
            assertTrue { cacheService.setFriends(uuid1, setOf(uuid2)) }

            cacheClient.connect {
                val expectedKey = cacheClient.binaryFormat.encodeToByteArray(String.serializer(), "user:${uuid1}:friends")
                assertThat(it.keys(expectedKey).toList()).hasSize(1)
            }
        }

        @Test
        fun `should define elements to relation`() = runTest {
            val uuid3 = UUID.randomUUID()
            val uuid4 = UUID.randomUUID()

            cacheService.setFriends(uuid1, setOf(uuid2, uuid3, uuid4))

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).containsExactlyInAnyOrder(
                    uuid2,
                    uuid3,
                    uuid4
                )
            }
        }

        @Test
        fun `should overwrite elements to relation`() = runTest {
            val uuid3 = UUID.randomUUID()
            val uuid4 = UUID.randomUUID()

            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.FRIENDS)
            }

            cacheService.setFriends(uuid1, setOf(uuid3, uuid4))

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).containsExactlyInAnyOrder(uuid3, uuid4)
            }
        }

        @Test
        fun `should not use pending request relation`() = runTest {
            val uuid3 = UUID.randomUUID()
            val uuid4 = UUID.randomUUID()

            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.PENDING_FRIENDS)
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_PENDING_FRIEND)
                add(it, uuid1, uuid2, FriendCacheService.Type.REMOVE_PENDING_FRIEND)
                add(it, uuid1, UUID.randomUUID(), FriendCacheService.Type.PENDING_FRIENDS)
                add(it, uuid1, UUID.randomUUID(), FriendCacheService.Type.ADD_PENDING_FRIEND)
                add(it, uuid1, UUID.randomUUID(), FriendCacheService.Type.REMOVE_PENDING_FRIEND)
            }

            cacheService.setFriends(uuid1, setOf(uuid3, uuid4))

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).containsExactlyInAnyOrder(uuid3, uuid4)
            }
        }

        @Test
        fun `should not use friend relation`() = runTest {
            val uuid3 = UUID.randomUUID()
            val uuid4 = UUID.randomUUID()

            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.FRIENDS)

                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_FRIEND)
                add(it, uuid1, uuid2, FriendCacheService.Type.REMOVE_FRIEND)
                add(it, uuid1, UUID.randomUUID(), FriendCacheService.Type.ADD_FRIEND)
                add(it, uuid1, UUID.randomUUID(), FriendCacheService.Type.REMOVE_FRIEND)
            }

            cacheService.setFriends(uuid1, setOf(uuid3, uuid4))

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).containsExactlyInAnyOrder(uuid3, uuid4)
            }
        }
    }

    @Nested
    inner class SetPendingFriends {

        private lateinit var uuid1: UUID
        private lateinit var uuid2: UUID

        @BeforeTest
        fun onBefore() {
            uuid1 = UUID.randomUUID()
            uuid2 = UUID.randomUUID()
        }

        @Test
        fun `should define the key`() = runTest {
            assertTrue { cacheService.setPendingFriends(uuid1, setOf(uuid2)) }

            cacheClient.connect {
                val expectedKey = cacheClient.binaryFormat.encodeToByteArray(String.serializer(), "user:${uuid1}:friends:pending")
                assertThat(it.keys(expectedKey).toList()).hasSize(1)
            }
        }

        @Test
        fun `should define elements to relation`() = runTest {
            val uuid3 = UUID.randomUUID()
            val uuid4 = UUID.randomUUID()

            cacheService.setPendingFriends(uuid1, setOf(uuid2, uuid3, uuid4))

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_FRIENDS)).containsExactlyInAnyOrder(
                    uuid2,
                    uuid3,
                    uuid4
                )
            }
        }

        @Test
        fun `should overwrite elements to relation`() = runTest {
            val uuid3 = UUID.randomUUID()
            val uuid4 = UUID.randomUUID()

            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.PENDING_FRIENDS)
            }

            cacheService.setPendingFriends(uuid1, setOf(uuid3, uuid4))

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_FRIENDS)).containsExactlyInAnyOrder(
                    uuid3,
                    uuid4
                )
            }
        }

        @Test
        fun `should not use friend relation`() = runTest {
            val uuid3 = UUID.randomUUID()
            val uuid4 = UUID.randomUUID()

            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.FRIENDS)
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_FRIEND)
                add(it, uuid1, uuid2, FriendCacheService.Type.REMOVE_FRIEND)
                add(it, uuid1, UUID.randomUUID(), FriendCacheService.Type.FRIENDS)
                add(it, uuid1, UUID.randomUUID(), FriendCacheService.Type.ADD_FRIEND)
                add(it, uuid1, UUID.randomUUID(), FriendCacheService.Type.REMOVE_FRIEND)
            }

            cacheService.setPendingFriends(uuid1, setOf(uuid3, uuid4))

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_FRIENDS)).containsExactlyInAnyOrder(
                    uuid3,
                    uuid4
                )
            }
        }

        @Test
        fun `should not use pending request relation`() = runTest {
            val uuid3 = UUID.randomUUID()
            val uuid4 = UUID.randomUUID()

            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.PENDING_FRIENDS)

                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_PENDING_FRIEND)
                add(it, uuid1, uuid2, FriendCacheService.Type.REMOVE_PENDING_FRIEND)
                add(it, uuid1, UUID.randomUUID(), FriendCacheService.Type.ADD_PENDING_FRIEND)
                add(it, uuid1, UUID.randomUUID(), FriendCacheService.Type.REMOVE_PENDING_FRIEND)
            }

            cacheService.setPendingFriends(uuid1, setOf(uuid3, uuid4))

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_FRIENDS)).containsExactlyInAnyOrder(
                    uuid3,
                    uuid4
                )
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
        fun `should returns true if the relation exists`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.FRIENDS)
            }

            assertTrue { cacheService.isFriend(uuid1, uuid2) }
        }

        @Test
        fun `should returns true if the relation exists in added`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_FRIEND)
            }

            assertTrue { cacheService.isFriend(uuid1, uuid2) }
        }

        @Test
        fun `should returns false if the relation exists in remove or pending relation`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.REMOVE_FRIEND)
                add(it, uuid1, uuid2, FriendCacheService.Type.PENDING_FRIENDS)
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_PENDING_FRIEND)
                add(it, uuid1, uuid2, FriendCacheService.Type.REMOVE_PENDING_FRIEND)
            }

            assertFalse { cacheService.isFriend(uuid1, uuid2) }
        }

        @Test
        fun `should returns true if the relation exists in list and add`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.FRIENDS)
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_FRIEND)
            }

            assertTrue { cacheService.isFriend(uuid1, uuid2) }
        }

        @Test
        fun `should returns false if the relation exists in list and remove`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.FRIENDS)
                add(it, uuid1, uuid2, FriendCacheService.Type.REMOVE_FRIEND)
            }

            assertFalse { cacheService.isFriend(uuid1, uuid2) }
        }

        @Test
        fun `should returns false if the relation exists in add and remove`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_FRIEND)
                add(it, uuid1, uuid2, FriendCacheService.Type.REMOVE_FRIEND)
            }

            assertFalse { cacheService.isFriend(uuid1, uuid2) }
        }

        @Test
        fun `should returns true if the relation exists between other relationship in list`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.FRIENDS)
                add(it, uuid1, UUID.randomUUID(), FriendCacheService.Type.FRIENDS)
                add(it, uuid1, UUID.randomUUID(), FriendCacheService.Type.FRIENDS)
            }

            assertTrue { cacheService.isFriend(uuid1, uuid2) }
        }

        @Test
        fun `should returns true if the relation exists between other relationship in add`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_FRIEND)
                add(it, uuid1, UUID.randomUUID(), FriendCacheService.Type.ADD_FRIEND)
                add(it, uuid1, UUID.randomUUID(), FriendCacheService.Type.ADD_FRIEND)
            }

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
        fun `should returns true if the relation exists`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.PENDING_FRIENDS)
            }

            assertTrue { cacheService.isPendingFriend(uuid1, uuid2) }
        }

        @Test
        fun `should returns true if the relation exists in added`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_PENDING_FRIEND)
            }

            assertTrue { cacheService.isPendingFriend(uuid1, uuid2) }
        }

        @Test
        fun `should returns false if the relation exists in remove or pending relation`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.REMOVE_PENDING_FRIEND)
                add(it, uuid1, uuid2, FriendCacheService.Type.FRIENDS)
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_FRIEND)
                add(it, uuid1, uuid2, FriendCacheService.Type.REMOVE_FRIEND)
            }

            assertFalse { cacheService.isPendingFriend(uuid1, uuid2) }
        }

        @Test
        fun `should returns true if the relation exists in list and add`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.PENDING_FRIENDS)
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_PENDING_FRIEND)
            }

            assertTrue { cacheService.isPendingFriend(uuid1, uuid2) }
        }

        @Test
        fun `should returns false if the relation exists in list and remove`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.PENDING_FRIENDS)
                add(it, uuid1, uuid2, FriendCacheService.Type.REMOVE_PENDING_FRIEND)
            }

            assertFalse { cacheService.isPendingFriend(uuid1, uuid2) }
        }

        @Test
        fun `should returns false if the relation exists in add and remove`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_PENDING_FRIEND)
                add(it, uuid1, uuid2, FriendCacheService.Type.REMOVE_PENDING_FRIEND)
            }

            assertFalse { cacheService.isPendingFriend(uuid1, uuid2) }
        }

        @Test
        fun `should returns true if the relation exists between other relationship in list`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.PENDING_FRIENDS)
                add(it, uuid1, UUID.randomUUID(), FriendCacheService.Type.PENDING_FRIENDS)
                add(it, uuid1, UUID.randomUUID(), FriendCacheService.Type.PENDING_FRIENDS)
            }

            assertTrue { cacheService.isPendingFriend(uuid1, uuid2) }
        }

        @Test
        fun `should returns true if the relation exists between other relationship in add`() = runTest {
            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_PENDING_FRIEND)
                add(it, uuid1, UUID.randomUUID(), FriendCacheService.Type.ADD_PENDING_FRIEND)
                add(it, uuid1, UUID.randomUUID(), FriendCacheService.Type.ADD_PENDING_FRIEND)
            }

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
        val result = connection.sadd(key, friends)
        assertTrue { result != null && result > 0 }
    }

    private fun encodeKeyWithType(
        uuid: UUID,
        type: FriendCacheService.Type
    ) = cacheClient.binaryFormat.encodeToByteArray(
        String.serializer(),
        cacheService.prefixKey.format(uuid) + type.key
    )
}