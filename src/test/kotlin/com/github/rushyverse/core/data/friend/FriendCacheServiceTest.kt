package com.github.rushyverse.core.data.friend

import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.container.createRedisContainer
import com.github.rushyverse.core.data.FriendCacheService
import com.github.rushyverse.core.data.UserCacheManager
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
    private lateinit var userCacheService: UserCacheManager

    @BeforeTest
    fun onBefore() = runBlocking {
        cacheClient = CacheClient {
            uri = RedisURI.create(redisContainer.url)
        }
        userCacheService = UserCacheManager()
    }

    @AfterTest
    fun onAfter(): Unit = runBlocking {
        cacheClient.closeAsync().await()
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
        fun `should add several relations`() = runTest {
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
        fun `should add several relations`() = runTest {
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
        fun `should add several relations`() = runTest {
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
        fun `should add several relations`() = runTest {
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)
            assertThat(cacheService.getFriends(uuid1).toList()).isEmpty()
        }

        @Test
        fun `should returns elements from relation`() = runTest {
            val uuid3 = UUID.randomUUID()
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.FRIENDS)
                add(it, uuid1, uuid3, FriendCacheService.Type.ADD_FRIEND)
            }

            assertThat(cacheService.getFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2, uuid3)
        }

        @Test
        fun `should returns elements from empty relation and not empty added relation`() = runTest {
            val uuid3 = UUID.randomUUID()
            val cacheService = FriendCacheService(cacheClient, userCacheService)

            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_FRIEND)
                add(it, uuid1, uuid3, FriendCacheService.Type.ADD_FRIEND)
            }

            assertThat(cacheService.getFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2, uuid3)
        }

        @Test
        fun `should returns elements from relation and empty added relation`() = runTest {
            val cacheService = FriendCacheService(cacheClient, userCacheService)

            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.FRIENDS)
            }

            assertThat(cacheService.getFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2)
        }

        @Test
        fun `should not returns duplicate from relations`() = runTest {
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)
            assertThat(cacheService.getPendingFriends(uuid1).toList()).isEmpty()
        }

        @Test
        fun `should returns elements from relation`() = runTest {
            val uuid3 = UUID.randomUUID()
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.PENDING_FRIENDS)
                add(it, uuid1, uuid3, FriendCacheService.Type.ADD_PENDING_FRIEND)
            }

            assertThat(cacheService.getPendingFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2, uuid3)
        }

        @Test
        fun `should returns elements from empty relation and not empty added relation`() = runTest {
            val uuid3 = UUID.randomUUID()
            val cacheService = FriendCacheService(cacheClient, userCacheService)

            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_PENDING_FRIEND)
                add(it, uuid1, uuid3, FriendCacheService.Type.ADD_PENDING_FRIEND)
            }

            assertThat(cacheService.getPendingFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2, uuid3)
        }

        @Test
        fun `should returns elements from relation and empty added relation`() = runTest {
            val cacheService = FriendCacheService(cacheClient, userCacheService)

            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.PENDING_FRIENDS)
            }

            assertThat(cacheService.getPendingFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2)
        }

        @Test
        fun `should not returns duplicate from relations`() = runTest {
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
        fun `should define elements to relation`() = runTest {
            val uuid3 = UUID.randomUUID()
            val uuid4 = UUID.randomUUID()
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
        fun `should define elements to relation`() = runTest {
            val uuid3 = UUID.randomUUID()
            val uuid4 = UUID.randomUUID()
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)
            assertFalse { cacheService.isFriend(uuid1, uuid2) }
        }

        @Test
        fun `should returns true if the relation exists`() = runTest {
            val cacheService = FriendCacheService(cacheClient, userCacheService)

            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.FRIENDS)
            }

            assertTrue { cacheService.isFriend(uuid1, uuid2) }
        }

        @Test
        fun `should returns true if the relation exists in added`() = runTest {
            val cacheService = FriendCacheService(cacheClient, userCacheService)

            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_FRIEND)
            }

            assertTrue { cacheService.isFriend(uuid1, uuid2) }
        }

        @Test
        fun `should returns false if the relation exists in remove or pending relation`() = runTest {
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.FRIENDS)
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_FRIEND)
            }

            assertTrue { cacheService.isFriend(uuid1, uuid2) }
        }

        @Test
        fun `should returns false if the relation exists in list and remove`() = runTest {
            val cacheService = FriendCacheService(cacheClient, userCacheService)

            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.FRIENDS)
                add(it, uuid1, uuid2, FriendCacheService.Type.REMOVE_FRIEND)
            }

            assertFalse { cacheService.isFriend(uuid1, uuid2) }
        }

        @Test
        fun `should returns false if the relation exists in add and remove`() = runTest {
            val cacheService = FriendCacheService(cacheClient, userCacheService)

            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_FRIEND)
                add(it, uuid1, uuid2, FriendCacheService.Type.REMOVE_FRIEND)
            }

            assertFalse { cacheService.isFriend(uuid1, uuid2) }
        }

        @Test
        fun `should returns true if the relation exists between other relationship in list`() = runTest {
            val cacheService = FriendCacheService(cacheClient, userCacheService)

            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.FRIENDS)
                add(it, uuid1, UUID.randomUUID(), FriendCacheService.Type.FRIENDS)
                add(it, uuid1, UUID.randomUUID(), FriendCacheService.Type.FRIENDS)
            }

            assertTrue { cacheService.isFriend(uuid1, uuid2) }
        }

        @Test
        fun `should returns true if the relation exists between other relationship in add`() = runTest {
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)
            assertFalse { cacheService.isPendingFriend(uuid1, uuid2) }
        }

        @Test
        fun `should returns true if the relation exists`() = runTest {
            val cacheService = FriendCacheService(cacheClient, userCacheService)

            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.PENDING_FRIENDS)
            }

            assertTrue { cacheService.isPendingFriend(uuid1, uuid2) }
        }

        @Test
        fun `should returns true if the relation exists in added`() = runTest {
            val cacheService = FriendCacheService(cacheClient, userCacheService)

            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_PENDING_FRIEND)
            }

            assertTrue { cacheService.isPendingFriend(uuid1, uuid2) }
        }

        @Test
        fun `should returns false if the relation exists in remove or pending relation`() = runTest {
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
            val cacheService = FriendCacheService(cacheClient, userCacheService)

            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.PENDING_FRIENDS)
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_PENDING_FRIEND)
            }

            assertTrue { cacheService.isPendingFriend(uuid1, uuid2) }
        }

        @Test
        fun `should returns false if the relation exists in list and remove`() = runTest {
            val cacheService = FriendCacheService(cacheClient, userCacheService)

            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.PENDING_FRIENDS)
                add(it, uuid1, uuid2, FriendCacheService.Type.REMOVE_PENDING_FRIEND)
            }

            assertFalse { cacheService.isPendingFriend(uuid1, uuid2) }
        }

        @Test
        fun `should returns false if the relation exists in add and remove`() = runTest {
            val cacheService = FriendCacheService(cacheClient, userCacheService)

            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.ADD_PENDING_FRIEND)
                add(it, uuid1, uuid2, FriendCacheService.Type.REMOVE_PENDING_FRIEND)
            }

            assertFalse { cacheService.isPendingFriend(uuid1, uuid2) }
        }

        @Test
        fun `should returns true if the relation exists between other relationship in list`() = runTest {
            val cacheService = FriendCacheService(cacheClient, userCacheService)

            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.PENDING_FRIENDS)
                add(it, uuid1, UUID.randomUUID(), FriendCacheService.Type.PENDING_FRIENDS)
                add(it, uuid1, UUID.randomUUID(), FriendCacheService.Type.PENDING_FRIENDS)
            }

            assertTrue { cacheService.isPendingFriend(uuid1, uuid2) }
        }

        @Test
        fun `should returns true if the relation exists between other relationship in add`() = runTest {
            val cacheService = FriendCacheService(cacheClient, userCacheService)

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
        userCacheService.getFormattedKey(uuid) + type.key
    )
}