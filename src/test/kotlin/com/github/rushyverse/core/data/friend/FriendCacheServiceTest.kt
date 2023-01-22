package com.github.rushyverse.core.data.friend

import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.container.createRedisContainer
import com.github.rushyverse.core.data.FriendCacheService
import com.github.rushyverse.core.data.UserCacheManager
import com.github.rushyverse.core.serializer.UUIDSerializer
import io.lettuce.core.RedisURI
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import kotlinx.coroutines.flow.map
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
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertTrue

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

        @Test
        fun `add relationship if relation exists in friends`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            val cacheService = FriendCacheService(cacheClient, userCacheService)

            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.FRIENDS)
            }

            assertTrue { cacheService.addFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_REQUESTS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_REQUEST)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()

                FriendCacheService.Type.values().forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }

        @Test
        fun `add relationship if relation doesn't exist in friends`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            val cacheService = FriendCacheService(cacheClient, userCacheService)

            assertTrue { cacheService.addFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_REQUESTS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_REQUEST)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()

                FriendCacheService.Type.values().forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }

        @Test
        fun `add relationship will delete relation in remove relationship`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            val cacheService = FriendCacheService(cacheClient, userCacheService)

            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.REMOVE_FRIEND)
            }

            assertTrue { cacheService.addFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_REQUESTS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_REQUEST)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()

                FriendCacheService.Type.values().forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }

        @Test
        fun `add relationship will not change relation in pending requests`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            val cacheService = FriendCacheService(cacheClient, userCacheService)

            cacheClient.connect {
                add(it, uuid1, uuid2, FriendCacheService.Type.PENDING_REQUESTS)
            }

            assertTrue { cacheService.addFriend(uuid1, uuid2) }

            cacheClient.connect {
                assertThat(getAll(it, uuid1, FriendCacheService.Type.FRIENDS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_FRIEND)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_REQUESTS)).containsExactlyInAnyOrder(uuid2)
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_REQUEST)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()

                FriendCacheService.Type.values().forEach { type ->
                    assertThat(getAll(it, uuid2, type)).isEmpty()
                }
            }
        }

        @Test
        fun `add relation will not change relation from friend`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

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
                assertThat(getAll(it, uuid1, FriendCacheService.Type.PENDING_REQUESTS)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.ADD_PENDING_REQUEST)).isEmpty()
                assertThat(getAll(it, uuid1, FriendCacheService.Type.REMOVE_FRIEND)).isEmpty()

                FriendCacheService.Type.values().forEach { type ->
                    assertThat(getAll(it, uuid2, type)).containsExactlyInAnyOrder(uuid1)
                }
            }
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
    ): Boolean {
        val key = encodeKeyWithType(uuid, type)
        val friends = cacheClient.binaryFormat.encodeToByteArray(UUIDSerializer, friend)
        val result = connection.sadd(key, friends)
        return result != null && result > 0
    }

    private fun encodeKeyWithType(
        uuid: UUID,
        type: FriendCacheService.Type
    ) = cacheClient.binaryFormat.encodeToByteArray(
        String.serializer(),
        userCacheService.getFormattedKey(uuid) + ":" + type.key
    )
}