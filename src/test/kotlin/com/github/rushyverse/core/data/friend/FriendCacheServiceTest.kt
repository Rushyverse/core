package com.github.rushyverse.core.data.friend

import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.cache.CacheClient.Default.binaryFormat
import com.github.rushyverse.core.container.createRedisContainer
import com.github.rushyverse.core.data.FriendCacheService
import com.github.rushyverse.core.serializer.UUIDSerializer
import io.lettuce.core.RedisURI
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.builtins.serializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.util.*
import kotlin.test.*
import kotlin.time.Duration.Companion.seconds

@Testcontainers
class FriendCacheServiceTest {

    companion object {
        @JvmStatic
        @Container
        private val redisContainer = createRedisContainer()
    }

    private lateinit var cacheClient: CacheClient

    @BeforeTest
    fun onBefore() = runBlocking {
        cacheClient = CacheClient {
            uri = RedisURI.create(redisContainer.url)
        }
    }

    @AfterTest
    fun onAfter() {
        cacheClient.close()
    }

    @Nested
    inner class AddFriend {

        @Test
        fun `should return false if A is already friend with B`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            var service = FriendCacheService(cacheClient, duplicateForFriend = true)

            assertTrue { service.addFriend(uuid1, uuid2) }

            assertFalse { service.addFriend(uuid1, uuid2) }
            assertFalse { service.addFriend(uuid2, uuid1) }

            service = FriendCacheService(cacheClient, duplicateForFriend = false)

            assertFalse { service.addFriend(uuid1, uuid2) }
            assertFalse { service.addFriend(uuid2, uuid1) }

            assertEquals(listOf(uuid2), getFriends(service, uuid1).toList())
            assertEquals(listOf(uuid1), getFriends(service, uuid2).toList())
        }

        @Test
        fun `should return true if A is not friend with B with duplicate`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            val service = FriendCacheService(cacheClient, duplicateForFriend = true)

            assertTrue { service.addFriend(uuid1, uuid2) }

            assertEquals(listOf(uuid2), getFriends(service, uuid1).toList())
            assertEquals(listOf(uuid1), getFriends(service, uuid2).toList())
        }

        @Test
        fun `should return true if A is already friend with B without duplicate`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            val service = FriendCacheService(cacheClient, duplicateForFriend = false)

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertEquals(listOf(uuid2), getFriends(service, uuid1).toList())
            assertEquals(emptyList(), getFriends(service, uuid2).toList())

            assertFalse { service.addFriend(uuid1, uuid2) }
            assertEquals(listOf(uuid2), getFriends(service, uuid1).toList())
            assertEquals(emptyList(), getFriends(service, uuid2).toList())

            assertTrue { service.addFriend(uuid2, uuid1) }
            assertEquals(listOf(uuid2), getFriends(service, uuid1).toList())
            assertEquals(listOf(uuid1), getFriends(service, uuid2).toList())
        }

        @Test
        fun `should add friend with expiration without duplicate`() = runBlocking {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            val service = FriendCacheService(cacheClient, expiration = 1.seconds, duplicateForFriend = false)

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertEquals(1L, getTTL(service, uuid1))

            delay(0.5.seconds)

            assertEquals(listOf(uuid2), getFriends(service, uuid1).toList())
            assertEquals(emptyList(), getFriends(service, uuid2).toList())

            delay(0.5.seconds)

            assertEquals(emptyList(), getFriends(service, uuid1).toList())
            assertEquals(emptyList(), getFriends(service, uuid2).toList())
        }

        @Test
        fun `should add friend with expiration with duplicate`() = runBlocking {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            val service = FriendCacheService(cacheClient, expiration = 1.seconds, duplicateForFriend = true)

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertEquals(1L, getTTL(service, uuid1))
            assertEquals(1L, getTTL(service, uuid2))

            delay(0.5.seconds)

            assertEquals(listOf(uuid2), getFriends(service, uuid1).toList())
            assertEquals(listOf(uuid1), getFriends(service, uuid2).toList())

            delay(0.5.seconds)

            assertEquals(emptyList(), getFriends(service, uuid1).toList())
            assertEquals(emptyList(), getFriends(service, uuid2).toList())
        }

        @Test
        fun `should not set expiration if relation not added`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            val service = FriendCacheService(cacheClient, duplicateForFriend = true)

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertEquals(-1, getTTL(service, uuid1))
            assertEquals(-1, getTTL(service, uuid2))

            val serviceWithExpiration = FriendCacheService(cacheClient, expiration = 40.seconds, duplicateForFriend = true)
            assertFalse { serviceWithExpiration.addFriend(uuid1, uuid2) }
            assertEquals(-1, getTTL(service, uuid1))
            assertEquals(-1, getTTL(service, uuid2))
        }

        @Test
        fun `should reset expiration when a new friend is added`() = runBlocking {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            val uuid3 = UUID.randomUUID()

            val service = FriendCacheService(cacheClient, expiration = 5.seconds, duplicateForFriend = false)

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertEquals(5, getTTL(service, uuid1))

            delay(1.seconds)

            assertEquals(4, getTTL(service, uuid1))

            assertTrue { service.addFriend(uuid1, uuid3) }
            assertEquals(5, getTTL(service, uuid1))
        }

        @Test
        fun `should add several friends without duplicate`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            val uuid3 = UUID.randomUUID()

            val service = FriendCacheService(cacheClient, duplicateForFriend = false)

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertEquals(listOf(uuid2), getFriends(service, uuid1).toList())

            assertTrue { service.addFriend(uuid1, uuid3) }
            assertThat(getFriends(service, uuid1).toList()).containsExactlyInAnyOrder(uuid3, uuid2)
        }

        @Test
        fun `should add several friends with duplicate`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            val uuid3 = UUID.randomUUID()

            val service = FriendCacheService(cacheClient, duplicateForFriend = true)

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertEquals(listOf(uuid2), getFriends(service, uuid1).toList())
            assertEquals(listOf(uuid1), getFriends(service, uuid2).toList())
            assertEquals(emptyList(), getFriends(service, uuid3).toList())

            assertTrue { service.addFriend(uuid1, uuid3) }
            assertThat(getFriends(service, uuid1).toList()).containsExactlyInAnyOrder(uuid3, uuid2)
            assertEquals(listOf(uuid1), getFriends(service, uuid2).toList())
            assertEquals(listOf(uuid1), getFriends(service, uuid3).toList())
        }

    }

    private suspend fun getFriends(
        service: FriendCacheService,
        uuid: UUID
    ) = cacheClient.connect {
        val keySerial = createKey(service, uuid)
        it.smembers(keySerial).mapNotNull { member ->
            binaryFormat.decodeFromByteArray(UUIDSerializer, member)
        }
    }

    private suspend fun getTTL(service: FriendCacheService, uuid: UUID): Long? {
        return cacheClient.connect {
            it.ttl(createKey(service, uuid))
        }
    }

    private fun createKey(
        service: FriendCacheService,
        uuid1: UUID
    ): ByteArray {
        val keySerial =
            cacheClient.binaryFormat.encodeToByteArray(String.serializer(), service.prefixKey + uuid1.toString())
        return keySerial
    }
}