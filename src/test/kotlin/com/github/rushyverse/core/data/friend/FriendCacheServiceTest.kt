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
    inner class DefaultParameter {

        @Test
        fun `default prefix key`() {
            val service = FriendCacheService(cacheClient)
            assertEquals("friend:", service.prefixKey)
        }

        @Test
        fun `default duplicate`() {
            val service = FriendCacheService(cacheClient)
            assertFalse { service.duplicateForFriend }
        }

        @Test
        fun `default expiration`() {
            val service = FriendCacheService(cacheClient)
            assertNull(service.expiration)
        }

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

            assertEquals(listOf(uuid2), getFriends(service, uuid1))
            assertEquals(listOf(uuid1), getFriends(service, uuid2))
        }

        @Test
        fun `should return true if A is not friend with B with duplicate`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            val service = FriendCacheService(cacheClient, duplicateForFriend = true)

            assertTrue { service.addFriend(uuid1, uuid2) }

            assertEquals(listOf(uuid2), getFriends(service, uuid1))
            assertEquals(listOf(uuid1), getFriends(service, uuid2))
        }

        @Test
        fun `should return true if A is already friend with B without duplicate`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            val service = FriendCacheService(cacheClient, duplicateForFriend = false)

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertEquals(listOf(uuid2), getFriends(service, uuid1))
            assertEquals(emptyList(), getFriends(service, uuid2))

            assertFalse { service.addFriend(uuid1, uuid2) }
            assertEquals(listOf(uuid2), getFriends(service, uuid1))
            assertEquals(emptyList(), getFriends(service, uuid2))

            assertTrue { service.addFriend(uuid2, uuid1) }
            assertEquals(listOf(uuid2), getFriends(service, uuid1))
            assertEquals(listOf(uuid1), getFriends(service, uuid2))
        }

        @Test
        fun `should return true when A becomes friend with B but B is already friend with A with duplicate`() =
            runTest {
                val uuid1 = UUID.randomUUID()
                val uuid2 = UUID.randomUUID()

                val service = FriendCacheService(cacheClient, duplicateForFriend = false)

                assertTrue { service.addFriend(uuid1, uuid2) }
                assertEquals(listOf(uuid2), getFriends(service, uuid1))

                val serviceWithDuplicate = FriendCacheService(cacheClient, duplicateForFriend = true)

                assertTrue { serviceWithDuplicate.addFriend(uuid2, uuid1) }
                assertEquals(listOf(uuid2), getFriends(service, uuid1))
                assertEquals(listOf(uuid1), getFriends(service, uuid2))
            }

        @Test
        fun `should add friend with expiration without duplicate`() = runBlocking {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            val service = FriendCacheService(cacheClient, expiration = 1.seconds, duplicateForFriend = false)

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertEquals(1L, getTTL(service, uuid1))

            delay(0.5.seconds)

            assertEquals(listOf(uuid2), getFriends(service, uuid1))
            assertEquals(emptyList(), getFriends(service, uuid2))

            delay(0.5.seconds)

            assertEquals(emptyList(), getFriends(service, uuid1))
            assertEquals(emptyList(), getFriends(service, uuid2))
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

            assertEquals(listOf(uuid2), getFriends(service, uuid1))
            assertEquals(listOf(uuid1), getFriends(service, uuid2))

            delay(0.5.seconds)

            assertEquals(emptyList(), getFriends(service, uuid1))
            assertEquals(emptyList(), getFriends(service, uuid2))
        }

        @Test
        fun `should not set expiration if relation not added`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            val service = FriendCacheService(cacheClient, duplicateForFriend = true)

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertEquals(-1, getTTL(service, uuid1))
            assertEquals(-1, getTTL(service, uuid2))

            val serviceWithExpiration =
                FriendCacheService(cacheClient, expiration = 40.seconds, duplicateForFriend = true)
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
            assertEquals(listOf(uuid2), getFriends(service, uuid1))

            assertTrue { service.addFriend(uuid1, uuid3) }
            assertThat(getFriends(service, uuid1)).containsExactlyInAnyOrder(uuid3, uuid2)
        }

        @Test
        fun `should add several friends with duplicate`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            val uuid3 = UUID.randomUUID()

            val service = FriendCacheService(cacheClient, duplicateForFriend = true)

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertEquals(listOf(uuid2), getFriends(service, uuid1))
            assertEquals(listOf(uuid1), getFriends(service, uuid2))
            assertEquals(emptyList(), getFriends(service, uuid3))

            assertTrue { service.addFriend(uuid1, uuid3) }
            assertThat(getFriends(service, uuid1)).containsExactlyInAnyOrder(uuid3, uuid2)
            assertEquals(listOf(uuid1), getFriends(service, uuid2))
            assertEquals(listOf(uuid1), getFriends(service, uuid3))
        }

    }

    @Nested
    inner class RemoveFriend {

        @Test
        fun `should return false when A is not friend with B without duplicate`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            val service = FriendCacheService(cacheClient, duplicateForFriend = false)

            assertFalse { service.removeFriend(uuid1, uuid2) }
        }

        @Test
        fun `should return true when A is friend with B without duplicate`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            val service = FriendCacheService(cacheClient, duplicateForFriend = false)

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.removeFriend(uuid1, uuid2) }

            assertEquals(emptyList(), getFriends(service, uuid1))
        }

        @Test
        fun `should return true when A is friend with B with duplicate`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            val service = FriendCacheService(cacheClient, duplicateForFriend = true)

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.removeFriend(uuid1, uuid2) }

            assertEquals(emptyList(), getFriends(service, uuid1))
            assertEquals(emptyList(), getFriends(service, uuid2))
        }

        @Test
        fun `should return true when A is not friend with B but B is friend with A with duplicate`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            val service = FriendCacheService(cacheClient, duplicateForFriend = false)

            assertTrue { service.addFriend(uuid1, uuid2) }

            val serviceWithDuplicate = FriendCacheService(cacheClient, duplicateForFriend = true)
            assertTrue { serviceWithDuplicate.removeFriend(uuid1, uuid2) }

            assertEquals(emptyList(), getFriends(serviceWithDuplicate, uuid1))
        }

        @Test
        fun `should return false when A is not friend with B and vice versa with duplicate`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            val serviceWithDuplicate = FriendCacheService(cacheClient, duplicateForFriend = true)
            assertFalse { serviceWithDuplicate.removeFriend(uuid1, uuid2) }
        }

        @Test
        fun `should remove a friend from the friends list without duplicate`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            val uuid3 = UUID.randomUUID()
            val uuid4 = UUID.randomUUID()

            val service = FriendCacheService(cacheClient, duplicateForFriend = false)

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.addFriend(uuid1, uuid3) }
            assertTrue { service.addFriend(uuid1, uuid4) }

            assertTrue { service.addFriend(uuid2, uuid3) }
            assertTrue { service.addFriend(uuid2, uuid1) }

            assertTrue { service.removeFriend(uuid1, uuid2) }

            assertThat(getFriends(service, uuid2)).containsExactlyInAnyOrder(uuid1, uuid3)

            assertThat(getFriends(service, uuid1)).containsExactlyInAnyOrder(uuid3, uuid4)
            assertTrue { service.removeFriend(uuid1, uuid4) }
            assertThat(getFriends(service, uuid1)).containsExactlyInAnyOrder(uuid3)
        }

        @Test
        fun `should remove a friend from the friends list with duplicate`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            val uuid3 = UUID.randomUUID()
            val uuid4 = UUID.randomUUID()

            val service = FriendCacheService(cacheClient, duplicateForFriend = true)

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.addFriend(uuid1, uuid3) }
            assertTrue { service.addFriend(uuid1, uuid4) }

            assertTrue { service.addFriend(uuid2, uuid3) }

            assertTrue { service.removeFriend(uuid1, uuid2) }

            assertThat(getFriends(service, uuid2)).containsExactlyInAnyOrder(uuid3)
            assertThat(getFriends(service, uuid3)).containsExactlyInAnyOrder(uuid1, uuid2)
            assertThat(getFriends(service, uuid4)).containsExactlyInAnyOrder(uuid1)
            assertThat(getFriends(service, uuid1)).containsExactlyInAnyOrder(uuid3, uuid4)

            assertTrue { service.removeFriend(uuid1, uuid4) }
            assertThat(getFriends(service, uuid1)).containsExactlyInAnyOrder(uuid3)
            assertThat(getFriends(service, uuid3)).containsExactlyInAnyOrder(uuid1, uuid2)
        }

        @Test
        fun `should not set expiration when expiration is null`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            val uuid3 = UUID.randomUUID()
            val uuid4 = UUID.randomUUID()

            val service = FriendCacheService(cacheClient, expiration = null, duplicateForFriend = false)

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.addFriend(uuid1, uuid3) }
            assertTrue { service.addFriend(uuid1, uuid4) }

            assertTrue { service.removeFriend(uuid1, uuid2) }
            assertEquals(-1, getTTL(service, uuid1))
        }

        @Test
        fun `should set expiration when expiration is defined without duplicate`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            val uuid3 = UUID.randomUUID()
            val uuid4 = UUID.randomUUID()

            val service = FriendCacheService(cacheClient, expiration = null, duplicateForFriend = false)

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.addFriend(uuid1, uuid3) }
            assertTrue { service.addFriend(uuid1, uuid4) }

            assertEquals(-1, getTTL(service, uuid1))

            val serviceWithExpiration =
                FriendCacheService(cacheClient, expiration = 10.seconds, duplicateForFriend = false)
            assertTrue { serviceWithExpiration.removeFriend(uuid1, uuid2) }
            assertEquals(10, getTTL(serviceWithExpiration, uuid1))
            assertEquals(-2, getTTL(serviceWithExpiration, uuid2))
            assertEquals(-2, getTTL(serviceWithExpiration, uuid3))
            assertEquals(-2, getTTL(serviceWithExpiration, uuid4))
        }

        @Test
        fun `should set expiration when expiration is defined with duplicate`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            val uuid3 = UUID.randomUUID()
            val uuid4 = UUID.randomUUID()

            val service = FriendCacheService(cacheClient, expiration = null, duplicateForFriend = true)

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.addFriend(uuid1, uuid3) }
            assertTrue { service.addFriend(uuid1, uuid4) }

            assertTrue { service.addFriend(uuid3, uuid2) }

            assertEquals(-1, getTTL(service, uuid1))

            val serviceWithExpiration =
                FriendCacheService(cacheClient, expiration = 10.seconds, duplicateForFriend = true)
            assertTrue { serviceWithExpiration.removeFriend(uuid1, uuid2) }
            assertEquals(10, getTTL(serviceWithExpiration, uuid1))
            assertEquals(10, getTTL(serviceWithExpiration, uuid2))
            assertEquals(-1, getTTL(serviceWithExpiration, uuid3))
            assertEquals(-1, getTTL(serviceWithExpiration, uuid4))
        }

        @Test
        fun `should not set expiration when friend is not removed`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            val uuid3 = UUID.randomUUID()
            val uuid4 = UUID.randomUUID()

            val service = FriendCacheService(cacheClient, expiration = null, duplicateForFriend = false)

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.addFriend(uuid1, uuid3) }
            assertTrue { service.addFriend(uuid1, uuid4) }

            assertTrue { service.addFriend(uuid2, uuid3) }

            assertEquals(-1, getTTL(service, uuid1))

            val serviceWithExpiration =
                FriendCacheService(cacheClient, expiration = 10.seconds, duplicateForFriend = true)
            assertTrue { serviceWithExpiration.removeFriend(uuid1, uuid2) }
            assertEquals(10, getTTL(serviceWithExpiration, uuid1))
            assertEquals(-1, getTTL(serviceWithExpiration, uuid2))
        }

        @Test
        fun `should delete key the last friend is removed`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            val service = FriendCacheService(cacheClient, expiration = null, duplicateForFriend = false)

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.addFriend(uuid2, uuid1) }

            assertTrue { service.removeFriend(uuid1, uuid2) }
            assertFalse { keyExists(service, uuid1) }
            assertTrue { keyExists(service, uuid2) }
        }
    }

    @Nested
    inner class GetFriends {

        @Test
        fun `should return empty list when no friends`() = runTest {
            val uuid = UUID.randomUUID()
            val service = FriendCacheService(cacheClient)
            assertEquals(emptySet(), service.getFriends(uuid))
        }

        @Test
        fun `should return friends`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            val uuid3 = UUID.randomUUID()

            val service = FriendCacheService(cacheClient, duplicateForFriend = false)

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.addFriend(uuid1, uuid3) }

            assertThat(service.getFriends(uuid1)).containsExactlyInAnyOrder(uuid2, uuid3)
        }

        @Test
        fun `should return friends when valid uuid`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            val uuid3 = UUID.randomUUID()

            val service = FriendCacheService(cacheClient, duplicateForFriend = false)

            assertTrue { service.addFriend(uuid1, uuid2) }
            cacheClient.connect {
                it.sadd(createKey(service, uuid1), "invalid".toByteArray())
            }
            assertTrue { service.addFriend(uuid1, uuid3) }

            assertThat(service.getFriends(uuid1)).containsExactlyInAnyOrder(uuid2, uuid3)
        }

    }

    @Nested
    inner class SetFriends {

        @Test
        fun `should returns true and delete key when empty list`() = runTest {
            val uuid = UUID.randomUUID()
            val service = FriendCacheService(cacheClient, duplicateForFriend = false)

            assertTrue { service.addFriend(uuid, UUID.randomUUID()) }

            assertTrue { service.setFriends(uuid, emptySet()) }
            assertFalse { keyExists(service, uuid) }

        }

        @Test
        fun `should delete old friends before adding news`() = runTest {
            val uuid = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            val uuid3 = UUID.randomUUID()
            val uuid4 = UUID.randomUUID()

            val service = FriendCacheService(cacheClient, duplicateForFriend = false)

            assertTrue { service.addFriend(uuid, uuid2) }
            assertTrue { service.addFriend(uuid, uuid3) }

            assertTrue { service.setFriends(uuid, setOf(uuid4)) }
            assertEquals(listOf(uuid4), getFriends(service, uuid))
        }

        @Test
        fun `should add friend to friends when duplicate is enabled`() = runTest {
            val uuid = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            val uuid3 = UUID.randomUUID()
            val uuid4 = UUID.randomUUID()

            val service = FriendCacheService(cacheClient, duplicateForFriend = true)

            assertTrue { service.addFriend(uuid, uuid2) }
            assertTrue { service.addFriend(uuid, uuid3) }

            assertTrue { service.addFriend(uuid4, uuid2) }

            assertTrue { service.setFriends(uuid, setOf(uuid4)) }
            assertEquals(listOf(uuid4), getFriends(service, uuid))
            assertThat(getFriends(service, uuid4)).containsExactlyInAnyOrder(uuid, uuid2)
        }

        @Test
        fun `should set expiration for the key when friends list is not empty`() = runTest {
            val uuid = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            val service = FriendCacheService(cacheClient, expiration = 20.seconds, duplicateForFriend = true)

            assertTrue { service.setFriends(uuid, setOf(uuid2)) }
            assertEquals(20, getTTL(service, uuid))
        }

        @Test
        fun `should not set expiration for the key when friends list is empty`() = runTest {
            val uuid = UUID.randomUUID()

            val service = FriendCacheService(cacheClient, expiration = 20.seconds, duplicateForFriend = true)

            assertTrue { service.setFriends(uuid, emptySet()) }
            assertEquals(-2, getTTL(service, uuid))
        }

    }

    private suspend fun keyExists(
        service: FriendCacheService,
        uuid: UUID
    ) = cacheClient.connect {
        val numberOfKeys = it.exists(createKey(service, uuid))
        numberOfKeys != null && numberOfKeys > 0
    }

    @Nested
    inner class IsFriend {

        @Test
        fun `should return false when A is not friend with B without duplicate`() = runTest {
            val uuid = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            val service = FriendCacheService(cacheClient, duplicateForFriend = false)

            assertFalse { service.isFriend(uuid, uuid2) }

            assertTrue { service.addFriend(uuid2, uuid) }

            assertFalse { service.isFriend(uuid, uuid2) }
        }

        @Test
        fun `should return false when A is not friend with B with duplicate`() = runTest {
            val uuid = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            val service = FriendCacheService(cacheClient, duplicateForFriend = false)

            assertFalse { service.isFriend(uuid, uuid2) }

            assertTrue { service.addFriend(uuid2, uuid) }

            val serviceWithDuplicate = FriendCacheService(cacheClient, duplicateForFriend = true)

            assertFalse { serviceWithDuplicate.isFriend(uuid, uuid2) }
        }

        @Test
        fun `should return true when A is friend with B with duplicate`() = runTest {
            val uuid = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            val service = FriendCacheService(cacheClient, duplicateForFriend = true)

            assertTrue { service.addFriend(uuid, uuid2) }

            assertTrue { service.isFriend(uuid, uuid2) }
            assertTrue { service.isFriend(uuid2, uuid) }

        }

        @Test
        fun `should return true when A has only B as friend`() = runTest {
            val uuid = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            val service = FriendCacheService(cacheClient, duplicateForFriend = false)

            assertTrue { service.addFriend(uuid, uuid2) }

            assertTrue { service.isFriend(uuid, uuid2) }

            assertFalse { service.isFriend(uuid2, uuid) }
        }

        @Test
        fun `should return true when A has several friends including B`() = runTest {
            val uuid = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            val service = FriendCacheService(cacheClient, duplicateForFriend = false)

            assertTrue { service.addFriend(uuid, UUID.randomUUID()) }
            assertTrue { service.addFriend(uuid, UUID.randomUUID()) }
            assertTrue { service.addFriend(uuid, uuid2) }
            assertTrue { service.addFriend(uuid, UUID.randomUUID()) }

            assertTrue { service.isFriend(uuid, uuid2) }
        }
    }

    private suspend fun getFriends(
        service: FriendCacheService,
        uuid: UUID
    ) = cacheClient.connect {
        val keySerial = createKey(service, uuid)
        it.smembers(keySerial).mapNotNull { member ->
            binaryFormat.decodeFromByteArray(UUIDSerializer, member)
        }.toList()
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