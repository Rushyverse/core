@file:OptIn(ExperimentalLettuceCoroutinesApi::class)

package com.github.rushyverse.core.cache

import com.github.rushyverse.core.container.createRedisContainer
import com.github.rushyverse.core.serializer.UUIDSerializer
import com.github.rushyverse.core.utils.assertCoroutineContextFromScope
import com.github.rushyverse.core.utils.getRandomString
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisURI
import io.lettuce.core.support.BoundedPoolConfig
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.*
import kotlinx.coroutines.future.await
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.builtins.serializer
import org.junit.jupiter.api.Nested
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.random.Random
import kotlin.test.*

@Testcontainers
class CacheClientTest {

    companion object {
        @JvmStatic
        @Container
        private val redisContainer = createRedisContainer()
    }

    @Nested
    inner class Builder {

        @Test
        fun `default pool config has not max`() = runTest {
            val client = CacheClient {
                uri = RedisURI.create(redisContainer.url)
            }

            val cm = client.connectionManager
            assertEquals(-1, cm.poolPubSub.maxTotal)
            assertEquals(-1, cm.poolStateful.maxTotal)

        }

        @Test
        fun `pool configuration is used to create pool`() = runTest {
            val poolConfig = BoundedPoolConfig.builder()
                .maxIdle(Random.nextInt(0, 100))
                .minIdle(Random.nextInt(-100, 0))
                .maxTotal(Random.nextInt(1000, 2000))
                .build()

            val client = CacheClient {
                uri = RedisURI.create(redisContainer.url)
                poolConfiguration = poolConfig
            }

            val connectionManager = client.connectionManager
            val poolStateful = connectionManager.poolStateful
            assertEquals(poolConfig.maxIdle, poolStateful.maxIdle)
            assertEquals(poolConfig.minIdle, poolStateful.minIdle)
            assertEquals(poolConfig.maxTotal, poolStateful.maxTotal)

            val poolPubSub = connectionManager.poolPubSub
            assertEquals(poolConfig.maxIdle, poolPubSub.maxIdle)
            assertEquals(poolConfig.minIdle, poolPubSub.minIdle)
            assertEquals(poolConfig.maxTotal, poolPubSub.maxTotal)

            client.closeAsync().await()
        }

    }

    @Nested
    inner class Close {

        @Test
        fun `should cancel all jobs`() = runTest {
            val client = CacheClient {
                uri = RedisURI.create(redisContainer.url)
            }

            val job = client.launch {
                delay(Long.MAX_VALUE)
            }

            client.closeAsync().await()
            assertTrue { client.coroutineContext.job.isCancelled }
            assertTrue { client.releasePubSubScope.coroutineContext.job.isCancelled }
            assertTrue { job.isCancelled }
        }

        @Test
        fun `should stop the redis client`() = runTest {
            val redisClient = mockk<RedisClient>()
            every { redisClient.shutdownAsync() } returns CompletableFuture.completedFuture(mockk())
            val client = CacheClient {
                uri = RedisURI.create(redisContainer.url)
                client = redisClient
            }

            client.closeAsync().await()

            verify(exactly = 1) { redisClient.shutdownAsync() }

            assertNull(client.connectionManager.closeAsync().getNow(mockk()))
        }

        @Test
        fun `should close connection manager`() = runTest {
            val client = CacheClient {
                uri = RedisURI.create(redisContainer.url)
            }

            client.closeAsync().await()
            assertNull(client.connectionManager.closeAsync().getNow(mockk()))
        }


    }

    @Nested
    inner class Connect {

        private lateinit var client: CacheClient

        @BeforeTest
        fun onBefore(): Unit = runBlocking {
            client = CacheClient {
                uri = RedisURI.create(redisContainer.url)
            }
        }

        @AfterTest
        fun onAfter(): Unit = runBlocking {
            client.closeAsync().await()
        }

        @Test
        fun `should connect to redis`() = runTest {
            val pool = client.connectionManager.poolStateful

            client.connect {
                assertEquals(0, pool.idle)
                assertEquals(1, pool.objectCount)
            }

            assertEquals(1, pool.idle)
            assertEquals(1, pool.objectCount)
        }

        @Test
        fun `should return result in body`() = runTest {
            val expected = getRandomString()
            val value = client.connect { expected }
            assertEquals(expected, value)
        }

    }

    @Nested
    inner class Subscribe {

        private lateinit var client: CacheClient

        @BeforeTest
        fun onBefore(): Unit = runBlocking {
            client = CacheClient {
                uri = RedisURI.create(redisContainer.url)
            }
        }

        @AfterTest
        fun onAfter(): Unit = runBlocking {
            client.closeAsync().await()
        }

        @Nested
        inner class OneChannel {

            @Test
            fun `should use pool pubSub`() = runBlocking {
                val pool = client.connectionManager.poolPubSub
                val channel = "test"

                val latch = CountDownLatch(1)
                val job = client.subscribe(channel) { _ ->
                    assertEquals(0, pool.idle)
                    assertEquals(1, pool.objectCount)
                    latch.countDown()
                }

                assertEquals(0, pool.idle)
                assertEquals(1, pool.objectCount)

                val channelByteArray = client.binaryFormat.encodeToByteArray(String.serializer(), channel)
                val messageByteArray = client.binaryFormat.encodeToByteArray(String.serializer(), getRandomString())

                client.connect {
                    it.publish(channelByteArray, messageByteArray)
                }

                latch.await()

                job.cancel()

                // Check that the release of the connection is done in another coroutine
                assertEquals(0, pool.idle)
                assertEquals(1, pool.objectCount)

                val latchRelease = CountDownLatch(10)

                while (pool.idle == 0 && !latchRelease.await(100, TimeUnit.MILLISECONDS)) {
                    latchRelease.countDown()
                }

                assertEquals(1, pool.idle)
                assertEquals(1, pool.objectCount)
            }

            @Test
            fun `receive string message`() = runTest {
                val channel = "test"
                val expectedMessage = getRandomString()

                val latch = CountDownLatch(1)
                var receivedMessage: String? = null
                client.subscribe(channel) { message ->
                    receivedMessage = message
                    latch.countDown()
                }

                val channelByteArray = client.binaryFormat.encodeToByteArray(String.serializer(), channel)
                val messageByteArray = client.binaryFormat.encodeToByteArray(String.serializer(), expectedMessage)

                client.connect {
                    it.publish(channelByteArray, messageByteArray)
                }

                latch.await()

                assertEquals(expectedMessage, receivedMessage)
            }

            @Test
            fun `receive custom type message`() = runTest {
                val channel = "test"
                val expectedMessage = UUID.randomUUID()

                val latch = CountDownLatch(1)
                var receivedMessage: UUID? = null
                client.subscribe(channel, UUIDSerializer) { message ->
                    receivedMessage = message
                    latch.countDown()
                }

                val channelByteArray = client.binaryFormat.encodeToByteArray(String.serializer(), channel)
                val messageByteArray = client.binaryFormat.encodeToByteArray(UUIDSerializer, expectedMessage)

                client.connect {
                    it.publish(channelByteArray, messageByteArray)
                }

                latch.await()

                assertEquals(expectedMessage, receivedMessage)
            }

            @Test
            fun `throw exception for the first message but continue to receive`() = runTest {
                val channel = "test"
                val expectedMessage = getRandomString()

                val latch = CountDownLatch(2)
                client.subscribe(channel) { _ ->
                    latch.countDown()
                    if (latch.count == 1L) {
                        error("Error")
                    }
                }

                val channelByteArray = client.binaryFormat.encodeToByteArray(String.serializer(), channel)
                val messageByteArray = client.binaryFormat.encodeToByteArray(String.serializer(), expectedMessage)

                client.connect {
                    it.publish(channelByteArray, messageByteArray)
                    it.publish(channelByteArray, messageByteArray)
                }

                latch.await()
            }

            @Test
            fun `doesn't receive message for other channel`() = runTest {
                val expectedChannel = "test"
                val expectedMessage = getRandomString()

                val latch = CountDownLatch(1)

                client.subscribe(expectedChannel) { message ->
                    assertEquals(expectedMessage, message)
                    latch.countDown()
                }

                client.connect {
                    it.publish(
                        client.binaryFormat.encodeToByteArray(String.serializer(), getRandomString()),
                        client.binaryFormat.encodeToByteArray(String.serializer(), getRandomString())
                    )
                    it.publish(
                        client.binaryFormat.encodeToByteArray(String.serializer(), expectedChannel),
                        client.binaryFormat.encodeToByteArray(String.serializer(), expectedMessage)
                    )
                }

                latch.await()
            }

            @Test
            fun `should receive in a coroutine from scope`() = runTest {
                val expectedChannel = "test"

                val latch = CountDownLatch(1)

                val scope = CoroutineScope(Dispatchers.Default)
                client.subscribe(expectedChannel, scope = scope) { _ ->
                    assertCoroutineContextFromScope(
                        scope,
                        currentCoroutineContext()
                    )
                    latch.countDown()
                }

                client.connect {
                    it.publish(
                        client.binaryFormat.encodeToByteArray(String.serializer(), expectedChannel),
                        client.binaryFormat.encodeToByteArray(String.serializer(), getRandomString())
                    )
                }

                latch.await()
            }

        }

        @Nested
        inner class SeveralChannel {

            @Test
            fun `should use pool pubSub`() = runTest {
                val pool = client.connectionManager.poolPubSub
                val channel = "test"

                val latch = CountDownLatch(1)
                val job = client.subscribe(*arrayOf(channel, getRandomString())) { _, _ ->
                    assertEquals(0, pool.idle)
                    assertEquals(1, pool.objectCount)
                    latch.countDown()
                }

                assertEquals(0, pool.idle)
                assertEquals(1, pool.objectCount)

                val channelByteArray = client.binaryFormat.encodeToByteArray(String.serializer(), channel)
                val messageByteArray = client.binaryFormat.encodeToByteArray(String.serializer(), getRandomString())

                client.connect {
                    it.publish(channelByteArray, messageByteArray)
                }

                latch.await()

                job.cancel()

                // Check that the release of the connection is done in another coroutine
                assertEquals(0, pool.idle)
                assertEquals(1, pool.objectCount)

                val latchRelease = CountDownLatch(10)

                while (pool.idle == 0 && !latchRelease.await(100, TimeUnit.MILLISECONDS)) {
                    latchRelease.countDown()
                }

                assertEquals(1, pool.idle)
                assertEquals(1, pool.objectCount)
            }

            @Test
            fun `receive string message`() = runTest {
                val channel = "test"
                val expectedMessage = getRandomString()

                val latch = CountDownLatch(1)
                var receivedMessage: String? = null
                client.subscribe(*arrayOf(channel, getRandomString())) { _, message ->
                    receivedMessage = message
                    latch.countDown()
                }

                val channelByteArray = client.binaryFormat.encodeToByteArray(String.serializer(), channel)
                val messageByteArray = client.binaryFormat.encodeToByteArray(String.serializer(), expectedMessage)

                client.connect {
                    it.publish(channelByteArray, messageByteArray)
                }

                latch.await()

                assertEquals(expectedMessage, receivedMessage)
            }

            @Test
            fun `receive custom type message`() = runTest {
                val channel = "test"
                val expectedMessage = UUID.randomUUID()

                val latch = CountDownLatch(1)
                var receivedMessage: UUID? = null
                client.subscribe(
                    *arrayOf(channel, getRandomString()),
                    messageSerializer = UUIDSerializer
                ) { _, message ->
                    receivedMessage = message
                    latch.countDown()
                }

                val channelByteArray = client.binaryFormat.encodeToByteArray(String.serializer(), channel)
                val messageByteArray = client.binaryFormat.encodeToByteArray(UUIDSerializer, expectedMessage)

                client.connect {
                    it.publish(channelByteArray, messageByteArray)
                }

                latch.await()

                assertEquals(expectedMessage, receivedMessage)
            }

            @Test
            fun `throw exception for the first message but continue to receive`() = runTest {
                val channel = "test"
                val expectedMessage = getRandomString()

                val latch = CountDownLatch(2)
                client.subscribe(*arrayOf(channel, getRandomString())) { _, _ ->
                    latch.countDown()
                    if (latch.count == 1L) {
                        error("Error")
                    }
                }

                val channelByteArray = client.binaryFormat.encodeToByteArray(String.serializer(), channel)
                val messageByteArray = client.binaryFormat.encodeToByteArray(String.serializer(), expectedMessage)

                client.connect {
                    it.publish(channelByteArray, messageByteArray)
                    it.publish(channelByteArray, messageByteArray)
                }

                latch.await()
            }

            @Test
            fun `doesn't receive message for other channel`() = runTest {
                val expectedChannel = "test"
                val expectedMessage = getRandomString()

                val latch = CountDownLatch(1)

                client.subscribe(*arrayOf(expectedChannel, getRandomString())) { channel, message ->
                    assertEquals(expectedChannel, channel)
                    assertEquals(expectedMessage, message)
                    latch.countDown()
                }

                client.connect {
                    it.publish(
                        client.binaryFormat.encodeToByteArray(String.serializer(), getRandomString()),
                        client.binaryFormat.encodeToByteArray(String.serializer(), getRandomString())
                    )
                    it.publish(
                        client.binaryFormat.encodeToByteArray(String.serializer(), expectedChannel),
                        client.binaryFormat.encodeToByteArray(String.serializer(), expectedMessage)
                    )
                }

                latch.await()
            }

            @Test
            fun `receive message for all channels`() = runTest {
                val expectedInfo = List(10) { getRandomString() to getRandomString() }

                val latch = CountDownLatch(expectedInfo.size)
                val receivedMessages = mutableListOf<Pair<String, String>>()
                client.subscribe(*expectedInfo.map { it.first }.toTypedArray()) { channel, message ->
                    receivedMessages += Pair(channel, message)
                    latch.countDown()
                }

                client.connect {
                    expectedInfo.forEach { (channel, message) ->
                        it.publish(
                            client.binaryFormat.encodeToByteArray(String.serializer(), channel),
                            client.binaryFormat.encodeToByteArray(String.serializer(), message)
                        )
                    }
                }

                latch.await()

                assertEquals(expectedInfo, receivedMessages)
            }

            @Test
            fun `should receive in a coroutine from scope`() = runTest {
                val expectedChannel = "test"

                val latch = CountDownLatch(1)

                val scope = CoroutineScope(Dispatchers.Default)
                client.subscribe(*arrayOf(expectedChannel, getRandomString()), scope = scope) { _, _ ->
                    assertCoroutineContextFromScope(
                        scope,
                        currentCoroutineContext()
                    )
                    latch.countDown()
                }

                client.connect {
                    it.publish(
                        client.binaryFormat.encodeToByteArray(String.serializer(), expectedChannel),
                        client.binaryFormat.encodeToByteArray(String.serializer(), getRandomString())
                    )
                }

                latch.await()
            }

        }

    }
}