package com.github.rushyverse.core.cache

import com.github.rushyverse.core.cache.message.IdentifiableMessage
import com.github.rushyverse.core.cache.message.IdentifiableMessageSerializer
import com.github.rushyverse.core.container.createRedisContainer
import com.github.rushyverse.core.serializer.UUIDSerializer
import com.github.rushyverse.core.utils.assertCoroutineContextUseDispatcher
import com.github.rushyverse.core.utils.getRandomString
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisURI
import io.lettuce.core.support.BoundedPoolConfig
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.future.await
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.KSerializer
import kotlinx.serialization.builtins.serializer
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Timeout
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.coroutineContext
import kotlin.random.Random
import kotlin.test.*
import kotlin.time.Duration.Companion.seconds

@Timeout(5, unit = TimeUnit.SECONDS)
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

        @Test
        fun `default coroutine scope should use dispatcher IO`() = runTest {
            val client = CacheClient {
                uri = RedisURI.create(redisContainer.url)
            }

            assertCoroutineContextUseDispatcher(client.coroutineContext, Dispatchers.IO)
        }

        @Test
        fun `coroutine scope should use dispatcher defined`() = runTest {
            val client = CacheClient {
                uri = RedisURI.create(redisContainer.url)
                coroutineScope = CoroutineScope(Dispatchers.Default)
            }

            assertCoroutineContextUseDispatcher(client.coroutineContext, Dispatchers.Default)
        }

        @Test
        fun `release scope should use dispatcher IO`() = runTest {
            val client = CacheClient {
                uri = RedisURI.create(redisContainer.url)
            }

            assertCoroutineContextUseDispatcher(client.releasePubSubScope.coroutineContext, Dispatchers.IO)
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
                val channel = getRandomString()

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

                delay(100)
                client.connect {
                    it.publish(channelByteArray, messageByteArray)
                }

                latch.await()

                job.cancel()

                val latchRelease = CountDownLatch(10)

                while (pool.idle == 0 && !latchRelease.await(100, TimeUnit.MILLISECONDS)) {
                    latchRelease.countDown()
                }

                assertEquals(1, pool.idle)
                assertEquals(1, pool.objectCount)
            }

            @Test
            fun `receive string message`() = runTest {
                val channel = getRandomString()
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
                val channel = getRandomString()
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
            fun `throw exception in handler for the first message but continue to receive`() = runTest {
                val channel = getRandomString()
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
            fun `throw exception in parse message for the first message but continue to receive`() = runTest {
                val channel = getRandomString()

                val latch = CountDownLatch(1)
                client.subscribe(channel, UUIDSerializer) { _ ->
                    latch.countDown()
                }

                val channelByteArray = client.binaryFormat.encodeToByteArray(String.serializer(), channel)

                client.connect {
                    it.publish(
                        channelByteArray,
                        client.binaryFormat.encodeToByteArray(String.serializer(), getRandomString())
                    )
                    it.publish(
                        channelByteArray,
                        client.binaryFormat.encodeToByteArray(UUIDSerializer, UUID.randomUUID())
                    )
                }

                latch.await()
            }

            @Test
            fun `doesn't receive message for other channel`() = runTest {
                val expectedChannel = getRandomString()
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
                val expectedChannel = getRandomString()

                val latch = CountDownLatch(1)

                val scope = CoroutineScope(Dispatchers.Default)
                client.subscribe(expectedChannel, scope = scope) { _ ->
                    assertCoroutineContextUseDispatcher(currentCoroutineContext(), Dispatchers.Default)
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
            fun `should throws exception if channel is empty`() = runTest {
                assertFailsWith<IllegalArgumentException> {
                    client.subscribe(emptyArray()) { _, _ -> }
                }
            }

            @Test
            fun `should use pool pubSub`() = runTest {
                val pool = client.connectionManager.poolPubSub
                val channel = getRandomString()

                val latch = CountDownLatch(1)
                val job = client.subscribe(arrayOf(channel, getRandomString())) { _, _ ->
                    assertEquals(0, pool.idle)
                    assertEquals(1, pool.objectCount)
                    latch.countDown()
                }

                assertEquals(0, pool.idle)
                assertEquals(1, pool.objectCount)

                val channelByteArray = client.binaryFormat.encodeToByteArray(String.serializer(), channel)
                val messageByteArray = client.binaryFormat.encodeToByteArray(String.serializer(), getRandomString())

                delay(100)
                client.connect {
                    it.publish(channelByteArray, messageByteArray)
                }

                latch.await()

                job.cancel()

                val latchRelease = CountDownLatch(10)

                while (pool.idle == 0 && !latchRelease.await(100, TimeUnit.MILLISECONDS)) {
                    latchRelease.countDown()
                }

                assertEquals(1, pool.idle)
                assertEquals(1, pool.objectCount)
            }

            @Test
            fun `receive string message`() = runTest {
                val channel = getRandomString()
                val expectedMessage = getRandomString()

                val latch = CountDownLatch(1)
                var receivedMessage: String? = null
                client.subscribe(arrayOf(channel, getRandomString())) { _, message ->
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
                val channel = getRandomString()
                val expectedMessage = UUID.randomUUID()

                val latch = CountDownLatch(1)
                var receivedMessage: UUID? = null
                client.subscribe(
                    arrayOf(channel, getRandomString()),
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
                val channel = getRandomString()
                val expectedMessage = getRandomString()

                val latch = CountDownLatch(2)
                client.subscribe(arrayOf(channel, getRandomString())) { _, _ ->
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
            fun `throw exception in parse message for the first message but continue to receive`() = runTest {
                val channel = getRandomString()

                val latch = CountDownLatch(1)
                client.subscribe(arrayOf(channel, getRandomString()), UUIDSerializer) { _, _ ->
                    latch.countDown()
                }

                val channelByteArray = client.binaryFormat.encodeToByteArray(String.serializer(), channel)

                client.connect {
                    it.publish(
                        channelByteArray,
                        client.binaryFormat.encodeToByteArray(String.serializer(), getRandomString())
                    )
                    it.publish(
                        channelByteArray,
                        client.binaryFormat.encodeToByteArray(UUIDSerializer, UUID.randomUUID())
                    )
                }

                latch.await()
            }

            @Test
            fun `doesn't receive message for other channel`() = runTest {
                val expectedChannel = getRandomString()
                val expectedMessage = getRandomString()

                val latch = CountDownLatch(1)

                client.subscribe(arrayOf(expectedChannel, getRandomString())) { channel, message ->
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
                client.subscribe(expectedInfo.map { it.first }.toTypedArray()) { channel, message ->
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
                val dispatcher = Dispatchers.Default
                assertScopeForSubscription(CoroutineScope(dispatcher), dispatcher)
            }

            @Test
            fun `should receive in a coroutine from default scope`() = runTest {
                assertScopeForSubscription(client, Dispatchers.IO)
            }

            private suspend fun assertScopeForSubscription(
                scope: CoroutineScope,
                dispatcher: CoroutineDispatcher
            ) {
                val expectedChannel = getRandomString()

                val latch = CountDownLatch(1)

                client.subscribe(arrayOf(expectedChannel, getRandomString()), scope = scope) { _, _ ->
                    assertCoroutineContextUseDispatcher(coroutineContext, dispatcher)
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

    @Nested
    inner class Publish {

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
        fun `should use pool pubSub`() = runBlocking {
            val pool = client.connectionManager.poolPubSub

            assertEquals(0, pool.idle)
            assertEquals(0, pool.objectCount)

            client.publish(getRandomString(), getRandomString(), String.serializer())

            assertEquals(1, pool.idle)
            assertEquals(1, pool.objectCount)
        }

        @Test
        fun `should publish string message`() = runTest {
            val channel = getRandomString()
            val message = getRandomString()

            val latch = CountDownLatch(1)
            subscribeToChannel(channel, String.serializer()) {
                assertEquals(message, it)
                latch.countDown()
            }

            client.publish(channel, message, String.serializer())

            latch.await()
        }

        @Test
        fun `should publish custom type message`() = runTest {
            val channel = getRandomString()
            val message = UUID.randomUUID()

            val latch = CountDownLatch(1)

            subscribeToChannel(channel, UUIDSerializer) {
                assertEquals(message, it)
                latch.countDown()
            }

            client.publish(channel, message, UUIDSerializer)

            latch.await()
        }

        private suspend fun <T> subscribeToChannel(
            channel: String,
            messageSerializer: KSerializer<T>,
            body: suspend (T) -> Unit
        ): Job {
            val binaryFormat = client.binaryFormat
            val connection = client.connectionManager.getPubSubConnection()
            val reactiveConnection = connection.reactive()
            reactiveConnection.subscribe(
                binaryFormat.encodeToByteArray(String.serializer(), channel)
            ).awaitFirstOrNull()

            return reactiveConnection.observeChannels().asFlow()
                .onEach {
                    val message = binaryFormat.decodeFromByteArray(messageSerializer, it.message)
                    body(message)
                }.launchIn(CoroutineScope(Dispatchers.IO))
        }

    }

    @Nested
    inner class PublishIdentifiableMessage {

        @Test
        fun `should publish identifiable message`() = runTest {
            val client = CacheClient {
                uri = RedisURI.create(redisContainer.url)
            }

            val channel = getRandomString()
            val message = getRandomString()
            val id = getRandomString()

            val messageSerializer = String.serializer()

            val latch = CountDownLatch(1)
            client.subscribe(
                channel,
                IdentifiableMessageSerializer(messageSerializer)
            ) {
                assertEquals(IdentifiableMessage(id, message), it)
                latch.countDown()
            }

            client.publishIdentifiableMessage(
                channel,
                id,
                message,
                messageSerializer,
            )

            latch.await()
        }
    }

    @Nested
    inner class PublishAndWaitResponse {

        @Test
        fun `should publish and wait response`() = runTest {
            val client = CacheClient {
                uri = RedisURI.create(redisContainer.url)
            }

            val channel = getRandomString()
            val channelResponse = getRandomString()
            val message = getRandomString()
            val expectedResponse = UUID.randomUUID()

            val messageSerializer = String.serializer()
            val responseSerializer = UUIDSerializer

            val id = getRandomString()
            var isReceived = false

            client.subscribe(
                channel,
                IdentifiableMessageSerializer(messageSerializer)
            ) {
                assertEquals(IdentifiableMessage(id, message), it)
                isReceived = true
                client.publishIdentifiableMessage(
                    channelResponse,
                    id,
                    expectedResponse,
                    responseSerializer
                )
            }

            val response = client.publishAndWaitResponse(
                channelSubscribe = channelResponse,
                channelPublish = channel,
                messagePublish = message,
                messageSerializer = messageSerializer,
                responseSerializer = responseSerializer,
                id = id
            ) {
                assertEquals(expectedResponse, it)
                it
            }

            assertEquals(expectedResponse, response)
            assertTrue(isReceived)
        }

        @Test
        fun `should cancel created job for subscribe when finish by message`() = runTest {
            val client = CacheClient {
                uri = RedisURI.create(redisContainer.url)
            }

            val channel = getRandomString()
            val channelResponse = getRandomString()
            val message = getRandomString()
            val expectedResponse = UUID.randomUUID()

            val messageSerializer = String.serializer()
            val responseSerializer = UUIDSerializer

            val id = getRandomString()
            var hasChildrenDuringSubscription = false

            val coroutineScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
            assertEquals(coroutineScope.coroutineContext.job.children.count(), 0)

            client.subscribe(
                channel,
                IdentifiableMessageSerializer(messageSerializer)
            ) {
                hasChildrenDuringSubscription = coroutineScope.coroutineContext.job.children.count() > 0
                client.publishIdentifiableMessage(
                    channelResponse,
                    id,
                    expectedResponse,
                    responseSerializer
                )
            }

            client.publishAndWaitResponse(
                channelSubscribe = channelResponse,
                channelPublish = channel,
                messagePublish = message,
                messageSerializer = messageSerializer,
                responseSerializer = responseSerializer,
                id = id,
                subscribeScope = coroutineScope
            ) {
                assertEquals(expectedResponse, it)
                it
            }

            assertEquals(coroutineScope.coroutineContext.job.children.count(), 0)
            assertTrue(hasChildrenDuringSubscription)
        }

        @Test
        fun `should not be triggered when the id is not the same`() = runBlocking {
            val client = CacheClient {
                uri = RedisURI.create(redisContainer.url)
            }

            val channel = getRandomString()
            val channelResponse = getRandomString()
            val message = getRandomString()
            val expectedResponse = UUID.randomUUID()

            val messageSerializer = String.serializer()
            val responseSerializer = UUIDSerializer

            val id = getRandomString()

            val latch = CountDownLatch(1)
            val latchReceiveWrongId = CountDownLatch(1)

            client.subscribe(
                channel,
                IdentifiableMessageSerializer(messageSerializer)
            ) {
                latchReceiveWrongId.countDown()
            }

            client.launch {
                client.publishAndWaitResponse(
                    channelSubscribe = channelResponse,
                    channelPublish = channel,
                    messagePublish = message,
                    messageSerializer = messageSerializer,
                    responseSerializer = responseSerializer,
                    id = id
                ) {
                    latch.countDown()
                }
            }

            client.publishIdentifiableMessage(
                channelResponse,
                getRandomString(),
                expectedResponse,
                responseSerializer
            )

            latchReceiveWrongId.await()
            assertEquals(1, latch.count)

            client.publishIdentifiableMessage(
                channelResponse,
                id,
                expectedResponse,
                responseSerializer
            )

            latch.await()
        }

        @Test
        fun `should not be triggered when the message is not deserializable`(): Unit = runBlocking {
            val client = CacheClient {
                uri = RedisURI.create(redisContainer.url)
            }

            val channel = getRandomString()
            val channelSubscribe = getRandomString()
            val message = getRandomString()
            val expectedResponse = UUID.randomUUID()

            val messageSerializer = String.serializer()
            val responseSerializer = UUIDSerializer

            val id = getRandomString()

            val latch = CountDownLatch(1)
            val latchReceiveWrongId = CountDownLatch(1)

            client.subscribe(
                channelSubscribe,
                IdentifiableMessageSerializer(Int.serializer())
            ) {
                latchReceiveWrongId.countDown()
            }

            client.launch {
                client.publishAndWaitResponse(
                    channelSubscribe = channelSubscribe,
                    channelPublish = channel,
                    messagePublish = message,
                    messageSerializer = messageSerializer,
                    responseSerializer = responseSerializer,
                    id = id
                ) {
                    latch.countDown()
                }
            }

            client.publishIdentifiableMessage(
                channelSubscribe,
                getRandomString(),
                1,
                Int.serializer()
            )

            latchReceiveWrongId.await()
            assertEquals(1, latch.count)

            client.publishIdentifiableMessage(
                channelSubscribe,
                id,
                expectedResponse,
                responseSerializer
            )

            latch.await()
        }

        @Test
        fun `should trigger the body only once times when several message with same id are sent`() = runTest {
            val client = CacheClient {
                uri = RedisURI.create(redisContainer.url)
            }

            val channel = getRandomString()
            val channelResponse = getRandomString()
            val message = getRandomString()
            val expectedResponse = UUID.randomUUID()

            val messageSerializer = String.serializer()
            val responseSerializer = UUIDSerializer

            val id = getRandomString()

            client.subscribe(
                channel,
                IdentifiableMessageSerializer(messageSerializer)
            ) {
                List(10) {
                    client.async {
                        client.publishIdentifiableMessage(
                            channelResponse,
                            id,
                            expectedResponse,
                            responseSerializer
                        )
                    }
                }.awaitAll()
            }

            val atomicInteger = AtomicInteger(0)

            client.publishAndWaitResponse(
                channelSubscribe = channelResponse,
                channelPublish = channel,
                messagePublish = message,
                messageSerializer = messageSerializer,
                responseSerializer = responseSerializer,
                id = id
            ) {
                yield()
                atomicInteger.incrementAndGet()
                yield()
            }

            assertEquals(1, atomicInteger.get())
        }

        @Test
        @Timeout(3, unit = TimeUnit.SECONDS)
        fun `should let finish body execution`() = runTest {
            val client = CacheClient {
                uri = RedisURI.create(redisContainer.url)
            }

            val channel = getRandomString()
            val channelResponse = getRandomString()
            val message = getRandomString()
            val expectedResponse = UUID.randomUUID()

            val messageSerializer = String.serializer()
            val responseSerializer = UUIDSerializer

            val id = getRandomString()

            val latch = CountDownLatch(2)

            client.subscribe(
                channel,
                IdentifiableMessageSerializer(messageSerializer)
            ) {
                client.publishIdentifiableMessage(
                    channelResponse,
                    id,
                    expectedResponse,
                    responseSerializer
                )
            }

            client.publishAndWaitResponse(
                channelSubscribe = channelResponse,
                channelPublish = channel,
                messagePublish = message,
                messageSerializer = messageSerializer,
                responseSerializer = responseSerializer,
                id = id
            ) {
                latch.countDown()
                yield()
                latch.countDown()
            }

            latch.await()
        }

        @Test
        @Timeout(3, unit = TimeUnit.SECONDS)
        fun `should stop subscribe job when exception is thrown in body`() = runTest {
            val client = CacheClient {
                uri = RedisURI.create(redisContainer.url)
            }

            val channel = getRandomString()
            val channelResponse = getRandomString()
            val message = getRandomString()
            val expectedResponse = UUID.randomUUID()

            val messageSerializer = String.serializer()
            val responseSerializer = UUIDSerializer

            val id = getRandomString()

            client.subscribe(
                channel,
                IdentifiableMessageSerializer(messageSerializer)
            ) {
                client.publishIdentifiableMessage(
                    channelResponse,
                    id,
                    expectedResponse,
                    responseSerializer
                )
            }

            val coroutineScope = CoroutineScope(Dispatchers.IO)

            client.publishAndWaitResponse(
                channelSubscribe = channelResponse,
                channelPublish = channel,
                messagePublish = message,
                messageSerializer = messageSerializer,
                responseSerializer = responseSerializer,
                id = id,
                subscribeScope = coroutineScope
            ) {
                error("Excepted exception")
            }

            assertTrue { coroutineScope.coroutineContext.job.children.count() == 0 }
        }

        @Test
        @Timeout(3, unit = TimeUnit.SECONDS)
        fun `should stop listening after timeout`() = runTest {
            val client = CacheClient {
                uri = RedisURI.create(redisContainer.url)
            }

            val timeout = 1.seconds

            val currentTime = System.currentTimeMillis()

            client.publishAndWaitResponse(
                channelSubscribe = getRandomString(),
                channelPublish = getRandomString(),
                messagePublish = getRandomString(),
                messageSerializer = String.serializer(),
                responseSerializer = UUIDSerializer,
                timeout = timeout
            ) {
                error("Should not be called")
            }

            val elapsedTime = System.currentTimeMillis() - currentTime
            assertTrue(elapsedTime >= timeout.inWholeMilliseconds)
        }

        @Test
        @Timeout(3, unit = TimeUnit.SECONDS)
        fun `should cancel created job for subscribe when finish by timeout`() = runTest {
            val client = CacheClient {
                uri = RedisURI.create(redisContainer.url)
            }

            val timeout = 1.seconds
            val coroutineScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
            assertEquals(coroutineScope.coroutineContext.job.children.count(), 0)

            client.publishAndWaitResponse(
                channelSubscribe = getRandomString(),
                channelPublish = getRandomString(),
                messagePublish = getRandomString(),
                messageSerializer = String.serializer(),
                responseSerializer = UUIDSerializer,
                timeout = timeout
            ) {
                error("Should not be called")
            }

            assertEquals(coroutineScope.coroutineContext.job.children.count(), 0)
        }
    }
}