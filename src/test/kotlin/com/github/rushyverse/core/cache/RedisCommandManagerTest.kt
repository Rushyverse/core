@file:OptIn(ExperimentalLettuceCoroutinesApi::class)

package com.github.rushyverse.core.cache

import com.github.rushyverse.core.container.createRedisContainer
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisURI
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.coroutines
import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection
import io.lettuce.core.support.BoundedPoolConfig
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.assertThrows
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals

@Testcontainers
class RedisCommandManagerTest {

    companion object {
        @JvmStatic
        @Container
        private val redisContainer = createRedisContainer()
    }

    @Nested
    inner class Invoke {

        @Test
        fun `create Connection Manager instance`() = runTest {
            val maxConnections = 10
            val connectionManager = RedisConnectionManager(
                RedisClient.create(),
                ByteArrayCodec.INSTANCE,
                RedisURI.create(redisContainer.url),
                BoundedPoolConfig.builder().maxTotal(maxConnections).build()
            )

            val poolStateful = connectionManager.poolStateful
            assertEquals(maxConnections, poolStateful.maxTotal)
            assertEquals(0, poolStateful.objectCount)
            assertEquals(0, poolStateful.idle)

            val poolPubSub = connectionManager.poolPubSub
            assertEquals(maxConnections, poolPubSub.maxTotal)
            assertEquals(0, poolPubSub.objectCount)
            assertEquals(0, poolPubSub.idle)

            connectionManager.closeAsync().await()
        }

    }

    @Nested
    inner class GetStatefulConnection {

        private lateinit var connectionManager: RedisConnectionManager

        @BeforeTest
        fun onBefore() = runBlocking {
            connectionManager = RedisConnectionManager(
                RedisClient.create(),
                ByteArrayCodec.INSTANCE,
                RedisURI.create(redisContainer.url),
                BoundedPoolConfig.builder().maxTotal(1).build()
            )
        }

        @AfterTest
        fun onAfter(): Unit = runBlocking {
            connectionManager.closeAsync().await()
        }

        @Test
        fun `should interact with redis`() = runTest {
            val connectionStateful = connectionManager.getStatefulConnection()
            assertEquals("PONG", connectionStateful.coroutines().ping())
            releaseConnection(connectionStateful)
        }

        @Test
        fun `should create new connection and idle it`() = runTest {
            val pool = connectionManager.poolStateful
            assertEquals(0, pool.objectCount)
            assertEquals(0, pool.idle)
            assertEquals(1, pool.maxTotal)

            var connectionStateful = connectionManager.getStatefulConnection()
            assertEquals(1, pool.objectCount)
            assertEquals(0, pool.idle)
            assertEquals(1, pool.maxTotal)
            releaseConnection(connectionStateful)

            assertEquals(1, pool.objectCount)
            assertEquals(1, pool.idle)
            assertEquals(1, pool.maxTotal)

            connectionStateful = connectionManager.getStatefulConnection()
            assertEquals(1, pool.objectCount)
            assertEquals(0, pool.idle)
            assertEquals(1, pool.maxTotal)
            releaseConnection(connectionStateful)
        }

        @Test
        fun `throw exception when connection is not available`() = runTest {
            val connectionStateful = connectionManager.getStatefulConnection()

            val exception = assertThrows<NoSuchElementException> {
                connectionManager.getStatefulConnection()
            }
            assertEquals("Pool exhausted", exception.message)
            releaseConnection(connectionStateful)
        }

        private fun releaseConnection(connectionStateful: StatefulRedisConnection<ByteArray, ByteArray>) {
            connectionManager.poolStateful.release(connectionStateful)
        }

    }

    @Nested
    inner class GetPubSubConnection {

        private lateinit var connectionManager: RedisConnectionManager

        @BeforeTest
        fun onBefore() = runBlocking {
            connectionManager = RedisConnectionManager(
                RedisClient.create(),
                ByteArrayCodec.INSTANCE,
                RedisURI.create(redisContainer.url),
                BoundedPoolConfig.builder().maxTotal(1).build()
            )
        }

        @AfterTest
        fun onAfter(): Unit = runBlocking {
            connectionManager.closeAsync().await()
        }

        @Test
        fun `should interact with redis`() = runTest {
            val connectionStateful = connectionManager.getPubSubConnection()
            assertEquals("PONG", connectionStateful.coroutines().ping())
            releaseConnection(connectionStateful)
        }

        @Test
        fun `should create new connection and idle it`() = runTest {
            val pool = connectionManager.poolPubSub
            assertEquals(0, pool.objectCount)
            assertEquals(0, pool.idle)
            assertEquals(1, pool.maxTotal)

            var connectionStateful = connectionManager.getPubSubConnection()
            assertEquals(1, pool.objectCount)
            assertEquals(0, pool.idle)
            assertEquals(1, pool.maxTotal)
            releaseConnection(connectionStateful)

            assertEquals(1, pool.objectCount)
            assertEquals(1, pool.idle)
            assertEquals(1, pool.maxTotal)

            connectionStateful = connectionManager.getPubSubConnection()
            assertEquals(1, pool.objectCount)
            assertEquals(0, pool.idle)
            assertEquals(1, pool.maxTotal)
            releaseConnection(connectionStateful)
        }

        @Test
        fun `throw exception when connection is not available`() = runTest {
            val connectionStateful = connectionManager.getPubSubConnection()

            val exception = assertThrows<NoSuchElementException> {
                connectionManager.getPubSubConnection()
            }
            assertEquals("Pool exhausted", exception.message)
            releaseConnection(connectionStateful)
        }

        private fun releaseConnection(connectionStateful: StatefulRedisPubSubConnection<ByteArray, ByteArray>) {
            connectionManager.poolPubSub.release(connectionStateful)
        }

    }

    @Nested
    inner class ReleaseStatefulConnection {

        private lateinit var connectionManager: RedisConnectionManager

        @BeforeTest
        fun onBefore() = runBlocking {
            connectionManager = RedisConnectionManager(
                RedisClient.create(),
                ByteArrayCodec.INSTANCE,
                RedisURI.create(redisContainer.url),
                BoundedPoolConfig.builder().maxTotal(1).build()
            )
        }

        @AfterTest
        fun onAfter(): Unit = runBlocking {
            connectionManager.closeAsync().await()
        }

        @Test
        fun `should release connection`() = runTest {
            val connectionStateful = connectionManager.getStatefulConnection()
            val pool = connectionManager.poolStateful
            assertEquals(1, pool.objectCount)
            assertEquals(0, pool.idle)
            assertEquals(1, pool.maxTotal)

            connectionManager.releaseConnection(connectionStateful)

            assertEquals(1, pool.objectCount)
            assertEquals(1, pool.idle)
            assertEquals(1, pool.maxTotal)
        }

    }

    @Nested
    inner class ReleasePubSubConnection {

        private lateinit var connectionManager: RedisConnectionManager

        @BeforeTest
        fun onBefore() = runBlocking {
            connectionManager = RedisConnectionManager(
                RedisClient.create(),
                ByteArrayCodec.INSTANCE,
                RedisURI.create(redisContainer.url),
                BoundedPoolConfig.builder().maxTotal(1).build()
            )
        }

        @AfterTest
        fun onAfter(): Unit = runBlocking {
            connectionManager.closeAsync().await()
        }

        @Test
        fun `should release connection`() = runTest {
            val connectionStateful = connectionManager.getPubSubConnection()
            val pool = connectionManager.poolPubSub
            assertEquals(1, pool.objectCount)
            assertEquals(0, pool.idle)
            assertEquals(1, pool.maxTotal)

            connectionManager.releaseConnection(connectionStateful)

            assertEquals(1, pool.objectCount)
            assertEquals(1, pool.idle)
            assertEquals(1, pool.maxTotal)
        }

    }

    @Nested
    inner class Close {

        @Test
        fun `should close pools`() = runTest {
            val connectionManager = RedisConnectionManager(
                RedisClient.create(),
                ByteArrayCodec.INSTANCE,
                RedisURI.create(redisContainer.url),
                BoundedPoolConfig.builder().maxTotal(1).build()
            )

            val poolStateful = connectionManager.poolStateful
            val poolPubSub = connectionManager.poolPubSub

            connectionManager.closeAsync().await()

            var ex = assertThrows<IllegalStateException> {
                poolStateful.acquire().await()
            }
            assertEquals("AsyncPool is closed", ex.message)

            ex = assertThrows {
                poolPubSub.acquire().await()
            }
            assertEquals("AsyncPool is closed", ex.message)

        }
    }
}