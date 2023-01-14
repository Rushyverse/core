@file:OptIn(ExperimentalSerializationApi::class, ExperimentalLettuceCoroutinesApi::class)

package com.github.rushyverse.core.cache

import com.github.rushyverse.core.extension.acquire
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisURI
import io.lettuce.core.api.AsyncCloseable
import io.lettuce.core.api.coroutines
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.support.BoundedPoolConfig
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.serialization.BinaryFormat
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.protobuf.ProtoBuf
import mu.KotlinLogging
import java.util.concurrent.CompletableFuture

private val logger = KotlinLogging.logger { }

/**
 * Wrapper of [RedisClient] using pool to manage connection.
 * @property uri URI to connect the client.
 * @property client Redis client.
 * @property binaryFormat Object to encode and decode information.
 * @property connectionManager Connection manager to interact with the cache.
 * @property releasePubSubScope Scope to release the connections.
 */
class CacheClient(
    val uri: RedisURI,
    val client: RedisClient,
    val binaryFormat: BinaryFormat,
    val connectionManager: IRedisConnectionManager,
    coroutineScope: CoroutineScope
) : AsyncCloseable, CoroutineScope by coroutineScope {

    companion object {
        suspend inline operator fun invoke(builder: Builder.() -> Unit): CacheClient =
            Builder().apply(builder).build()
    }

    object Default {
        /**
         * @see [CacheClient.binaryFormat].
         */
        val binaryFormat: ProtoBuf = ProtoBuf {
            encodeDefaults = false
        }

        /**
         * Codec to encode/decode keys and values.
         */
        val codec: ByteArrayCodec get() = ByteArrayCodec.INSTANCE
    }

    /**
     * Builder class to simplify the creation of [CacheClient].
     * @property uri @see [CacheClient.uri].
     * @property client @see [CacheClient.client].
     * @property binaryFormat @see [CacheClient.binaryFormat].
     * @property codec @see Codec to encode/decode keys and values.
     * @property poolConfiguration Configuration to create the pool of connections to interact with cache.
     */
    @Suppress("MemberVisibilityCanBePrivate")
    class Builder {
        lateinit var uri: RedisURI
        var client: RedisClient? = null
        var binaryFormat: BinaryFormat = Default.binaryFormat
        var codec: RedisCodec<ByteArray, ByteArray> = Default.codec
        var poolConfiguration: BoundedPoolConfig? = null
        var coroutineScope: CoroutineScope? = null

        /**
         * Build the instance of [CacheClient] with the values defined in builder.
         * @return A new instance.
         */
        suspend fun build(): CacheClient {
            val redisClient: RedisClient = client ?: RedisClient.create()
            val poolConfig = poolConfiguration ?: BoundedPoolConfig.builder().maxTotal(-1).build()

            return CacheClient(
                uri = uri,
                client = redisClient,
                binaryFormat = binaryFormat,
                connectionManager = RedisConnectionManager(redisClient, codec, uri, poolConfig),
                coroutineScope = coroutineScope ?: CoroutineScope(Dispatchers.IO + SupervisorJob())
            )
        }
    }

    val releasePubSubScope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    /**
     * Use a connection from the [IRedisConnectionManager.poolStateful] to interact with the cache.
     * At the end of the method, the connection is returned to the pool.
     * @param body Function using the connection.
     * @return An instance from [body].
     */
    suspend inline fun <T> connect(body: (RedisCoroutinesCommands<ByteArray, ByteArray>) -> T): T {
        return connectionManager.poolStateful.acquire { body(it.coroutines()) }
    }

    /**
     * Subscribe to a channel.
     * @param channel Channel to subscribe.
     * @param scope Scope to launch the flow.
     * @param body Function to execute when a message is received.
     * @return A [Job] to cancel the subscription.
     */
    suspend fun subscribe(
        channel: String,
        scope: CoroutineScope = this,
        body: suspend (String) -> Unit
    ): Job = subscribe(channel = channel, messageSerializer = String.serializer(), scope = scope, body = body)

    /**
     * Subscribe to a channel.
     * @param channel Channel to subscribe.
     * @param messageSerializer Serializer to decode the message.
     * @param scope Scope to launch the flow.
     * @param body Function to execute when a message is received.
     * @return A [Job] to cancel the subscription.
     */
    suspend fun <T> subscribe(
        channel: String,
        messageSerializer: KSerializer<T>,
        scope: CoroutineScope = this,
        body: suspend (T) -> Unit
    ): Job = subscribe(channels = arrayOf(channel), messageSerializer, scope = scope) { _, message ->
        body(message)
    }

    /**
     * Subscribe to multiple channels.
     * @param channels Channels to subscribe.
     * @param scope Scope to launch the flow.
     * @param body Function to execute when a message is received.
     * @return A [Job] to cancel the subscription.
     */
    suspend fun subscribe(
        vararg channels: String,
        scope: CoroutineScope = this,
        body: suspend (String, String) -> Unit
    ): Job = subscribe(channels = channels, messageSerializer = String.serializer(), scope = scope, body = body)

    /**
     * Subscribe to multiple channels.
     * @param channels Channels to subscribe.
     * @param messageSerializer Serializer to decode the message.
     * @param scope Scope to launch the flow.
     * @param body Function to execute when a message is received.
     * @return A [Job] to cancel the subscription.
     */
    suspend fun <T> subscribe(
        vararg channels: String,
        messageSerializer: KSerializer<T>,
        scope: CoroutineScope = this,
        body: suspend (String, T) -> Unit
    ): Job {
        val stringSerializer = String.serializer()
        val channelsByteArray = channels.map { binaryFormat.encodeToByteArray(stringSerializer, it) }.toTypedArray()

        val connection = connectionManager.getPubSubConnection()
        val reactiveConnection = connection.reactive()
        reactiveConnection.subscribe(*channelsByteArray).awaitFirstOrNull()

        return reactiveConnection.observeChannels().asFlow()
            .onEach {
                val channel = binaryFormat.decodeFromByteArray(stringSerializer, it.channel)
                val message = binaryFormat.decodeFromByteArray(messageSerializer, it.message)
                runCatching { body(channel, message) }.onFailure { throwable -> logger.catching(throwable) }
            }
            .catch { logger.error(it) { "Error while receiving message from cache" } }
            .launchIn(scope)
            .apply {
                invokeOnCompletion {
                    releasePubSubScope.launch {
                        reactiveConnection.unsubscribe(*channelsByteArray).awaitFirstOrNull()
                        connectionManager.releaseConnection(connection)
                    }
                }
            }
    }

    /**
     * Publish a message to a channel.
     * @param channel Channel to publish.
     * @param messageSerializer Serializer to encode the message.
     * @param message Message to publish.
     */
    suspend fun <T> publish(channel: String, messageSerializer: KSerializer<T>, message: T) {
        val channelByteArray = binaryFormat.encodeToByteArray(String.serializer(), channel)
        val messageByteArray = binaryFormat.encodeToByteArray(messageSerializer, message)
        connectionManager.poolPubSub.acquire {
            it.coroutines().publish(channelByteArray, messageByteArray)
        }
    }

    override fun closeAsync(): CompletableFuture<Void> {
        return CompletableFuture.allOf(
            connectionManager.closeAsync(),
            client.shutdownAsync(),
            CompletableFuture.completedFuture(cancel()),
            CompletableFuture.completedFuture(releasePubSubScope.cancel())
        )
    }
}