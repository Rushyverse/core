@file:OptIn(ExperimentalSerializationApi::class)

package com.github.rushyverse.core.cache

import com.github.rushyverse.core.cache.message.IdentifiableMessage
import com.github.rushyverse.core.cache.message.publishIdentifiableMessage
import com.github.rushyverse.core.cache.message.subscribeIdentifiableMessage
import com.github.rushyverse.core.extension.acquire
import com.github.rushyverse.core.extension.toTypedArray
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
import java.util.*
import java.util.concurrent.CompletableFuture
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes

private val logger = KotlinLogging.logger { }

/**
 * Wrapper of [RedisClient] using pool to manage connection.
 * @property uri URI to connect the client.
 * @property client Redis client.
 * @property binaryFormat Object to encode and decode information.
 * @property connectionManager Connection manager to interact with the cache.
 * @property releasePubSubScope Scope to release the connections.
 */
public class CacheClient(
    public val uri: RedisURI,
    public val client: RedisClient,
    public val binaryFormat: BinaryFormat,
    public val connectionManager: IRedisConnectionManager,
    coroutineScope: CoroutineScope
) : AsyncCloseable, CoroutineScope by coroutineScope {

    public companion object {
        public suspend inline operator fun invoke(builder: Builder.() -> Unit): CacheClient =
            Builder().apply(builder).build()
    }

    public object Default {
        /**
         * @see [CacheClient.binaryFormat].
         */
        public val binaryFormat: ProtoBuf = ProtoBuf {
            encodeDefaults = false
        }

        /**
         * Codec to encode/decode keys and values.
         */
        public val codec: ByteArrayCodec get() = ByteArrayCodec.INSTANCE
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
    public class Builder {
        public lateinit var uri: RedisURI
        public var client: RedisClient? = null
        public var binaryFormat: BinaryFormat = Default.binaryFormat
        public var codec: RedisCodec<ByteArray, ByteArray> = Default.codec
        public var poolConfiguration: BoundedPoolConfig? = null
        public var coroutineScope: CoroutineScope? = null

        /**
         * Build the instance of [CacheClient] with the values defined in builder.
         * @return A new instance.
         */
        public suspend fun build(): CacheClient {
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

    public val releasePubSubScope: CoroutineScope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    /**
     * Use a connection from the [IRedisConnectionManager.poolStateful] to interact with the cache.
     * At the end of the method, the connection is returned to the pool.
     * @param body Function using the connection.
     * @return An instance from [body].
     */
    public suspend inline fun <T> connect(body: (RedisCoroutinesCommands<ByteArray, ByteArray>) -> T): T {
        return connectionManager.poolStateful.acquire { body(it.coroutines()) }
    }

    /**
     * Subscribe to a channel.
     * @param channel Channel to subscribe.
     * @param scope Scope to launch the flow.
     * @param body Function to execute when a message is received.
     * @return A [Job] to cancel the subscription.
     */
    public suspend fun subscribe(
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
    public suspend fun <T> subscribe(
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
    public suspend fun subscribe(
        channels: Array<String>,
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
    public suspend fun <T> subscribe(
        channels: Array<String>,
        messageSerializer: KSerializer<T>,
        scope: CoroutineScope = this,
        body: suspend (String, T) -> Unit
    ): Job {
        require(channels.isNotEmpty()) { "At least one channel is required" }

        val stringSerializer = String.serializer()
        val channelsByteArray = channels.asSequence()
            .map { binaryFormat.encodeToByteArray(stringSerializer, it) }
            .toTypedArray(channels.size)

        val connection = connectionManager.getPubSubConnection()
        val reactiveConnection = connection.reactive()
        reactiveConnection.subscribe(*channelsByteArray).awaitFirstOrNull()

        return reactiveConnection.observeChannels().asFlow()
            .onEach {
                runCatching {
                    val channel = binaryFormat.decodeFromByteArray(stringSerializer, it.channel)
                    val message = binaryFormat.decodeFromByteArray(messageSerializer, it.message)
                    body(channel, message)
                }.onFailure { throwable -> logger.catching(throwable) }
            }
            .catch { logger.error(it) { "Error while receiving message from channels [${channels.joinToString(", ")}]" } }
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
     * @param message Message to publish.
     * @param messageSerializer Serializer to encode the message.
     */
    public suspend fun <T> publish(channel: String, message: T, messageSerializer: KSerializer<T>) {
        val channelByteArray = binaryFormat.encodeToByteArray(String.serializer(), channel)
        val messageByteArray = binaryFormat.encodeToByteArray(messageSerializer, message)
        connectionManager.poolPubSub.acquire {
            it.coroutines().publish(channelByteArray, messageByteArray)
        }
    }

    /**
     * Publish the [messagePublish] to the [channelPublish] and wait for a response on the [channelSubscribe].
     * The [messagePublish] is wrapped in a [IdentifiableMessage] with the [id] as [IdentifiableMessage.id].
     * The [channelSubscribe] must deserialize the message to a [IdentifiableMessage] and check if the [IdentifiableMessage.id] is the same as the [id].
     * The subscription is cancelled after the [timeout] or when the first message with the same [id] is received.
     * @param channelPublish Channel to publish the [messagePublish].
     * @param channelSubscribe Channel to subscribe to the response.
     * @param messagePublish Message that will be wrapped in a [IdentifiableMessage] and published to the [channelPublish].
     * @param messageSerializer Serializer to encode the [messagePublish].
     * @param responseSerializer Serializer to decode the response.
     * @param id Id of the [messagePublish] to match the response.
     * @param subscribeScope Scope to launch the subscription.
     * @param timeout Timeout to cancel the subscription.
     * @param body Function to execute when a message is received.
     * @return The result of the [body] function or null if the [timeout] is reached.
     */
    public suspend fun <M, R, T> publishAndWaitResponse(
        channelPublish: String,
        channelSubscribe: String,
        messagePublish: M,
        messageSerializer: KSerializer<M>,
        responseSerializer: KSerializer<R>,
        id: String = UUID.randomUUID().toString(),
        subscribeScope: CoroutineScope = this,
        timeout: Duration = 1.minutes,
        body: suspend (R) -> T
    ): T? {
        var result: T? = null

        lateinit var subscribeJob: Job
        subscribeJob = subscribeIdentifiableMessage(
            channel = channelSubscribe,
            messageSerializer = responseSerializer,
            scope = subscribeScope
        ) { messageId, data ->
            if (messageId == id) {
                try {
                    result = body(data)
                } finally {
                    subscribeJob.cancel()
                }
            }
        }

        publishIdentifiableMessage(
            channel = channelPublish,
            id = id,
            message = messagePublish,
            messageSerializer = messageSerializer
        )

        val timeoutJob = subscribeScope.launch {
            delay(timeout)
            subscribeJob.cancel()
        }

        subscribeJob.invokeOnCompletion { timeoutJob.cancel() }
        joinAll(subscribeJob, timeoutJob)

        return result
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