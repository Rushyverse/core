package com.github.rushyverse.core.cache.message

import com.github.rushyverse.core.cache.CacheClient
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationException
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.buildClassSerialDescriptor
import kotlinx.serialization.encoding.*

/**
 * Publish a response message to a channel.
 * A response message is an [IdentifiableMessage] with an [id] and a [message].
 * @receiver Cache client to publish the message.
 * @param channel Channel to publish.
 * @param id ID of the message.
 * @param message Message to publish.
 * @param messageSerializer Serializer to encode the message.
 */
public suspend fun <T> CacheClient.publishIdentifiableMessage(
    channel: String,
    id: String,
    message: T,
    messageSerializer: KSerializer<T>
): Unit = publish(
    channel = channel,
    message = IdentifiableMessage(id, message),
    messageSerializer = IdentifiableMessageSerializer(messageSerializer)
)

/**
 * Subscribe to a channel.
 * The message received must be deserializable by [IdentifiableMessageSerializer].
 * The id and the data of the [IIdentifiableMessage] will be passed to the [body] callback.
 * @receiver Cache client to subscribe.
 * @param channel Channel to subscribe.
 * @param messageSerializer Serializer to decode the message.
 * @param scope Scope to launch the flow.
 * @param body Function to execute when a message is received.
 * @return A [Job] to cancel the subscription.
 */
public suspend fun <T> CacheClient.subscribeIdentifiableMessage(
    channel: String,
    messageSerializer: KSerializer<T>,
    scope: CoroutineScope = this,
    body: suspend (id: String, message: T) -> Unit
): Job = subscribeIdentifiableMessage(
    channels = arrayOf(channel),
    messageSerializer = messageSerializer,
    scope = scope
) { _, id, message ->
    body(id, message)
}

/**
 * Subscribe to multiple channels.
 * The message received must be deserializable by [IdentifiableMessageSerializer].
 * The id and the data of the [IIdentifiableMessage] will be passed to the [body] callback.
 * @receiver Cache client to subscribe.
 * @param channels Channels to subscribe.
 * @param messageSerializer Serializer to decode the message.
 * @param scope Scope to launch the flow.
 * @param body Function to execute when a message is received.
 * @return A [Job] to cancel the subscription.
 */
public suspend fun <T> CacheClient.subscribeIdentifiableMessage(
    channels: Array<String>,
    messageSerializer: KSerializer<T>,
    scope: CoroutineScope = this,
    body: suspend (channel: String, id: String, message: T) -> Unit
): Job = subscribe(
    channels = channels,
    messageSerializer = IdentifiableMessageSerializer(messageSerializer),
    scope = scope
) { channel, message ->
    body(channel, message.id, message.data)
}

/**
 * A message that can be identified by an id.
 * @param T The type of the data.
 * @property id ID of the message.
 * @property data The data of the message.
 */
public interface IIdentifiableMessage<T> {
    public val id: String
    public val data: T
}

/**
 * A message that can be identified by an id.
 * @param T The type of the data.
 * @property id ID of the message.
 * @property data The data of the message.
 */
public data class IdentifiableMessage<T>(override val id: String, override val data: T) : IIdentifiableMessage<T>

/**
 * A serializer for [IdentifiableMessage].
 * @param T The type of the data.
 * @property dataSerializer The serializer for the [data field][IIdentifiableMessage.data].
 */
public class IdentifiableMessageSerializer<T>(private val dataSerializer: KSerializer<T>) :
    KSerializer<IIdentifiableMessage<T>> {

    private val stringSerializer = String.serializer()

    override val descriptor: SerialDescriptor = buildClassSerialDescriptor("Message") {
        element("id", stringSerializer.descriptor)
        element("data", dataSerializer.descriptor)
    }

    override fun serialize(encoder: Encoder, value: IIdentifiableMessage<T>) {
        encoder.encodeStructure(descriptor) {
            encodeSerializableElement(descriptor, 0, stringSerializer, value.id)
            encodeSerializableElement(descriptor, 1, dataSerializer, value.data)
        }
    }

    override fun deserialize(decoder: Decoder): IIdentifiableMessage<T> {
        return decoder.decodeStructure(descriptor) {
            var id: String? = null
            var data: T? = null

            if (decodeSequentially()) {
                id = decodeSerializableElement(descriptor, 0, stringSerializer)
                data = decodeSerializableElement(descriptor, 1, dataSerializer)
            } else {
                while (true) {
                    when (val index = decodeElementIndex(descriptor)) {
                        0 -> id = decodeSerializableElement(descriptor, index, stringSerializer)
                        1 -> data = decodeSerializableElement(descriptor, index, dataSerializer)
                        CompositeDecoder.DECODE_DONE -> break
                        else -> error("Unexpected index: $index")
                    }
                }
            }

            IdentifiableMessage(
                id = id ?: throw SerializationException("The field id is missing"),
                data = data ?: throw SerializationException("The field data is missing")
            )
        }
    }
}