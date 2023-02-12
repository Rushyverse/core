package com.github.rushyverse.core.cache.message

import kotlinx.serialization.KSerializer
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.buildClassSerialDescriptor
import kotlinx.serialization.encoding.*

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
                id = id ?: error("The field id is missing"),
                data = data ?: error("The field data is missing")
            )
        }
    }


}