package com.github.rushyverse.core.cache.message

import com.github.rushyverse.core.utils.getRandomString
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.buildClassSerialDescriptor
import kotlinx.serialization.descriptors.element
import kotlinx.serialization.encoding.*
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.assertThrows
import kotlin.test.Test
import kotlin.test.assertEquals

@Serializable(with = ColorAsObjectSerializer::class)
data class Color(val rgb: Int)

object ColorAsObjectSerializer : KSerializer<Color> {

    override val descriptor: SerialDescriptor =
        buildClassSerialDescriptor("Color") {
            element<Int>("r")
            element<Int>("g")
            element<Int>("b")
        }

    override fun serialize(encoder: Encoder, value: Color) =
        encoder.encodeStructure(descriptor) {
            encodeIntElement(descriptor, 0, (value.rgb shr 16) and 0xff)
            encodeIntElement(descriptor, 1, (value.rgb shr 8) and 0xff)
            encodeIntElement(descriptor, 2, value.rgb and 0xff)
        }

    override fun deserialize(decoder: Decoder): Color =
        decoder.decodeStructure(descriptor) {
            var r = -1
            var g = -1
            var b = -1
            while (true) {
                when (val index = decodeElementIndex(descriptor)) {
                    0 -> r = decodeIntElement(descriptor, 0)
                    1 -> g = decodeIntElement(descriptor, 1)
                    2 -> b = decodeIntElement(descriptor, 2)
                    CompositeDecoder.DECODE_DONE -> break
                    else -> error("Unexpected index: $index")
                }
            }
            require(r in 0..255 && g in 0..255 && b in 0..255)
            Color((r shl 16) or (g shl 8) or b)
        }

}

class IdentifiableMessageSerializerTest {

    @Nested
    inner class Serialize {

        @Test
        fun `should serialize string message`() {
            val serializer = IdentifiableMessageSerializer(String.serializer())
            val id = getRandomString()
            val message = IdentifiableMessage(id, "data")
            val expected = "{\"id\":\"$id\",\"data\":\"data\"}"
            val actual = Json.encodeToString(serializer, message)
            assertEquals(expected, actual)
        }

        @Test
        fun `should serialize message`() {
            val serializer = IdentifiableMessageSerializer(ColorAsObjectSerializer)
            val id = getRandomString()
            val message = IdentifiableMessage(id, Color(0x05F0C1))
            val expected = "{\"id\":\"$id\",\"data\":{\"r\":5,\"g\":240,\"b\":193}}"
            val actual = Json.encodeToString(serializer, message)
            assertEquals(expected, actual)
        }

    }

    @Nested
    inner class Deserialize {

        @Test
        fun `should deserialize string message`() {
            val serializer = IdentifiableMessageSerializer(String.serializer())
            val id = getRandomString()
            val data = getRandomString()
            val message = IdentifiableMessage(id, data)
            val expected = "{\"id\":\"$id\",\"data\":\"$data\"}"
            val actual = Json.decodeFromString(serializer, expected)
            assertEquals(message, actual)
        }

        @Test
        fun `should deserialize message`() {
            val serializer = IdentifiableMessageSerializer(ColorAsObjectSerializer)
            val id = getRandomString()
            val message = IdentifiableMessage(id, Color(0x05F0C1))
            val expected = "{\"id\":\"$id\",\"data\":{\"r\":5,\"g\":240,\"b\":193}}"
            val actual = Json.decodeFromString(serializer, expected)
            assertEquals(message, actual)
        }

        @Test
        fun `should throws exception if id is missing`() {
            val serializer = IdentifiableMessageSerializer(ColorAsObjectSerializer)
            val expected = "{\"data\":{\"r\":5,\"g\":240,\"b\":193}}"
            val exception = assertThrows<SerializationException> {
                Json.decodeFromString(serializer, expected)
            }
            assertEquals("The field id is missing", exception.message)
        }

        @Test
        fun `should throws exception if data is missing`() {
            val serializer = IdentifiableMessageSerializer(ColorAsObjectSerializer)
            val expected = "{\"id\":\"test\"}"
            val exception = assertThrows<SerializationException> {
                Json.decodeFromString(serializer, expected)
            }
            assertEquals("The field data is missing", exception.message)
        }
    }
}