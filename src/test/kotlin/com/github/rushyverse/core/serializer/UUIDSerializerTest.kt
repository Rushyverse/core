package com.github.rushyverse.core.serializer

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonPrimitive
import java.util.*
import kotlin.test.Test
import kotlin.test.assertEquals

class UUIDSerializerTest {

    @Test
    fun `should serialize UUID`() {
        val uuid = UUID.randomUUID()
        val serialized = Json.encodeToJsonElement(UUIDSerializer, uuid)
        assertEquals(uuid.toString(), serialized.jsonPrimitive.content)
    }

    @Test
    fun `should deserialize UUID`() {
        val uuid = UUID.randomUUID()
        val serialized = Json.encodeToJsonElement(UUIDSerializer, uuid)
        val deserialized = Json.decodeFromJsonElement(UUIDSerializer, serialized)
        assertEquals(uuid, deserialized)
    }
}
