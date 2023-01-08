package com.github.rushyverse.core.supplier.http

import com.github.rushyverse.core.utils.createProfileId
import com.github.rushyverse.core.utils.createProfileSkin
import com.github.rushyverse.core.utils.getRandomString
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class FallbackEntitySupplierTest {

    private lateinit var fallbackEntitySupplier: HttpFallbackEntitySupplier

    @BeforeTest
    fun onBefore() {
        fallbackEntitySupplier = HttpFallbackEntitySupplier(mockk(getRandomString()), mockk(getRandomString()))
    }

    interface FallbackTest {
        fun `data is not present into both supplier`()
        fun `data is present into one of both supplier`()
    }

    @Nested
    @DisplayName("Get player uuid")
    inner class GetId : FallbackTest {

        @Test
        override fun `data is not present into both supplier`() = runBlocking {
            val first = fallbackEntitySupplier.first
            val second = fallbackEntitySupplier.second
            val id = createProfileId()
            val name = id.name

            coEvery { first.getUUID(any()) } returns null
            coEvery { second.getUUID(any()) } returns null

            assertNull(fallbackEntitySupplier.getUUID(name))

            coVerify(exactly = 1) { first.getUUID(name) }
            coVerify(exactly = 1) { second.getUUID(name) }
        }

        @Test
        override fun `data is present into one of both supplier`() = runBlocking {
            val first = fallbackEntitySupplier.first
            val second = fallbackEntitySupplier.second
            val id = createProfileId()
            val name = id.name

            coEvery { first.getUUID(any()) } returns id
            coEvery { second.getUUID(any()) } returns null

            assertEquals(id, fallbackEntitySupplier.getUUID(name))
            coVerify(exactly = 1) { first.getUUID(name) }
            coVerify(exactly = 0) { second.getUUID(name) }

            coEvery { first.getUUID(any()) } returns null
            coEvery { second.getUUID(any()) } returns id

            assertEquals(id, fallbackEntitySupplier.getUUID(name))

            coVerify(exactly = 2) { first.getUUID(name) }
            coVerify(exactly = 1) { second.getUUID(name) }
        }

    }

    @Nested
    @DisplayName("Get skin by id")
    inner class GetSkin : FallbackTest {

        @Test
        override fun `data is not present into both supplier`() = runBlocking {
            val first = fallbackEntitySupplier.first
            val second = fallbackEntitySupplier.second
            val skin = createProfileSkin()
            val id = skin.id

            coEvery { first.getSkin(any()) } returns null
            coEvery { second.getSkin(any()) } returns null

            assertNull(fallbackEntitySupplier.getSkin(id))

            coVerify(exactly = 1) { first.getSkin(id) }
            coVerify(exactly = 1) { second.getSkin(id) }
        }

        @Test
        override fun `data is present into one of both supplier`() = runBlocking {
            val first = fallbackEntitySupplier.first
            val second = fallbackEntitySupplier.second
            val skin = createProfileSkin()
            val id = skin.id

            coEvery { first.getSkin(any()) } returns skin
            coEvery { second.getSkin(any()) } returns null

            assertEquals(skin, fallbackEntitySupplier.getSkin(id))
            coVerify(exactly = 1) { first.getSkin(id) }
            coVerify(exactly = 0) { second.getSkin(id) }

            coEvery { first.getSkin(any()) } returns null
            coEvery { second.getSkin(any()) } returns skin

            assertEquals(skin, fallbackEntitySupplier.getSkin(id))

            coVerify(exactly = 2) { first.getSkin(id) }
            coVerify(exactly = 1) { second.getSkin(id) }
        }

    }
}