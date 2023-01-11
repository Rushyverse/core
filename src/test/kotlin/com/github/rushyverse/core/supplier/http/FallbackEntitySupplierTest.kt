package com.github.rushyverse.core.supplier.http

import com.github.rushyverse.core.utils.createProfileId
import com.github.rushyverse.core.utils.createProfileSkin
import com.github.rushyverse.core.utils.getRandomString
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import kotlinx.coroutines.test.runTest
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
        override fun `data is not present into both supplier`() = runTest {
            val first = fallbackEntitySupplier.first
            val second = fallbackEntitySupplier.second
            val id = createProfileId()
            val name = id.name

            coEvery { first.getIdByName(any()) } returns null
            coEvery { second.getIdByName(any()) } returns null

            assertNull(fallbackEntitySupplier.getIdByName(name))

            coVerify(exactly = 1) { first.getIdByName(name) }
            coVerify(exactly = 1) { second.getIdByName(name) }
        }

        @Test
        override fun `data is present into one of both supplier`() = runTest {
            val first = fallbackEntitySupplier.first
            val second = fallbackEntitySupplier.second
            val id = createProfileId()
            val name = id.name

            coEvery { first.getIdByName(any()) } returns id
            coEvery { second.getIdByName(any()) } returns null

            assertEquals(id, fallbackEntitySupplier.getIdByName(name))
            coVerify(exactly = 1) { first.getIdByName(name) }
            coVerify(exactly = 0) { second.getIdByName(name) }

            coEvery { first.getIdByName(any()) } returns null
            coEvery { second.getIdByName(any()) } returns id

            assertEquals(id, fallbackEntitySupplier.getIdByName(name))

            coVerify(exactly = 2) { first.getIdByName(name) }
            coVerify(exactly = 1) { second.getIdByName(name) }
        }

    }

    @Nested
    @DisplayName("Get skin by id")
    inner class GetSkin : FallbackTest {

        @Test
        override fun `data is not present into both supplier`() = runTest {
            val first = fallbackEntitySupplier.first
            val second = fallbackEntitySupplier.second
            val skin = createProfileSkin()
            val id = skin.id

            coEvery { first.getSkinByUUID(any()) } returns null
            coEvery { second.getSkinByUUID(any()) } returns null

            assertNull(fallbackEntitySupplier.getSkinByUUID(id))

            coVerify(exactly = 1) { first.getSkinByUUID(id) }
            coVerify(exactly = 1) { second.getSkinByUUID(id) }
        }

        @Test
        override fun `data is present into one of both supplier`() = runTest {
            val first = fallbackEntitySupplier.first
            val second = fallbackEntitySupplier.second
            val skin = createProfileSkin()
            val id = skin.id

            coEvery { first.getSkinByUUID(any()) } returns skin
            coEvery { second.getSkinByUUID(any()) } returns null

            assertEquals(skin, fallbackEntitySupplier.getSkinByUUID(id))
            coVerify(exactly = 1) { first.getSkinByUUID(id) }
            coVerify(exactly = 0) { second.getSkinByUUID(id) }

            coEvery { first.getSkinByUUID(any()) } returns null
            coEvery { second.getSkinByUUID(any()) } returns skin

            assertEquals(skin, fallbackEntitySupplier.getSkinByUUID(id))

            coVerify(exactly = 2) { first.getSkinByUUID(id) }
            coVerify(exactly = 1) { second.getSkinByUUID(id) }
        }

    }
}