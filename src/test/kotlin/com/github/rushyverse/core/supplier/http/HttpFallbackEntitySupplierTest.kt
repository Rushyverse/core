package com.github.rushyverse.core.supplier.http

import com.github.rushyverse.core.utils.createProfileId
import com.github.rushyverse.core.utils.createProfileSkin
import com.github.rushyverse.core.utils.getRandomString
import io.mockk.*
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class HttpFallbackEntitySupplierTest {

    private lateinit var fallbackEntitySupplier: HttpFallbackEntitySupplier

    @BeforeTest
    fun onBefore() {
        fallbackEntitySupplier = HttpFallbackEntitySupplier(mockk(getRandomString()), mockk(getRandomString()))
    }

    @Test
    fun `get configuration will get from first supplier`() = runTest {
        val firstSupplier = fallbackEntitySupplier.first

        val configuration = mockk<HttpSupplierConfiguration>(getRandomString())
        every { firstSupplier.configuration } returns configuration

        assertEquals(configuration, fallbackEntitySupplier.configuration)
        verify(exactly = 1) { firstSupplier.configuration }
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

            coEvery { first.getSkinById(any()) } returns null
            coEvery { second.getSkinById(any()) } returns null

            assertNull(fallbackEntitySupplier.getSkinById(id))

            coVerify(exactly = 1) { first.getSkinById(id) }
            coVerify(exactly = 1) { second.getSkinById(id) }
        }

        @Test
        override fun `data is present into one of both supplier`() = runTest {
            val first = fallbackEntitySupplier.first
            val second = fallbackEntitySupplier.second
            val skin = createProfileSkin()
            val id = skin.id

            coEvery { first.getSkinById(any()) } returns skin
            coEvery { second.getSkinById(any()) } returns null

            assertEquals(skin, fallbackEntitySupplier.getSkinById(id))
            coVerify(exactly = 1) { first.getSkinById(id) }
            coVerify(exactly = 0) { second.getSkinById(id) }

            coEvery { first.getSkinById(any()) } returns null
            coEvery { second.getSkinById(any()) } returns skin

            assertEquals(skin, fallbackEntitySupplier.getSkinById(id))

            coVerify(exactly = 2) { first.getSkinById(id) }
            coVerify(exactly = 1) { second.getSkinById(id) }
        }

    }
}
