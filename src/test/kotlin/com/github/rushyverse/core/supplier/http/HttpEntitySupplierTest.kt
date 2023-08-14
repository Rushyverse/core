package com.github.rushyverse.core.supplier.http

import com.github.rushyverse.core.utils.randomProfileId
import com.github.rushyverse.core.utils.randomProfileSkin
import com.github.rushyverse.core.utils.randomString
import io.github.universeproject.kotlinmojangapi.MojangAPI
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

class HttpEntitySupplierTest {

    private lateinit var mojangAPI: MojangAPI
    private lateinit var restEntitySupplier: HttpEntitySupplier

    @BeforeTest
    fun onBefore() {
        mojangAPI = mockk(randomString())
        val configuration = HttpSupplierConfiguration(mojangAPI, mockk(), mockk())
        restEntitySupplier = HttpEntitySupplier(configuration)
    }

    interface RestTest {
        fun `data not found from rest`()
        fun `data is retrieved from rest`()
    }

    @Nested
    @DisplayName("Get player uuid")
    inner class GetId : RestTest {

        @Test
        override fun `data not found from rest`() = runTest {
            coEvery { mojangAPI.getUUID(any<String>()) } returns null
            val id = randomString()
            assertNull(restEntitySupplier.getIdByName(id))
            coVerify(exactly = 1) { mojangAPI.getUUID(id) }
        }

        @Test
        override fun `data is retrieved from rest`() = runTest {
            val profileId = randomProfileId()
            val name = profileId.name
            coEvery { mojangAPI.getUUID(name) } returns profileId
            assertEquals(profileId, restEntitySupplier.getIdByName(name))
            coVerify(exactly = 1) { mojangAPI.getUUID(name) }
        }

    }

    @Nested
    @DisplayName("Get skin by id")
    inner class GetSkin : RestTest {

        @Test
        override fun `data not found from rest`() = runTest {
            coEvery { mojangAPI.getSkin(any()) } returns null
            val name = randomString()
            assertNull(restEntitySupplier.getSkinById(name))
            coVerify(exactly = 1) { mojangAPI.getSkin(name) }
        }

        @Test
        override fun `data is retrieved from rest`() = runTest {
            val skin = randomProfileSkin()
            val id = skin.id
            coEvery { mojangAPI.getSkin(id) } returns skin
            assertEquals(skin, restEntitySupplier.getSkinById(id))
            coVerify(exactly = 1) { mojangAPI.getSkin(id) }
        }

    }
}
