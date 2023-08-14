package com.github.rushyverse.core.data.mojang

import com.github.rushyverse.core.data.MojangService
import com.github.rushyverse.core.supplier.http.HttpSupplierConfiguration
import com.github.rushyverse.core.supplier.http.IHttpEntitySupplier
import com.github.rushyverse.core.utils.randomProfileId
import com.github.rushyverse.core.utils.randomProfileSkin
import com.github.rushyverse.core.utils.randomString
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import kotlin.test.BeforeTest
import kotlin.test.assertEquals
import kotlin.test.assertNull

class MojangServiceTest {

    private lateinit var serviceImpl: MojangService
    private lateinit var configuration: HttpSupplierConfiguration
    private val supplier get() = serviceImpl.supplier

    @BeforeTest
    fun onBefore() {
        configuration = HttpSupplierConfiguration(
            mockk(randomString()),
            mockk(randomString()),
            mockk(randomString()),
        )

        val mockSupplier = mockk<IHttpEntitySupplier>(randomString())
        every { mockSupplier.configuration } returns configuration

        serviceImpl = MojangService(mockSupplier)
    }

    interface MojangServiceTest {
        fun `data is not found into supplier`()
        fun `data is retrieved from supplier`()
    }

    @Test
    fun `change supplier strategy`() {
        val initStrategy = serviceImpl.supplier
        val newStrategy = mockk<IHttpEntitySupplier>(randomString())

        val newService = serviceImpl.withStrategy {
            assertEquals(configuration, it)
            newStrategy
        }
        assertEquals(newStrategy, newService.supplier)
        assertEquals(initStrategy, serviceImpl.supplier)
    }

    @Nested
    @DisplayName("Get skin by name")
    inner class GetSkinByName : MojangServiceTest {

        @Test
        override fun `data is not found into supplier`() = runTest {
            val profileId = randomProfileId()
            val profileSkin = randomProfileSkin(profileId)
            val id = profileId.id
            val name = profileId.name
            coEvery { supplier.getIdByName(any()) } returns null
            coEvery { supplier.getSkinById(id) } returns profileSkin
            assertNull(serviceImpl.getSkinByName(name))

            coEvery { supplier.getIdByName(name) } returns profileId
            coEvery { supplier.getSkinById(any()) } returns null
            assertNull(serviceImpl.getSkinByName(name))
        }

        @Test
        override fun `data is retrieved from supplier`() = runTest {
            val profileId = randomProfileId()
            val profileSkin = randomProfileSkin(profileId)
            val id = profileId.id
            val name = profileId.name
            coEvery { supplier.getIdByName(name) } returns profileId
            coEvery { supplier.getSkinById(id) } returns profileSkin
            assertEquals(profileSkin, serviceImpl.getSkinByName(name))
        }
    }

    @Nested
    @DisplayName("Get player uuid")
    inner class GetId : MojangServiceTest {

        @Test
        override fun `data is not found into supplier`() = runTest {
            coEvery { supplier.getIdByName(any()) } returns null
            assertNull(serviceImpl.getIdByName(randomString()))
        }

        @Test
        override fun `data is retrieved from supplier`() = runTest {
            val id = randomProfileId()
            val name = id.name
            coEvery { supplier.getIdByName(name) } returns id
            assertEquals(id, serviceImpl.getIdByName(name))
        }
    }

    @Nested
    @DisplayName("Get skin by id")
    inner class GetSkin : MojangServiceTest {

        @Test
        override fun `data is not found into supplier`() = runTest {
            coEvery { supplier.getSkinById(any()) } returns null
            assertNull(serviceImpl.getSkinById(randomString()))
        }

        @Test
        override fun `data is retrieved from supplier`() = runTest {
            val skin = randomProfileSkin()
            val id = skin.id
            coEvery { supplier.getSkinById(id) } returns skin
            assertEquals(skin, serviceImpl.getSkinById(id))
        }
    }
}
