package com.github.rushyverse.core.data.profileId

import com.github.rushyverse.core.data.ProfileIdService
import com.github.rushyverse.core.supplier.http.IHttpEntitySupplier
import com.github.rushyverse.core.utils.createProfileId
import com.github.rushyverse.core.utils.getRandomString
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import kotlin.test.BeforeTest
import kotlin.test.assertEquals
import kotlin.test.assertNull

class ProfileIdServiceTest {

    private lateinit var serviceImpl: ProfileIdService
    private val supplier get() = serviceImpl.supplier

    @BeforeTest
    fun onBefore() {
        serviceImpl = ProfileIdService(mockk(getRandomString()))
    }

    interface ProfileIdServiceServiceTest {
        fun `data is not found into supplier`()
        fun `data is retrieved from supplier`()
    }

    @Test
    fun `change supplier strategy`() {
        val initStrategy = serviceImpl.supplier
        val newStrategy = mockk<IHttpEntitySupplier>(getRandomString())

        val newService = serviceImpl.withStrategy(newStrategy)
        assertEquals(newStrategy, newService.supplier)
        assertEquals(initStrategy, serviceImpl.supplier)
    }

    @Nested
    @DisplayName("Get by name")
    inner class GetByName : ProfileIdServiceServiceTest {

        @Test
        override fun `data is not found into supplier`() = runTest {
            coEvery { supplier.getUUID(any()) } returns null
            assertNull(serviceImpl.getByName(getRandomString()))
        }

        @Test
        override fun `data is retrieved from supplier`() = runTest {
            val profile = createProfileId()
            val name = profile.name
            coEvery { supplier.getUUID(name) } returns profile
            assertEquals(profile, serviceImpl.getByName(name))
            coVerify(exactly = 1) { supplier.getUUID(name) }
        }
    }
}