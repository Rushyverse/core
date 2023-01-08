package com.github.rushyverse.core.data.profileSkin

import com.github.rushyverse.core.data.ProfileSkinService
import com.github.rushyverse.core.supplier.http.IHttpEntitySupplier
import com.github.rushyverse.core.utils.createProfileSkin
import com.github.rushyverse.core.utils.getRandomString
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import kotlin.test.BeforeTest
import kotlin.test.assertEquals
import kotlin.test.assertNull

class ProfileSkinServiceImplTest {

    private lateinit var serviceImpl: ProfileSkinService
    private val supplier get() = serviceImpl.supplier

    @BeforeTest
    fun onBefore() {
        serviceImpl = ProfileSkinService(mockk(getRandomString()))
    }

    interface ProfileSkinServiceTest {
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
    @DisplayName("Get by uuid")
    inner class GetByUUID : ProfileSkinServiceTest {

        @Test
        override fun `data is not found into supplier`() = runBlocking {
            coEvery { supplier.getSkin(any()) } returns null
            assertNull(serviceImpl.getByUUID(getRandomString()))
        }

        @Test
        override fun `data is retrieved from supplier`() = runBlocking {
            val profile = createProfileSkin()
            val id = profile.id
            coEvery { supplier.getSkin(id) } returns profile
            assertEquals(profile, serviceImpl.getByUUID(id))
            coVerify(exactly = 1) { supplier.getSkin(id) }
        }
    }
}