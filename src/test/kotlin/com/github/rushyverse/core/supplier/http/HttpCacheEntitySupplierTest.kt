package com.github.rushyverse.core.supplier.http

import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.data.ProfileIdCacheService
import com.github.rushyverse.core.data.ProfileSkinCacheService
import com.github.rushyverse.core.utils.createProfileId
import com.github.rushyverse.core.utils.createProfileSkin
import io.mockk.coEvery
import io.mockk.coJustRun
import io.mockk.coVerify
import io.mockk.mockk
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Nested
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals

class HttpCacheEntitySupplierTest {

    private lateinit var cacheClient: CacheClient

    @BeforeTest
    fun onBefore() {
        cacheClient = mockk()
    }

    @Nested
    inner class ProfileIdService {

        @Nested
        inner class DefaultParameter {

            @Test
            fun `default values`() {
                val service = ProfileIdCacheService(cacheClient)
                assertEquals("profileId:", service.prefixKey)
            }

        }

        @Test
        fun `get id use the mock method`() = runTest {
            val cacheService = mockk<ProfileIdCacheService>()
            val configuration = HttpSupplierConfiguration(mockk(), mockk(), cacheService)
            val supplier = HttpCacheEntitySupplier(configuration)

            val profile = createProfileId()
            val name = profile.name
            coEvery { cacheService.getIdByName(name) } returns profile

            assertEquals(profile, supplier.getIdByName(name))
            coVerify(exactly = 1) { cacheService.getIdByName(name) }
        }

        @Test
        fun `save id use the mock method`() = runTest {
            val cacheService = mockk<ProfileIdCacheService>()
            val configuration = HttpSupplierConfiguration(mockk(), mockk(), cacheService)
            val supplier = HttpCacheEntitySupplier(configuration)

            val profile = createProfileId()
            coJustRun { cacheService.save(profile) }

            supplier.save(profile)
            coVerify(exactly = 1) { cacheService.save(profile) }
        }

    }

    @Nested
    inner class ProfileSkinService {

        @Nested
        inner class DefaultParameter {

            @Test
            fun `default values`() {
                val service = ProfileSkinCacheService(cacheClient)
                assertEquals("skin:", service.prefixKey)
            }

        }

        @Test
        fun `get skin use the mock method`() = runTest {
            val profileSkinCacheService = mockk<ProfileSkinCacheService>()
            val configuration = HttpSupplierConfiguration(mockk(), profileSkinCacheService, mockk())
            val supplier = HttpCacheEntitySupplier(configuration)

            val profile = createProfileSkin()
            val uuid = profile.id
            coEvery { profileSkinCacheService.getSkinById(uuid) } returns profile

            assertEquals(profile, supplier.getSkinById(uuid))
            coVerify(exactly = 1) { profileSkinCacheService.getSkinById(uuid) }
        }

        @Test
        fun `save id use the mock method`() = runTest {
            val profileSkinCacheService = mockk<ProfileSkinCacheService>()
            val configuration = HttpSupplierConfiguration(mockk(), profileSkinCacheService, mockk())
            val supplier = HttpCacheEntitySupplier(configuration)

            val profile = createProfileSkin()
            coJustRun { profileSkinCacheService.save(profile) }

            supplier.save(profile)
            coVerify(exactly = 1) { profileSkinCacheService.save(profile) }
        }
    }
}