package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.data.IFriendCacheService
import com.github.rushyverse.core.data.IFriendDatabaseService
import io.mockk.mockk
import kotlin.test.Test
import kotlin.test.assertEquals

class IEntitySupplierTest {

    @Test
    fun `database supplier corresponding to the class`() {
        val service = mockk<IFriendDatabaseService>()
        val configuration = DatabaseSupplierServices(mockk<IFriendCacheService>() to service)
        val supplier = IEntitySupplier.database(configuration)
        assertEquals(DatabaseEntitySupplier::class, supplier::class)
        assertEquals(service, supplier.service)
    }

    @Test
    fun `cache supplier corresponding to the class`() {
        val service = mockk<IFriendCacheService>()
        val configuration = DatabaseSupplierServices(service to mockk())
        val supplier = IEntitySupplier.cache(configuration)
        assertEquals(CacheEntitySupplier::class, supplier::class)
        assertEquals(service, supplier.service)
    }

    @Test
    fun `cachingDatabase supplier corresponding to the class`() {
        val cacheService = mockk<IFriendCacheService>()
        val databaseService = mockk<IFriendDatabaseService>()
        val configuration = DatabaseSupplierServices(cacheService to databaseService)
        val supplier = IEntitySupplier.cachingDatabase(configuration)
        assertEquals(StoreEntitySupplier::class, supplier::class)
        assertEquals(CacheEntitySupplier::class, supplier.cache::class)
        assertEquals(DatabaseEntitySupplier::class, supplier.supplier::class)
    }

    @Test
    fun `cacheWithDatabaseFallback supplier corresponding to the class`() {
        val cacheService = mockk<IFriendCacheService>()
        val databaseService = mockk<IFriendDatabaseService>()
        val configuration = DatabaseSupplierServices(cacheService to databaseService)
        val supplier = IEntitySupplier.cacheWithDatabaseFallback(configuration)
        assertEquals(FallbackEntitySupplier::class, supplier::class)
        assertEquals(CacheEntitySupplier::class, supplier.getPriority::class)
        assertEquals(DatabaseEntitySupplier::class, supplier.setPriority::class)
    }

    @Test
    fun `cacheWithCachingDatabaseFallback supplier corresponding to the class`() {
        val cacheService = mockk<IFriendCacheService>()
        val databaseService = mockk<IFriendDatabaseService>()
        val configuration = DatabaseSupplierServices(cacheService to databaseService)
        val supplier = IEntitySupplier.cacheWithCachingDatabaseFallback(configuration)
        assertEquals(FallbackEntitySupplier::class, supplier::class)
        assertEquals(CacheEntitySupplier::class, supplier.getPriority::class)
        assertEquals(StoreEntitySupplier::class, supplier.setPriority::class)
    }

}
