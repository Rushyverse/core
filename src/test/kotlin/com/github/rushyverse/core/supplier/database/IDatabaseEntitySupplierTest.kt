package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.data.IFriendCacheService
import com.github.rushyverse.core.data.IFriendDatabaseService
import io.mockk.mockk
import kotlin.test.Test
import kotlin.test.assertEquals

class IDatabaseEntitySupplierTest {

    @Test
    fun `database supplier corresponding to the class`() {
        val service = mockk<IFriendDatabaseService>()
        val configuration = DatabaseSupplierServices(mockk<IFriendCacheService>() to service)
        val supplier = IDatabaseEntitySupplier.database(configuration)
        assertEquals(DatabaseEntitySupplier::class, supplier::class)
        assertEquals(service, supplier.service)
    }

    @Test
    fun `cache supplier corresponding to the class`() {
        val service = mockk<IFriendCacheService>()
        val configuration = DatabaseSupplierServices(service to mockk())
        val supplier = IDatabaseEntitySupplier.cache(configuration)
        assertEquals(DatabaseCacheEntitySupplier::class, supplier::class)
        assertEquals(service, supplier.friendCacheService)
    }

    @Test
    fun `cachingDatabase supplier corresponding to the class`() {
        val cacheService = mockk<IFriendCacheService>()
        val databaseService = mockk<IFriendDatabaseService>()
        val configuration = DatabaseSupplierServices(cacheService to databaseService)
        val supplier = IDatabaseEntitySupplier.cachingDatabase(configuration)
        assertEquals(DatabaseStoreEntitySupplier::class, supplier::class)
        assertEquals(DatabaseCacheEntitySupplier::class, supplier.cache::class)
        assertEquals(DatabaseEntitySupplier::class, supplier.supplier::class)
    }

    @Test
    fun `cacheWithDatabaseFallback supplier corresponding to the class`() {
        val cacheService = mockk<IFriendCacheService>()
        val databaseService = mockk<IFriendDatabaseService>()
        val configuration = DatabaseSupplierServices(cacheService to databaseService)
        val supplier = IDatabaseEntitySupplier.cacheWithDatabaseFallback(configuration)
        assertEquals(DatabaseFallbackEntitySupplier::class, supplier::class)
        assertEquals(DatabaseCacheEntitySupplier::class, supplier.getPriority::class)
        assertEquals(DatabaseEntitySupplier::class, supplier.setPriority::class)
    }

    @Test
    fun `cacheWithCachingDatabaseFallback supplier corresponding to the class`() {
        val cacheService = mockk<IFriendCacheService>()
        val databaseService = mockk<IFriendDatabaseService>()
        val configuration = DatabaseSupplierServices(cacheService to databaseService)
        val supplier = IDatabaseEntitySupplier.cacheWithCachingDatabaseFallback(configuration)
        assertEquals(DatabaseFallbackEntitySupplier::class, supplier::class)
        assertEquals(DatabaseCacheEntitySupplier::class, supplier.getPriority::class)
        assertEquals(DatabaseStoreEntitySupplier::class, supplier.setPriority::class)
    }

}
