package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.data.IFriendCacheService
import com.github.rushyverse.core.data.IFriendDatabaseService
import com.github.rushyverse.core.data.IGuildCacheService
import io.mockk.mockk
import kotlin.test.Test
import kotlin.test.assertEquals

class IDatabaseEntitySupplierTest {

    @Test
    fun `database supplier corresponding to the class`() {
        val service = mockk<IFriendDatabaseService>()
        val configuration = DatabaseSupplierConfiguration(
            mockk<IFriendCacheService>() to service,
            mockk<IGuildCacheService>() to mockk(),
        )
        val supplier = IDatabaseEntitySupplier.database(configuration)
        assertEquals(DatabaseEntitySupplier::class, supplier::class)
    }

    @Test
    fun `cache supplier corresponding to the class`() {
        val service = mockk<IFriendCacheService>()
        val configuration = DatabaseSupplierConfiguration(
            service to mockk(),
            mockk<IGuildCacheService>() to mockk()
        )
        val supplier = IDatabaseEntitySupplier.cache(configuration)
        assertEquals(DatabaseCacheEntitySupplier::class, supplier::class)
    }

    @Test
    fun `cachingDatabase supplier corresponding to the class`() {
        val cacheService = mockk<IFriendCacheService>()
        val databaseService = mockk<IFriendDatabaseService>()
        val configuration = DatabaseSupplierConfiguration(
            cacheService to databaseService,
            mockk<IGuildCacheService>() to mockk()
        )
        val supplier = IDatabaseEntitySupplier.cachingDatabase(configuration)
        assertEquals(DatabaseStoreEntitySupplier::class, supplier::class)
        assertEquals(DatabaseCacheEntitySupplier::class, supplier.cache::class)
        assertEquals(DatabaseEntitySupplier::class, supplier.supplier::class)
    }

    @Test
    fun `cacheWithDatabaseFallback supplier corresponding to the class`() {
        val cacheService = mockk<IFriendCacheService>()
        val databaseService = mockk<IFriendDatabaseService>()
        val configuration = DatabaseSupplierConfiguration(
            cacheService to databaseService,
            mockk<IGuildCacheService>() to mockk()
        )
        val supplier = IDatabaseEntitySupplier.cacheWithDatabaseFallback(configuration)
        assertEquals(DatabaseFallbackEntitySupplier::class, supplier::class)
        assertEquals(DatabaseCacheEntitySupplier::class, supplier.getPriority::class)
        assertEquals(DatabaseEntitySupplier::class, supplier.setPriority::class)
    }

    @Test
    fun `cacheWithCachingDatabaseFallback supplier corresponding to the class`() {
        val cacheService = mockk<IFriendCacheService>()
        val databaseService = mockk<IFriendDatabaseService>()
        val configuration = DatabaseSupplierConfiguration(
            cacheService to databaseService,
            mockk<IGuildCacheService>() to mockk()
        )
        val supplier = IDatabaseEntitySupplier.cacheWithCachingDatabaseFallback(configuration)
        assertEquals(DatabaseFallbackEntitySupplier::class, supplier::class)
        assertEquals(DatabaseCacheEntitySupplier::class, supplier.getPriority::class)
        assertEquals(DatabaseStoreEntitySupplier::class, supplier.setPriority::class)
    }

}
