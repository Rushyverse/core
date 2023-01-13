package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.data.IFriendService

/**
 * A class that will defer the requesting of entities to a [supplier].
 * Copies of this class with a different [supplier] can be made through [withStrategy].
 *
 * Unless stated otherwise, all members that fetch entities will delegate to the [supplier].
 */
interface IDatabaseStrategizable {

    /**
     * The supplier used to request entities.
     */
    val supplier: IDatabaseEntitySupplier


    /**
     * Returns a copy of this class with a new [supplier] provided by the [strategy].
     */
    fun withStrategy(strategy: IDatabaseEntitySupplier): IDatabaseStrategizable
}

/**
 * An abstraction that allows for requesting entities.
 *
 * @see DatabaseEntitySupplier
 * @see DatabaseCacheEntitySupplier
 * @see DatabaseStoreEntitySupplier
 * @see DatabaseFallbackEntitySupplier
 */
interface IDatabaseEntitySupplier : IFriendService {

    companion object {

        /**
         * A supplier providing a strategy which exclusively uses database calls to fetch entities.
         * See [DatabaseEntitySupplier] for more details.
         */
        fun database(configuration: DatabaseSupplierServices): DatabaseEntitySupplier =
            DatabaseEntitySupplier(configuration.friendServices.second)

        /**
         * A supplier providing a strategy which exclusively uses cache to fetch entities.
         * See [DatabaseCacheEntitySupplier] for more details.
         */
        fun cache(configuration: DatabaseSupplierServices): DatabaseCacheEntitySupplier =
            DatabaseCacheEntitySupplier(configuration.friendServices.first)

        /**
         * A supplier providing a strategy which exclusively uses database calls to fetch entities.
         * fetched entities are stored in [cache].
         * See [DatabaseStoreEntitySupplier] for more details.
         */
        fun cachingDatabase(configuration: DatabaseSupplierServices): DatabaseStoreEntitySupplier =
            DatabaseStoreEntitySupplier(cache(configuration), database(configuration))

        /**
         * A supplier providing a strategy which will first operate on the [cache] supplier. When an entity
         * is not present from cache it will be fetched from [database] instead. Operations that return flows
         * will only fall back to rest when the returned flow contained no elements.
         */
        fun cacheWithDatabaseFallback(configuration: DatabaseSupplierServices): DatabaseFallbackEntitySupplier =
            DatabaseFallbackEntitySupplier(getPriority = cache(configuration), setPriority = database(configuration))

        /**
         * A supplier providing a strategy which will first operate on the [cache] supplier. When an entity
         * is not present from cache it will be fetched from [cachingDatabase] instead which will update [cache] with fetched elements.
         * Operations that return flows will only fall back to rest when the returned flow contained no elements.
         */
        fun cacheWithCachingDatabaseFallback(configuration: DatabaseSupplierServices): DatabaseFallbackEntitySupplier =
            DatabaseFallbackEntitySupplier(getPriority = cache(configuration), setPriority = cachingDatabase(configuration))

    }
}
