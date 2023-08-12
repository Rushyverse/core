package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.data.IFriendService
import com.github.rushyverse.core.data.IGuildService
import com.github.rushyverse.core.data.IPlayerService

/**
 * A class that will defer the requesting of entities to a [supplier].
 * Copies of this class with a different [supplier] can be made through [withStrategy].
 *
 * Unless stated otherwise, all members that fetch entities will delegate to the [supplier].
 */
public interface IDatabaseStrategizable {

    /**
     * The supplier used to request entities.
     */
    public val supplier: IDatabaseEntitySupplier

    /**
     * Returns a copy of this class with a new [supplier] provided by the [getStrategy].
     * @param getStrategy A function that will provide a new [IDatabaseEntitySupplier] based
     * on the [DatabaseSupplierConfiguration] configuration.
     */
    public fun withStrategy(
        getStrategy: (DatabaseSupplierConfiguration) -> IDatabaseEntitySupplier
    ): IDatabaseStrategizable
}

/**
 * An abstraction that allows for requesting entities.
 * @property configuration The configuration used to create this supplier.
 *
 * @see DatabaseEntitySupplier
 * @see DatabaseCacheEntitySupplier
 * @see DatabaseStoreEntitySupplier
 * @see DatabaseFallbackEntitySupplier
 */
public interface IDatabaseEntitySupplier : IFriendService, IGuildService, IPlayerService {

    public companion object {

        /**
         * A supplier providing a strategy which exclusively uses database calls to fetch entities.
         * See [DatabaseEntitySupplier] for more details.
         */
        public fun database(configuration: DatabaseSupplierConfiguration): DatabaseEntitySupplier =
            DatabaseEntitySupplier(configuration)

        /**
         * A supplier providing a strategy which exclusively uses cache to fetch entities.
         * See [DatabaseCacheEntitySupplier] for more details.
         */
        public fun cache(configuration: DatabaseSupplierConfiguration): DatabaseCacheEntitySupplier =
            DatabaseCacheEntitySupplier(configuration)

        /**
         * A supplier providing a strategy which exclusively uses database calls to fetch entities.
         * fetched entities are stored in [cache].
         * See [DatabaseStoreEntitySupplier] for more details.
         */
        public fun cachingDatabase(configuration: DatabaseSupplierConfiguration): DatabaseStoreEntitySupplier =
            DatabaseStoreEntitySupplier(cache(configuration), database(configuration))

        /**
         * A supplier providing a strategy which will first operate on the [cache] supplier. When an entity
         * is not present from cache it will be fetched from [database] instead. Operations that return flows
         * will only fall back to rest when the returned flow contained no elements.
         */
        public fun cacheWithDatabaseFallback(
            configuration: DatabaseSupplierConfiguration
        ): DatabaseFallbackEntitySupplier = DatabaseFallbackEntitySupplier(
            getPriority = cache(configuration),
            setPriority = database(configuration)
        )

        /**
         * A supplier providing a strategy which will first operate on the [cache] supplier. When an entity
         * is not present from cache it will be fetched from [cachingDatabase] instead which will
         * update [cache] with fetched elements.
         * Operations that return flows will only fall back to rest when the returned flow contained no elements.
         */
        public fun cacheWithCachingDatabaseFallback(
            configuration: DatabaseSupplierConfiguration
        ): DatabaseFallbackEntitySupplier = DatabaseFallbackEntitySupplier(
            getPriority = cache(configuration),
            setPriority = cachingDatabase(configuration)
        )

    }

    /**
     * Configuration of services to manage data.
     */
    public val configuration: DatabaseSupplierConfiguration
}
