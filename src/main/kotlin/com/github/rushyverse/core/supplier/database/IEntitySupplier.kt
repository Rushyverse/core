package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.supplier.SupplierConfiguration
import java.util.*

/**
 * An abstraction that allows for requesting entities.
 *
 * @see DatabaseEntitySupplier
 * @see CacheEntitySupplier
 */
public interface IEntitySupplier {

    public companion object {

        /**
         * A supplier providing a strategy which exclusively uses database calls to fetch entities.
         * See [DatabaseEntitySupplier] for more details.
         */
        public fun database(): DatabaseEntitySupplier = DatabaseEntitySupplier()

        /**
         * A supplier providing a strategy which exclusively uses cache to fetch entities.
         * See [CacheEntitySupplier] for more details.
         */
        public fun cache(configuration: SupplierConfiguration): CacheEntitySupplier =
            CacheEntitySupplier(configuration.friendCache)

        /**
         * A supplier providing a strategy which exclusively uses database calls to fetch entities.
         * fetched entities are stored in [cache].
         * See [StoreEntitySupplier] for more details.
         */
        public fun cachingDatabase(configuration: SupplierConfiguration): StoreEntitySupplier =
            StoreEntitySupplier(cache(configuration), database())

        /**
         * A supplier providing a strategy which will first operate on the [cache] supplier. When an entity
         * is not present from cache it will be fetched from [database] instead. Operations that return flows
         * will only fall back to rest when the returned flow contained no elements.
         */
        public fun cacheWithDatabaseFallback(configuration: SupplierConfiguration): IEntitySupplier =
            FallbackEntitySupplier(getPriority = cache(configuration), setPriority = database())

        /**
         * A supplier providing a strategy which will first operate on the [cache] supplier. When an entity
         * is not present from cache it will be fetched from [cachingDatabase] instead which will update [cache] with fetched elements.
         * Operations that return flows will only fall back to rest when the returned flow contained no elements.
         */
        public fun cacheWithCachingDatabaseFallback(configuration: SupplierConfiguration): IEntitySupplier =
            FallbackEntitySupplier(getPriority = cache(configuration), setPriority = cachingDatabase(configuration))

    }

    public suspend fun addFriend(uuid: UUID, friend: UUID): Boolean

    public suspend fun removeFriend(uuid: UUID, friend: UUID): Boolean

    public suspend fun getFriends(uuid: UUID): Set<UUID>

    public suspend fun isFriend(uuid: UUID, friend: UUID): Boolean

}
