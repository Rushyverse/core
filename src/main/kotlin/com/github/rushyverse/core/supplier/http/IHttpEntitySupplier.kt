package com.github.rushyverse.core.supplier.http

import com.github.rushyverse.core.data.IProfileIdService
import com.github.rushyverse.core.data.IProfileSkinService

/**
 * A class that will defer the requesting of entities to a [supplier].
 * Copies of this class with a different [supplier] can be made through [withStrategy].
 *
 * Unless stated otherwise, all members that fetch entities will delegate to the [supplier].
 */
public interface IHttpStrategizable {

    /**
     * The supplier used to request entities.
     */
    public val supplier: IHttpEntitySupplier


    /**
     * Returns a copy of this class with a new [supplier] provided by the [strategy].
     */
    public fun withStrategy(strategy: IHttpEntitySupplier): IHttpStrategizable
}

/**
 * An abstraction that allows for requesting entities.
 *
 * @see HttpEntitySupplier
 * @see HttpCacheEntitySupplier
 * @see HttpStoreEntitySupplier
 */
public interface IHttpEntitySupplier : IProfileSkinService, IProfileIdService {

    public companion object {

        /**
         * A supplier providing a strategy which exclusively uses database calls to fetch entities.
         * See [HttpEntitySupplier] for more details.
         */
        public fun rest(configuration: HttpSupplierServices): IHttpEntitySupplier =
            HttpEntitySupplier(configuration.mojangAPI)

        /**
         * A supplier providing a strategy which exclusively uses cache to fetch entities.
         * See [HttpCacheEntitySupplier] for more details.
         */
        public fun cache(configuration: HttpSupplierServices): HttpCacheEntitySupplier =
            HttpCacheEntitySupplier(configuration.profileSkinCache, configuration.profileIdCache)

        /**
         * A supplier providing a strategy which exclusively uses database calls to fetch entities.
         * fetched entities are stored in [cache].
         * See [HttpStoreEntitySupplier] for more details.
         */
        public fun cachingRest(configuration: HttpSupplierServices): HttpStoreEntitySupplier =
            HttpStoreEntitySupplier(cache(configuration), rest(configuration))

        /**
         * A supplier providing a strategy which will first operate on the [cache] supplier. When an entity
         * is not present from cache it will be fetched from [rest] instead. Operations that return flows
         * will only fall back to rest when the returned flow contained no elements.
         */
        public fun cacheWithRestFallback(configuration: HttpSupplierServices): IHttpEntitySupplier =
            HttpFallbackEntitySupplier(cache(configuration), rest(configuration))

        /**
         * A supplier providing a strategy which will first operate on the [cache] supplier. When an entity
         * is not present from cache it will be fetched from [cachingRest] instead which will update [cache] with fetched elements.
         * Operations that return flows will only fall back to rest when the returned flow contained no elements.
         */
        public fun cacheWithCachingRestFallback(configuration: HttpSupplierServices): IHttpEntitySupplier =
            HttpFallbackEntitySupplier(cache(configuration), cachingRest(configuration))
    }

}
