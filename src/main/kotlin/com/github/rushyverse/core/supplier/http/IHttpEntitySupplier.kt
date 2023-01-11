package com.github.rushyverse.core.supplier.http

import io.github.universeproject.kotlinmojangapi.ProfileId
import io.github.universeproject.kotlinmojangapi.ProfileSkin

/**
 * An abstraction that allows for requesting entities.
 *
 * @see HttpEntitySupplier
 * @see HttpCacheEntitySupplier
 * @see HttpStoreEntitySupplier
 */
public interface IHttpEntitySupplier {

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

    /**
     * Retrieve the id information about a player with his name.
     * @param name Player's name.
     * @return Information about the player's id.
     */
    public suspend fun getUUID(name: String): ProfileId?

    /**
     * Retrieve the skin data for a player.
     * A player is represented by his UUID.
     * @param uuid Player's UUID.
     * @return Information about player's skin.
     */
    public suspend fun getSkin(uuid: String): ProfileSkin?

}
