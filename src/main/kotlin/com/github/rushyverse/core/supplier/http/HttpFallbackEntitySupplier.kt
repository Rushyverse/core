package com.github.rushyverse.core.supplier.http

import io.github.universeproject.kotlinmojangapi.ProfileId
import io.github.universeproject.kotlinmojangapi.ProfileSkin

/**
 * [IHttpEntitySupplier] that uses the first supplier to retrieve a data, if the value is null,
 * get the data through the second supplier.
 * To keep consistency, it is recommended to use the same [HttpSupplierConfiguration] for both suppliers.
 * The value of [configuration] depends on one of the suppliers.
 * @property first Used first to interact with a data.
 * @property second Used second to interact with a data.
 */
public class HttpFallbackEntitySupplier(
    public val first: IHttpEntitySupplier,
    public val second: IHttpEntitySupplier
) : IHttpEntitySupplier {

    override val configuration: HttpSupplierConfiguration
        get() = first.configuration

    override suspend fun getSkinById(id: String): ProfileSkin? {
        return invoke { it.getSkinById(id) }
    }

    override suspend fun getIdByName(name: String): ProfileId? {
        return invoke { it.getIdByName(name) }
    }

    /**
     * Invoke the body by [first] supplier, if the value returned is null, invoke the body by [second] supplier.
     * @param body Function executed by one or both supplier.
     * @return The instance returns by suppliers.
     */
    private inline fun <reified T> invoke(body: (IHttpEntitySupplier) -> T?): T? {
        return body(first) ?: body(second)
    }
}
