package com.github.rushyverse.core.supplier.http

import io.github.universeproject.kotlinmojangapi.ProfileId
import io.github.universeproject.kotlinmojangapi.ProfileSkin

/**
 * Creates supplier providing a strategy which will first operate on this supplier. When an entity
 * is not present from the first supplier it will be fetched from [other] instead. Operations that return flows
 * will only fall back to [other] when the returned flow contained no elements.
 */
public infix fun IHttpEntitySupplier.withFallback(other: IHttpEntitySupplier): IHttpEntitySupplier =
    HttpFallbackEntitySupplier(this, other)

/**
 * [IHttpEntitySupplier] that uses the first supplier to retrieve a data, if the value is null, get the data through the second supplier.
 */
public class HttpFallbackEntitySupplier(
    public val first: IHttpEntitySupplier,
    public val second: IHttpEntitySupplier
) : IHttpEntitySupplier {

    override suspend fun getUUID(name: String): ProfileId? {
        return invoke { it.getUUID(name) }
    }

    override suspend fun getSkin(uuid: String): ProfileSkin? {
        return invoke { it.getSkin(uuid) }
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
