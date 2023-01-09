package com.github.rushyverse.core.supplier.database

/**
 * A class that will defer the requesting of entities to a [supplier].
 * Copies of this class with a different [supplier] can be made through [withStrategy].
 *
 * Unless stated otherwise, all members that fetch entities will delegate to the [supplier].
 */
public interface Strategizable {

    /**
     * The supplier used to request entities.
     */
    public val supplier: IEntitySupplier


    /**
     * Returns a copy of this class with a new [supplier] provided by the [strategy].
     */
    public fun withStrategy(strategy: IEntitySupplier): Strategizable
}
