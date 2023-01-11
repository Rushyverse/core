package com.github.rushyverse.core.supplier.http

/**
 * A class that will defer the requesting of entities to a [supplier].
 * Copies of this class with a different [supplier] can be made through [withStrategy].
 *
 * Unless stated otherwise, all members that fetch entities will delegate to the [supplier].
 */
interface IHttpStrategizable {

    /**
     * The supplier used to request entities.
     */
    val supplier: IHttpEntitySupplier


    /**
     * Returns a copy of this class with a new [supplier] provided by the [strategy].
     */
    fun withStrategy(strategy: IHttpEntitySupplier): IHttpStrategizable
}
