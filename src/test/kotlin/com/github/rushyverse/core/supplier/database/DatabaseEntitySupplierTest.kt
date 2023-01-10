package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.data.FriendDatabaseService
import io.mockk.mockk
import kotlin.test.BeforeTest

class DatabaseEntitySupplierTest {

    private lateinit var service: FriendDatabaseService
    private lateinit var databaseEntitySupplier: DatabaseEntitySupplier

    @BeforeTest
    fun onBefore() {
        service = mockk()
        databaseEntitySupplier = DatabaseEntitySupplier(service)
    }

}