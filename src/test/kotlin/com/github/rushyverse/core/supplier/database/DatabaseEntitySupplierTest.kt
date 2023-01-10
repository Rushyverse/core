package com.github.rushyverse.core.supplier.database

import com.github.rushyverse.core.container.createPSQLContainer
import com.github.rushyverse.core.data.Friends
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.transactions.transaction
import org.postgresql.Driver
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import kotlin.test.BeforeTest

@Testcontainers
class DatabaseEntitySupplierTest {

    companion object {
        @JvmStatic
        @Container
        private val psqlContainer = createPSQLContainer()
    }

    private lateinit var databaseEntitySupplier: DatabaseEntitySupplier

    @BeforeTest
    fun onBefore() {
        Database.connect(
            url = psqlContainer.jdbcUrl,
            driver = Driver::class.java.name,
            user = psqlContainer.username,
            password = psqlContainer.password
        )
        transaction {
            SchemaUtils.create(Friends)
        }

        databaseEntitySupplier = DatabaseEntitySupplier()
    }

}