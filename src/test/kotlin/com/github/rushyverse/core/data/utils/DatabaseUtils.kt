package com.github.rushyverse.core.data.utils

import io.r2dbc.spi.ConnectionFactoryOptions
import io.r2dbc.spi.Option
import kotlinx.coroutines.flow.toList
import org.komapper.core.dsl.QueryDsl
import org.komapper.core.dsl.metamodel.EntityMetamodel
import org.komapper.dialect.postgresql.PostgreSqlDialect
import org.komapper.r2dbc.R2dbcDatabase
import org.testcontainers.containers.PostgreSQLContainer

object DatabaseUtils {

    fun createConnectionOptions(container: PostgreSQLContainer<*>) = ConnectionFactoryOptions.builder()
        .option(ConnectionFactoryOptions.DRIVER, PostgreSqlDialect.driver)
        .option(ConnectionFactoryOptions.USER, container.username)
        .option(ConnectionFactoryOptions.PASSWORD, container.password)
        .option(ConnectionFactoryOptions.HOST, container.host)
        .option(ConnectionFactoryOptions.PORT, container.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT))
        .option(ConnectionFactoryOptions.DATABASE, container.databaseName)
        .option(Option.valueOf("DB_CLOSE_DELAY"), "-1")
        .build()

    suspend fun <ENTITY : Any, ID : Any, META : EntityMetamodel<ENTITY, ID, META>> getAll(
        database: R2dbcDatabase,
        meta: META
    ): List<ENTITY> {
        val query = QueryDsl.from(meta)
        return database.flowQuery(query).toList()
    }

}