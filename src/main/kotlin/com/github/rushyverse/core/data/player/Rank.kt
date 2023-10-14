package com.github.rushyverse.core.data.player

import io.r2dbc.spi.Row
import io.r2dbc.spi.Statement
import org.komapper.r2dbc.spi.R2dbcUserDefinedDataType
import kotlin.reflect.KClass

public enum class Rank {
    PLAYER,
    ADMIN
}

public class RankType : R2dbcUserDefinedDataType<Rank> {

    override val name: String = "rank"

    override val klass: KClass<Rank> = Rank::class

    override val r2dbcType: Class<*> = Rank::class.javaObjectType

    override fun getValue(row: Row, index: Int): Rank? {
        return row[index, r2dbcType] as? Rank?
    }

    override fun getValue(row: Row, columnLabel: String): Rank? {
        return row[columnLabel, r2dbcType] as? Rank?
    }

    override fun toString(value: Rank): String {
        return value.name
    }

    override fun setValue(statement: Statement, name: String, value: Rank) {
        statement.bind(name, value)
    }

    override fun setValue(statement: Statement, index: Int, value: Rank) {
        statement.bind(index, value)
    }
}
