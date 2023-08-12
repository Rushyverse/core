package com.github.rushyverse.core.data

import com.github.rushyverse.core.cache.AbstractCacheService
import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.supplier.database.DatabaseSupplierConfiguration
import com.github.rushyverse.core.supplier.database.IDatabaseEntitySupplier
import com.github.rushyverse.core.supplier.database.IDatabaseStrategizable
import org.komapper.annotation.*
import org.komapper.core.dsl.QueryDsl
import org.komapper.core.dsl.operator.literal
import org.komapper.r2dbc.R2dbcDatabase
import java.util.*

public interface IPlayerService {

    public suspend fun save(player: Player): Boolean

    public suspend fun get(uuid: UUID): Player?

    public suspend fun remove(uuid: UUID): Boolean

}

public interface IPlayerCacheService : IPlayerService

public interface IPlayerDatabaseService : IPlayerService

public enum class Rank {
    PLAYER,
    ADMIN
}

@KomapperEntity
@KomapperTable("player")
public data class Player(
    @KomapperId
    val uuid: UUID,
    val rank: Rank,
)

/**
 * Implementation of [IPlayerCacheService] that uses [CacheClient] to manage data in cache.
 * @property cacheClient Cache client.
 */
public class PlayerCacheService(
    client: CacheClient,
    prefixKey: String = DEFAULT_PREFIX_KEY_USER_CACHE
) : AbstractCacheService(client, prefixKey), IPlayerCacheService {

    /**
     * Type of data stored in cache.
     * The key allows targeting a specific type of data.
     * @property key Key in the cache.
     */
    public enum class Type(public val key: String) {
        ADD_PLAYER("player:add"),
        REMOVE_PLAYER("player:remove"),
    }

    override suspend fun save(player: Player): Boolean {
        TODO("Not yet implemented")
    }

    override suspend fun get(uuid: UUID): Player? {
        TODO("Not yet implemented")
    }

    override suspend fun remove(uuid: UUID): Boolean {
        TODO("Not yet implemented")
    }

}

public class PlayerDatabaseService(public val database: R2dbcDatabase) : IPlayerDatabaseService {

    override suspend fun save(player: Player): Boolean {
        val meta = _Player.player
        val query = QueryDsl.insert(meta)
            .onDuplicateKeyUpdate()
            .set {
                meta.rank eq player.rank
            }
            .where {
                meta.uuid eq player.uuid
            }
            .single(player)

        return database.runQuery(query) > 0
    }

    override suspend fun get(uuid: UUID): Player? {
        val meta = _Player.player
        val query = QueryDsl.from(meta).where {
            meta.uuid eq uuid
        }
        return database.runQuery(query).firstOrNull()
    }

    override suspend fun remove(uuid: UUID): Boolean {
        val meta = _Player.player
        val query = QueryDsl.delete(meta).where {
            meta.uuid eq uuid
        }
        return database.runQuery(query) > 0
    }

}

public class PlayerService(override val supplier: IDatabaseEntitySupplier) : IPlayerService by supplier,
    IDatabaseStrategizable {

    override fun withStrategy(getStrategy: (DatabaseSupplierConfiguration) -> IDatabaseEntitySupplier): PlayerService {
        val newStrategy = getStrategy(supplier.configuration)
        return PlayerService(newStrategy)
    }
}
