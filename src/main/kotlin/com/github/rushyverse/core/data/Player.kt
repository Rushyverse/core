package com.github.rushyverse.core.data

import com.github.rushyverse.core.cache.AbstractCacheService
import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.cache.SUCCEED
import com.github.rushyverse.core.data._Player.Companion.player
import com.github.rushyverse.core.serializer.UUIDSerializer
import com.github.rushyverse.core.supplier.database.DatabaseSupplierConfiguration
import com.github.rushyverse.core.supplier.database.IDatabaseEntitySupplier
import com.github.rushyverse.core.supplier.database.IDatabaseStrategizable
import kotlinx.serialization.Serializable
import org.komapper.annotation.KomapperEntity
import org.komapper.annotation.KomapperId
import org.komapper.annotation.KomapperTable
import org.komapper.core.dsl.QueryDsl
import org.komapper.r2dbc.R2dbcDatabase
import java.util.*

/**
 * Service for managing players.
 */
public interface IPlayerService {

    /**
     * Save a player.
     * @param player Player to save.
     * @return `true` if the player was created or updated, `false` otherwise.
     */
    public suspend fun save(player: Player): Boolean

    /**
     * Get a player by their UUID.
     * @param uuid UUID of the player.
     * @return Player if found, `null` otherwise.
     */
    public suspend fun get(uuid: UUID): Player?

    /**
     * Remove a player by their UUID.
     * @param uuid UUID of the player.
     * @return `true` if the player was removed, `false` otherwise.
     */
    public suspend fun remove(uuid: UUID): Boolean
}

/**
 * Service for managing players in cache.
 */
public interface IPlayerCacheService : IPlayerService

/**
 * Service for managing players in database.
 */
public interface IPlayerDatabaseService : IPlayerService

/**
 * Rank of a player.
 */
public enum class Rank {
    PLAYER,
    ADMIN
}

/**
 * Table to store players in database.
 * @property uuid Unique identifier of the player.
 * @property rank Rank of the player.
 */
@KomapperEntity
@KomapperTable("player")
@Serializable
public data class Player(
    @KomapperId
    @Serializable(with = UUIDSerializer::class)
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
    // TODO Check to use map
    public enum class Type(public val key: String) {
        ADD_PLAYER("player:add"),
        REMOVE_PLAYER("player:remove"),
    }

    override suspend fun save(player: Player): Boolean {
        // Steps:
        // 1. Get the player from cache.
        // 2. If the player is not in cache, save it.
        // 3. If the player is in cache, check if it is the same as the one we want to save.
        // 4. If it is not the same, save it.
        // 5. If it is the same, do nothing.
        // 6. Remove the key in [Type.REMOVE_PLAYER] if it exists.
        val addKey = encodeFormattedKeyWithPrefix(Type.ADD_PLAYER.key, player.uuid.toString())
        val removeKey = encodeFormattedKeyWithPrefix(Type.REMOVE_PLAYER.key, player.uuid.toString())

        return cacheClient.connect { connection ->
            val playerStored = connection.get(addKey)

            val result = if(playerStored == null) {
                connection.set(addKey, encodeToByteArray(Player.serializer(), player)) == SUCCEED
            } else {
                val decodedPlayerStored = decodeFromByteArrayOrNull(Player.serializer(), playerStored)
                if(decodedPlayerStored != player) {
                    connection.set(addKey, encodeToByteArray(Player.serializer(), player)) == SUCCEED
                } else {
                    false
                }
            }

            result.or(connection.del(removeKey) == 1L)
        }
    }

    override suspend fun get(uuid: UUID): Player? {
        // Steps:
        // 1. Get the player from cache.
        // 2. If the player is not in cache, return null.
        // 3. If the player is in cache, return it.
        // When a player is saved, the key in [Type.REMOVE_PLAYER] is removed, so we don't need to check if
        // the player is registered as deleted.
        val key = encodeFormattedKeyWithPrefix(Type.ADD_PLAYER.key, uuid.toString())
        return cacheClient.connect { connection ->
            connection.get(key)?.let { decodeFromByteArrayOrNull(Player.serializer(), it) }
        }
    }

    override suspend fun remove(uuid: UUID): Boolean {
        // Steps:
        // 1. Check if id is already registered as deleted.
        // 2. If it is, do nothing.
        // 3. If it is not, save it as deleted.
        // 4. Remove the key in [Type.ADD_PLAYER] if it exists.
        val addKey = encodeFormattedKeyWithPrefix(Type.ADD_PLAYER.key, player.uuid.toString())
        val removeKey = encodeFormattedKeyWithPrefix(Type.REMOVE_PLAYER.key, player.uuid.toString())

        return cacheClient.connect { connection ->
            var result = false
            if(connection.exists(removeKey) == 0L) {
                result = connection.set(removeKey, encodeToByteArray(UUIDSerializer, uuid)) == SUCCEED
            }

            result.or(connection.del(addKey) == 1L)
        }
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
