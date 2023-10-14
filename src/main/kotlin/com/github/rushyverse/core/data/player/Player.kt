package com.github.rushyverse.core.data.player

import com.github.rushyverse.core.cache.AbstractCacheService
import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.serializer.UUIDSerializer
import com.github.rushyverse.core.supplier.database.DatabaseSupplierConfiguration
import com.github.rushyverse.core.supplier.database.IDatabaseEntitySupplier
import com.github.rushyverse.core.supplier.database.IDatabaseStrategizable
import kotlinx.serialization.Serializable
import org.komapper.annotation.*
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
    public suspend fun savePlayer(player: Player): Boolean

    /**
     * Get a player by their UUID.
     * @param uuid UUID of the player.
     * @return Player if found, `null` otherwise.
     */
    public suspend fun getPlayer(uuid: UUID): Player?

    /**
     * Remove a player by their UUID.
     * @param uuid UUID of the player.
     * @return `true` if the player was removed, `false` otherwise.
     */
    public suspend fun removePlayer(uuid: UUID): Boolean
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
    @KomapperEnum(type = EnumType.TYPE)
    val rank: Rank?,
    val language: String
)

/**
 * Implementation of [IPlayerCacheService] that uses [CacheClient] to manage data in cache.
 * @property cacheClient Cache client.
 */
public class PlayerCacheService(
    client: CacheClient,
    prefixKey: String = "player:"
) : AbstractCacheService(client, prefixKey), IPlayerCacheService {

    /**
     * Type of data stored in cache.
     * The key allows targeting a specific type of data.
     * @property key Key in the cache.
     */
    public enum class Type(public val key: String) {
        ADD_PLAYER("add"),
        REMOVE_PLAYER("remove"),
    }

    override suspend fun savePlayer(player: Player): Boolean {
        // Steps:
        // 1. Get the player from cache.
        // 2. If the player is not in cache, save it.
        // 3. If the player is in cache, check if it is the same as the one we want to save.
        // 4. If it is not the same, save it.
        // 5. If it is the same, do nothing.
        // 6. Remove the key in [Type.REMOVE_PLAYER] if it exists.
        val uuidByteArray = encodeToByteArray(UUIDSerializer, player.uuid)
        val addKey = encodeKeyWithPrefix(Type.ADD_PLAYER.key)

        return cacheClient.connect { connection ->
            val playerStored = connection.hget(addKey, uuidByteArray)

            val playerSerializer = Player.serializer()
            val result = if (playerStored == null) {
                // Save the player if it is not in cache.
                connection.hset(addKey, uuidByteArray, encodeToByteArray(playerSerializer, player)) == true
            } else {
                // Verify if the player is the same as the one we want to save.
                // If it is not, save it to update the cache.
                val decodedPlayerStored = decodeFromByteArrayOrNull(playerSerializer, playerStored)
                if (decodedPlayerStored != player) {
                    connection.hset(addKey, uuidByteArray, encodeToByteArray(playerSerializer, player))
                    true
                } else {
                    false
                }
            }

            val removeKey = encodeKeyWithPrefix(Type.REMOVE_PLAYER.key)
            result.or(connection.srem(removeKey, uuidByteArray) == 1L)
        }
    }

    override suspend fun getPlayer(uuid: UUID): Player? {
        // Steps:
        // 1. Get the player from cache.
        // 2. If the player is not in cache, return null.
        // 3. If the player is in cache, return it.
        // When a player is saved, the key in [Type.REMOVE_PLAYER] is removed, so we don't need to check if
        // the player is registered as deleted.
        val uuidByteArray = encodeToByteArray(UUIDSerializer, uuid)
        val addKey = encodeKeyWithPrefix(Type.ADD_PLAYER.key)
        return cacheClient.connect { connection ->
            connection.hget(addKey, uuidByteArray)?.let { decodeFromByteArrayOrNull(Player.serializer(), it) }
        }
    }

    override suspend fun removePlayer(uuid: UUID): Boolean {
        // Steps:
        // 1. Check if id is already registered as deleted.
        // 2. If it is, do nothing.
        // 3. If it is not, save it as deleted.
        // 4. Remove the key in [Type.ADD_PLAYER] if it exists.
        val uuidByteArray = encodeToByteArray(UUIDSerializer, uuid)
        val addKey = encodeKeyWithPrefix(Type.ADD_PLAYER.key)
        val removeKey = encodeKeyWithPrefix(Type.REMOVE_PLAYER.key)

        return cacheClient.connect { connection ->
            val removePlayer = connection.sadd(removeKey, uuidByteArray) == 1L
            removePlayer.or(connection.hdel(addKey, uuidByteArray) == 1L)
        }
    }

}

/**
 * Implementation of [IPlayerDatabaseService] that uses [R2dbcDatabase] to manage data in database.
 * @property database Database client.
 */
public class PlayerDatabaseService(public val database: R2dbcDatabase) : IPlayerDatabaseService {

    override suspend fun savePlayer(player: Player): Boolean {
        val meta = _Player.player
        val query = QueryDsl.insert(meta)
            .onDuplicateKeyUpdate()
            .set {
                meta.rank eq player.rank
            }
            .where {
                meta.uuid eq player.uuid
                meta.rank notEq player.rank
            }
            .single(player)

        return database.runQuery(query) > 0
    }

    override suspend fun getPlayer(uuid: UUID): Player? {
        val meta = _Player.player
        val query = QueryDsl.from(meta).where {
            meta.uuid eq uuid
        }
        return database.runQuery(query).firstOrNull()
    }

    override suspend fun removePlayer(uuid: UUID): Boolean {
        val meta = _Player.player
        val query = QueryDsl.delete(meta).where {
            meta.uuid eq uuid
        }
        return database.runQuery(query) > 0
    }

}

/**
 * Service for managing players.
 * @property supplier Supplier of the service.
 */
public class PlayerService(override val supplier: IDatabaseEntitySupplier) : IPlayerService by supplier,
    IDatabaseStrategizable {

    override fun withStrategy(getStrategy: (DatabaseSupplierConfiguration) -> IDatabaseEntitySupplier): PlayerService {
        val newStrategy = getStrategy(supplier.configuration)
        return PlayerService(newStrategy)
    }
}
