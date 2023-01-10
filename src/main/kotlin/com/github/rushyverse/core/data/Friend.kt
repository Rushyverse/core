package com.github.rushyverse.core.data

import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.cache.CacheClient.Default.binaryFormat
import com.github.rushyverse.core.cache.CacheService
import com.github.rushyverse.core.extension.toTypedArray
import com.github.rushyverse.core.serializer.UUIDSerializer
import com.github.rushyverse.core.supplier.database.IEntitySupplier
import com.github.rushyverse.core.supplier.database.Strategizable
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.flow.toSet
import org.jetbrains.exposed.dao.id.IntIdTable
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.transactions.experimental.newSuspendedTransaction
import java.util.*
import kotlin.time.Duration

/**
 * Service to manage the friendship relationship.
 */
public interface IFriendService {

    /**
     * Add a new relationship of friendship between two entities.
     * @param uuid ID of the first entity.
     * @param friend ID of the second entity.
     * @return `true` if the relationship was added successfully, `false` otherwise.
     */
    public suspend fun addFriend(uuid: UUID, friend: UUID): Boolean

    /**
     * Remove a relationship of friendship between two entities.
     * @param uuid ID of the first entity.
     * @param friend ID of the second entity.
     * @return `true` if the relationship was removed successfully, `false` otherwise.
     */
    public suspend fun removeFriend(uuid: UUID, friend: UUID): Boolean

    /**
     * Get all the friends of an entity.
     * @param uuid ID of the entity.
     * @return Set of IDs of the friends.
     */
    public suspend fun getFriends(uuid: UUID): Set<UUID>

    /**
     * Check if two entities are friends.
     * @param uuid ID of the first entity.
     * @param friend ID of the second entity.
     * @return `true` if the two entities are friends, `false` otherwise.
     */
    public suspend fun isFriend(uuid: UUID, friend: UUID): Boolean
}

/**
 * Service to manage the friendship relationship in cache.
 */
public interface IFriendCacheService : IFriendService {

    /**
     * Set the friends of an entity.
     * @param uuid ID of  the entity.
     * @param friends Set of new friends.
     * @return `true` if the friends were set successfully, `false` otherwise.
     */
    public suspend fun setFriends(uuid: UUID, friends: Set<UUID>): Boolean

}

/**
 * Service to manage the friendship relationship in database.
 */
public interface IFriendDatabaseService : IFriendService

/**
 * Table to store the friendship relationship in database.
 */
public object Friends : IntIdTable("friends") {
    public val uuid1: Column<UUID> = uuid("uuid1").uniqueIndex()
    public val uuid2: Column<UUID> = uuid("uuid2").uniqueIndex()
}

/**
 * Implementation of [IFriendCacheService] that uses [CacheClient] to manage data in cache.
 * @property client Cache client.
 * @property expiration Expiration time applied when a new relationship is set.
 * @property duplicateForFriend `true` if all operations should be applied to the friend.
 * When `true`, if `uuid` is friends with `friend`, `friend` will be friends with `uuid`.
 * This behavior is applied for each available operations
 */
public class FriendCacheService(
    public val client: CacheClient,
    public val expiration: Duration? = null,
    prefixKey: String = "friend:",
    public val duplicateForFriend: Boolean = false
) : CacheService(prefixKey), IFriendCacheService {

    override suspend fun addFriend(uuid: UUID, friend: UUID): Boolean {
        return client.connect {
            if (!duplicateForFriend) {
                return@connect addFriend(it, uuid, listOf(friend))
            }

            coroutineScope {
                val duplicateTask = async { addFriend(it, friend, listOf(uuid)) }
                val primaryResult = addFriend(it, uuid, listOf(friend))
                val duplicateResult = duplicateTask.await()
                primaryResult || duplicateResult
            }
        }
    }

    private suspend fun addFriend(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        uuid: UUID,
        friend: Collection<UUID>,
    ): Boolean {
        if (friend.isEmpty()) return true

        val size = friend.size
        val key = encodeKey(binaryFormat, uuid.toString())
        val friends = friend.asSequence().map { encodeToByteArray(binaryFormat, UUIDSerializer, it) }.toTypedArray(size)

        val result = connection.sadd(key, *friends)
        val isAdded = result != null && result > 0
        if (expiration != null && isAdded) {
            connection.pexpire(key, expiration.inWholeMilliseconds)
        }

        return isAdded
    }

    override suspend fun removeFriend(uuid: UUID, friend: UUID): Boolean {
        return client.connect {
            if (!duplicateForFriend) {
                return@connect removeFriend(it, uuid, friend)
            }

            coroutineScope {
                val duplicateTask = async { removeFriend(it, friend, uuid) }
                val primaryResult = removeFriend(it, uuid, friend)
                val duplicateResult = duplicateTask.await()
                primaryResult || duplicateResult
            }
        }
    }

    private suspend fun removeFriend(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        uuid: UUID,
        friend: UUID,
    ): Boolean {
        val key = encodeKey(binaryFormat, uuid.toString())
        val value = encodeToByteArray(binaryFormat, UUIDSerializer, friend)
        val result = connection.srem(key, value)
        val isRemoved = result != null && result > 0

        if (expiration != null && isRemoved) {
            connection.pexpire(key, expiration.inWholeMilliseconds)
        }

        return isRemoved
    }

    override suspend fun getFriends(uuid: UUID): Set<UUID> {
        return client.connect {
            val binaryFormat = client.binaryFormat
            val key = encodeKey(binaryFormat, uuid.toString())
            it.smembers(key).mapNotNull { member ->
                decodeFromByteArrayOrNull(binaryFormat, UUIDSerializer, member)
            }.toSet()
        }
    }

    override suspend fun setFriends(uuid: UUID, friends: Set<UUID>): Boolean {
        return client.connect {
            if (!duplicateForFriend) {
                deleteKey(it, uuid)
                return@connect addFriend(it, uuid, friends)
            }

            coroutineScope {
                val uuidList = listOf(uuid)
                val duplicateTask = friends.map { friendUUID ->
                    async { addFriend(it, friendUUID, uuidList) }
                }

                deleteKey(it, uuid)
                addFriend(it, uuid, friends).also { duplicateTask.awaitAll() }
            }
        }
    }

    override suspend fun isFriend(uuid: UUID, friend: UUID): Boolean {
        return client.connect {
            val binaryFormat = client.binaryFormat
            val key = encodeKey(binaryFormat, uuid.toString())
            val value = encodeToByteArray(binaryFormat, UUIDSerializer, friend)
            it.sismember(key, value)
        } == true
    }

    /**
     * Delete the key of the given entity.
     * @param it Redis connection.
     * @param uuid ID of the entity.
     * @return The number of keys that were removed.
     */
    private suspend fun deleteKey(it: RedisCoroutinesCommands<ByteArray, ByteArray>, uuid: UUID): Long? {
        return it.del(encodeKey(client.binaryFormat, uuid.toString()))
    }
}

/**
 * Implementation of [IFriendDatabaseService] to manage data in database.
 */
public class FriendDatabaseService : IFriendDatabaseService {

    override suspend fun addFriend(uuid: UUID, friend: UUID): Boolean {
        return newSuspendedTransaction {
            Friends.insert {
                it[uuid1] = uuid
                it[uuid2] = friend
            }
        }.insertedCount == 1
    }

    override suspend fun removeFriend(uuid: UUID, friend: UUID): Boolean {
        return newSuspendedTransaction {
            Friends.deleteWhere(1) {
                (uuid1.eq(uuid) and uuid2.eq(friend)) or (uuid1.eq(friend) and uuid2.eq(uuid))
            }
        } > 0
    }

    override suspend fun getFriends(uuid: UUID): Set<UUID> {
        return newSuspendedTransaction {
            Friends.select { (Friends.uuid1.eq(uuid) or Friends.uuid2.eq(uuid)) }
                .withDistinct()
                .mapTo(mutableSetOf()) {
                    val uuid1 = it[Friends.uuid1]
                    val uuid2 = it[Friends.uuid2]
                    if (uuid1 == uuid) uuid2 else uuid1
                }
        }
    }

    override suspend fun isFriend(uuid: UUID, friend: UUID): Boolean {
        return newSuspendedTransaction {
            !Friends.select {
                (Friends.uuid1.eq(uuid) and Friends.uuid2.eq(friend)) or (Friends.uuid1.eq(friend) and Friends.uuid2.eq(
                    uuid
                ))
            }.empty()
        }
    }
}

/**
 * Implementation of [IFriendService] to manage friends according to a [IEntitySupplier].
 */
public class FriendService(override val supplier: IEntitySupplier) : IFriendService, Strategizable {

    override suspend fun addFriend(uuid: UUID, friend: UUID): Boolean {
        return supplier.addFriend(uuid, friend)
    }

    override suspend fun removeFriend(uuid: UUID, friend: UUID): Boolean {
        return supplier.removeFriend(uuid, friend)
    }

    override suspend fun getFriends(uuid: UUID): Set<UUID> {
        return supplier.getFriends(uuid)
    }

    override suspend fun isFriend(uuid: UUID, friend: UUID): Boolean {
        return supplier.isFriend(uuid, friend)
    }

    override fun withStrategy(strategy: IEntitySupplier): FriendService = FriendService(strategy)
}