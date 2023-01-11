package com.github.rushyverse.core.data

import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.cache.AbstractCacheService
import com.github.rushyverse.core.data._Friends.Companion.friends
import com.github.rushyverse.core.extension.toTypedArray
import com.github.rushyverse.core.serializer.UUIDSerializer
import com.github.rushyverse.core.supplier.database.IEntitySupplier
import com.github.rushyverse.core.supplier.database.Strategizable
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.mapNotNull
import org.komapper.annotation.KomapperAutoIncrement
import org.komapper.annotation.KomapperEntity
import org.komapper.annotation.KomapperId
import org.komapper.core.dsl.QueryDsl
import org.komapper.core.dsl.expression.WhereDeclaration
import org.komapper.core.dsl.operator.and
import org.komapper.core.dsl.operator.or
import org.komapper.r2dbc.R2dbcDatabase
import java.util.*
import kotlin.time.Duration

/**
 * Service to manage the friendship relationship.
 */
interface IFriendService {

    /**
     * Add a new relationship of friendship between two entities.
     * @param uuid ID of the first entity.
     * @param friend ID of the second entity.
     * @return `true` if the relationship was added successfully, `false` otherwise.
     */
    suspend fun addFriend(uuid: UUID, friend: UUID): Boolean

    /**
     * Remove a relationship of friendship between two entities.
     * @param uuid ID of the first entity.
     * @param friend ID of the second entity.
     * @return `true` if the relationship was removed successfully, `false` otherwise.
     */
    suspend fun removeFriend(uuid: UUID, friend: UUID): Boolean

    /**
     * Get all the friends of an entity.
     * @param uuid ID of the entity.
     * @return Set of IDs of the friends.
     */
    suspend fun getFriends(uuid: UUID): Flow<UUID>

    /**
     * Check if two entities are friends.
     * @param uuid ID of the first entity.
     * @param friend ID of the second entity.
     * @return `true` if the two entities are friends, `false` otherwise.
     */
    suspend fun isFriend(uuid: UUID, friend: UUID): Boolean
}

/**
 * Service to manage the friendship relationship in cache.
 */
interface IFriendCacheService : IFriendService {

    /**
     * Set the friends of an entity.
     * @param uuid ID of  the entity.
     * @param friends Set of new friends.
     * @return `true` if the friends were set successfully, `false` otherwise.
     */
    suspend fun setFriends(uuid: UUID, friends: Set<UUID>): Boolean

}

/**
 * Service to manage the friendship relationship in database.
 */
interface IFriendDatabaseService : IFriendService

/**
 * Table to store the friendship relationship in database.
 */
@KomapperEntity
data class Friends(
    val uuid1: UUID,
    val uuid2: UUID,
    @KomapperId
    @KomapperAutoIncrement
    val id: Int = 0,
)

/**
 * Implementation of [IFriendCacheService] that uses [CacheClient] to manage data in cache.
 * @property client Cache client.
 * @property expirationKey Expiration time applied when a new relationship is set.
 * @property duplicateForFriend `true` if all operations should be applied to the friend.
 * When `true`, if `uuid` is friends with `friend`, `friend` will be friends with `uuid`.
 * This behavior is applied for each available operations
 */
class FriendCacheService(
    client: CacheClient,
    expirationKey: Duration? = null,
    prefixKey: String = "friend:",
    val duplicateForFriend: Boolean = false
) : AbstractCacheService(client, prefixKey, expirationKey), IFriendCacheService {

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
        val key = encodeKey(uuid.toString())
        val friends = friend.asSequence().map { encodeToByteArray(UUIDSerializer, it) }.toTypedArray(size)

        val result = connection.sadd(key, *friends)
        val isAdded = result != null && result > 0
        if (expirationKey != null && isAdded) {
            connection.pexpire(key, expirationKey.inWholeMilliseconds)
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
        val key = encodeKey(uuid.toString())
        val value = encodeToByteArray(UUIDSerializer, friend)
        val result = connection.srem(key, value)
        val isRemoved = result != null && result > 0

        if (expirationKey != null && isRemoved) {
            connection.pexpire(key, expirationKey.inWholeMilliseconds)
        }

        return isRemoved
    }

    override suspend fun getFriends(uuid: UUID): Flow<UUID> {
        val key = encodeKey(uuid.toString())

        return client.connect {
            it.smembers(key).mapNotNull { member ->
                decodeFromByteArrayOrNull(UUIDSerializer, member)
            }
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
        val key = encodeKey(uuid.toString())
        val value = encodeToByteArray(UUIDSerializer, friend)

        return client.connect { it.sismember(key, value) } == true
    }

    /**
     * Delete the key of the given entity.
     * @param it Redis connection.
     * @param uuid ID of the entity.
     * @return The number of keys that were removed.
     */
    private suspend fun deleteKey(it: RedisCoroutinesCommands<ByteArray, ByteArray>, uuid: UUID): Long? {
        return it.del(encodeKey(uuid.toString()))
    }
}

/**
 * Implementation of [IFriendDatabaseService] to manage data in database.
 */
class FriendDatabaseService(val database: R2dbcDatabase) : IFriendDatabaseService {

    override suspend fun addFriend(uuid: UUID, friend: UUID): Boolean {
        if (isFriend(uuid, friend)) return false
        val query = QueryDsl.insert(friends).single(Friends(uuid, friend))
        database.runQuery(query)
        return true
    }

    override suspend fun removeFriend(uuid: UUID, friend: UUID): Boolean {
        val where = createWhereBidirectional(uuid, friend)
        val query = QueryDsl.delete(friends).where(where)
        return database.runQuery(query) > 0
    }

    override suspend fun getFriends(uuid: UUID): Flow<UUID> {
        val w1: WhereDeclaration = { friends.uuid1 eq uuid }
        val w2: WhereDeclaration = { friends.uuid2 eq uuid }
        val where = (w1.or(w2))

        val query = QueryDsl.from(friends).where(where)
        return database.flowQuery(query).map {
            val uuid1 = it.uuid1
            if (uuid1 == uuid) it.uuid2 else uuid1
        }
    }

    override suspend fun isFriend(uuid: UUID, friend: UUID): Boolean {
        val where = createWhereBidirectional(uuid, friend)
        val query = QueryDsl.from(friends).where(where).limit(1)
        return database.runQuery(query).isNotEmpty()
    }

    /**
     * Create a where clause to check if [Friends.uuid1] is [uuid] and [Friends.uuid2] is [friend] or vice versa.
     * @param uuid ID of the entity.
     * @param friend ID of the friend.
     * @return Where clause.
     */
    private fun createWhereBidirectional(uuid: UUID, friend: UUID): WhereDeclaration {
        val w1: WhereDeclaration = { friends.uuid1 eq uuid }
        val w2: WhereDeclaration = { friends.uuid2 eq friend }

        val w3: WhereDeclaration = { friends.uuid1 eq friend }
        val w4: WhereDeclaration = { friends.uuid2 eq uuid }

        return (w1.and(w2)).or(w3.and(w4))
    }
}

/**
 * Implementation of [IFriendService] to manage friends according to a [IEntitySupplier].
 */
class FriendService(override val supplier: IEntitySupplier) : IFriendService, Strategizable {

    override suspend fun addFriend(uuid: UUID, friend: UUID): Boolean {
        return supplier.addFriend(uuid, friend)
    }

    override suspend fun removeFriend(uuid: UUID, friend: UUID): Boolean {
        return supplier.removeFriend(uuid, friend)
    }

    override suspend fun getFriends(uuid: UUID): Flow<UUID> {
        return supplier.getFriends(uuid)
    }

    override suspend fun isFriend(uuid: UUID, friend: UUID): Boolean {
        return supplier.isFriend(uuid, friend)
    }

    override fun withStrategy(strategy: IEntitySupplier): FriendService = FriendService(strategy)
}