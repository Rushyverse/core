package com.github.rushyverse.core.data

import com.github.rushyverse.core.cache.AbstractUserCacheService
import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.data._Friends.Companion.friends
import com.github.rushyverse.core.extension.toTypedArray
import com.github.rushyverse.core.serializer.UUIDSerializer
import com.github.rushyverse.core.supplier.database.IDatabaseEntitySupplier
import com.github.rushyverse.core.supplier.database.IDatabaseStrategizable
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import kotlinx.coroutines.flow.*
import org.komapper.annotation.KomapperAutoIncrement
import org.komapper.annotation.KomapperEntity
import org.komapper.annotation.KomapperId
import org.komapper.annotation.KomapperTable
import org.komapper.core.dsl.QueryDsl
import org.komapper.core.dsl.expression.WhereDeclaration
import org.komapper.core.dsl.operator.and
import org.komapper.core.dsl.operator.or
import org.komapper.r2dbc.R2dbcDatabase
import java.util.*

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
     * Add a new pending relationship of friendship between two entities.
     * @param uuid ID of the first entity.
     * @param friend ID of the second entity.
     * @return `true` if the relationship was added successfully, `false` otherwise.
     */
    public suspend fun addPendingFriend(uuid: UUID, friend: UUID): Boolean

    /**
     * Remove a relationship of friendship between two entities.
     * @param uuid ID of the first entity.
     * @param friend ID of the second entity.
     * @return `true` if the relationship was removed successfully, `false` otherwise.
     */
    public suspend fun removeFriend(uuid: UUID, friend: UUID): Boolean

    /**
     * Remove a pending relationship of friendship between two entities.
     * @param uuid ID of the first entity.
     * @param friend ID of the second entity.
     * @return `true` if the relationship was removed successfully, `false` otherwise.
     */
    public suspend fun removePendingFriend(uuid: UUID, friend: UUID): Boolean

    /**
     * Get all the friends of an entity.
     * @param uuid ID of the entity.
     * @return Set of IDs of the friends.
     */
    public suspend fun getFriends(uuid: UUID): Flow<UUID>

    /**
     * Get all the pending requests of an entity.
     * @param uuid ID of the entity.
     * @return Set of IDs of the pending requests.
     */
    public suspend fun getPendingFriends(uuid: UUID): Flow<UUID>

    /**
     * Check if two entities are friends.
     * @param uuid ID of the first entity.
     * @param friend ID of the second entity.
     * @return `true` if the two entities are friends, `false` otherwise.
     */
    public suspend fun isFriend(uuid: UUID, friend: UUID): Boolean

    /**
     * Check if two entities have a pending request.
     * @param uuid ID of the first entity.
     * @param friend ID of the second entity.
     * @return `true` if the two entities have a pending request, `false` otherwise.
     */
    public suspend fun isPendingFriend(uuid: UUID, friend: UUID): Boolean
}

/**
 * Service to manage the friendship relationship in cache.
 */
public interface IFriendCacheService : IFriendService {

    public val cacheClient: CacheClient

    /**
     * Set the friends of an entity.
     * @param uuid ID of  the entity.
     * @param friends Set of new friends.
     * @return `true` if the friends were set successfully, `false` otherwise.
     */
    public suspend fun setFriends(uuid: UUID, friends: Set<UUID>): Boolean

    /**
     * Set the pending requests of an entity.
     * @param uuid ID of  the entity.
     * @param friends Set of new pending requests.
     * @return `true` if the pending requests were set successfully, `false` otherwise.
     */
    public suspend fun setPendingFriends(uuid: UUID, friends: Set<UUID>): Boolean

}

/**
 * Service to manage the friendship relationship in database.
 */
public interface IFriendDatabaseService : IFriendService

/**
 * Table to store the friendship relationship in database.
 */
@KomapperEntity
@KomapperTable("friends")
public data class Friends(
    val uuid1: UUID,
    val uuid2: UUID,
    val pending: Boolean,
    @KomapperId
    @KomapperAutoIncrement
    val id: Int = 0,
)

/**
 * Implementation of [IFriendCacheService] that uses [CacheClient] to manage data in cache.
 * @property cacheClient Cache client.
 */
public class FriendCacheService(
    client: CacheClient,
    userCacheManager: UserCacheManager,
) : AbstractUserCacheService(client, userCacheManager), IFriendCacheService {

    public enum class Type(public val key: String) {
        FRIENDS("friends"),
        ADD_FRIEND("friends:add"),
        REMOVE_FRIEND("friends:remove"),

        PENDING_FRIENDS("friends:pending"),
        ADD_PENDING_FRIEND("friends:pending:add"),
        REMOVE_PENDING_FRIEND("friends:pending:remove"),
    }

    override suspend fun addFriend(uuid: UUID, friend: UUID): Boolean {
        return addInFirstAndDeleteInSecondRelation(uuid, friend, Type.ADD_FRIEND, Type.REMOVE_FRIEND)
    }

    override suspend fun addPendingFriend(uuid: UUID, friend: UUID): Boolean {
        return addInFirstAndDeleteInSecondRelation(uuid, friend, Type.ADD_PENDING_FRIEND, Type.REMOVE_PENDING_FRIEND)
    }

    override suspend fun removeFriend(uuid: UUID, friend: UUID): Boolean {
        return addInFirstAndDeleteInSecondRelation(uuid, friend, Type.REMOVE_FRIEND, Type.ADD_FRIEND)
    }

    override suspend fun removePendingFriend(uuid: UUID, friend: UUID): Boolean {
        return addInFirstAndDeleteInSecondRelation(uuid, friend, Type.REMOVE_PENDING_FRIEND, Type.ADD_PENDING_FRIEND)
    }

    override suspend fun getFriends(uuid: UUID): Flow<UUID> {
        return mergeFirstAndSecondThenRemoveThirdRelation(uuid, Type.FRIENDS, Type.ADD_FRIEND, Type.REMOVE_FRIEND)
    }

    override suspend fun getPendingFriends(uuid: UUID): Flow<UUID> {
        return mergeFirstAndSecondThenRemoveThirdRelation(
            uuid,
            Type.PENDING_FRIENDS,
            Type.ADD_PENDING_FRIEND,
            Type.REMOVE_PENDING_FRIEND
        )
    }

    override suspend fun setFriends(uuid: UUID, friends: Set<UUID>): Boolean {
        return setAll(uuid, friends, Type.FRIENDS)
    }

    override suspend fun setPendingFriends(uuid: UUID, friends: Set<UUID>): Boolean {
        return setAll(uuid, friends, Type.PENDING_FRIENDS)
    }

    override suspend fun isFriend(uuid: UUID, friend: UUID): Boolean {
        return relationExistsInFirstOrSecondButNotInThird(
            uuid,
            friend,
            Type.FRIENDS,
            Type.ADD_FRIEND,
            Type.REMOVE_FRIEND
        )
    }

    override suspend fun isPendingFriend(uuid: UUID, friend: UUID): Boolean {
        return relationExistsInFirstOrSecondButNotInThird(
            uuid,
            friend,
            Type.PENDING_FRIENDS,
            Type.ADD_PENDING_FRIEND,
            Type.REMOVE_PENDING_FRIEND
        )
    }

    /**
     * Check if a relation exists in [list] or [add] but not in [remove].
     * The relationship must be unidirectional from [uuid] to [friend].
     * @param uuid ID of the first entity.
     * @param friend ID of the second entity.
     * @param list List of relations.
     * @param add List of relations to add.
     * @param remove List of relations to remove.
     * @return `true` if the relation exists, `false` otherwise.
     */
    private suspend fun relationExistsInFirstOrSecondButNotInThird(
        uuid: UUID,
        friend: UUID,
        list: Type,
        add: Type,
        remove: Type,
    ): Boolean {
        return cacheClient.connect {
            (isMember(it, uuid, friend, list) || isMember(it, uuid, friend, add)) && !isMember(it, uuid, friend, remove)
        }
    }

    /**
     * Add a relation in [add] and delete it in [remove].
     * The relationship is unidirectional from [uuid] to [friend].
     * @param uuid ID of the first entity.
     * @param friend ID of the second entity.
     * @param addList List of relations to add.
     * @param removeList List of relations to remove.
     * @return `true` if the relation was added successfully, `false` otherwise.
     */
    private suspend fun addInFirstAndDeleteInSecondRelation(
        uuid: UUID,
        friend: UUID,
        addList: Type,
        removeList: Type,
    ): Boolean {
        return cacheClient.connect { connection ->
            val added = add(connection, uuid, listOf(friend), addList)
            val removed = remove(connection, uuid, friend, removeList)
            added || removed
        }
    }

    /**
     * Check if [friend] is a member of [uuid] for the given [type].
     * @param connection Redis connection.
     * @param uuid UUID of the user.
     * @param friend UUID of the friend.
     * @param type Type of the relationship.
     * @return True if [friend] is a member of [uuid] for the given [type], false otherwise.
     */
    private suspend fun isMember(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        uuid: UUID,
        friend: UUID,
        type: Type
    ): Boolean {
        val key = encodeUserKey(uuid.toString(), type.key)
        val value = encodeToByteArray(UUIDSerializer, friend)
        return connection.sismember(key, value) == true
    }

    /**
     * Add [friends] to [uuid] for the given [type].
     * @param connection Redis connection.
     * @param uuid UUID of the user.
     * @param friends UUIDs of the friends.
     * @param type Type of the relationship.
     * @return True if [friends] were added to [uuid] for the given [type], false otherwise.
     */
    private suspend fun add(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        uuid: UUID,
        friends: Collection<UUID>,
        type: Type
    ): Boolean {
        if (friends.isEmpty()) return true

        val size = friends.size
        val key = encodeUserKey(uuid.toString(), type.key)
        val friends = friends.asSequence().map { encodeToByteArray(UUIDSerializer, it) }.toTypedArray(size)

        val result = connection.sadd(key, *friends)
        return result != null && result > 0
    }

    /**
     * Remove [friend] from [uuid] for the given [type].
     * @param connection Redis connection.
     * @param uuid UUID of the user.
     * @param friend UUID of the friend.
     * @param type Type of the relationship.
     * @return True if [friend] were removed from [uuid] for the given [type], false otherwise.
     */
    private suspend fun remove(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        uuid: UUID,
        friend: UUID,
        type: Type
    ): Boolean {
        val key = encodeUserKey(uuid.toString(), type.key)
        val value = encodeToByteArray(UUIDSerializer, friend)
        val result = connection.srem(key, value)
        return result != null && result > 0
    }

    /**
     * Get all the members of [uuid] for the given [list] and [added] merged without the elements in [removed].
     * @param uuid UUID of the user.
     * @param list Type where the members are stored.
     * @param added Type where the members are added.
     * @param removed Type where the members are removed.
     * @return Flow of the members of [uuid] for the given [list] and [added] merged without the elements in [removed].
     */
    private suspend fun mergeFirstAndSecondThenRemoveThirdRelation(
        uuid: UUID,
        list: Type,
        added: Type,
        removed: Type
    ): Flow<UUID> {
        return cacheClient.connect { connection ->
            val removed = getAll(connection, uuid, removed).toSet()

            listOf(getAll(connection, uuid, list), getAll(connection, uuid, added))
                .merge()
                .distinctUntilChanged()
                .filter { it !in removed }
        }
    }

    /**
     * Get all the members of [uuid] for the given [type].
     * @param connection Redis connection.
     * @param uuid UUID of the user.
     * @param type Type where the members are stored.
     * @return Flow of the members of [uuid] for the given [type].
     */
    private fun getAll(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        uuid: UUID,
        type: Type
    ): Flow<UUID> {
        val key = encodeUserKey(uuid.toString(), type.key)
        return connection.smembers(key).mapNotNull { member ->
            decodeFromByteArrayOrNull(UUIDSerializer, member)
        }
    }

    /**
     * Remove the key linked to the [uuid] and the type [type] and set the new [friends] to the key.
     * @param uuid ID of the entity.
     * @param friends All id that will be set to the key.
     * @param type Type of the relationship.
     * @return The result of the transaction.
     */
    private suspend fun setAll(
        uuid: UUID,
        friends: Set<UUID>,
        type: Type
    ): Boolean {
        return cacheClient.connect {
            it.del(encodeUserKey(uuid.toString(), type.key))
            add(it, uuid, friends, type)
        }
    }
}

/**
 * Implementation of [IFriendDatabaseService] to manage data in database.
 */
public class FriendDatabaseService(public val database: R2dbcDatabase) : IFriendDatabaseService {

    override suspend fun addFriend(uuid: UUID, friend: UUID): Boolean {
        val query = QueryDsl.insert(friends).single(Friends(uuid, friend, false))
        database.runQuery(query)
        return true
    }

    override suspend fun addPendingFriend(uuid: UUID, friend: UUID): Boolean {
        val query = QueryDsl.insert(friends).single(Friends(uuid, friend, true))
        database.runQuery(query)
        return true
    }

    override suspend fun removeFriend(uuid: UUID, friend: UUID): Boolean {
        val where = createWhereBidirectional(uuid, friend)
        val query = QueryDsl.delete(friends).where(where)
        return database.runQuery(query) > 0
    }

    override suspend fun removePendingFriend(uuid: UUID, friend: UUID): Boolean {
        TODO("Not yet implemented")
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

    override suspend fun getPendingFriends(uuid: UUID): Flow<UUID> {
        TODO("Not yet implemented")
    }

    override suspend fun isFriend(uuid: UUID, friend: UUID): Boolean {
        val where = createWhereBidirectional(uuid, friend)
        val query = QueryDsl.from(friends).where(where).limit(1)
        return database.runQuery(query).isNotEmpty()
    }

    override suspend fun isPendingFriend(uuid: UUID, friend: UUID): Boolean {
        TODO("Not yet implemented")
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
 * Implementation of [IFriendService] to manage friends according to a [IDatabaseEntitySupplier].
 */
public class FriendService(override val supplier: IDatabaseEntitySupplier) : IFriendService, IDatabaseStrategizable {

    override suspend fun addFriend(uuid: UUID, friend: UUID): Boolean {
        return supplier.addFriend(uuid, friend)
    }

    override suspend fun addPendingFriend(uuid: UUID, friend: UUID): Boolean {
        return supplier.addPendingFriend(uuid, friend)
    }

    override suspend fun removeFriend(uuid: UUID, friend: UUID): Boolean {
        return supplier.removeFriend(uuid, friend)
    }

    override suspend fun removePendingFriend(uuid: UUID, friend: UUID): Boolean {
        return supplier.removePendingFriend(uuid, friend)
    }

    override suspend fun getFriends(uuid: UUID): Flow<UUID> {
        return supplier.getFriends(uuid)
    }

    override suspend fun getPendingFriends(uuid: UUID): Flow<UUID> {
        return supplier.getPendingFriends(uuid)
    }

    override suspend fun isFriend(uuid: UUID, friend: UUID): Boolean {
        return supplier.isFriend(uuid, friend)
    }

    override suspend fun isPendingFriend(uuid: UUID, friend: UUID): Boolean {
        return supplier.isPendingFriend(uuid, friend)
    }

    override fun withStrategy(strategy: IDatabaseEntitySupplier): FriendService = FriendService(strategy)
}