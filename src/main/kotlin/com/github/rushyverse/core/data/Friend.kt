package com.github.rushyverse.core.data

import com.github.rushyverse.core.cache.AbstractCacheService
import com.github.rushyverse.core.cache.CacheClient
import com.github.rushyverse.core.extension.toTypedArray
import com.github.rushyverse.core.serializer.UUIDSerializer
import com.github.rushyverse.core.supplier.database.DatabaseSupplierServices
import com.github.rushyverse.core.supplier.database.IDatabaseEntitySupplier
import com.github.rushyverse.core.supplier.database.IDatabaseStrategizable
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.*
import org.komapper.annotation.*
import org.komapper.core.dsl.QueryDsl
import org.komapper.core.dsl.expression.WhereDeclaration
import org.komapper.core.dsl.query.bind
import org.komapper.core.dsl.query.where
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

    /**
     * Retrieve all data stored in cache for a specific type of data.
     * @param uuid ID of the entity.
     * @param type Type of data to retrieve.
     * @return Map of IDs to data.
     */
    public suspend fun getAll(uuid: UUID, type: FriendCacheService.Type): Flow<UUID>

}

/**
 * Service to manage the friendship relationship in database.
 */
public interface IFriendDatabaseService : IFriendService {

    /**
     * Add a new relationship of friendship between the [uuid] entity and all entities in [friends].
     * @param uuid ID of the first entity.
     * @param friends IDs of entities.
     * @return `true` if at least one relationship was added successfully, `false` otherwise.
     */
    public suspend fun addFriends(uuid: UUID, friends: List<UUID>): Boolean

    /**
     * Add a new pending relationship of friendship between the [uuid] entity and all entities in [friends].
     * @param uuid ID of the first entity.
     * @param friends IDs of entities.
     * @return `true` if at least one relationship was added successfully, `false` otherwise.
     */
    public suspend fun addPendingFriends(uuid: UUID, friends: List<UUID>): Boolean

    /**
     * Remove a relationship of friendship between the [uuid] entity and all entities in [friends].
     * @param uuid ID of the first entity.
     * @param friends IDs of entities.
     * @return `true` if at least one relationship was removed successfully, `false` otherwise.
     */
    public suspend fun removeFriends(uuid: UUID, friends: List<UUID>): Boolean

    /**
     * Remove a pending relationship of friendship between the [uuid] entity and all entities in [friends].
     * @param uuid ID of the first entity.
     * @param friends IDs of entities.
     * @return `true` if at least one relationship was removed successfully, `false` otherwise.
     */
    public suspend fun removePendingFriends(uuid: UUID, friends: List<UUID>): Boolean


}

/**
 * Ids of the friendship relationship.
 * @property uuid1 First UUID
 * @property uuid2 Second UUID
 */
public data class FriendId(val uuid1: UUID, val uuid2: UUID)

/**
 * Table to store the friendship relationship in database.
 */
@KomapperEntity
@KomapperTable("friend")
public data class Friend(
    @KomapperEmbeddedId
    val uuid: FriendId,
    val pending: Boolean,
) {

    public companion object {

        /**
         * Create the table in database.
         * @param database Database to create the table in.
         */
        public suspend fun createTable(database: R2dbcDatabase) {
            val meta = _Friend.friend
            val uuid = meta.uuid
            val tableName = meta.tableName()
            val uuid1Name = uuid.uuid1.name
            val uuid2Name = uuid.uuid2.name

            database.withTransaction {
                database.runQuery(QueryDsl.create(meta))
                database.runQuery(
                    QueryDsl.executeScript(
                        """
                        CREATE UNIQUE INDEX ${tableName}_unique_idx
                        ON $tableName (GREATEST($uuid1Name, $uuid2Name), LEAST($uuid1Name, $uuid2Name));
                    """.trimIndent()
                    )
                )
            }
        }

    }

    val uuid1: UUID
        get() = uuid.uuid1

    val uuid2: UUID
        get() = uuid.uuid2

    public constructor(uuid1: UUID, uuid2: UUID, pending: Boolean) : this(FriendId(uuid1, uuid2), pending)
}

/**
 * Implementation of [IFriendCacheService] that uses [CacheClient] to manage data in cache.
 * @property cacheClient Cache client.
 */
public class FriendCacheService(
    client: CacheClient,
    prefixKey: String = DEFAULT_PREFIX_KEY_USER_CACHE
) : AbstractCacheService(client, prefixKey), IFriendCacheService {

    public enum class Type(public val key: String) {
        FRIENDS("friends"),
        ADD_FRIEND("friends:add"),
        REMOVE_FRIEND("friends:remove"),

        PENDING_FRIENDS("friends:pending"),
        ADD_PENDING_FRIEND("friends:pending:add"),
        REMOVE_PENDING_FRIEND("friends:pending:remove"),
    }

    public companion object {

        /**
         * Retrieve data of the user [uuid] from cache related to [Type.ADD_FRIEND], [Type.REMOVE_FRIEND],
         * [Type.ADD_PENDING_FRIEND] and [Type.REMOVE_PENDING_FRIEND] and send them to the database.
         * @param uuid ID of the user.
         * @param services Services to manage the friendship relationship.
         * @return `true` if at least one relationship was added or removed successfully, `false` otherwise.
         */
        public suspend fun cacheToDatabase(uuid: UUID, services: DatabaseSupplierServices): Boolean {
            val cache = IDatabaseEntitySupplier.cache(services)
            val database = IDatabaseEntitySupplier.database(services)

            suspend fun saveInDatabaseIfNotEmpty(
                flow: Flow<UUID>,
                save: suspend (UUID, List<UUID>) -> Boolean
            ): Boolean {
                val list = flow.toList()
                return list.isNotEmpty() && save(uuid, list)
            }

            return coroutineScope {
                listOf(
                    async { saveInDatabaseIfNotEmpty(cache.getAll(uuid, Type.ADD_FRIEND), database::addFriends) },
                    async {
                        saveInDatabaseIfNotEmpty(cache.getAll(uuid, Type.REMOVE_FRIEND), database::removeFriends)
                    },
                    async {
                        saveInDatabaseIfNotEmpty(
                            cache.getAll(uuid, Type.ADD_PENDING_FRIEND),
                            database::addPendingFriends
                        )
                    },
                    async {
                        saveInDatabaseIfNotEmpty(
                            cache.getAll(uuid, Type.REMOVE_PENDING_FRIEND),
                            database::removePendingFriends
                        )
                    }
                ).awaitAll().any { it }
            }
        }

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

    override suspend fun getAll(uuid: UUID, type: Type): Flow<UUID> {
        return cacheClient.connect { connection ->
            getAll(connection, uuid, type)
        }
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
        val key = encodeFormatKey(type.key, uuid.toString())
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
        val key = encodeFormatKey(type.key, uuid.toString())
        val friendsSerialized = friends.asSequence().map { encodeToByteArray(UUIDSerializer, it) }.toTypedArray(size)

        val result = connection.sadd(key, *friendsSerialized)
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
        val key = encodeFormatKey(type.key, uuid.toString())
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
            val removedFriend = getAll(connection, uuid, removed).toSet()

            listOf(getAll(connection, uuid, list), getAll(connection, uuid, added))
                .merge()
                .distinctUntilChanged()
                .filter { it !in removedFriend }
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
        val key = encodeFormatKey(type.key, uuid.toString())
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
            it.del(encodeFormatKey(type.key, uuid.toString()))
            add(it, uuid, friends, type)
        }
    }
}

/**
 * Implementation of [IFriendDatabaseService] to manage data in database.
 */
public class FriendDatabaseService(public val database: R2dbcDatabase) : IFriendDatabaseService {

    override suspend fun addFriend(uuid: UUID, friend: UUID): Boolean {
        return add(uuid, friend, false)
    }

    override suspend fun addFriends(uuid: UUID, friends: List<UUID>): Boolean {
        return addAll(uuid, friends, false)
    }

    override suspend fun addPendingFriend(uuid: UUID, friend: UUID): Boolean {
        return add(uuid, friend, true)
    }

    override suspend fun addPendingFriends(uuid: UUID, friends: List<UUID>): Boolean {
        return addAll(uuid, friends, true)
    }

    override suspend fun removeFriend(uuid: UUID, friend: UUID): Boolean {
        return remove(uuid, friend, false)
    }

    override suspend fun removeFriends(uuid: UUID, friends: List<UUID>): Boolean {
        return removeAll(uuid, friends, false)
    }

    override suspend fun removePendingFriend(uuid: UUID, friend: UUID): Boolean {
        return remove(uuid, friend, true)
    }

    override suspend fun removePendingFriends(uuid: UUID, friends: List<UUID>): Boolean {
        return removeAll(uuid, friends, true)
    }

    override suspend fun getFriends(uuid: UUID): Flow<UUID> {
        return getAll(uuid, false)
    }

    override suspend fun getPendingFriends(uuid: UUID): Flow<UUID> {
        return getAll(uuid, true)
    }

    override suspend fun isFriend(uuid: UUID, friend: UUID): Boolean {
        return isFriend(uuid, friend, false)
    }

    override suspend fun isPendingFriend(uuid: UUID, friend: UUID): Boolean {
        return isFriend(uuid, friend, true)
    }

    /**
     * Add a new relationship between [uuid] and [friend] with the status [pending].
     * @param uuid ID of the user.
     * @param friend ID of the friend.
     * @param pending `true` if the relationship is pending, `false` otherwise.
     * @return Always return `true`.
     */
    private suspend fun add(uuid: UUID, friend: UUID, pending: Boolean): Boolean {
        return insertOrUpdate(uuid, friend, pending)
    }

    /**
     * Add a new relationship between [uuid] and [friendIds] with the status [pending].
     * @param uuid ID of the user.
     * @param friendIds ID of the friends.
     * @param pending `true` if the relationship is pending, `false` otherwise.
     * @return Always return `true`.
     */
    private suspend fun addAll(uuid: UUID, friendIds: List<UUID>, pending: Boolean): Boolean {
        if (friendIds.isEmpty()) return true
        return database.withTransaction {
            insertOrUpdateAll(uuid, friendIds, pending)
        }
    }

    /**
     * Remove the [friend] from the [uuid] with the given [pending] state.
     * @param uuid ID of the entity.
     * @param friend ID of the friend.
     * @param pending State of the relationship.
     * @return `true` if the relationship was removed, `false` otherwise.
     */
    private suspend fun remove(uuid: UUID, friend: UUID, pending: Boolean): Boolean {
        val meta = _Friend.friend
        val where = where {
            meta.pending eq pending
            and(createWhereBidirectional(uuid, friend))
        }
        val query = QueryDsl.delete(meta).where(where)
        return database.runQuery(query) > 0
    }

    /**
     * Remove the [friendIds] from the [uuid] with the given [pending] state.
     * @param uuid ID of the entity.
     * @param friendIds ID of the friends.
     * @param pending State of the relationship.
     * @return `true` if the relationship was removed, `false` otherwise.
     */
    private suspend fun removeAll(uuid: UUID, friendIds: List<UUID>, pending: Boolean): Boolean {
        if (friendIds.isEmpty()) return true

        val meta = _Friend.friend
        val metaUUID = meta.uuid

        val where = where {
            meta.pending eq pending
            and {
                metaUUID.uuid1 eq uuid
                and { metaUUID.uuid2 inList friendIds }
                or {
                    metaUUID.uuid2 eq uuid
                    and { metaUUID.uuid1 inList friendIds }
                }
            }
        }

        val query = QueryDsl.delete(_Friend.friend).where(where)
        return database.runQuery(query) > 0
    }

    /**
     * Get the existing relationship between [uuid] and [friend].
     * If the relation exists, will update the [pending] state.
     * Otherwise, will create a new relationship.
     * @param uuid First ID.
     * @param friend Second ID.
     * @param pending State of the relationship.
     * @return `true` if the relationship was created or updated, `false` otherwise.
     */
    private suspend fun insertOrUpdate(
        uuid: UUID,
        friend: UUID,
        pending: Boolean
    ): Boolean {
        return insertOrUpdateAll(uuid, listOf(friend), pending)
    }

    /**
     * Get the existing relationship between [uuid] and [friendIds].
     * If the relation exists, will update the [pending] state.
     * Otherwise, will create a new relationship.
     * @param uuid First ID.
     * @param friendIds Ids of future friends, must not be empty.
     * @param pending State of the relationship.
     * @return `true` if the relationship was created or updated, `false` otherwise.
     */
    private suspend fun insertOrUpdateAll(
        uuid: UUID,
        friendIds: List<UUID>,
        pending: Boolean
    ): Boolean {
        val meta = _Friend.friend
        val metaUUID = meta.uuid
        val uuid1Name = metaUUID.uuid1.name
        val uuid2Name = metaUUID.uuid2.name
        val pendingName = meta.pending.name

        val query = QueryDsl.executeTemplate(
            """
                    INSERT INTO ${meta.tableName()} ($uuid1Name, $uuid2Name, $pendingName) 
                    VALUES ${friendIds.indices.joinToString(", ") { "(/*uuid*/'', /*uuid$it*/'', /*pending*/false)" }}
                    ON CONFLICT (GREATEST($uuid1Name, $uuid2Name), LEAST($uuid1Name, $uuid2Name)) 
                    DO UPDATE SET $pendingName = /*pending*/false WHERE ("${meta.tableName()}".$pendingName = true AND /*pending*/false = false)
                    """.trimIndent()
        )
            .bind("uuid", uuid)
            .let {
                friendIds.foldIndexed(it) { index, acc, friendId ->
                    acc.bind("uuid$index", friendId)
                }
            }
            .bind("pending", pending)

        return database.runQuery(query) > 0
    }

    /**
     * Get all friends of [uuid] for the given [pending].
     * @param uuid UUID of the user.
     * @param pending `true` to get all pending friends, `false` to get all friends.
     * @return Flow of the friends of [uuid] for the given [pending].
     */
    private fun getAll(uuid: UUID, pending: Boolean): Flow<UUID> {
        val meta = _Friend.friend
        val metaUUID = meta.uuid

        val where = where {
            meta.pending eq pending
            and {
                metaUUID.uuid1 eq uuid
                or { metaUUID.uuid2 eq uuid }
            }
        }

        val query = QueryDsl.from(meta).where(where)
        return database.flowQuery(query).map {
            val uuid1 = it.uuid1
            if (uuid1 == uuid) it.uuid2 else uuid1
        }
    }

    /**
     * Check if [friend] is a friend of [uuid] with the given [pending] status.
     * @param uuid UUID of the user.
     * @param friend UUID of the friend.
     * @param pending `true` to check if the friend is pending, `false` to check if the friend is accepted.
     * @return `true` if [friend] is a friend of [uuid] with the given [pending] status, `false` otherwise.
     */
    private suspend fun isFriend(uuid: UUID, friend: UUID, pending: Boolean): Boolean {
        val meta = _Friend.friend
        val where = where {
            meta.pending eq pending
            and(createWhereBidirectional(uuid, friend))
        }
        val query = QueryDsl.from(meta).where(where).limit(1)
        return database.runQuery(query).isNotEmpty()
    }

    /**
     * Create a where clause to check if [Friend.uuid1] is [uuid] and [Friend.uuid2] is [friend] or vice versa.
     * @param uuid ID of the entity.
     * @param friend ID of the friend.
     * @return Where clause.
     */
    private fun createWhereBidirectional(uuid: UUID, friend: UUID): WhereDeclaration {
        val metaUUID = _Friend.friend.uuid
        val uuid1 = metaUUID.uuid1
        val uuid2 = metaUUID.uuid2
        return where {
            uuid1 eq uuid
            and { uuid2 eq friend }
            or {
                uuid1 eq friend
                and { uuid2 eq uuid }
            }
        }
    }
}

/**
 * Implementation of [IFriendService] to manage friends according to a [IDatabaseEntitySupplier].
 */
public class FriendService(override val supplier: IDatabaseEntitySupplier) : IFriendService by supplier,
    IDatabaseStrategizable {
    override fun withStrategy(strategy: IDatabaseEntitySupplier): FriendService = FriendService(strategy)
}