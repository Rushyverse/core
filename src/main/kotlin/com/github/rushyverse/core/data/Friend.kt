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
import org.jetbrains.exposed.sql.Column
import java.util.*
import kotlin.time.Duration

public object Friends : IntIdTable("friends") {
    public val uuid1: Column<UUID> = uuid("uuid1").uniqueIndex()
    public val uuid2: Column<UUID> = uuid("uuid2").uniqueIndex()
}

public interface IFriendCacheService {

    public suspend fun addFriend(uuid: UUID, friend: UUID): Boolean

    public suspend fun removeFriend(uuid: UUID, friend: UUID): Boolean

    public suspend fun getFriends(uuid: UUID): Set<UUID>

    public suspend fun isFriend(uuid: UUID, friend: UUID): Boolean

    public suspend fun setFriends(uuid: UUID, friends: Set<UUID>): Boolean

}

public interface IFriendService : Strategizable {

    public suspend fun addFriend(uuid: UUID, friend: UUID): Boolean

    public suspend fun removeFriend(uuid: UUID, friend: UUID): Boolean

    public suspend fun getFriends(uuid: UUID): Set<UUID>

    public suspend fun isFriend(uuid: UUID, friend: UUID): Boolean
}

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
        if(friend.isEmpty()) return true

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

    private suspend fun deleteKey(it: RedisCoroutinesCommands<ByteArray, ByteArray>, uuid: UUID): Long? {
        return it.del(encodeKey(client.binaryFormat, uuid.toString()))
    }

    override suspend fun isFriend(uuid: UUID, friend: UUID): Boolean {
        return client.connect {
            val binaryFormat = client.binaryFormat
            val key = encodeKey(binaryFormat, uuid.toString())
            val value = encodeToByteArray(binaryFormat, UUIDSerializer, friend)
            it.sismember(key, value)
        } == true
    }
}

public class FriendService(override val supplier: IEntitySupplier) : IFriendService {

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

    override fun withStrategy(strategy: IEntitySupplier): IFriendService = FriendService(strategy)
}