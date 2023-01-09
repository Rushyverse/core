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
            coroutineScope {
                val duplicate = if (duplicateForFriend) {
                    async { addFriend(it, friend, listOf(uuid)) }
                } else null

                addFriend(it, uuid, listOf(friend)) && (duplicate?.await() ?: true)
            }
        }
    }

    private suspend fun addFriend(
        connection: RedisCoroutinesCommands<ByteArray, ByteArray>,
        uuid: UUID,
        friend: Collection<UUID>,
    ): Boolean {
        val size = friend.size
        val key = encodeKey(binaryFormat, uuid.toString())
        val friends = friend.asSequence().map { encodeToByteArray(binaryFormat, UUIDSerializer, it) }.toTypedArray(size)

        val result = connection.sadd(key, *friends)
        val isAdded = result != null && result > 0
        if(expiration != null && isAdded) {
            connection.pexpire(key, expiration.inWholeMilliseconds)
        }

        return isAdded
    }

    override suspend fun removeFriend(uuid: UUID, friend: UUID): Boolean {
        return client.connect {
            coroutineScope {
                val duplicate = if (duplicateForFriend) {
                    async { removeFriend(it, friend, uuid) }
                } else null

                removeFriend(it, uuid, friend) && (duplicate?.await() ?: true)
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
        return if (expiration != null && result == 1L) {
            connection.pexpire(key, expiration.inWholeMilliseconds)
            true
        } else false
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
            coroutineScope {
                val duplicate = if (duplicateForFriend) {
                    val uuidList = listOf(uuid)
                    friends.map { friendUUID ->
                        async {
                            addFriend(it, friendUUID, uuidList)
                        }
                    }
                } else null

                val binaryFormat = client.binaryFormat
                val key = encodeKey(binaryFormat, uuid.toString())
                it.del(key)

                addFriend(it, uuid, friends) && (duplicate?.awaitAll()?.any { it } ?: true)
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