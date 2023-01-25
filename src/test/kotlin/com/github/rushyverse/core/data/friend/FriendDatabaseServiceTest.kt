package com.github.rushyverse.core.data.friend

import com.github.rushyverse.core.container.createPSQLContainer
import com.github.rushyverse.core.data.FriendDatabaseService
import com.github.rushyverse.core.data.Friends
import com.github.rushyverse.core.data._Friends.Companion.friends
import io.r2dbc.spi.ConnectionFactoryOptions
import io.r2dbc.spi.Option
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.komapper.core.dsl.QueryDsl
import org.komapper.dialect.postgresql.PostgreSqlDialect
import org.komapper.r2dbc.R2dbcDatabase
import org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.util.*
import kotlin.test.*

@Testcontainers
class FriendDatabaseServiceTest {

    companion object {
        @JvmStatic
        @Container
        private val psqlContainer = createPSQLContainer()
    }

    private lateinit var service: FriendDatabaseService
    private lateinit var database: R2dbcDatabase

    @BeforeTest
    fun onBefore() = runBlocking {
        val options = ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DRIVER, PostgreSqlDialect.driver)
            .option(ConnectionFactoryOptions.USER, psqlContainer.username)
            .option(ConnectionFactoryOptions.PASSWORD, psqlContainer.password)
            .option(ConnectionFactoryOptions.HOST, psqlContainer.host)
            .option(ConnectionFactoryOptions.PORT, psqlContainer.getMappedPort(POSTGRESQL_PORT))
            .option(ConnectionFactoryOptions.DATABASE, psqlContainer.databaseName)
            .option(Option.valueOf("DB_CLOSE_DELAY"), "-1")
            .build()

        database = R2dbcDatabase(options)

        database.runQuery(QueryDsl.create(friends))

        service = FriendDatabaseService(database)
    }

    @AfterTest
    fun onAfter() = runBlocking {
        database.runQuery(QueryDsl.drop(friends))
    }

    @Nested
    inner class AddFriend {

        @Test
        fun `should add several friends`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            val uuid3 = UUID.randomUUID()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.addFriend(uuid1, uuid3) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friends(uuid1, uuid2, false, 1),
                Friends(uuid1, uuid3, false, 2)
            )
        }

        @Test
        fun `should add friend if relation doesn't exist`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addFriend(uuid1, uuid2) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friends(uuid1, uuid2, false, 1),
            )
        }

        @Test
        fun `should add friend if relation already exists`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.addFriend(uuid1, uuid2) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friends(uuid1, uuid2, false, 1),
                Friends(uuid1, uuid2, false, 2)
            )
        }

        @Test
        fun `should add friend if relation doesn't exist but exists in the other direction`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.addFriend(uuid2, uuid1) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friends(uuid1, uuid2, false, 1),
                Friends(uuid2, uuid1, false, 2)
            )
        }

    }

    @Nested
    inner class AddFriends {

        @Test
        fun `should add no friends`() = runTest {
            val uuid1 = UUID.randomUUID()

            assertTrue { service.addFriends(uuid1, emptyList()) }

            assertThat(getAll()).isEmpty()
        }

        @Test
        fun `should add one friend`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addFriends(uuid1, listOf(uuid2)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friends(uuid1, uuid2, false, 1)
            )
        }

        @Test
        fun `should add several friends`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            val uuid3 = UUID.randomUUID()

            assertTrue { service.addFriends(uuid1, listOf(uuid2, uuid3)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friends(uuid1, uuid2, false, 1),
                Friends(uuid1, uuid3, false, 2)
            )
        }

        @Test
        fun `should add friend if relation already exists`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            repeat(2) { assertTrue { service.addFriends(uuid1, listOf(uuid2)) } }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friends(uuid1, uuid2, false, 1),
                Friends(uuid1, uuid2, false, 2)
            )
        }

        @Test
        fun `should add friend if relation doesn't exist but exists in the other direction`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addFriends(uuid1, listOf(uuid2)) }
            assertTrue { service.addFriends(uuid2, listOf(uuid1)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friends(uuid1, uuid2, false, 1),
                Friends(uuid2, uuid1, false, 2)
            )
        }

    }

    @Nested
    inner class AddPendingFriend {

        @Test
        fun `should add several pending friends`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            val uuid3 = UUID.randomUUID()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }
            assertTrue { service.addPendingFriend(uuid1, uuid3) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friends(uuid1, uuid2, true, 1),
                Friends(uuid1, uuid3, true, 2)
            )
        }

        @Test
        fun `should add pending friend if relation doesn't exist`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friends(uuid1, uuid2, true, 1),
            )
        }

        @Test
        fun `should add pending friend if relation already exists`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }
            assertTrue { service.addPendingFriend(uuid1, uuid2) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friends(uuid1, uuid2, true, 1),
                Friends(uuid1, uuid2, true, 2)
            )
        }

        @Test
        fun `should add pending friend if relation doesn't exist but exists in the other direction`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }
            assertTrue { service.addPendingFriend(uuid2, uuid1) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friends(uuid1, uuid2, true, 1),
                Friends(uuid2, uuid1, true, 2)
            )
        }

    }

    @Nested
    inner class AddPendingFriends {

        @Test
        fun `should add no pending friends`() = runTest {
            val uuid1 = UUID.randomUUID()

            assertTrue { service.addPendingFriends(uuid1, emptyList()) }

            assertThat(getAll()).isEmpty()
        }

        @Test
        fun `should add one pending friend`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addPendingFriends(uuid1, listOf(uuid2)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friends(uuid1, uuid2, true, 1)
            )
        }

        @Test
        fun `should add several pending friends`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            val uuid3 = UUID.randomUUID()

            assertTrue { service.addPendingFriends(uuid1, listOf(uuid2, uuid3)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friends(uuid1, uuid2, true, 1),
                Friends(uuid1, uuid3, true, 2)
            )
        }

        @Test
        fun `should add pending friend if relation already exists`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            repeat(2) { assertTrue { service.addPendingFriends(uuid1, listOf(uuid2)) } }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friends(uuid1, uuid2, true, 1),
                Friends(uuid1, uuid2, true, 2)
            )
        }

        @Test
        fun `should add pending friend if relation doesn't exist but exists in the other direction`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addPendingFriends(uuid1, listOf(uuid2)) }
            assertTrue { service.addPendingFriends(uuid2, listOf(uuid1)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friends(uuid1, uuid2, true, 1),
                Friends(uuid2, uuid1, true, 2)
            )
        }

    }

    @Nested
    inner class RemoveFriend {

        @Test
        fun `should remove if relation exists`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.removeFriend(uuid1, uuid2) }

            assertThat(getAll()).isEmpty()
        }

        @Test
        fun `should remove if relation exists in the other direction`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.removeFriend(uuid2, uuid1) }

            assertThat(getAll()).isEmpty()
        }

        @Test
        fun `should remove relationship bidirectional`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.addFriend(uuid2, uuid1) }
            assertTrue { service.removeFriend(uuid1, uuid2) }

            assertThat(getAll()).isEmpty()
        }

        @Test
        fun `should not remove if relation doesn't exist`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertFalse { service.removeFriend(uuid1, uuid2) }
            assertThat(getAll()).isEmpty()
        }

        @Test
        fun `should not remove pending relationship`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }
            assertFalse { service.removeFriend(uuid1, uuid2) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friends(uuid1, uuid2, true, 1)
            )
        }

    }

    @Nested
    inner class RemoveFriends {

        @Test
        fun `should remove no friends`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addFriend(uuid1, uuid2) }

            assertTrue { service.removeFriends(uuid1, emptyList()) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friends(uuid1, uuid2, false, 1)
            )
        }

        @Test
        fun `should remove one friend`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addFriend(uuid1, uuid2) }

            assertTrue { service.removeFriends(uuid1, listOf(uuid2)) }

            assertThat(getAll()).isEmpty()
        }

        @Test
        fun `should remove several friends`() = runTest {
            val uuid1 = UUID.randomUUID()
            val friends = List(10) { UUID.randomUUID() }

            assertTrue { service.addFriends(uuid1, friends) }

            assertTrue { service.removeFriends(uuid1, friends) }

            assertThat(getAll()).isEmpty()
        }

        @Test
        fun `should remove partial several friends`() = runTest {
            val uuid1 = UUID.randomUUID()
            val friends = List(10) { UUID.randomUUID() }
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addFriends(uuid1, friends) }
            assertTrue { service.addFriend(uuid1, uuid2) }

            assertTrue { service.removeFriends(uuid1, friends) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friends(uuid1, uuid2, false, 11)
            )
        }

        @Test
        fun `should remove if relation exists in the other direction`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.removeFriends(uuid2, listOf(uuid1)) }

            assertThat(getAll()).isEmpty()
        }

        @Test
        fun `should remove relationship bidirectional`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            val uuid3 = UUID.randomUUID()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.addFriend(uuid2, uuid1) }
            assertTrue { service.addFriend(uuid3, uuid1) }
            assertTrue { service.removeFriends(uuid1, listOf(uuid2, uuid3)) }

            assertThat(getAll()).isEmpty()
        }

        @Test
        fun `should not remove if relation doesn't exist`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertFalse { service.removeFriends(uuid1, listOf(uuid2)) }
            assertThat(getAll()).isEmpty()
        }

        @Test
        fun `should not remove pending relationship`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            val friends = List(10) { UUID.randomUUID() }

            assertTrue { service.addPendingFriend(uuid1, uuid2) }
            assertTrue { service.addFriends(uuid1, friends) }
            assertTrue { service.addFriend(uuid1, uuid2) }

            assertTrue { service.removeFriends(uuid1, friends + uuid2) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friends(uuid1, uuid2, true, 1)
            )
        }

    }

    @Nested
    inner class RemovePendingFriend {

        @Test
        fun `should remove if relation exists`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }
            assertTrue { service.removePendingFriend(uuid1, uuid2) }

            assertThat(getAll()).isEmpty()
        }

        @Test
        fun `should remove if relation exists in the other direction`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }
            assertTrue { service.removePendingFriend(uuid2, uuid1) }

            assertThat(getAll()).isEmpty()
        }

        @Test
        fun `should remove relationship bidirectional`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }
            assertTrue { service.addPendingFriend(uuid2, uuid1) }
            assertTrue { service.removePendingFriend(uuid1, uuid2) }

            assertThat(getAll()).isEmpty()
        }

        @Test
        fun `should not remove if relation doesn't exist`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertFalse { service.removePendingFriend(uuid1, uuid2) }
            assertThat(getAll()).isEmpty()
        }

        @Test
        fun `should not remove validated relationship`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertFalse { service.removePendingFriend(uuid1, uuid2) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friends(uuid1, uuid2, false, 1)
            )
        }

    }

    @Nested
    inner class GetFriends {

        @Test
        fun `should return empty list if no relation`() = runTest {
            val uuid = UUID.randomUUID()
            assertTrue { service.getFriends(uuid).toList().isEmpty() }
        }

        @Test
        fun `should return list of friends`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            val uuid3 = UUID.randomUUID()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.addFriend(uuid1, uuid3) }

            assertThat(service.getFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2, uuid3)
        }

        @Test
        fun `should return list of friends in the other direction`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            val uuid3 = UUID.randomUUID()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.addFriend(uuid3, uuid1) }

            assertThat(service.getFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2, uuid3)
        }

        @Test
        fun `should return list of non pending relationship`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            val uuid3 = UUID.randomUUID()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.addFriend(uuid3, uuid1) }
            assertTrue { service.addPendingFriend(uuid1, UUID.randomUUID()) }
            assertTrue { service.addPendingFriend(uuid1, UUID.randomUUID()) }
            assertTrue { service.addPendingFriend(UUID.randomUUID(), uuid1) }

            assertThat(service.getFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2, uuid3)
        }

    }

    @Nested
    inner class GetPendingFriends {

        @Test
        fun `should return empty list if no relation`() = runTest {
            val uuid = UUID.randomUUID()
            assertTrue { service.getPendingFriends(uuid).toList().isEmpty() }
        }

        @Test
        fun `should return list of relation`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            val uuid3 = UUID.randomUUID()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }
            assertTrue { service.addPendingFriend(uuid1, uuid3) }

            assertThat(service.getPendingFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2, uuid3)
        }

        @Test
        fun `should return list of relations in the other direction`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            val uuid3 = UUID.randomUUID()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }
            assertTrue { service.addPendingFriend(uuid3, uuid1) }

            assertThat(service.getPendingFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2, uuid3)
        }

        @Test
        fun `should return list of non pending relationship`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            val uuid3 = UUID.randomUUID()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }
            assertTrue { service.addPendingFriend(uuid3, uuid1) }
            assertTrue { service.addFriend(uuid1, UUID.randomUUID()) }
            assertTrue { service.addFriend(uuid1, UUID.randomUUID()) }
            assertTrue { service.addFriend(UUID.randomUUID(), uuid1) }

            assertThat(service.getPendingFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2, uuid3)
        }

    }

    @Nested
    inner class IsFriend {

        @Test
        fun `should return true if A is friend with B`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.isFriend(uuid1, uuid2) }
        }

        @Test
        fun `should return true if B is friend with A`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.isFriend(uuid2, uuid1) }
        }

        @Test
        fun `should return true if are friend bidirectional`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.addFriend(uuid2, uuid1) }

            assertTrue { service.isFriend(uuid2, uuid1) }
        }

        @Test
        fun `should return false if no relation`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertFalse { service.isFriend(uuid1, uuid2) }
        }

        @Test
        fun `should return false if pending relationship`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }
            assertFalse { service.isFriend(uuid1, uuid2) }

            assertTrue { service.addPendingFriend(uuid2, uuid1) }
            assertFalse { service.isFriend(uuid1, uuid2) }
        }
    }

    @Nested
    inner class IsPendingFriend {

        @Test
        fun `should return true if A is friend with B`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }
            assertTrue { service.isPendingFriend(uuid1, uuid2) }
        }

        @Test
        fun `should return true if B is friend with A`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }
            assertTrue { service.isPendingFriend(uuid2, uuid1) }
        }

        @Test
        fun `should return true if are friend bidirectional`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }
            assertTrue { service.addPendingFriend(uuid2, uuid1) }

            assertTrue { service.isPendingFriend(uuid2, uuid1) }
        }

        @Test
        fun `should return false if no relation`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertFalse { service.isPendingFriend(uuid1, uuid2) }
        }

        @Test
        fun `should return false if validate relationship`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertFalse { service.isPendingFriend(uuid1, uuid2) }

            assertTrue { service.addFriend(uuid2, uuid1) }
            assertFalse { service.isPendingFriend(uuid1, uuid2) }
        }
    }

    private suspend fun getAll(): List<Friends> {
        val query = QueryDsl.from(friends)
        return database.flowQuery(query).toList()
    }
}