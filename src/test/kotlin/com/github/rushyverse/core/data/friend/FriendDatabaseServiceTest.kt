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
        }

        @Test
        fun `should add friend if relation doesn't exist`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addFriend(uuid1, uuid2) }

            val result = getAllFriends()
            assertEquals(listOf(Friends(uuid1, uuid2, 1)), result)
        }

        @Test
        fun `should add friend if relation already exists`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.addFriend(uuid1, uuid2) }

            val result = getAllFriends()
            assertThat(result).containsExactlyInAnyOrder(Friends(uuid1, uuid2, 1), Friends(uuid1, uuid2, 2))
        }

        @Test
        fun `should add friend if relation doesn't exist but exists in the other direction`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.addFriend(uuid2, uuid1) }

            val result = getAllFriends()
            assertThat(result).containsExactlyInAnyOrder(Friends(uuid1, uuid2, 1), Friends(uuid2, uuid1, 2))
        }

    }

    @Nested
    inner class RemoveFriend {

        @Test
        fun `should remove friend if relation exists`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.removeFriend(uuid1, uuid2) }

            assertEquals(emptyList(), getAllFriends())
        }

        @Test
        fun `should remove friend if relation exists in the other direction`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.removeFriend(uuid2, uuid1) }

            assertEquals(emptyList(), getAllFriends())
        }

        @Test
        fun `should not remove friend if relation doesn't exist`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertFalse { service.removeFriend(uuid1, uuid2) }
        }

    }

    @Nested
    inner class GetFriends {

        @Test
        fun `should return empty list if no friends`() = runTest {
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

            database.runQuery(
                QueryDsl.insert(friends)
                    .multiple(Friends(uuid1, uuid2), Friends(uuid2, uuid1))
            )

            assertTrue { service.isFriend(uuid2, uuid1) }
        }


        @Test
        fun `should return false if not friends`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertFalse { service.isFriend(uuid1, uuid2) }
        }
    }

    private suspend fun getAllFriends(): List<Friends> {
        val query = QueryDsl.from(friends)
        return database.flowQuery(query).toList()
    }

}