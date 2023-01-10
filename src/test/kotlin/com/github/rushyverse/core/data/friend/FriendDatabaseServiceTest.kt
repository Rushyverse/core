package com.github.rushyverse.core.data.friend

import com.github.rushyverse.core.container.createPSQLContainer
import com.github.rushyverse.core.data.FriendDatabaseService
import com.github.rushyverse.core.data.Friends
import kotlinx.coroutines.test.runTest
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.select
import org.jetbrains.exposed.sql.transactions.experimental.newSuspendedTransaction
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.jupiter.api.Nested
import org.postgresql.Driver
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.util.*
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@Testcontainers
class FriendDatabaseServiceTest {

    companion object {
        @JvmStatic
        @Container
        private val psqlContainer = createPSQLContainer()
    }

    private lateinit var service: FriendDatabaseService

    @BeforeTest
    fun onBefore() {
        Database.connect(
            url = psqlContainer.jdbcUrl,
            driver = Driver::class.java.name,
            user = psqlContainer.username,
            password = psqlContainer.password
        )
        transaction {
            SchemaUtils.create(Friends)
        }

        service = FriendDatabaseService()
    }

    @Nested
    inner class AddFriend {

        @Test
        fun `should add friend if relation doesn't exist`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addFriend(uuid1, uuid2) }

            newSuspendedTransaction {
                val result = Friends.select { Friends.uuid1.eq(uuid1) and Friends.uuid2.eq(uuid2) }.single()
                assertTrue { result[Friends.uuid1] == uuid1 }
                assertTrue { result[Friends.uuid2] == uuid2 }

                assertTrue { Friends.select { Friends.uuid1.eq(uuid2) and Friends.uuid2.eq(uuid1) }.empty() }
            }
        }

        @Test
        fun `should not add friend if relation already exists`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertFalse { service.addFriend(uuid1, uuid2) }
        }

        @Test
        fun `should add friend if relation doesn't exist but exists in the other direction`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.addFriend(uuid2, uuid1) }

            newSuspendedTransaction {
                var result = Friends.select { Friends.uuid1.eq(uuid1) and Friends.uuid2.eq(uuid2) }.single()
                assertTrue { result[Friends.uuid1] == uuid1 }
                assertTrue { result[Friends.uuid2] == uuid2 }

                result = Friends.select { Friends.uuid1.eq(uuid2) and Friends.uuid2.eq(uuid1) }.single()
                assertTrue { result[Friends.uuid2] == uuid1 }
                assertTrue { result[Friends.uuid1] == uuid2 }
            }
        }

    }

    @Nested
    inner class RemoveFriend {

    }

    @Nested
    inner class GetFriends {

    }

    @Nested
    inner class IsFriend {

    }

}