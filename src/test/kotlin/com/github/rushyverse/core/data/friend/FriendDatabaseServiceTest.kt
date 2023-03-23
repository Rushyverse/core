package com.github.rushyverse.core.data.friend

import com.github.rushyverse.core.container.createPSQLContainer
import com.github.rushyverse.core.data.Friend
import com.github.rushyverse.core.data.FriendDatabaseService
import com.github.rushyverse.core.data._Friend
import com.github.rushyverse.core.data.utils.DatabaseUtils
import com.github.rushyverse.core.data.utils.DatabaseUtils.createConnectionOptions
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.komapper.core.dsl.QueryDsl
import org.komapper.r2dbc.R2dbcDatabase
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
        database = R2dbcDatabase(createConnectionOptions(psqlContainer))

        Friend.createTable(database)
        service = FriendDatabaseService(database)
    }

    @AfterTest
    fun onAfter() = runBlocking {
        database.runQuery(QueryDsl.drop(_Friend.friend))
    }

    @Nested
    inner class AddFriend {

        @Test
        fun `should add one friend`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addFriend(uuid1, uuid2) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false)
            )
        }

        @Test
        fun `should add several friends`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            val uuid3 = UUID.randomUUID()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.addFriend(uuid1, uuid3) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false),
                Friend(uuid1, uuid3, false)
            )
        }

        @Test
        fun `should not add friend if relation already exists`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertFalse { service.addFriend(uuid1, uuid2) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false)
            )
        }

        @Test
        fun `should not add friend if relation doesn't exist but exists in the other direction`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertFalse { service.addFriend(uuid2, uuid1) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false),
            )
        }

        @Test
        fun `should update friend if relation already exists but is pending`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }
            assertTrue { service.addFriend(uuid1, uuid2) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false),
            )

            val uuid3 = UUID.randomUUID()

            assertTrue { service.addPendingFriend(uuid1, uuid3) }
            assertTrue { service.addFriend(uuid3, uuid1) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false),
                Friend(uuid1, uuid3, false),
            )
        }

        @Test
        fun `should update only the target relationship`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            val uuid3 = UUID.randomUUID()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }
            assertTrue { service.addPendingFriend(uuid1, uuid3) }
            assertTrue { service.addFriend(uuid1, uuid2) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false),
                Friend(uuid1, uuid3, true),
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
                Friend(uuid1, uuid2, false)
            )
        }

        @Test
        fun `should add several friends`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            val uuid3 = UUID.randomUUID()

            assertTrue { service.addFriends(uuid1, listOf(uuid2, uuid3)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false),
                Friend(uuid1, uuid3, false)
            )
        }

        @Test
        fun `should not add friend if relation already exists`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addFriends(uuid1, listOf(uuid2)) }
            assertFalse { service.addFriends(uuid1, listOf(uuid2)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false)
            )
        }

        @Test
        fun `should not add if the relation already exists by the other way`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addFriends(uuid1, listOf(uuid2)) }
            assertFalse { service.addFriends(uuid2, listOf(uuid1)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false),
            )
        }

        @Test
        fun `should returns true if at least one relation is insert`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            val uuid3 = UUID.randomUUID()

            assertTrue { service.addFriends(uuid1, listOf(uuid2)) }
            assertTrue { service.addFriends(uuid1, listOf(uuid2, uuid3)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false),
                Friend(uuid1, uuid3, false)
            )
        }

        @Test
        fun `should returns true if at least one relation is update`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            val uuid3 = UUID.randomUUID()

            assertTrue { service.addFriends(uuid1, listOf(uuid2)) }
            assertTrue { service.addPendingFriend(uuid1, uuid3) }
            assertTrue { service.addFriends(uuid1, listOf(uuid2, uuid3)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false),
                Friend(uuid1, uuid3, false)
            )
        }

        @Test
        fun `should returns false if no relation is insert or update`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            val uuid3 = UUID.randomUUID()

            assertTrue { service.addFriends(uuid1, listOf(uuid2, uuid3)) }
            assertFalse { service.addFriends(uuid1, listOf(uuid2, uuid3)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false),
                Friend(uuid1, uuid3, false)
            )
        }

        @Test
        fun `should update friend if relation already exists but is pending`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }
            assertTrue { service.addFriends(uuid1, listOf(uuid2)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false),
            )

            val uuid3 = UUID.randomUUID()

            assertTrue { service.addPendingFriend(uuid1, uuid3) }
            assertTrue { service.addFriends(uuid3, listOf(uuid1)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false),
                Friend(uuid1, uuid3, false),
            )
        }

        @Test
        fun `should update only the target relationship`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            val uuid3 = UUID.randomUUID()
            val uuid4 = UUID.randomUUID()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }
            assertTrue { service.addPendingFriend(uuid1, uuid3) }
            assertTrue { service.addPendingFriend(uuid1, uuid4) }

            assertTrue { service.addFriends(uuid1, listOf(uuid2)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false),
                Friend(uuid1, uuid3, true),
                Friend(uuid1, uuid4, true),
            )

            assertTrue { service.addFriends(uuid1, listOf(uuid3, uuid4)) }
            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false),
                Friend(uuid1, uuid3, false),
                Friend(uuid1, uuid4, false),
            )
        }

        @Test
        fun `should update the pending relationship even if non pending relation is added`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            val uuid3 = UUID.randomUUID()
            val uuid4 = UUID.randomUUID()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }
            assertTrue { service.addPendingFriend(uuid1, uuid3) }
            assertTrue { service.addFriend(uuid1, uuid4) }

            assertTrue { service.addFriends(uuid1, listOf(uuid2, uuid3, uuid4)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false),
                Friend(uuid1, uuid3, false),
                Friend(uuid1, uuid4, false),
            )
        }

    }

    @Nested
    inner class AddPendingFriend {

        @Test
        fun `should add one friend`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, true)
            )
        }

        @Test
        fun `should add several friends`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            val uuid3 = UUID.randomUUID()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }
            assertTrue { service.addPendingFriend(uuid1, uuid3) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, true),
                Friend(uuid1, uuid3, true)
            )
        }

        @Test
        fun `should not add friend if relation already exists`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }
            assertFalse { service.addPendingFriend(uuid1, uuid2) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, true)
            )
        }

        @Test
        fun `should not add friend if relation doesn't exist but exists in the other direction`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }
            assertFalse { service.addPendingFriend(uuid2, uuid1) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, true),
            )
        }

        @Test
        fun `should not update friend if relation already exists but is not pending`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertFalse { service.addPendingFriend(uuid1, uuid2) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false),
            )

            val uuid3 = UUID.randomUUID()

            assertTrue { service.addFriend(uuid1, uuid3) }
            assertFalse { service.addPendingFriend(uuid3, uuid1) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false),
                Friend(uuid1, uuid3, false),
            )
        }
    }

    @Nested
    inner class AddPendingFriends {

        @Test
        fun `should add no friends`() = runTest {
            val uuid1 = UUID.randomUUID()

            assertTrue { service.addPendingFriends(uuid1, emptyList()) }

            assertThat(getAll()).isEmpty()
        }

        @Test
        fun `should add one friend`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addPendingFriends(uuid1, listOf(uuid2)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, true)
            )
        }

        @Test
        fun `should add several friends`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            val uuid3 = UUID.randomUUID()

            assertTrue { service.addPendingFriends(uuid1, listOf(uuid2, uuid3)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, true),
                Friend(uuid1, uuid3, true)
            )
        }

        @Test
        fun `should not add friend if relation already exists`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addPendingFriends(uuid1, listOf(uuid2)) }
            assertFalse { service.addPendingFriends(uuid1, listOf(uuid2)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, true)
            )
        }

        @Test
        fun `should not add if the relation already exists by the other way`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addPendingFriends(uuid1, listOf(uuid2)) }
            assertFalse { service.addPendingFriends(uuid2, listOf(uuid1)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, true),
            )
        }

        @Test
        fun `should returns true if at least one relation is insert`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            val uuid3 = UUID.randomUUID()

            assertTrue { service.addPendingFriends(uuid1, listOf(uuid2)) }
            assertTrue { service.addPendingFriends(uuid1, listOf(uuid2, uuid3)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, true),
                Friend(uuid1, uuid3, true)
            )
        }

        @Test
        fun `should returns false if no relation is insert`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()
            val uuid3 = UUID.randomUUID()

            assertTrue { service.addPendingFriends(uuid1, listOf(uuid2, uuid3)) }
            assertFalse { service.addPendingFriends(uuid1, listOf(uuid2, uuid3)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, true),
                Friend(uuid1, uuid3, true)
            )
        }

        @Test
        fun `should not update friend if relation already exists but is not pending`() = runTest {
            val uuid1 = UUID.randomUUID()
            val uuid2 = UUID.randomUUID()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertFalse { service.addPendingFriends(uuid1, listOf(uuid2)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false),
            )

            val uuid3 = UUID.randomUUID()

            assertTrue { service.addFriend(uuid1, uuid3) }
            assertFalse { service.addPendingFriends(uuid3, listOf(uuid1)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false),
                Friend(uuid1, uuid3, false),
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
            assertTrue { service.removeFriend(uuid2, uuid1) }

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
                Friend(uuid1, uuid2, true)
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
                Friend(uuid1, uuid2, false)
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
                Friend(uuid1, uuid2, false)
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
            val uuid3 = UUID.randomUUID()
            val friends = List(10) { UUID.randomUUID() }

            assertTrue { service.addPendingFriend(uuid1, uuid2) }
            assertTrue { service.addPendingFriend(uuid3, uuid1) }
            assertTrue { service.addFriends(uuid1, friends) }

            assertTrue { service.removeFriends(uuid1, friends) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, true),
                Friend(uuid3, uuid1, true),
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
            assertTrue { service.removePendingFriend(uuid2, uuid1) }

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
                Friend(uuid1, uuid2, false)
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

            assertTrue { service.isFriend(uuid2, uuid1) }
            assertTrue { service.isFriend(uuid1, uuid2) }
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
            assertFalse { service.isFriend(uuid2, uuid1) }
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

            assertTrue { service.isPendingFriend(uuid1, uuid2) }
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
        }
    }

    private suspend fun getAll(): List<Friend> {
        return DatabaseUtils.getAll(database, _Friend.friend)
    }
}