package com.github.rushyverse.core.data.friend

import com.github.rushyverse.core.container.createPSQLContainer
import com.github.rushyverse.core.data.Friend
import com.github.rushyverse.core.data.FriendDatabaseService
import com.github.rushyverse.core.data._Friend
import com.github.rushyverse.core.data.player.PlayerDatabaseService
import com.github.rushyverse.core.data.player._Player
import com.github.rushyverse.core.data.utils.DatabaseUtils
import com.github.rushyverse.core.data.utils.DatabaseUtils.createR2dbcDatabase
import com.github.rushyverse.core.utils.createPlayer
import io.r2dbc.spi.R2dbcException
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.komapper.core.dsl.QueryDsl
import org.komapper.core.dsl.query.bind
import org.komapper.r2dbc.R2dbcDatabase
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.MountableFile
import java.util.*
import kotlin.test.*

@Testcontainers
class FriendDatabaseServiceTest {

    companion object {
        @JvmStatic
        @Container
        private val psqlContainer = createPSQLContainer()
            .withCopyToContainer(
                MountableFile.forClasspathResource("sql/player.sql"),
                "/docker-entrypoint-initdb.d/1.sql"
            )
            .withCopyToContainer(
                MountableFile.forClasspathResource("sql/friend.sql"),
                "/docker-entrypoint-initdb.d/2.sql"
            )
    }

    private lateinit var service: FriendDatabaseService
    private lateinit var playerService: PlayerDatabaseService
    private lateinit var database: R2dbcDatabase

    @BeforeTest
    fun onBefore() = runBlocking {
        database = createR2dbcDatabase(psqlContainer)
        service = FriendDatabaseService(database)
        playerService = PlayerDatabaseService(database)
    }

    @AfterTest
    fun onAfter() = runBlocking<Unit> {
        database.runQuery(QueryDsl.delete(_Friend.friend).all().options { it.copy(allowMissingWhereClause = true) })
        database.runQuery(QueryDsl.delete(_Player.player).all().options { it.copy(allowMissingWhereClause = true) })
    }

    @Nested
    inner class AddFriend {

        @ParameterizedTest
        @ValueSource(strings = ["/*uuid*/'', ''", "'', /*uuid*/''"])
        fun `should reject empty uuid`(uuidTags: String) = runTest {
            val meta = _Friend.friend
            val friendUuidColumn1Name = meta.uuid.uuid1.columnName
            val friendUuidColumn2Name = meta.uuid.uuid2.columnName
            val friendPendingColumnName = meta.pending.columnName

            val query = QueryDsl.executeTemplate(
                """
                INSERT INTO ${meta.tableName()} ($friendUuidColumn1Name, $friendUuidColumn2Name, $friendPendingColumnName) 
                VALUES ($uuidTags, false);
                """.trimIndent()
            ).bind("uuid", saveNewPlayer())

            val exception = assertThrows<R2dbcException> {
                database.runQuery(query)
            }

            assertThat(exception).hasMessage("invalid input syntax for type uuid: \"\"")
            assertThat(exception.sqlState).isEqualTo("22P02")
        }

        @Test
        fun `should add one friend`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

            assertTrue { service.addFriend(uuid1, uuid2) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false)
            )
        }

        @Test
        fun `should add several friends`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()
            val uuid3 = saveNewPlayer()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.addFriend(uuid1, uuid3) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false),
                Friend(uuid1, uuid3, false)
            )
        }

        @Test
        fun `should not add friend if relation already exists`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertFalse { service.addFriend(uuid1, uuid2) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false)
            )
        }

        @Test
        fun `should not add friend if relation doesn't exist but exists in the other direction`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertFalse { service.addFriend(uuid2, uuid1) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false),
            )
        }

        @Test
        fun `should update friend if relation already exists but is pending`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }
            assertTrue { service.addFriend(uuid1, uuid2) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false),
            )

            val uuid3 = saveNewPlayer()

            assertTrue { service.addPendingFriend(uuid1, uuid3) }
            assertTrue { service.addFriend(uuid3, uuid1) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false),
                Friend(uuid1, uuid3, false),
            )
        }

        @Test
        fun `should update only the target relationship`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()
            val uuid3 = saveNewPlayer()

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
            val uuid1 = saveNewPlayer()

            assertTrue { service.addFriends(uuid1, emptyList()) }

            assertThat(getAll()).isEmpty()
        }

        @Test
        fun `should add one friend`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

            assertTrue { service.addFriends(uuid1, listOf(uuid2)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false)
            )
        }

        @Test
        fun `should add several friends`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()
            val uuid3 = saveNewPlayer()

            assertTrue { service.addFriends(uuid1, listOf(uuid2, uuid3)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false),
                Friend(uuid1, uuid3, false)
            )
        }

        @Test
        fun `should not add friend if relation already exists`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

            assertTrue { service.addFriends(uuid1, listOf(uuid2)) }
            assertFalse { service.addFriends(uuid1, listOf(uuid2)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false)
            )
        }

        @Test
        fun `should not add if the relation already exists by the other way`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

            assertTrue { service.addFriends(uuid1, listOf(uuid2)) }
            assertFalse { service.addFriends(uuid2, listOf(uuid1)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false),
            )
        }

        @Test
        fun `should returns true if at least one relation is insert`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()
            val uuid3 = saveNewPlayer()

            assertTrue { service.addFriends(uuid1, listOf(uuid2)) }
            assertTrue { service.addFriends(uuid1, listOf(uuid2, uuid3)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false),
                Friend(uuid1, uuid3, false)
            )
        }

        @Test
        fun `should returns true if at least one relation is update`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()
            val uuid3 = saveNewPlayer()

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
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()
            val uuid3 = saveNewPlayer()

            assertTrue { service.addFriends(uuid1, listOf(uuid2, uuid3)) }
            assertFalse { service.addFriends(uuid1, listOf(uuid2, uuid3)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false),
                Friend(uuid1, uuid3, false)
            )
        }

        @Test
        fun `should update friend if relation already exists but is pending`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }
            assertTrue { service.addFriends(uuid1, listOf(uuid2)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false),
            )

            val uuid3 = saveNewPlayer()

            assertTrue { service.addPendingFriend(uuid1, uuid3) }
            assertTrue { service.addFriends(uuid3, listOf(uuid1)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false),
                Friend(uuid1, uuid3, false),
            )
        }

        @Test
        fun `should update only the target relationship`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()
            val uuid3 = saveNewPlayer()
            val uuid4 = saveNewPlayer()

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
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()
            val uuid3 = saveNewPlayer()
            val uuid4 = saveNewPlayer()

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
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, true)
            )
        }

        @Test
        fun `should add several friends`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()
            val uuid3 = saveNewPlayer()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }
            assertTrue { service.addPendingFriend(uuid1, uuid3) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, true),
                Friend(uuid1, uuid3, true)
            )
        }

        @Test
        fun `should not add friend if relation already exists`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }
            assertFalse { service.addPendingFriend(uuid1, uuid2) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, true)
            )
        }

        @Test
        fun `should not add friend if relation doesn't exist but exists in the other direction`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }
            assertFalse { service.addPendingFriend(uuid2, uuid1) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, true),
            )
        }

        @Test
        fun `should not update friend if relation already exists but is not pending`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertFalse { service.addPendingFriend(uuid1, uuid2) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false),
            )

            val uuid3 = saveNewPlayer()

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
            val uuid1 = saveNewPlayer()

            assertTrue { service.addPendingFriends(uuid1, emptyList()) }

            assertThat(getAll()).isEmpty()
        }

        @Test
        fun `should add one friend`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

            assertTrue { service.addPendingFriends(uuid1, listOf(uuid2)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, true)
            )
        }

        @Test
        fun `should add several friends`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()
            val uuid3 = saveNewPlayer()

            assertTrue { service.addPendingFriends(uuid1, listOf(uuid2, uuid3)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, true),
                Friend(uuid1, uuid3, true)
            )
        }

        @Test
        fun `should not add friend if relation already exists`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

            assertTrue { service.addPendingFriends(uuid1, listOf(uuid2)) }
            assertFalse { service.addPendingFriends(uuid1, listOf(uuid2)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, true)
            )
        }

        @Test
        fun `should not add if the relation already exists by the other way`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

            assertTrue { service.addPendingFriends(uuid1, listOf(uuid2)) }
            assertFalse { service.addPendingFriends(uuid2, listOf(uuid1)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, true),
            )
        }

        @Test
        fun `should returns true if at least one relation is insert`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()
            val uuid3 = saveNewPlayer()

            assertTrue { service.addPendingFriends(uuid1, listOf(uuid2)) }
            assertTrue { service.addPendingFriends(uuid1, listOf(uuid2, uuid3)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, true),
                Friend(uuid1, uuid3, true)
            )
        }

        @Test
        fun `should returns false if no relation is insert`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()
            val uuid3 = saveNewPlayer()

            assertTrue { service.addPendingFriends(uuid1, listOf(uuid2, uuid3)) }
            assertFalse { service.addPendingFriends(uuid1, listOf(uuid2, uuid3)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, true),
                Friend(uuid1, uuid3, true)
            )
        }

        @Test
        fun `should not update friend if relation already exists but is not pending`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertFalse { service.addPendingFriends(uuid1, listOf(uuid2)) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false),
            )

            val uuid3 = saveNewPlayer()

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
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.removeFriend(uuid1, uuid2) }

            assertThat(getAll()).isEmpty()
        }

        @Test
        fun `should remove if relation exists in the other direction`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.removeFriend(uuid2, uuid1) }

            assertThat(getAll()).isEmpty()
        }

        @Test
        fun `should remove relationship bidirectional`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.removeFriend(uuid2, uuid1) }

            assertThat(getAll()).isEmpty()
        }

        @Test
        fun `should not remove if relation doesn't exist`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

            assertFalse { service.removeFriend(uuid1, uuid2) }
            assertThat(getAll()).isEmpty()
        }

        @Test
        fun `should not remove pending relationship`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

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
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

            assertTrue { service.addFriend(uuid1, uuid2) }

            assertTrue { service.removeFriends(uuid1, emptyList()) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false)
            )
        }

        @Test
        fun `should remove one friend`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

            assertTrue { service.addFriend(uuid1, uuid2) }

            assertTrue { service.removeFriends(uuid1, listOf(uuid2)) }

            assertThat(getAll()).isEmpty()
        }

        @Test
        fun `should remove several friends`() = runTest {
            val uuid1 = saveNewPlayer()
            val friends = List(10) { saveNewPlayer() }

            assertTrue { service.addFriends(uuid1, friends) }

            assertTrue { service.removeFriends(uuid1, friends) }

            assertThat(getAll()).isEmpty()
        }

        @Test
        fun `should remove partial several friends`() = runTest {
            val uuid1 = saveNewPlayer()
            val friends = List(10) { saveNewPlayer() }
            val uuid2 = saveNewPlayer()

            assertTrue { service.addFriends(uuid1, friends) }
            assertTrue { service.addFriend(uuid1, uuid2) }

            assertTrue { service.removeFriends(uuid1, friends) }

            assertThat(getAll()).containsExactlyInAnyOrder(
                Friend(uuid1, uuid2, false)
            )
        }

        @Test
        fun `should remove if relation exists in the other direction`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.removeFriends(uuid2, listOf(uuid1)) }

            assertThat(getAll()).isEmpty()
        }

        @Test
        fun `should remove relationship bidirectional`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()
            val uuid3 = saveNewPlayer()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.addFriend(uuid3, uuid1) }
            assertTrue { service.removeFriends(uuid1, listOf(uuid2, uuid3)) }

            assertThat(getAll()).isEmpty()
        }

        @Test
        fun `should not remove if relation doesn't exist`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

            assertFalse { service.removeFriends(uuid1, listOf(uuid2)) }
            assertThat(getAll()).isEmpty()
        }

        @Test
        fun `should not remove pending relationship`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()
            val uuid3 = saveNewPlayer()
            val friends = List(10) { saveNewPlayer() }

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
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }
            assertTrue { service.removePendingFriend(uuid1, uuid2) }

            assertThat(getAll()).isEmpty()
        }

        @Test
        fun `should remove if relation exists in the other direction`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }
            assertTrue { service.removePendingFriend(uuid2, uuid1) }

            assertThat(getAll()).isEmpty()
        }

        @Test
        fun `should remove relationship bidirectional`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }
            assertTrue { service.removePendingFriend(uuid2, uuid1) }

            assertThat(getAll()).isEmpty()
        }

        @Test
        fun `should not remove if relation doesn't exist`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

            assertFalse { service.removePendingFriend(uuid1, uuid2) }
            assertThat(getAll()).isEmpty()
        }

        @Test
        fun `should not remove validated relationship`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

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
            val uuid = saveNewPlayer()
            assertTrue { service.getFriends(uuid).toList().isEmpty() }
        }

        @Test
        fun `should return list of friends`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()
            val uuid3 = saveNewPlayer()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.addFriend(uuid1, uuid3) }

            assertThat(service.getFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2, uuid3)
        }

        @Test
        fun `should return list of friends in the other direction`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()
            val uuid3 = saveNewPlayer()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.addFriend(uuid3, uuid1) }

            assertThat(service.getFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2, uuid3)
        }

        @Test
        fun `should return list of non pending relationship`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()
            val uuid3 = saveNewPlayer()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.addFriend(uuid3, uuid1) }
            assertTrue { service.addPendingFriend(uuid1, saveNewPlayer()) }
            assertTrue { service.addPendingFriend(uuid1, saveNewPlayer()) }
            assertTrue { service.addPendingFriend(saveNewPlayer(), uuid1) }

            assertThat(service.getFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2, uuid3)
        }

    }

    @Nested
    inner class GetPendingFriends {

        @Test
        fun `should return empty list if no relation`() = runTest {
            val uuid = saveNewPlayer()
            assertTrue { service.getPendingFriends(uuid).toList().isEmpty() }
        }

        @Test
        fun `should return list of relation`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()
            val uuid3 = saveNewPlayer()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }
            assertTrue { service.addPendingFriend(uuid1, uuid3) }

            assertThat(service.getPendingFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2, uuid3)
        }

        @Test
        fun `should return list of relations in the other direction`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()
            val uuid3 = saveNewPlayer()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }
            assertTrue { service.addPendingFriend(uuid3, uuid1) }

            assertThat(service.getPendingFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2, uuid3)
        }

        @Test
        fun `should return list of non pending relationship`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()
            val uuid3 = saveNewPlayer()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }
            assertTrue { service.addPendingFriend(uuid3, uuid1) }
            assertTrue { service.addFriend(uuid1, saveNewPlayer()) }
            assertTrue { service.addFriend(uuid1, saveNewPlayer()) }
            assertTrue { service.addFriend(saveNewPlayer(), uuid1) }

            assertThat(service.getPendingFriends(uuid1).toList()).containsExactlyInAnyOrder(uuid2, uuid3)
        }

    }

    @Nested
    inner class IsFriend {

        @Test
        fun `should return true if A is friend with B`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.isFriend(uuid1, uuid2) }
        }

        @Test
        fun `should return true if B is friend with A`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertTrue { service.isFriend(uuid2, uuid1) }
        }

        @Test
        fun `should return true if are friend bidirectional`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

            assertTrue { service.addFriend(uuid1, uuid2) }

            assertTrue { service.isFriend(uuid2, uuid1) }
            assertTrue { service.isFriend(uuid1, uuid2) }
        }

        @Test
        fun `should return false if no relation`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

            assertFalse { service.isFriend(uuid1, uuid2) }
        }

        @Test
        fun `should return false if pending relationship`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }
            assertFalse { service.isFriend(uuid1, uuid2) }
            assertFalse { service.isFriend(uuid2, uuid1) }
        }
    }

    @Nested
    inner class IsPendingFriend {

        @Test
        fun `should return true if A is friend with B`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }
            assertTrue { service.isPendingFriend(uuid1, uuid2) }
        }

        @Test
        fun `should return true if B is friend with A`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }
            assertTrue { service.isPendingFriend(uuid2, uuid1) }
        }

        @Test
        fun `should return true if are friend bidirectional`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

            assertTrue { service.addPendingFriend(uuid1, uuid2) }

            assertTrue { service.isPendingFriend(uuid1, uuid2) }
            assertTrue { service.isPendingFriend(uuid2, uuid1) }
        }

        @Test
        fun `should return false if no relation`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

            assertFalse { service.isPendingFriend(uuid1, uuid2) }
        }

        @Test
        fun `should return false if validate relationship`() = runTest {
            val uuid1 = saveNewPlayer()
            val uuid2 = saveNewPlayer()

            assertTrue { service.addFriend(uuid1, uuid2) }
            assertFalse { service.isPendingFriend(uuid1, uuid2) }
        }
    }

    private suspend fun getAll(): List<Friend> {
        return DatabaseUtils.getAll(database, _Friend.friend)
    }

    private suspend fun saveNewPlayer(uuid: UUID = UUID.randomUUID()): UUID {
        val player = createPlayer(uuid)
        playerService.savePlayer(player)
        return player.uuid
    }
}
