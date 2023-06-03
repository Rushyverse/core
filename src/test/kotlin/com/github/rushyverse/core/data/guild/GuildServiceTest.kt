package com.github.rushyverse.core.data.guild

import com.github.rushyverse.core.data.Guild
import com.github.rushyverse.core.data.GuildInvite
import com.github.rushyverse.core.data.GuildMember
import com.github.rushyverse.core.data.GuildService
import com.github.rushyverse.core.supplier.database.DatabaseSupplierConfiguration
import com.github.rushyverse.core.supplier.database.IDatabaseEntitySupplier
import com.github.rushyverse.core.utils.getRandomString
import io.mockk.*
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.time.Instant
import kotlin.random.Random
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class GuildServiceTest {

    private lateinit var service: GuildService
    private val supplier get() = service.supplier

    @BeforeTest
    fun onBefore() {
        service = GuildService(mockk(getRandomString()))
    }

    @Test
    fun `should create a new instance with another strategy`() {
        val configuration = mockk<DatabaseSupplierConfiguration>(getRandomString())
        val strategy = mockk<IDatabaseEntitySupplier>(getRandomString())
        every { strategy.configuration } returns configuration

        val service = GuildService(strategy)
        assertEquals(strategy, service.supplier)

        val strategy2 = mockk<IDatabaseEntitySupplier>(getRandomString())
        val service2 = service.withStrategy {
            assertEquals(configuration, it)
            strategy2
        }
        assertEquals(strategy2, service2.supplier)

    }

    @Nested
    inner class CreateGuild {

        @Test
        fun `should create in supplier`() = runTest {
            val slot1 = slot<String>()
            val slot2 = slot<String>()

            coEvery { supplier.createGuild(capture(slot1), capture(slot2)) } returns mockk()

            val name = getRandomString()
            val owner = getRandomString()
            service.createGuild(name, owner)
            coVerify(exactly = 1) { supplier.createGuild(name, owner) }

            assertEquals(name, slot1.captured)
            assertEquals(owner, slot2.captured)
        }

        @ParameterizedTest
        @ValueSource(booleans = [true, false])
        fun `should return supplier result`(result: Boolean) = runTest {
            coEvery { supplier.deleteGuild(any()) } returns result
            assertEquals(result, service.deleteGuild(Random.nextInt()))
            coVerify(exactly = 1) { supplier.deleteGuild(any()) }
        }

    }

    @Nested
    inner class DeleteGuild {

        @Test
        fun `should remove in supplier`() = runTest {
            val slot1 = slot<Int>()

            coEvery { supplier.deleteGuild(capture(slot1)) } returns true

            val id = Random.nextInt()
            assertTrue { service.deleteGuild(id) }
            coVerify(exactly = 1) { supplier.deleteGuild(id) }

            assertEquals(id, slot1.captured)
        }

        @ParameterizedTest
        @ValueSource(booleans = [true, false])
        fun `should return supplier result`(result: Boolean) = runTest {
            coEvery { supplier.deleteGuild(any()) } returns result
            assertEquals(result, service.deleteGuild(Random.nextInt()))
            coVerify(exactly = 1) { supplier.deleteGuild(any()) }
        }

    }

    @Nested
    inner class GetGuildById {

        @Test
        fun `should get in supplier`() = runTest {
            val slot1 = slot<Int>()

            coEvery { supplier.getGuild(capture(slot1)) } returns mockk()

            val id = Random.nextInt()
            service.getGuild(id)
            coVerify(exactly = 1) { supplier.getGuild(id) }

            assertEquals(id, slot1.captured)
        }

        @Test
        fun `should return supplier result`() = runTest {
            val guild = mockk<Guild>()
            coEvery { supplier.getGuild(any<Int>()) } returns guild
            assertEquals(guild, service.getGuild(Random.nextInt()))
            coVerify(exactly = 1) { supplier.getGuild(any<Int>()) }
        }

    }

    @Nested
    inner class GetGuildByName {

        @Test
        fun `should get in supplier`() = runTest {
            val slot1 = slot<String>()

            coEvery { supplier.getGuild(capture(slot1)) } returns emptyFlow()

            val name = getRandomString()
            assertThat(service.getGuild(name).toList()).isEmpty()
            coVerify(exactly = 1) { supplier.getGuild(any<String>()) }

            assertEquals(name, slot1.captured)
        }

        @Test
        fun `should return empty collection when supplier returns empty collection`() = runTest {
            coEvery { supplier.getGuild(any<String>()) } returns emptyFlow()
            assertEquals(emptyList(), service.getGuild(getRandomString()).toList())
            coVerify(exactly = 1) { supplier.getGuild(any<String>()) }
        }

        @Test
        fun `should return not empty collection when supplier returns not empty collection`() = runTest {
            val expected = List(5) { mockk<Guild>(getRandomString()) }
            coEvery { supplier.getGuild(any<String>()) } returns expected.asFlow()
            assertThat(service.getGuild(getRandomString()).toList()).containsExactlyElementsOf(expected)
            coVerify(exactly = 1) { supplier.getGuild(any<String>()) }
        }

    }

    @Nested
    inner class IsOwner {

        @Test
        fun `should check in supplier`() = runTest {
            val slot1 = slot<Int>()
            val slot2 = slot<String>()

            coEvery { supplier.isOwner(capture(slot1), capture(slot2)) } returns true

            val id = Random.nextInt()
            val entity = getRandomString()
            assertTrue { service.isOwner(id, entity) }
            coVerify(exactly = 1) { supplier.isOwner(id, entity) }

            assertEquals(id, slot1.captured)
            assertEquals(entity, slot2.captured)
        }

        @ParameterizedTest
        @ValueSource(booleans = [true, false])
        fun `should return supplier result`(result: Boolean) = runTest {
            coEvery { supplier.isOwner(any(), any()) } returns result
            assertEquals(result, service.isOwner(Random.nextInt(), getRandomString()))
            coVerify(exactly = 1) { supplier.isOwner(any(), any()) }
        }

    }

    @Nested
    inner class IsMember {

        @Test
        fun `should check in supplier`() = runTest {
            val slot1 = slot<Int>()
            val slot2 = slot<String>()

            coEvery { supplier.isMember(capture(slot1), capture(slot2)) } returns true

            val id = Random.nextInt()
            val entity = getRandomString()
            assertTrue { service.isMember(id, entity) }
            coVerify(exactly = 1) { supplier.isMember(id, entity) }

            assertEquals(id, slot1.captured)
            assertEquals(entity, slot2.captured)
        }

        @ParameterizedTest
        @ValueSource(booleans = [true, false])
        fun `should return supplier result`(result: Boolean) = runTest {
            coEvery { supplier.isMember(any(), any()) } returns result
            assertEquals(result, service.isMember(Random.nextInt(), getRandomString()))
            coVerify(exactly = 1) { supplier.isMember(any(), any()) }
        }
    }

    @Nested
    inner class AddMember {

        @Test
        fun `should add in supplier`() = runTest {
            val slot1 = slot<Int>()
            val slot2 = slot<String>()

            coEvery { supplier.addMember(capture(slot1), capture(slot2)) } returns true

            val id = Random.nextInt()
            val entity = getRandomString()
            assertTrue { service.addMember(id, entity) }
            coVerify(exactly = 1) { supplier.addMember(id, entity) }

            assertEquals(id, slot1.captured)
            assertEquals(entity, slot2.captured)
        }

        @ParameterizedTest
        @ValueSource(booleans = [true, false])
        fun `should return supplier result`(result: Boolean) = runTest {
            coEvery { supplier.addMember(any(), any()) } returns result
            assertEquals(result, service.addMember(Random.nextInt(), getRandomString()))
            coVerify(exactly = 1) { supplier.addMember(any(), any()) }
        }
    }

    @Nested
    inner class AddInvitation {

        @Test
        fun `should add in supplier`() = runTest {
            val slot1 = slot<Int>()
            val slot2 = slot<String>()
            val slot3 = slot<Instant>()

            coEvery { supplier.addInvitation(capture(slot1), capture(slot2), capture(slot3)) } returns true

            val id = Random.nextInt()
            val entity = getRandomString()
            val expiredAt = mockk<Instant>()
            assertTrue { service.addInvitation(id, entity, expiredAt) }
            coVerify(exactly = 1) { supplier.addInvitation(id, entity, expiredAt) }

            assertEquals(id, slot1.captured)
            assertEquals(entity, slot2.captured)
        }

        @ParameterizedTest
        @ValueSource(booleans = [true, false])
        fun `should return supplier result`(result: Boolean) = runTest {
            coEvery { supplier.addInvitation(any(), any(), any()) } returns result
            assertEquals(result, service.addInvitation(Random.nextInt(), getRandomString(), null))
            coVerify(exactly = 1) { supplier.addInvitation(any(), any(), any()) }
        }
    }

    @Nested
    inner class HasInvitation {

        @Test
        fun `should check in supplier`() = runTest {
            val slot1 = slot<Int>()
            val slot2 = slot<String>()

            coEvery { supplier.hasInvitation(capture(slot1), capture(slot2)) } returns true

            val id = Random.nextInt()
            val entity = getRandomString()
            assertTrue { service.hasInvitation(id, entity) }
            coVerify(exactly = 1) { supplier.hasInvitation(id, entity) }

            assertEquals(id, slot1.captured)
            assertEquals(entity, slot2.captured)
        }

        @ParameterizedTest
        @ValueSource(booleans = [true, false])
        fun `should return supplier result`(result: Boolean) = runTest {
            coEvery { supplier.hasInvitation(any(), any()) } returns result
            assertEquals(result, service.hasInvitation(Random.nextInt(), getRandomString()))
            coVerify(exactly = 1) { supplier.hasInvitation(any(), any()) }
        }
    }

    @Nested
    inner class RemoveMember {

        @Test
        fun `should remove in supplier`() = runTest {
            val slot1 = slot<Int>()
            val slot2 = slot<String>()

            coEvery { supplier.removeMember(capture(slot1), capture(slot2)) } returns true

            val id = Random.nextInt()
            val entity = getRandomString()
            assertTrue { service.removeMember(id, entity) }
            coVerify(exactly = 1) { supplier.removeMember(id, entity) }

            assertEquals(id, slot1.captured)
            assertEquals(entity, slot2.captured)
        }

        @ParameterizedTest
        @ValueSource(booleans = [true, false])
        fun `should return supplier result`(result: Boolean) = runTest {
            coEvery { supplier.removeMember(any(), any()) } returns result
            assertEquals(result, service.removeMember(Random.nextInt(), getRandomString()))
            coVerify(exactly = 1) { supplier.removeMember(any(), any()) }
        }
    }

    @Nested
    inner class RemoveInvitation {

        @Test
        fun `should remove in supplier`() = runTest {
            val slot1 = slot<Int>()
            val slot2 = slot<String>()

            coEvery { supplier.removeInvitation(capture(slot1), capture(slot2)) } returns true

            val id = Random.nextInt()
            val entity = getRandomString()
            assertTrue { service.removeInvitation(id, entity) }
            coVerify(exactly = 1) { supplier.removeInvitation(id, entity) }

            assertEquals(id, slot1.captured)
            assertEquals(entity, slot2.captured)
        }

        @ParameterizedTest
        @ValueSource(booleans = [true, false])
        fun `should return supplier result`(result: Boolean) = runTest {
            coEvery { supplier.removeInvitation(any(), any()) } returns result
            assertEquals(result, service.removeInvitation(Random.nextInt(), getRandomString()))
            coVerify(exactly = 1) { supplier.removeInvitation(any(), any()) }
        }
    }

    @Nested
    inner class GetMembers {

        @Test
        fun `should get in supplier`() = runTest {
            val slot1 = slot<Int>()

            coEvery { supplier.getMembers(capture(slot1)) } returns emptyFlow()

            val id = Random.nextInt()
            assertThat(service.getMembers(id).toList()).isEmpty()
            coVerify(exactly = 1) { supplier.getMembers(any<Int>()) }

            assertEquals(id, slot1.captured)
        }

        @Test
        fun `should return empty collection when supplier returns empty collection`() = runTest {
            coEvery { supplier.getMembers(any<Int>()) } returns emptyFlow()
            assertEquals(emptyList(), service.getMembers(Random.nextInt()).toList())
            coVerify(exactly = 1) { supplier.getMembers(any<Int>()) }
        }

        @Test
        fun `should return not empty collection when supplier returns not empty collection`() = runTest {
            val expected = List(5) { mockk<GuildMember>(getRandomString()) }
            coEvery { supplier.getMembers(any<Int>()) } returns expected.asFlow()
            assertThat(service.getMembers(Random.nextInt()).toList()).containsExactlyElementsOf(expected)
            coVerify(exactly = 1) { supplier.getMembers(any<Int>()) }
        }
    }

    @Nested
    inner class GetInvitations {

        @Test
        fun `should get in supplier`() = runTest {
            val slot1 = slot<Int>()

            coEvery { supplier.getInvitations(capture(slot1)) } returns emptyFlow()

            val id = Random.nextInt()
            assertThat(service.getInvitations(id).toList()).isEmpty()
            coVerify(exactly = 1) { supplier.getInvitations(any<Int>()) }

            assertEquals(id, slot1.captured)
        }

        @Test
        fun `should return empty collection when supplier returns empty collection`() = runTest {
            coEvery { supplier.getInvitations(any<Int>()) } returns emptyFlow()
            assertEquals(emptyList(), service.getInvitations(Random.nextInt()).toList())
            coVerify(exactly = 1) { supplier.getInvitations(any<Int>()) }
        }

        @Test
        fun `should return not empty collection when supplier returns not empty collection`() = runTest {
            val expected = List(5) { mockk<GuildInvite>(getRandomString()) }
            coEvery { supplier.getInvitations(any<Int>()) } returns expected.asFlow()
            assertThat(service.getInvitations(Random.nextInt()).toList()).containsExactlyElementsOf(expected)
            coVerify(exactly = 1) { supplier.getInvitations(any<Int>()) }
        }
    }
}
