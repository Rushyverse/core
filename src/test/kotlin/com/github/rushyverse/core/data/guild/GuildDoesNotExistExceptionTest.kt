package com.github.rushyverse.core.data.guild

import com.github.rushyverse.core.data.GuildDoesNotExistException
import com.github.rushyverse.core.utils.getRandomString
import io.r2dbc.spi.R2dbcDataIntegrityViolationException
import kotlin.test.Test
import kotlin.test.assertEquals

class GuildDoesNotExistExceptionTest {

    @Test
    fun `should set reason with guild id`() {
        repeat(10) {
            val ex = GuildDoesNotExistException(it, null)
            assertEquals("Guild with ID $it does not exist.", ex.message)
        }
    }

    @Test
    fun `should set cause`() {
        val cause = R2dbcDataIntegrityViolationException(getRandomString())
        val ex = GuildDoesNotExistException(0, cause)
        assertEquals(cause, ex.cause)
    }

}