package com.github.rushyverse.core.container

import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.containers.wait.strategy.WaitStrategy

class RedisContainer<SELF : RedisContainer<SELF>>(image: String) : GenericContainer<SELF>(image) {

    companion object {
        private const val REDIS_PORT = 6379
    }

    val url get() = "redis://${host}:${getMappedPort(REDIS_PORT)}/0"

    init {
        addExposedPort(REDIS_PORT)
    }

    override fun getWaitStrategy(): WaitStrategy {
        return Wait.forLogMessage(".*Ready to accept connections.*", 1)
    }
}
