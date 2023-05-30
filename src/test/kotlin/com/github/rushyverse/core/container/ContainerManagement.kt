package com.github.rushyverse.core.container

import org.testcontainers.containers.PostgreSQLContainer

fun createPSQLContainer(): PostgreSQLContainer<*> = PostgreSQLContainer("postgres:15.2")
    .withDatabaseName("db")
    .withUsername("test")
    .withPassword("test")

fun createRedisContainer(): RedisContainer<*> = RedisContainer("redis:7.0.8")