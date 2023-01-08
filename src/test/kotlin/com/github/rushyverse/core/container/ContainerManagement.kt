package com.github.rushyverse.core.container

import org.testcontainers.containers.PostgreSQLContainer

fun createPSQLContainer(): PostgreSQLContainer<*> = PostgreSQLContainer("postgres:alpine")
    .withDatabaseName("db")
    .withUsername("test")
    .withPassword("test")

fun createRedisContainer(): RedisContainer<*> = RedisContainer("redis:7.0.7")