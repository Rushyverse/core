package com.github.rushyverse.core.data

import java.util.*

public class UserCacheManager(public val key: String = "user:%s") {

    public fun getFormattedKey(uuid: UUID): String {
        return getFormattedKey(uuid.toString())
    }

    public fun getFormattedKey(uuid: String): String {
        return key.format(uuid)
    }

}