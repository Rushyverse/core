package com.github.rushyverse.core.data

import java.util.*

/**
 * Manager to interact with user data in cache.
 * @property prefixKey Prefix key to identify the user's data.
 */
public class UserCacheManager(public val prefixKey: String = "user:%s:") {


    /**
     * Get the formatted key for the user.
     * @param uuid User's ID.
     * @return Formatted key.
     */
    public fun getFormattedKey(uuid: UUID): String {
        return getFormattedKey(uuid.toString())
    }

    /**
     * Get the formatted key for the user.
     * @param uuid User's ID.
     * @return Formatted key.
     */
    public fun getFormattedKey(uuid: String): String {
        return prefixKey.format(uuid)
    }

}