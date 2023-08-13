package com.github.rushyverse.core.utils

import com.github.rushyverse.mojang.api.ProfileId
import com.github.rushyverse.mojang.api.ProfileSkin
import java.util.*

val stringGenerator = generateSequence { UUID.randomUUID().toString() }.distinct().iterator()

fun getRandomString() = stringGenerator.next()

fun createProfileId(): ProfileId {
    return ProfileId(name = getRandomString(), id = getRandomString())
}

fun createProfileSkin(id: ProfileId? = null): ProfileSkin {
    return ProfileSkin(
        id = id?.id ?: getRandomString(),
        name = id?.name ?: getRandomString(),
        properties = emptyList()
    )
}
