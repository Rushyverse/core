package com.github.rushyverse.core.utils

import com.github.rushyverse.core.data.Player
import com.github.rushyverse.core.data.Rank
import io.github.universeproject.kotlinmojangapi.ProfileId
import io.github.universeproject.kotlinmojangapi.ProfileSkin
import java.util.*

val stringGenerator = generateSequence { UUID.randomUUID().toString() }.distinct().iterator()

fun randomString() = stringGenerator.next()

fun randomProfileId(): ProfileId {
    return ProfileId(name = randomString(), id = randomString())
}

fun randomProfileSkin(id: ProfileId? = null): ProfileSkin {
    return ProfileSkin(
        id = id?.id ?: randomString(),
        name = id?.name ?: randomString(),
        properties = emptyList()
    )
}

fun createPlayer(): Player {
    return Player(
        uuid = UUID.randomUUID(),
        rank = Rank.entries.random()
    )
}

public fun randomEntityId() = UUID.randomUUID()
