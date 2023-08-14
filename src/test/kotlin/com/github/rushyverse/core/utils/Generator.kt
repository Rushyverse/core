package com.github.rushyverse.core.utils

import com.github.rushyverse.core.data.player.Player
import com.github.rushyverse.core.data.player.Rank
import com.github.rushyverse.core.data.player.SupportedLanguage
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

fun createPlayer(uuid: UUID = UUID.randomUUID()): Player {
    return Player(
        uuid = uuid,
        rank = Rank.entries.random(),
        language = SupportedLanguage.entries.random()
    )
}

public fun randomEntityId() = UUID.randomUUID()
