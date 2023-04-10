import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.8.20"
    kotlin("plugin.serialization") version "1.8.20"
    id("org.jetbrains.dokka") version "1.8.10"
    id("com.google.devtools.ksp") version "1.8.20-1.0.10"
    `java-library`
    `maven-publish`
}

repositories {
    mavenCentral()
}

dependencies {
    val ktorVersion = "2.2.4"
    val ktSerializationVersion = "1.5.0"
    val coroutinesCoreVersion = "1.6.4"
    val loggingVersion = "3.0.5"
    val mockkVersion = "1.13.4"
    val junitVersion = "5.9.2"
    val testContainersVersion = "1.18.0"
    val lettuceVersion = "6.2.3.RELEASE"
    val kotlinMojangApi = "2.2.0"
    val nettyCodecVersion = "4.1.91.Final"
    val assertJcoreVersion = "3.24.2"
    val komapperVersion = "1.9.0"

    api(kotlin("stdlib"))
    api(kotlin("reflect"))

    api("io.github.universeproject:kotlin-mojang-api-jvm:$kotlinMojangApi")

    // Ktor to interact with external API through HTTP
    api("io.ktor:ktor-client-core:$ktorVersion")
    api("io.ktor:ktor-client-cio:$ktorVersion")
    api("io.ktor:ktor-client-serialization:$ktorVersion")
    api("io.ktor:ktor-serialization-kotlinx-json:$ktorVersion")
    api("io.ktor:ktor-client-content-negotiation:$ktorVersion")

    // Kotlin Serialization to serialize data for database and cache
    api("org.jetbrains.kotlinx:kotlinx-serialization-json:$ktSerializationVersion")
    api("org.jetbrains.kotlinx:kotlinx-serialization-protobuf:$ktSerializationVersion")

    // Interact with database
    platform("org.komapper:komapper-platform:$komapperVersion").let {
        api(it)
        ksp(it)
    }
    api("org.komapper:komapper-starter-r2dbc")
    api("org.komapper:komapper-dialect-postgresql-r2dbc")
    ksp("org.komapper:komapper-processor")

    // Redis cache
    api("io.lettuce:lettuce-core:$lettuceVersion")
    api("io.netty:netty-codec:$nettyCodecVersion")

    // Logging information
    api("io.github.microutils:kotlin-logging:$loggingVersion")

    testImplementation(kotlin("test-junit5"))
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:$coroutinesCoreVersion")

    // Create fake instance (mock) of components for tests
    testImplementation("io.mockk:mockk:$mockkVersion")

    // Junit to run tests
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
    testImplementation("org.assertj:assertj-core:$assertJcoreVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-params:$junitVersion")
    testImplementation("org.testcontainers:junit-jupiter:$testContainersVersion")
    testImplementation("org.testcontainers:postgresql:$testContainersVersion")
}

kotlin {
     explicitApi = org.jetbrains.kotlin.gradle.dsl.ExplicitApiMode.Strict

    sourceSets {
        main {
            kotlin {
                srcDir("build/generated")
            }
        }

        all {
            languageSettings {
                optIn("kotlin.RequiresOptIn")
                optIn("kotlin.ExperimentalStdlibApi")
                optIn("kotlin.contracts.ExperimentalContracts")
                optIn("kotlinx.coroutines.ExperimentalCoroutinesApi")
                optIn("io.lettuce.core.ExperimentalLettuceCoroutinesApi")
            }
        }
    }
}

val dokkaOutputDir = "${rootProject.projectDir}/dokka"

tasks {
    withType<KotlinCompile> {
        kotlinOptions.jvmTarget = JavaVersion.VERSION_17.toString()
    }

    test {
        useJUnitPlatform()
    }

    clean {
        delete(dokkaOutputDir)
    }

    val deleteDokkaOutputDir by register<Delete>("deleteDokkaOutputDirectory") {
        group = "documentation"
        delete(dokkaOutputDir)
    }

    dokkaHtml.configure {
        dependsOn(deleteDokkaOutputDir)
        outputDirectory.set(file(dokkaOutputDir))
    }
}

val sourcesJar by tasks.registering(Jar::class) {
    group = "build"
    archiveClassifier.set("sources")
    from(sourceSets.main.get().allSource)
}

val javadocJar = tasks.register<Jar>("javadocJar") {
    group = "documentation"
    dependsOn(tasks.dokkaHtml)
    archiveClassifier.set("javadoc")
    from(dokkaOutputDir)
}

publishing {
    val projectName = project.name

    publications {
        val projectOrganizationPath = "Rushyverse/$projectName"
        val projectGitUrl = "https://github.com/$projectOrganizationPath"

        create<MavenPublication>(projectName) {
            from(components["kotlin"])
            artifact(sourcesJar.get())
            artifact(javadocJar.get())

            pom {
                name.set(projectName)
                description.set(project.description)
                url.set(projectGitUrl)

                issueManagement {
                    system.set("GitHub")
                    url.set("$projectGitUrl/issues")
                }

                ciManagement {
                    system.set("GitHub Actions")
                }

                licenses {
                    license {
                        name.set("MIT")
                        url.set("https://mit-license.org")
                    }
                }

                developers {
                    developer {
                        name.set("Quentixx")
                        email.set("Quentixx@outlook.fr")
                        url.set("https://github.com/Quentixx")
                    }
                    developer {
                        name.set("Distractic")
                        email.set("Distractic@outlook.fr")
                        url.set("https://github.com/Distractic")
                    }
                }

                scm {
                    connection.set("scm:git:$projectGitUrl.git")
                    developerConnection.set("scm:git:git@github.com:$projectOrganizationPath.git")
                    url.set(projectGitUrl)
                }

                distributionManagement {
                    downloadUrl.set("$projectGitUrl/releases")
                }
            }
        }
    }
}