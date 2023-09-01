import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    embeddedKotlin("jvm")
    embeddedKotlin("plugin.serialization")
    id("org.jetbrains.dokka") version "1.8.20"
    id("com.google.devtools.ksp") version "1.9.0-1.0.13"
    id("io.gitlab.arturbosch.detekt") version "1.23.1"
    `java-library`
    `maven-publish`
    jacoco
}

val javaVersion get() = JavaVersion.VERSION_17
val javaVersionString get() = javaVersion.toString()
val javaVersionInt get() = javaVersionString.toInt()

detekt {
    // Allows having different behavior for CI.
    // When building a branch, we want to fail the build if detekt fails.
    // When building a PR, we want to ignore failures to report them in sonar.
    val envIgnoreFailures = System.getenv("DETEKT_IGNORE_FAILURES")?.toBooleanStrictOrNull() ?: false
    ignoreFailures = envIgnoreFailures

    config.from(file("config/detekt/detekt.yml"))
}

jacoco {
    reportsDirectory.set(file("${layout.buildDirectory.get()}/reports/jacoco"))
}

repositories {
    mavenCentral()
    maven("https://jitpack.io")
}

dependencies {
    val ktorVersion = "2.3.4"
    val ktSerializationVersion = "1.5.1"
    val coroutinesCoreVersion = "1.7.3"
    val loggingVersion = "3.0.5"
    val mockkVersion = "1.13.5"
    val junitVersion = "5.10.0"
    val testContainersVersion = "1.18.3"
    val lettuceVersion = "6.2.3.RELEASE"
    val mojangApi = "v1.0.1"
    val nettyCodecVersion = "4.1.96.Final"
    val assertJcoreVersion = "3.24.2"
    val komapperVersion = "1.12.0"
    val kotestVersion = "5.6.2"

    api(kotlin("stdlib"))
    api(kotlin("reflect"))

    api("com.github.Rushyverse:mojang-api:$mojangApi")

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
    testImplementation("io.kotest:kotest-assertions-core:$kotestVersion")
    testImplementation("io.kotest:kotest-assertions-json:$kotestVersion")

    // Create fake instance (mock) of components for tests
    testImplementation("io.mockk:mockk:$mockkVersion")

    // Junit to run tests
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
    testImplementation("org.assertj:assertj-core:$assertJcoreVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-params:$junitVersion")
    testImplementation("org.testcontainers:junit-jupiter:$testContainersVersion")
    testImplementation("org.testcontainers:postgresql:$testContainersVersion")
    testImplementation("org.komapper:komapper-dialect-postgresql-jdbc")
}

kotlin {
    explicitApi = org.jetbrains.kotlin.gradle.dsl.ExplicitApiMode.Strict
    jvmToolchain(javaVersionInt)

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
        kotlinOptions.jvmTarget = javaVersionString
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
        // CompileJava should be executed to build library in Jitpack
        dependsOn(deleteDokkaOutputDir, compileJava.get())
        outputDirectory.set(file(dokkaOutputDir))
    }

    jacocoTestReport {
        reports {
            xml.required.set(true)
            html.required.set(true)
            csv.required.set(false)
        }
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
