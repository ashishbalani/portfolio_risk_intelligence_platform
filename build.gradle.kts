import org.gradle.api.tasks.SourceSetContainer

plugins {
    alias(libs.plugins.spring.boot)
    alias(libs.plugins.spring.dependency.management) apply false
    id("java-library")
    id("java")
}

group = "com.portfolio.risk"
version = "0.1.0-SNAPSHOT"

subprojects {
    repositories {
        mavenCentral()
    }

    plugins.withType<JavaPlugin> {
        java {
            toolchain {
                languageVersion.set(JavaLanguageVersion.of(21))
            }
        }

        tasks.withType<Test>().configureEach {
            useJUnitPlatform()
        }

        dependencies {
            testRuntimeOnly(libs.junit.platform.launcher)
        }

        val sourceSets = the<SourceSetContainer>()
        val integrationTestSourceSet = sourceSets.create("integrationTest") {
            java.srcDir("src/integrationTest/java")
            resources.srcDir("src/integrationTest/resources")
            compileClasspath += sourceSets["main"].output + configurations["testRuntimeClasspath"]
            runtimeClasspath += output + compileClasspath
        }

        configurations[integrationTestSourceSet.implementationConfigurationName].extendsFrom(
            configurations["testImplementation"]
        )
        configurations[integrationTestSourceSet.runtimeOnlyConfigurationName].extendsFrom(
            configurations["testRuntimeOnly"]
        )

        tasks.register<Test>("integrationTest") {
            description = "Runs integration tests."
            group = "verification"
            testClassesDirs = integrationTestSourceSet.output.classesDirs
            classpath = integrationTestSourceSet.runtimeClasspath
            useJUnitPlatform()
            shouldRunAfter(tasks.named("test"))
        }

        tasks.named("check") {
            dependsOn("integrationTest")
        }
    }
}

// Root project should not produce a Spring Boot executable
tasks.matching { it.name == "bootJar" }.configureEach {
    enabled = false
}
