pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}

rootProject.name = "portfolio-risk-intelligence-platform"

include(
    "common",
    "refdata-service",
    "processing-service",
    "processing-service-streams",
    "api-service",
    "event-generator"
)
