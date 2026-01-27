plugins {
    alias(libs.plugins.spring.boot)
    alias(libs.plugins.spring.dependency.management)
    id("java")
}

dependencies {
    implementation(project(":common"))
    implementation(libs.spring.boot.starter)
    implementation(libs.spring.boot.starter.web)
    implementation(libs.spring.boot.starter.actuator)
    implementation(libs.spring.kafka)
    implementation(libs.kafka.streams)
    implementation(libs.springdoc.openapi)
    implementation(libs.micrometer.prometheus)
    implementation(libs.logstash.encoder)

    testImplementation(libs.spring.boot.starter.test)
    testImplementation("org.apache.kafka:kafka-streams-test-utils")
}
