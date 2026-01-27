plugins {
    alias(libs.plugins.spring.boot)
    alias(libs.plugins.spring.dependency.management)
    id("java")
}

dependencies {
    implementation(project(":common"))
    implementation(libs.spring.boot.starter)
    implementation(libs.spring.kafka)
    implementation(libs.logstash.encoder)

    testImplementation(libs.spring.boot.starter.test)
}
