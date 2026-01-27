plugins {
    id("java-library")
}

dependencies {
    api(platform(libs.spring.boot.bom))
    api(libs.jackson.annotations)
    implementation(libs.jackson.databind)
    implementation(libs.jackson.jsr310)
    implementation(libs.slf4j.api)
    implementation("org.springframework:spring-web")
    api(libs.jakarta.validation)
    compileOnly("jakarta.servlet:jakarta.servlet-api")

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.hibernate.validator)
    testRuntimeOnly(libs.jakarta.el.impl)
}
