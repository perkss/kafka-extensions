plugins {
    id("java")
}

dependencies {
    implementation(libs.slf4j)
    implementation(libs.kafka.clients)
    implementation(libs.kafka.streams)

    testImplementation(libs.logback.core)
    testImplementation(libs.bundles.test)
    testImplementation(libs.kafka.streams.test.utils)
    testImplementation(libs.test.containers.core)
    testImplementation("org.awaitility:awaitility:4.1.1")
    testImplementation(libs.test.containers.kafka)
    testImplementation("org.hamcrest:hamcrest:2.2")
    testRuntimeOnly(libs.logback.classic)
}