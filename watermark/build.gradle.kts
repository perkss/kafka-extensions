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
    testImplementation("org.hamcrest:hamcrest:2.2")
}