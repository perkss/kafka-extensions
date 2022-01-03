plugins {
    id("java")
}

dependencies {
    implementation(libs.slf4j)
    implementation(libs.logback.core)
    implementation(libs.kafka.clients)

    testImplementation(libs.bundles.test)

}