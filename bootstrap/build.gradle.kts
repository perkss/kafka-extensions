plugins {
    id("java")
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(libs.slf4j)
    implementation(libs.logback.core)
    implementation(libs.kafka.clients)

    testImplementation("org.junit.jupiter:junit-jupiter:5.7.2")

    implementation("com.google.guava:guava:30.1.1-jre")
}

tasks.test {
    useJUnitPlatform()
}