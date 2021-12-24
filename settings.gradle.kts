enableFeaturePreview("VERSION_CATALOGS")


dependencyResolutionManagement {
    versionCatalogs {
        create("libs") {
            version("kafka", "3.0.0")
            version("slf4j", "1.7.32")
            version("logback", "1.2.8")
            version("jupiter", "5.7.1")

            alias("kafka-clients").to("org.apache.kafka", "kafka-clients").versionRef("kafka")
            alias("kafka-streams").to("org.apache.kafka", "kafka-streams").versionRef("kafka")
            alias("kafka-streams-test-utils").to("org.apache.kafka", "kafka-streams-test-utils").versionRef("kafka")
            alias("slf4j").to("org.slf4j", "slf4j-api").versionRef("slf4j")
            alias("logback-core").to("ch.qos.logback", "logback-core").versionRef("logback")
            alias("junit-jupiter-api").to("org.junit.jupiter", "junit-jupiter-api").versionRef("jupiter")
            alias("junit-jupiter").to("org.junit.jupiter", "junit-jupiter").versionRef("jupiter")

            bundle("logging", listOf("logback-core", "slf4j"))
            bundle("test", listOf("junit-jupiter", "junit-jupiter-api"))
        }
    }
}

rootProject.name = "kafka-extensions"
include("bootstrap")
include("watermark")
