enableFeaturePreview("VERSION_CATALOGS")


dependencyResolutionManagement {
    versionCatalogs {
        create("libs") {
            version("kafka", "3.0.0")
            version("slf4j", "1.7.32")
            version("logback", "1.2.6")

            alias("kafka-clients").to("org.apache.kafka","kafka-clients").versionRef("kafka")
            alias("slf4j").to("org.slf4j","slf4j-api").versionRef("slf4j")
            alias("logback-core").to("ch.qos.logback","logback-core").versionRef("logback")

            bundle("logging", listOf("logback-core", "slf4j"))
        }
    }
}

rootProject.name = "kafka-extensions"
include("bootstrap")
