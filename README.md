# kafka-extensions

A set of extensions that are useful patterns to use. Dependencies are managed by Gradle Catalog.

## Running Locally

Please set up gradle. To run all tests the project requires Docker to run some integration tests
with [Test Containers](https://www.testcontainers.org/).

Once you have gradle setup and Docker running please run.

```shell
./gradlew clean build
```

## Watermarks

Watermark transformer created inspired by Flink watermark code where watermarks are added as headers into Kafka Stream
records. For further details see the [module](watermark/README.md).

## Boostrap Consumer

A module containing a rebalancing and sharding consumer that will reboot state from and to a certain offset in Kafka.
