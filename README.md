# Stargate JSON API

This project implements the stand-alone JSON API microservice for Stargate.
JSON API is an HTTP service that gives access to data stored in a Cassandra cluster using a JSON Document based interface.

Specifications and design documents for this service are defined in the [docs](docs) directory.

The project depends on the [sgv2-quarkus-common](https://github.com/stargate/stargate/blob/main/apis/sgv2-quarkus-common) module, which provides common functionality used by all Stargate V2 APIs.

##### Table of Contents
* [Quick Start](#quick-start)
* [Concepts](#concepts)
    * [Shared concepts](#shared-concepts)
* [Configuration properties](#configuration-properties)
* [Development guide](#development-guide)
    * [Running the application in dev mode](#running-the-application-in-dev-mode)
    * [Running integration tests](#running-integration-tests)
    * [Packaging and running the application](#packaging-and-running-the-application)
    * [Creating a native executable](#creating-a-native-executable)
    * [Creating a docker image](#creating-a-docker-image)
* [Quarkus Extensions](#quarkus-extensions)

## Quick Start

Most users will want to use the JSON API through a client library such as the [stargate-mongoose](https://github.com/stargate/stargate-mongoose) for JavaScript development. See the [stargate-mongoose-sample-apps](https://github.com/stargate/stargate-mongoose-sample-apps) for a quick demonstration.

The quickest way to test out the JSON API directly is to start a local copy of the service and supporting infrastructure using the [Docker compose](docker-compose) scripts:

```shell
cd docker-compose
./start_dse_70_dev_mode.sh
```

This starts an instance of the JSON API along with a Stargate coordinator node in "developer mode" (with DataStax Enterprise 7.0 embedded). 

> **Warning**
> Running this script with no options will use the latest `v1` tagged version of JSON API and latest `v2` tagged version of the Stargate coordinator. Therefore, if you have these tags already present in your local Docker from other development/testing, those are the images that will be used. See our Docker compose [README](docker-compose/README.md) to see additional options.

Once the services are up, you can access the Swagger endpoint at: http://localhost:8181/swagger-ui/

We also have a Postman collection you can use to learn about the various operations supported by the JSON API as part of the [Stargate-Cassandra](https://www.postman.com/datastax/workspace/stargate-cassandra/overview) workspace. 

## Concepts

### Shared concepts

Please read the [Stargate Shared Concepts](https://github.com/stargate/stargate/blob/main/apis/sgv2-quarkus-common/README.md#shared-concepts) in order to get basic concepts shared between all V2 API implementations.

## Configuration properties

There are two main configuration property prefixes used, `stargate.` and `quarkus.`.

The `quarkus.` properties are defined by the Quarkus framework, and the complete list of available properties can be found on the [All configuration options](https://quarkus.io/guides/all-config) page.
In addition, the related guide of each [Quarkus extension](#quarkus-extensions) used in the project provides an overview of the available config options.

The `stargate.` properties are defined by this project itself and by the [sgv2-quarkus-common configuration](https://github.com/stargate/stargate/blob/main/apis/sgv2-quarkus-common/CONFIGURATION.md).
The properties are defined by dedicated config classes annotated with the `@ConfigMapping`.
The list of currently available properties is documented in the [Configuration Guide](CONFIGURATION.md).

## Development guide

This project uses Quarkus, the Supersonic Subatomic Java Framework.
If you want to learn more about Quarkus, please visit its [website](https://quarkus.io/).

It's recommended that you install Quarkus CLI in order to have a better development experience.
See [CLI Tooling](https://quarkus.io/guides/cli-tooling) for more information.

Note that this project uses Java 17, please ensure that you have the target JDK installed on your system.

### Running the application in dev mode

You can run your application in dev mode that enables live coding using:
```shell script
docker run -d --rm -e CLUSTER_NAME=dse-cluster -e CLUSTER_VERSION=7.0 -e ENABLE_AUTH=true -e DEVELOPER_MODE=true -e DS_LICENSE=accept -e DSE=true -p 8081:8081 -p 8091:8091 -p 9042:9042 stargateio/coordinator-dse-next:v2

./mvnw compile quarkus:dev
```

The command above will first spin the single Stargate DSE coordinator in dev that the API would communicate to.

> **_NOTE:_**  Quarkus now ships with a Dev UI, which is available in dev mode only at http://localhost:8181/stargate/dev/.

#### Debugging

In development mode, Quarkus starts by default with debug mode enabled, listening to port `5005` without suspending the JVM.
You can attach the debugger at any point, the simplest option would be from IntelliJ using `Run -> Attach to Process`.

If you wish to debug from the start of the application, start with `-Ddebug=client` and create a debug configuration in a *Listen to remote JVM* mode.

See [Debugging](https://quarkus.io/guides/maven-tooling#debugging) for more information.

### Running integration tests

Integration tests are using the [Testcontainers](https://www.testcontainers.org/) library in order to set up all needed dependencies, a Stargate coordinator and a Cassandra data store.
They are separated from the unit tests and are running as part of the `integration-test` and `verify` Maven phases:

```shell script
./mvnw integration-test
```

#### Running from IDE

Running integration tests from an IDE is supported out of the box.
The tests will use the DSE Next as the data store by default.
Running a test with a different version of the data store or the Stargate coordinator requires changing the run configuration and specifying the following system properties:

* `testing.containers.cassandra-image` - version of the Cassandra docker image to use, for example: `datastax/dse-server:7.0.0-alpha.4`
* `testing.containers.stargate-image` - version of the Stargate coordinator docker image to use, for example: `stargateio/coordinator-4_0:v2.1` (must be V2.1 coordinator for the target data store)
* `testing.containers.cluster-dse` - optional and only needed if coordinator is used

#### Executing against a running application

The integration tests can also be executed against an already running instance of the application.
This can be achieved by setting the `quarkus.http.test-host` system property when running the tests.
You'll most likely need to specify the authentication token to use in the tests, by setting the `stargate.int-test.auth-token` system property.

```shell
./mvnw verify -DskipUnitTests -Dquarkus.http.test-host=1.2.3.4 -Dquarkus.http.test-port=4321 -Dstargate.int-test.auth-token=[AUTH_TOKEN]

```

#### Skipping integration tests

You can skip the integration tests during the maven build by disabling the `int-tests` profile using the `-DskipITs` property:

```shell script
./mvnw verify -DskipITs
```

#### Skipping unit tests

Alternatively you may want to run only integration tests but not unit tests (especially when changing integration tests).
This can be achieved using the command:

```
./mvnw verify -DskipUnitTests
```

#### Troubleshooting failure to run ITs

If your Integration Test run fails with some generic, non-descriptive error like:

```
[ERROR]   CollectionsResourceIntegrationTest » Runtime java.lang.reflect.InvocationTargetException
```

here are some things you should try:

* Make sure your Docker Engine has enough resources. For example following have been observed:
    * Docker Desktop defaults of 2 gigabytes of memory on Mac are not enough: try at least 4

### Packaging and running the application

The application can be packaged using:
```shell script
./mvnw package
```
It produces the `quarkus-run.jar` file in the `target/quarkus-app/` directory.
Be aware that it’s not an _über-jar_ as the dependencies are copied into the `target/quarkus-app/lib/` directory.

The application is now runnable using `java -jar target/quarkus-app/quarkus-run.jar`.

If you want to build an _über-jar_, execute the following command:
```shell script
./mvnw package -Dquarkus.package.type=uber-jar
```

The application, packaged as an _über-jar_, is now runnable using `java -jar target/*-runner.jar`.

### Creating a native executable

You can create a native executable using: 
```shell script
./mvnw package -Pnative
```

Or, if you don't have GraalVM installed, you can run the native executable build in a container using: 
```shell script
./mvnw package -Pnative -Dquarkus.native.container-build=true
```

You can then execute your native executable with: `./target/sgv2-jsonapi-${project.version}-runner`

If you want to learn more about building native executables, please consult https://quarkus.io/guides/maven-tooling.

### Creating a Docker image

You can create a Docker image named `io.stargate/jsonapi` using:
```shell script
./mvnw clean package -Dquarkus.container-image.build=true
```

> NOTE: Include the property `-Dquarkus.docker.buildx.platform=linux/amd64,linux/arm64` if you want to build for multiple platforms.

Or, if you want to create a native-runnable Docker image named `io.stargate/jsonapi-native` using:
```shell script
./mvnw clean package -Pnative -Dquarkus.native.container-build=true -Dquarkus.container-image.build=true
```

You can create a JVM-based Docker image (`stargateio/jsonapi-profiling`) with additional profiling tools by using:
```shell script
./mvnw -B -ntp package -DskipTests -Dquarkus.container-image.build=true -Dquarkus.container-image.name=jsonapi-profiling -Dquarkus.docker.dockerfile-jvm-path=src/main/docker/Dockerfile-profiling.jvm
```

If you want to learn more about building container images, please consult [Container images](https://quarkus.io/guides/container-image).

## Quarkus Extensions

This project uses various Quarkus extensions, modules that run on top of a Quarkus application.
You can list, add and remove the extensions using the `quarkus ext` command.

> *NOTE: Please check the shared extensions introduced by the [sgv2-quarkus-common](https://github.com/stargate/stargate/blob/main/apis/sgv2-quarkus-common/README.md#shared-quarkus-extensions) project.

### `quarkus-arc`
[Related guide](https://quarkus.io/guides/cdi-reference)

The Quarkus DI solution.

### `quarkus-container-image-docker`
[Related guide](https://quarkus.io/guides/container-image)

The project uses Docker for building the Container images.
Properties for Docker image building are defined in the [pom.xml](pom.xml) file.
Note that under the hood, the generated Dockerfiles under [src/main/docker](src/main/docker) are used when building the images.
When updating the Quarkus version, the Dockerfiles must be re-generated.

### `quarkus-smallrye-health`
[Related guide](https://quarkus.io/guides/smallrye-health)

The extension setups the health endpoints under `/stargate/health`.

### `quarkus-smallrye-openapi`
[Related guide](https://quarkus.io/guides/openapi-swaggerui)

The OpenAPI definitions are generated and available under `/api/json/openapi` endpoint.
The [StargateJsonApi](src/main/java/io/stargate/sgv2/jsonapi/StargateJsonApi.java) class defines the `@OpenAPIDefinition` annotation.
This definition defines the default *SecurityScheme* named `Token`, which expects the header based authentication with the HTTP Header `Token`.
The `info` portions of the Open API definitions are set using the `quarkus.smallrye-openapi.info-` configuration properties.
