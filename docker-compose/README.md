# Docker Compose scripts for JSONAPI with DSE 7

This directory provides two ways to start the JSON API and Stargate coordinator with DSE 7 using `docker compose`.

## Prerequisites

### Docker / Docker Compose Versions

Make sure that you have Docker engine 20.x installed, which should include Docker compose 2.x. Our compose files rely on features only available in the Docker compose v2 file format.

### Building the local Docker image
If you want to use a locally built version of the JSON API Docker image rather than pulling a released version from Docker Hub, run the following command at the root of the repository to build the image:

```bash
./mvnw clean package -Dquarkus.container-image.build=true -DskipTests
```

You can control the platform using the `-Dquarkus.docker.buildx.platform=linux/amd64,linux/arm64` property.

Follow instructions under the [Script options](#script-options) section to use the locally built image.

## Stargate JSON API with single node DSE 7 cluster

You can start a simple Stargate configuration with the following command:

```
./start_dse_70.sh
``` 

This convenience script verifies your Docker installation meets minimum requirements and brings up the configuration described in the `docker-compose.yml` file. The configuration includes health criteria for each container that is used to ensure the containers come up in the correct order.

The convenience script uses the `-d` and `--wait` options to track the startup progress, so that the compose command exits when all containers have started and reported healthy status within a specified timeout. 

The environment settings in the `.env` file include variables that describe the image tags that will be used by default, typically JSON API `v1`, Stargate `v2` and DSE `7.0.x`. The `start_dse_70.sh` script supports [options](#script-options) for overriding which image tags are used, including using a locally generated image as described [above](#building-the-local-docker-image).
We recommend doing a `docker compose pull` periodically to ensure you always have the latest patch versions of these tags.

Once done using the containers, you can stop them using the command `docker compose down`.

## Stargate JSON API with embedded DSE 7 in coordinator (developer mode)

This alternate configuration runs the Stargate coordinator node in developer mode, so that no separate Cassandra cluster is required.
This configuration is useful for development and testing since it initializes more quickly, but is not recommended for production deployments. It can be run with the command:

```
./start_dse_70_dev_mode.sh
``` 

This script supports the same [options](#script-options) as the `start_dse_70.sh` script. 

To stop the configuration, use the command:

```
docker-compose -f docker-compose-dev-mode.yml down
``` 

## Script options

Both convenience scripts support the following options:

* You can specify an image tag (version) of the JSON API using `-j [VERSION]`, or use the `-l` tag to use a locally built image with the latest snapshot version. 

* The scripts default to using the Java-based image for JSON API, you can specify to use the native GraalVM based variant using `-n`.

* You can change the default root log level for the JSON API using `-r [LEVEL]` (default `INFO`). Valid values: `ERROR`, `WARN`, `INFO`, `DEBUG`

* You can enable reguest logging for the JSON API using `-q`: if so, each request is logged under category `io.quarkus.http.access-log`

* You can specify an image tag (version) of the Stargate coordinator using `-t [VERSION]`.

## Notes

* The `.env` file defines variables for the docker compose project name (`COMPOSE_PROJECT_NAME`),
 the DSE Docker image tag to use (`DSETAG`), and the Stargate Docker image tag to use (`SGTAG`).

* When using the convenience scripts, the Docker image (`JSONTAG`) is the current (snapshot) version as defined in the top level project `pom.xml` file. It can be overridden with the `-t` option on either script:

  `./start_dse_70.sh -t v1.0.0`

* Running more than one of these multi-container environments on one host may require changing the port mapping to be changed to avoid conflicts on the host machine.

## Troubleshooting

If you see an error like:
```
Pulling coordinator (stargateio/jsonapi:1.0.0-SNAPSHOT)...
ERROR: manifest for stargateio/jsonapi:1.0.0-SNAPSHOT not found: manifest unknown: manifest unknown
```

you are trying to deploy a version that is neither publicly available (official release) nor built locally. You can either build the image locally (see [above](#building-the-local-docker-image)) or use a publicly available version (e.g. `v1.0.0`).

