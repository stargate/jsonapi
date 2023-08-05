#!/bin/bash

# require Docker Compose v2
if [[ ! $(docker compose version --short) =~ ^2. ]]; then
  echo "Docker compose v2 required. Please upgrade Docker Desktop to the latest version."
  exit 1
fi

# Default to INFO as root log level
LOGLEVEL=INFO

# Default to images used in project integration tests
DSETAG="$(../mvnw -f .. help:evaluate -Dexpression=stargate.int-test.cassandra.image-tag -q -DforceStdout)"
SGTAG="$(../mvnw -f .. help:evaluate -Dexpression=stargate.int-test.coordinator.image-tag -q -DforceStdout)"

# Default to latest released version
JSONTAG="v1"
JSONIMAGE="stargateio/jsonapi-native"

while getopts "lqir:t:j:" opt; do
  case $opt in
    l)
      JSONTAG="v$(../mvnw -f .. help:evaluate -Dexpression=project.version -q -DforceStdout)"
      ;;
    j)
      JSONTAG=$OPTARG
      ;;
    i)
      JSONIMAGE="stargateio/jsonapi"
      ;;
    q)
      REQUESTLOG=true
      ;;
    r)
      LOGLEVEL=$OPTARG
      ;;
    t)
      SGTAG=$OPTARG
      ;;
    \?)
      echo "Valid options:"
      echo "  -l - use JSON API Docker image from local build (see project README for build instructions)"
      echo "  -j <tag> - use JSON API Docker image tagged with specified JSON API version (will pull images from Docker Hub if needed)"
      echo "  -i - use Java-based image instead of default JSON API native image"
      echo "  -t <tag> - use Stargate coordinator Docker image tagged with specified  version (will pull images from Docker Hub if needed)"
      echo "  -q - enable request logging for APIs in 'io.quarkus.http.access-log' (default: disabled)"
      echo "  -r <log level> - specify root log level for APIs (defaults to INFO); usually DEBUG, WARN or ERROR"
      exit 1
      ;;
  esac
done

export LOGLEVEL
export REQUESTLOG
export DSETAG
export SGTAG
export JSONTAG
export JSONIMAGE

echo "Running with DSE $DSETAG, Stargate $SGTAG, JSON API $JSONIMAGE:$JSONTAG"

docker compose -f docker-compose-dev-mode.yml up -d --wait
