#!/usr/bin/env bash
# Builds paladin-solana in a docker container.
# Useful for running on machines that might not have cargo installed but can run docker (Flatcar Linux).

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

RUST_VERSION="${RUST_VERSION:-1.78}"

GIT_SHA="$(git rev-parse --short HEAD)"
GIT_TAG=

echo "Git hash: $GIT_SHA"

if [ -n "$GIT_TAG" ]; then
    echo "Git tag: $GIT_TAG"
    git checkout "tags/${GIT_TAG}" && git submodule update --init --recursive
fi

if ! command -v "docker" >/dev/null 2>&1; then
  echo "Docker is not installed"
  exit 1
fi

DOCKER_BUILDKIT=1 docker build \
  --build-arg RUST_VERSION=$RUST_VERSION \
  --build-arg CI_COMMIT=$GIT_SHA \
  -t paladin/build-solana \
  -f dev/Dockerfile . \
  --progress=plain

# Creates a temporary container, copies solana-validator built inside container there and
# removes the temporary container.
docker rm temp || true
docker container create --name temp paladin/build-solana
mkdir -p $SCRIPT_DIR/docker-output
# Outputs the solana-validator binary to $SOLANA/docker-output/solana-validator
docker container cp temp:/solana/docker-output/bin $SCRIPT_DIR/docker-output
docker rm temp