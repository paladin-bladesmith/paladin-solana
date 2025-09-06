ARG RUST_VERSION=1.78

FROM rust:${RUST_VERSION}-slim-bullseye AS builder

RUN set -x \
 && apt update \
 && apt install -y \
      clang \
      libclang-dev \
      cmake \
      libudev-dev \
      make \
      unzip \
      libssl-dev \
      pkg-config \
      zlib1g-dev \
      protobuf-compiler \
      curl \
 && rustc --version \
 && cargo --version \

WORKDIR /solana

COPY . .

RUN mkdir -p docker-output

ARG CI_COMMIT
# NOTE: Keep this here before build since variable is referenced during CI build step.
ENV CI_COMMIT=$CI_COMMIT

# Uses docker buildkit to cache the image.
# /usr/local/cargo/git needed for crossbeam patch
RUN --mount=type=cache,mode=0777,target=/solana/target \
    --mount=type=cache,mode=0777,target=/usr/local/cargo/registry \
    --mount=type=cache,mode=0777,target=/usr/local/cargo/git \
    ./scripts/cargo-install-all.sh --validator-only /solana/docker-output
