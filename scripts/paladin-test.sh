#!/usr/bin/env bash

#cargo test -p solana-core --features "dev-context-only-utils" tip_manager::tests:: -- --nocapture

./cargo test -p solana-core --features "dev-context-only-utils"
