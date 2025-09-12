#!/usr/bin/env bash

#cargo test -p solana-core --features "dev-context-only-utils" tip_manager::tests:: -- --nocapture

./cargo test -p solana-core --features "dev-context-only-utils"
./cargo test -p solana-cost-model --features "dev-context-only-utils"
./cargo test -p solana-poh --features "dev-context-only-utils"
./cargo test -p solana-streamer --features "dev-context-only-utils"
