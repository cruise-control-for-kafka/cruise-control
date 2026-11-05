#!/bin/bash

set -e

BASE_DIR=$(pwd)
SCHEMA="$BASE_DIR/cruise-control/build/generated/openapi/openapi.json"
WIKI_TARGET="$BASE_DIR/target/api_wiki"

rm -rf "$WIKI_TARGET"
mkdir -p "$WIKI_TARGET"

npx redoc-cli bundle "$SCHEMA" --output "$WIKI_TARGET/redoc-static.html"
