#!/bin/bash

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Change to the project root directory
cd "$SCRIPT_DIR"

# Create build directory if it doesn't exist
mkdir -p build

# Change to build directory
cd build

# Run cmake and make
cmake .. && make
