#!/usr/bin/env bash
# This script runs the tests for all modules in the project.

set -e

root=$(pwd)

# Define the directories to skip
skip_dirs=(
  "driver" # Reason: Only contains the test suite used by other modules
)

# Function to check if a directory should be skipped
should_skip() {
  local dir=$1
  for skip_dir in "${skip_dirs[@]}"; do
    if [[ "$dir" == "$root/$skip_dir" ]]; then
      return 0
    fi
  done
  return 1
}

# Update the test files for the drivers.
chmod +x ./driver/update-tests.sh && ./driver/update-tests.sh

# Find all directories containing a go.mod file and call the Go test
# command in each one.
for d in $(find "$(pwd)" -type f -name "go.mod" -exec dirname {} \;); do
  if should_skip "$d"; then
    continue
  fi
  cd "$d"
  go test -covermode=atomic ./...
done
