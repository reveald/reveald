#!/bin/bash

# Script to calculate the next version based on bump type
# Usage: ./get-next-version.sh <bump-type>
# bump-type: major, minor, patch

set -e

BUMP_TYPE=${1:-patch}

# Get the latest version tag
LATEST_TAG=$(git tag -l "v*" | grep -E "^v[0-9]+\.[0-9]+\.[0-9]+$" | sort -V | tail -1)

if [ -z "$LATEST_TAG" ]; then
    # No tags found, start with v0.0.1 or v1.0.0 based on bump type
    case "$BUMP_TYPE" in
        major)
            echo "1.0.0"
            ;;
        minor)
            echo "0.1.0"
            ;;
        patch|*)
            echo "0.0.1"
            ;;
    esac
    exit 0
fi

# Extract version numbers
VERSION=${LATEST_TAG#v}
IFS='.' read -r MAJOR MINOR PATCH <<< "$VERSION"

# Calculate next version based on bump type
case "$BUMP_TYPE" in
    major)
        MAJOR=$((MAJOR + 1))
        MINOR=0
        PATCH=0
        ;;
    minor)
        MINOR=$((MINOR + 1))
        PATCH=0
        ;;
    patch)
        PATCH=$((PATCH + 1))
        ;;
    *)
        echo "Error: Invalid bump type. Use: major, minor, or patch" >&2
        exit 1
        ;;
esac

# Output the next version
echo "${MAJOR}.${MINOR}.${PATCH}"