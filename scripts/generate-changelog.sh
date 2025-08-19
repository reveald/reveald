#!/bin/bash

# Script to generate changelog from git commits
# Usage: ./generate-changelog.sh [from-tag] [to-tag]

set -e

FROM_TAG=${1:-$(git describe --tags --abbrev=0 HEAD^ 2>/dev/null || echo "")}
TO_TAG=${2:-HEAD}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}Changelog Generator${NC}"
echo "===================="
echo ""

# Determine version
if [ "$TO_TAG" == "HEAD" ]; then
    VERSION="Unreleased"
else
    VERSION=${TO_TAG#v}
fi

# Get current date
DATE=$(date +"%Y-%m-%d")

echo "# Changelog"
echo ""
echo "## [$VERSION] - $DATE"
echo ""

# Function to extract conventional commit type
get_commit_type() {
    local message="$1"
    if [[ $message =~ ^(feat|fix|docs|style|refactor|perf|test|build|ci|chore|revert)(\(.+\))?!?:.*$ ]]; then
        echo "${BASH_REMATCH[1]}"
    else
        echo "other"
    fi
}

# Function to extract scope from conventional commit
get_commit_scope() {
    local message="$1"
    if [[ $message =~ ^[a-z]+\(([^)]+)\):.*$ ]]; then
        echo "${BASH_REMATCH[1]}"
    else
        echo ""
    fi
}

# Function to extract breaking change indicator
is_breaking_change() {
    local message="$1"
    if [[ $message =~ ^[a-z]+(\(.+\))?!:.*$ ]] || [[ $message =~ BREAKING[[:space:]]CHANGE ]]; then
        echo "true"
    else
        echo "false"
    fi
}

# Initialize arrays for different commit types
declare -a FEATURES
declare -a FIXES
declare -a DOCS
declare -a REFACTORS
declare -a PERFS
declare -a BUILDS
declare -a OTHERS
declare -a BREAKING

# Process commits
if [ -n "$FROM_TAG" ]; then
    RANGE="$FROM_TAG..$TO_TAG"
else
    RANGE="$TO_TAG"
fi

echo -e "${YELLOW}Processing commits from $RANGE...${NC}" >&2
echo "" >&2

while IFS= read -r line; do
    HASH=$(echo "$line" | cut -d' ' -f1)
    MESSAGE=$(echo "$line" | cut -d' ' -f2-)
    AUTHOR=$(git show -s --format='%an' $HASH)
    
    TYPE=$(get_commit_type "$MESSAGE")
    SCOPE=$(get_commit_scope "$MESSAGE")
    BREAKING=$(is_breaking_change "$MESSAGE")
    
    # Clean up the message (remove type prefix)
    CLEAN_MESSAGE=$(echo "$MESSAGE" | sed -E 's/^[a-z]+(\([^)]+\))?!?:[[:space:]]*//')
    
    # Format the entry
    if [ -n "$SCOPE" ]; then
        ENTRY="- **$SCOPE**: $CLEAN_MESSAGE ([${HASH:0:7}](../../commit/$HASH))"
    else
        ENTRY="- $CLEAN_MESSAGE ([${HASH:0:7}](../../commit/$HASH))"
    fi
    
    # Add to appropriate category
    if [ "$BREAKING" == "true" ]; then
        BREAKING+=("$ENTRY")
    fi
    
    case $TYPE in
        feat)
            FEATURES+=("$ENTRY")
            ;;
        fix)
            FIXES+=("$ENTRY")
            ;;
        docs)
            DOCS+=("$ENTRY")
            ;;
        refactor)
            REFACTORS+=("$ENTRY")
            ;;
        perf)
            PERFS+=("$ENTRY")
            ;;
        build|ci)
            BUILDS+=("$ENTRY")
            ;;
        *)
            OTHERS+=("$ENTRY")
            ;;
    esac
done < <(git log $RANGE --pretty=format:"%H %s" --reverse)

# Output organized changelog
if [ ${#BREAKING[@]} -gt 0 ]; then
    echo "### ⚠ BREAKING CHANGES"
    echo ""
    for entry in "${BREAKING[@]}"; do
        echo "$entry"
    done
    echo ""
fi

if [ ${#FEATURES[@]} -gt 0 ]; then
    echo "### 🚀 Features"
    echo ""
    for entry in "${FEATURES[@]}"; do
        echo "$entry"
    done
    echo ""
fi

if [ ${#FIXES[@]} -gt 0 ]; then
    echo "### 🐛 Bug Fixes"
    echo ""
    for entry in "${FIXES[@]}"; do
        echo "$entry"
    done
    echo ""
fi

if [ ${#PERFS[@]} -gt 0 ]; then
    echo "### ⚡ Performance Improvements"
    echo ""
    for entry in "${PERFS[@]}"; do
        echo "$entry"
    done
    echo ""
fi

if [ ${#REFACTORS[@]} -gt 0 ]; then
    echo "### ♻️ Code Refactoring"
    echo ""
    for entry in "${REFACTORS[@]}"; do
        echo "$entry"
    done
    echo ""
fi

if [ ${#DOCS[@]} -gt 0 ]; then
    echo "### 📝 Documentation"
    echo ""
    for entry in "${DOCS[@]}"; do
        echo "$entry"
    done
    echo ""
fi

if [ ${#BUILDS[@]} -gt 0 ]; then
    echo "### 🔧 Build System"
    echo ""
    for entry in "${BUILDS[@]}"; do
        echo "$entry"
    done
    echo ""
fi

if [ ${#OTHERS[@]} -gt 0 ]; then
    echo "### 📦 Other Changes"
    echo ""
    for entry in "${OTHERS[@]}"; do
        echo "$entry"
    done
    echo ""
fi

# Add statistics
echo "### 📊 Statistics"
echo ""
TOTAL_COMMITS=$(git rev-list --count $RANGE)
CONTRIBUTORS=$(git log $RANGE --format='%an' | sort -u | wc -l)
echo "- Total commits: $TOTAL_COMMITS"
echo "- Contributors: $CONTRIBUTORS"
echo ""

# Add comparison link if we have tags
if [ -n "$FROM_TAG" ] && [ "$TO_TAG" != "HEAD" ]; then
    echo "**Full Changelog**: [${FROM_TAG}...${TO_TAG}](../../compare/${FROM_TAG}...${TO_TAG})"
fi

echo -e "${GREEN}✓ Changelog generated successfully${NC}" >&2