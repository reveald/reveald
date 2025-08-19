# Releasing Guide

This document describes the release process for the Reveald library.

## Table of Contents

- [Quick Start](#quick-start)
- [Release Process Overview](#release-process-overview)
- [Prerequisites](#prerequisites)
- [Creating a Release](#creating-a-release)
- [Release Workflows](#release-workflows)
- [Versioning Strategy](#versioning-strategy)
- [Troubleshooting](#troubleshooting)

## Quick Start

To create a new release:

```bash
# Create and push a release (e.g., version 1.2.3)
make release VERSION=1.2.3

# Or preview what will be released
make release-dry-run VERSION=1.2.3
```

## Release Process Overview

The release process uses semantic versioning and GitHub Actions for automation:

1. **Single Version**: The library uses semantic versioning (MAJOR.MINOR.PATCH)
2. **Automated Workflow**: GitHub Actions handles testing and publishing
3. **One Command**: Use `make release VERSION=x.y.z` to trigger the entire process

## Prerequisites

Before creating a release:

1. Ensure you're on the `main` branch with latest changes:
   ```bash
   git checkout main
   git pull origin main
   ```

2. Check that all tests pass locally:
   ```bash
   make test
   ```

3. Review the current release status:
   ```bash
   make release-status
   ```

## Creating a Release

### Method 1: Using Make (Recommended)

The simplest way to create a release:

```bash
# Standard release
make release VERSION=1.2.3

# Pre-release
make release VERSION=1.2.3-beta.1
```

This command will:
1. Validate the version format
2. Create a git tag `v1.2.3`
3. Push the tag to GitHub
4. Trigger the automated release workflow

### Method 2: Using GitHub UI

1. Go to the GitHub Actions Release Workflow page
2. Click "Run workflow"
3. Select options:
   - **Version bump type**: Choose from dropdown:
     - `patch` - Bug fixes (1.0.0 → 1.0.1)
     - `minor` - New features (1.0.0 → 1.1.0)
     - `major` - Breaking changes (1.0.0 → 2.0.0)
     - `custom` - Specify exact version manually
   - **Custom version**: Only used when "custom" is selected
   - **Mark as pre-release**: Check for pre-releases
   - **Pre-release suffix**: Add suffix like `beta`, `alpha`, or `rc`
4. Click "Run workflow"

The workflow will automatically:
- Determine the current version
- Calculate the next version based on your selection
- Run all tests and create the release

### Method 3: Using Git Tags

```bash
# Create and push a version tag
git tag -a v1.2.3 -m "Release v1.2.3"
git push origin v1.2.3
```

## Release Workflows

### Automated Steps

Once a release is triggered, GitHub Actions will:

1. **Validate** the version format
2. **Test** the Go module
3. **Build** the package
4. **Create** a GitHub release with:
   - Automated release notes
   - Changelog since last release
   - Installation instructions

### Manual Operations

For more control, you can perform individual steps:

```bash
# Generate changelog
./scripts/generate-changelog.sh v1.2.2 v1.2.3 > CHANGELOG.md

# Preview release
make release-dry-run VERSION=1.2.3
```

## Versioning Strategy

### Version Format

We use semantic versioning (SemVer):
- **Format**: `MAJOR.MINOR.PATCH` (e.g., `1.2.3`)
- **Pre-release**: `MAJOR.MINOR.PATCH-LABEL` (e.g., `1.2.3-beta.1`)

### Version Increments

- **MAJOR** (1.0.0 → 2.0.0): Breaking API changes
- **MINOR** (1.0.0 → 1.1.0): New features, backward compatible
- **PATCH** (1.0.0 → 1.0.1): Bug fixes, backward compatible

### Installation

After release, the module is available via:

```bash
go get github.com/username/reveald@v1.2.3
```

## Troubleshooting

### Release Failed

If the release workflow fails:

1. Check the GitHub Actions logs
2. Fix any test failures or build errors
3. Delete the failed tag if created:
   ```bash
   git tag -d v1.2.3
   git push origin :refs/tags/v1.2.3
   ```
4. Retry the release

### Tag Already Exists

If you get "tag already exists" error:

```bash
# Check existing tags
git tag -l "v*"

# Delete local tag
git tag -d v1.2.3

# Delete remote tag (if needed)
git push origin :refs/tags/v1.2.3
```

### Missing Permissions

Ensure you have:
- Write access to the repository
- Permission to create tags
- GitHub token configured for workflows

### Go Module Not Found

After release, Go modules may take a few minutes to be available:

```bash
# Force Go proxy to update
GOPROXY=proxy.golang.org go list -m github.com/username/reveald@v1.2.3

# Or bypass proxy temporarily
GOPROXY=direct go get github.com/username/reveald@v1.2.3
```

## Best Practices

1. **Test First**: Always run tests before releasing
2. **Use Conventional Commits**: For better changelog generation
   - `feat:` for new features
   - `fix:` for bug fixes
   - `docs:` for documentation
   - `refactor:` for code refactoring
   - `perf:` for performance improvements
   - `test:` for test changes
   - `build:` for build system changes
   - `chore:` for maintenance tasks

3. **Document Breaking Changes**: Use `!` or `BREAKING CHANGE:` in commits
4. **Preview Releases**: Use `make release-dry-run` to preview
5. **Check Status**: Use `make release-status` before and after

## Getting Help

- Run `make help` for available commands
- Check `.github/workflows/release.yml` for workflow details
- See `scripts/` directory for release tools

## Release Checklist

Before releasing, ensure:

- [ ] On main branch with latest changes
- [ ] All tests pass locally (`make test`)
- [ ] Version number follows SemVer
- [ ] No uncommitted changes (`git status`)
- [ ] Release notes are ready (if manual)
- [ ] Team is notified (if major release)

## Examples

### Creating a patch release:
```bash
make release VERSION=1.2.4
```

### Creating a minor release:
```bash
make release VERSION=1.3.0
```

### Creating a pre-release:
```bash
make release VERSION=2.0.0-beta.1
```

### Checking what will be released:
```bash
make release-dry-run VERSION=1.2.4
make release-status
```