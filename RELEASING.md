# Releasing Cruise Control

This document describes the process for releasing Cruise Control artifacts and publishing to Maven Central.

## Release Process

### 1. Create a release branch

```bash
git checkout -b release-<Major>.<Minor>.x main
```

### 2. Build and verify

```bash
./gradlew clean build
```

Ensure all tests pass before proceeding.

### 3. Push the release branch and tag

```bash
git push <upstream-remote> release-<Major>.<Minor>.x
git tag <version>
git push <upstream-remote> <version>
```

### 4. Publish to Maven Central

#### Via GitHub Actions (recommended)

1. Go to **Actions > Publish to Maven Central**.
2. Click **Run workflow** and enter the git tag (e.g., `3.1.0-rc1`).
3. The workflow checks out the tag, builds, and publishes to a Sonatype staging repository.

#### Locally (for debugging)

Set the required environment variables:
```bash
export MAVEN_USERNAME=<sonatype-username>
export MAVEN_PASSWORD=<sonatype-password>
export ORG_GRADLE_PROJECT_signingKeyId=<gpg-key-id>
export ORG_GRADLE_PROJECT_signingKey=$(gpg --armor --export-secret-key <gpg-key-id>)
export ORG_GRADLE_PROJECT_signingPassword=<gpg-passphrase>
```

Then publish:
```bash
./gradlew publishToSonatype closeSonatypeStagingRepository -Pversion=<version>
```

### 5. Verify and release

Both release candidates and GA releases are published to Maven Central.

1. Log in to [central.sonatype.com](https://central.sonatype.com/).
(This requires a Sonatype account with access to the `io.cruise-control` namespace).
2. Navigate to **Deployments** and find the staging repository.
3. Verify the artifacts are correct.
4. Click **Publish** to release to Maven Central.

After publishing, artifacts will be available on Maven Central within a few minutes.

Downstream users can then depend on the release:
```xml
<groupId>io.cruise-control</groupId>
<artifactId>cruise-control-metrics-reporter</artifactId>
<version>3.1.0-rc1</version>
```

### 6. Create a GitHub release

Create a GitHub release from the tag, attaching release notes.
For RCs, mark the release as a **pre-release** in GitHub.

## Published Artifacts

The following artifacts are published under the `io.cruise-control` group in Maven Central:

| Artifact                           | Description                     |
|------------------------------------|---------------------------------|
| `cruise-control`                   | Cruise Control server           |
| `cruise-control-core`              | Cruise Control core library     |
| `cruise-control-metrics-reporter`  | Cruise Control metrics reporter |

