#!/usr/bin/env bash
# Install Node.js LTS and the pi coding agent into the service image.
# The build container is a minimal Debian slim running as root (no sudo).
set -euo pipefail

apt-get update
apt-get install -y --no-install-recommends curl ca-certificates

# Node.js LTS from NodeSource (provides node + npm).
curl -fsSL https://deb.nodesource.com/setup_lts.x | bash -
apt-get install -y --no-install-recommends nodejs

# Pi coding agent CLI, installed globally so `pi` is on PATH at runtime.
npm install -g --ignore-scripts @earendil-works/pi-coding-agent

# Sanity check the install during the build.
node --version
pi --version
