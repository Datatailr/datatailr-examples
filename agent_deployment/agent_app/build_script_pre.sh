#!/usr/bin/env bash
# Install Node.js LTS and the pi coding agent into the app image.
# The build container is a minimal Debian slim running as root (no sudo).
set -euo pipefail

apt-get update
apt-get install -y --no-install-recommends curl ca-certificates

# Node.js LTS from NodeSource (provides node + npm).
curl -fsSL https://deb.nodesource.com/setup_lts.x | bash -
apt-get install -y --no-install-recommends nodejs

# Search tools pi uses for its file/grep tools. Without these baked into the
# image, pi tries to download them at first run -- but the runtime is offline
# (PI_OFFLINE=1 and no outbound network), so it would skip them and degrade.
# Debian ships fd as `fdfind`; pi looks for `fd`, so symlink it. ripgrep's
# binary `rg` is already what pi expects.
apt-get install -y --no-install-recommends fd-find ripgrep
ln -sf "$(command -v fdfind)" /usr/local/bin/fd

# Pi coding agent CLI, installed globally so `pi` is on PATH at runtime.
npm install -g --ignore-scripts @earendil-works/pi-coding-agent

# Sanity check the install during the build.
node --version
pi --version
fd --version
rg --version
