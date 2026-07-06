#!/usr/bin/env bash
# Install Node.js LTS and the pi coding agent into the app image.
# The build container is a minimal Debian slim running as root (no sudo).
set -euo pipefail

apt-get update
apt-get install -y --no-install-recommends curl ca-certificates

# Git + SSH client so every agent (main and sub) can clone/push the shared repo
# over SSH using the deploy key from the Secrets Manager (specification §5).
apt-get install -y --no-install-recommends git openssh-client

# GitHub CLI, used by sub-agents to open/update PRs via the API token. Installed
# from the official apt repo; PR support degrades gracefully if this is absent.
curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \
  -o /usr/share/keyrings/githubcli-archive-keyring.gpg
chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
  > /etc/apt/sources.list.d/github-cli.list
apt-get update
apt-get install -y --no-install-recommends gh

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

# Expose the `spawn_subagent` delegation helper on PATH so the main agent's pi
# can call it from its bash tool (specification §6.2). The app package is
# extracted to the site-packages path below and is importable by name.
cat > /usr/local/bin/spawn_subagent <<'EOF'
#!/usr/bin/env bash
exec python -m agent_app.spawn_tool "$@"
EOF
chmod +x /usr/local/bin/spawn_subagent

# Expose the `check_subagents` monitoring helper on PATH so pi can inspect the
# sub-agents it spawned on demand (specification §6, §13).
cat > /usr/local/bin/check_subagents <<'EOF'
#!/usr/bin/env bash
exec python -m agent_app.monitor_tool "$@"
EOF
chmod +x /usr/local/bin/check_subagents

# Sanity check the install during the build.
node --version
pi --version
fd --version
rg --version
git --version
gh --version || true
