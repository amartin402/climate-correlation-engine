#!/usr/bin/env bash
set -euo pipefail

echo ">>> Starting devcontainer setup..."

# -------------------------------------------------------
# 1. SHELL PROMPT
# -------------------------------------------------------
grep -qxF 'export PS1="> "' ~/.bashrc \
  || echo 'export PS1="> "' >> ~/.bashrc

# -------------------------------------------------------
# 2. SYSTEM PACKAGES
#
# The universal devcontainer image ships with a broken
# Yarn apt repo (expired GPG key). We remove it before
# running apt-get update so the script doesn't fail.
# -------------------------------------------------------
echo ">>> Fixing apt sources..."
sudo rm -f /etc/apt/sources.list.d/yarn.list \
           /usr/share/keyrings/yarnkey.gpg \
           /etc/apt/trusted.gpg.d/yarn.gpg

echo ">>> Installing system packages..."
sudo apt-get update -qq
sudo apt-get install -y --no-install-recommends \
  iputils-ping \
  dnsutils \
  curl \
  wget \
  ca-certificates \
  tar

# -------------------------------------------------------
# 3. GOOGLE CLOUD SDK
# -------------------------------------------------------
echo ">>> Installing Google Cloud SDK..."

GCLOUD_DIR="$HOME/google-cloud-sdk"

if [ ! -d "$GCLOUD_DIR" ]; then
  curl -fsSL \
    https://dl.google.com/dl/cloudsdk/channels/rapid/google-cloud-sdk.tar.gz \
    -o /tmp/gcloud.tar.gz \
    --retry 5 \
    --retry-delay 5

  tar -xzf /tmp/gcloud.tar.gz -C "$HOME"
  rm -f /tmp/gcloud.tar.gz

  "$GCLOUD_DIR/install.sh" \
    --quiet \
    --bash-completion=false \
    --path-update=false
fi

# Make gcloud available for the rest of this script
export PATH="$GCLOUD_DIR/bin:$PATH"

# Persist PATH for future shell sessions
grep -qxF 'export PATH="$HOME/google-cloud-sdk/bin:$PATH"' ~/.bashrc \
  || echo 'export PATH="$HOME/google-cloud-sdk/bin:$PATH"' >> ~/.bashrc

echo ">>> gcloud version: $(gcloud --version 2>&1 | head -1)"

# -------------------------------------------------------
# 4. BRUIN CLI
# -------------------------------------------------------
echo ">>> Installing Bruin..."

# Ensure ~/.local/bin exists and is on PATH before install
mkdir -p "$HOME/.local/bin"
export PATH="$HOME/.local/bin:$PATH"
grep -qxF 'export PATH="$HOME/.local/bin:$PATH"' ~/.bashrc \
  || echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc

if ! command -v bruin &>/dev/null; then
  curl -fsSL https://raw.githubusercontent.com/bruin-data/bruin/main/install.sh | bash
fi

echo ">>> bruin version: $(bruin --version 2>&1 | head -1)"

# -------------------------------------------------------
# 5. PYTHON — pip + uv
# -------------------------------------------------------
echo ">>> Setting up Python tooling..."

python3 -m pip install --upgrade pip --quiet
python3 -m pip install uv --quiet

echo ">>> uv version: $(uv --version)"

# -------------------------------------------------------
# 6. PYTHON DEPENDENCIES
# -------------------------------------------------------
if [ -f requirements.txt ]; then
  echo ">>> Installing Python requirements via uv..."
  uv pip install --system -r requirements.txt
else
  echo ">>> No requirements.txt found — skipping Python dependency install."
fi

# -------------------------------------------------------
echo ">>> Devcontainer setup complete."