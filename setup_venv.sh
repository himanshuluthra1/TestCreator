#!/bin/bash
# ============================================================
# setup_venv.sh – First-time setup script for Hostinger Unix
#                 shared hosting (run via SSH terminal).
#
# Usage:
#   1. Upload all project files to ~/public_html (or your domain root)
#   2. SSH into your Hostinger account
#   3. cd ~/public_html
#   4. chmod +x setup_venv.sh
#   5. ./setup_venv.sh
# ============================================================

set -e

APP_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_DIR="$HOME/virtualenv/testcreator/3.11"

echo "==> App directory: $APP_DIR"

# --- 1. Create virtual environment (if not already present) ---
if [ ! -d "$VENV_DIR" ]; then
    echo "==> Creating virtual environment at $VENV_DIR ..."
    python3.11 -m venv "$VENV_DIR"
else
    echo "==> Virtual environment already exists, skipping creation."
fi

# --- 2. Activate and install dependencies ---
echo "==> Installing dependencies ..."
source "$VENV_DIR/bin/activate"
pip install --upgrade pip
pip install -r "$APP_DIR/requirements.txt"
deactivate

# --- 3. Create writable runtime directories ---
echo "==> Creating uploads/ and outputs/ directories ..."
mkdir -p "$APP_DIR/uploads"
mkdir -p "$APP_DIR/outputs"
chmod 755 "$APP_DIR/uploads"
chmod 755 "$APP_DIR/outputs"

# --- 4. Remind user to set SECRET_KEY ---
echo ""
echo "============================================================"
echo "  IMPORTANT: Set a secure SECRET_KEY before going live!"
echo ""
echo "  Option A – edit passenger_wsgi.py and replace:"
echo "    CHANGE_ME_TO_A_LONG_RANDOM_SECRET_KEY"
echo "  with a long random string (e.g., output of:)"
echo "    python3 -c \"import secrets; print(secrets.token_hex(32))\""
echo ""
echo "  Option B – set it as an environment variable in hPanel:"
echo "    hPanel → Websites → Python App → Environment Variables"
echo "    Variable name : SECRET_KEY"
echo "    Variable value: <your random key>"
echo "============================================================"
echo ""
echo "==> Setup complete. Restart your app in hPanel to apply."
