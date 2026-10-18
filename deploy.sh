#!/usr/bin/env bash
# =============================================================================
# Automated Deployment & Management Script for Linux VPS (Ubuntu / Debian)
# Usage:
#   chmod +x deploy.sh
#   ./deploy.sh setup    -> Setup system dependencies, venv, requirements & systemd
#   ./deploy.sh test     -> Test API connections (Binance, Telegram, Sheets, Groq, Cloudinary)
#   ./deploy.sh start    -> Start background systemd service
#   ./deploy.sh stop     -> Stop service
#   ./deploy.sh restart  -> Restart service
#   ./deploy.sh status   -> View current service status
#   ./deploy.sh logs     -> Tail live journalctl logs
# =============================================================================

set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_NAME="trader-bot"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
VENV_DIR="${PROJECT_DIR}/venv"
PYTHON_BIN="${VENV_DIR}/bin/python"

case "$1" in
  setup)
    echo "=== [1/5] Updating system packages & installing dependencies ==="
    sudo apt-get update -y
    sudo apt-get install -y python3 python3-pip python3-venv git curl fonts-dejavu-core libfreetype6 libpng16-16

    echo "=== [2/5] Creating Python virtual environment ==="
    if [ ! -d "$VENV_DIR" ]; then
      python3 -m venv "$VENV_DIR"
    fi

    echo "=== [3/5] Installing Python requirements ==="
    "$VENV_DIR/bin/pip" install --upgrade pip
    "$VENV_DIR/bin/pip" install -r "${PROJECT_DIR}/requirements.txt"

    echo "=== [4/5] Checking environment configuration (.env) ==="
    if [ ! -f "${PROJECT_DIR}/.env" ]; then
      if [ -f "${PROJECT_DIR}/.env.example" ]; then
        cp "${PROJECT_DIR}/.env.example" "${PROJECT_DIR}/.env"
        echo "⚠️  Created .env from .env.example. Please edit .env with your real API keys before starting!"
      else
        echo "❌ .env not found. Please create .env first."
      fi
    else
      echo "✅ .env file exists."
    fi

    echo "=== [5/5] Configuring systemd service ==="
    CURRENT_USER="$(whoami)"
    sudo bash -c "cat > ${SERVICE_FILE}" <<EOF
[Unit]
Description=Crypto Liquidity Sweep & SMC AI Trading Bot
After=network.target network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${CURRENT_USER}
WorkingDirectory=${PROJECT_DIR}
ExecStart=${PYTHON_BIN} trader_bot.py
Restart=always
RestartSec=10
EnvironmentFile=${PROJECT_DIR}/.env
StandardOutput=journal
StandardError=journal
SyslogIdentifier=trader_bot

[Install]
WantedBy=multi-user.target
EOF

    sudo systemctl daemon-reload
    sudo systemctl enable "${SERVICE_NAME}"
    echo "🎉 Setup complete! Next steps:"
    echo "   1. Edit ${PROJECT_DIR}/.env with your real credentials."
    echo "   2. Run: ./deploy.sh test"
    echo "   3. Run: ./deploy.sh start"
    ;;

  test)
    if [ ! -f "$PYTHON_BIN" ]; then
      echo "❌ Virtual environment not found. Run './deploy.sh setup' first."
      exit 1
    fi
    echo "🔍 Running API and connection diagnostics..."
    "$PYTHON_BIN" "${PROJECT_DIR}/trader_bot.py" --test
    ;;

  start)
    echo "🚀 Starting ${SERVICE_NAME} service..."
    sudo systemctl start "${SERVICE_NAME}"
    sudo systemctl status "${SERVICE_NAME}" --no-pager
    ;;

  stop)
    echo "🛑 Stopping ${SERVICE_NAME} service..."
    sudo systemctl stop "${SERVICE_NAME}"
    ;;

  restart)
    echo "🔄 Restarting ${SERVICE_NAME} service..."
    sudo systemctl restart "${SERVICE_NAME}"
    sudo systemctl status "${SERVICE_NAME}" --no-pager
    ;;

  status)
    sudo systemctl status "${SERVICE_NAME}" --no-pager
    ;;

  logs)
    echo "📜 Streaming logs (Ctrl+C to exit)..."
    sudo journalctl -u "${SERVICE_NAME}" -f -n 100
    ;;

  *)
    echo "Usage: $0 {setup|test|start|stop|restart|status|logs}"
    exit 1
    ;;
esac
