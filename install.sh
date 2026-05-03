#!/usr/bin/env bash
# install.sh — Hardcover Sync bare-metal installer
# Maakt een venv, installeert dependencies en registreert de systemd service.
# Uitvoeren als root of met sudo.

set -euo pipefail

# ── Configuratie ───────────────────────────────────────────────────────────────
APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_USER="${SUDO_USER:-$(whoami)}"
VENV_DIR="${APP_DIR}/.venv"
SERVICE_NAME="hardcover-sync"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
PYTHON="${PYTHON:-python3}"

# ── Kleuren ────────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'
AMBER='\033[0;33m'
RED='\033[0;31m'
NC='\033[0m'

info()  { echo -e "${GREEN}[✓]${NC} $*"; }
warn()  { echo -e "${AMBER}[!]${NC} $*"; }
error() { echo -e "${RED}[✗]${NC} $*"; exit 1; }

# ── Root check ─────────────────────────────────────────────────────────────────
if [[ $EUID -ne 0 ]]; then
  error "Uitvoeren als root: sudo bash install.sh"
fi

echo ""
echo "  📚  Hardcover Sync — bare-metal installer"
echo "  Installatiemap : ${APP_DIR}"
echo "  Systeemgebruiker: ${APP_USER}"
echo ""

# ── Python check ───────────────────────────────────────────────────────────────
if ! command -v "$PYTHON" &>/dev/null; then
  error "Python niet gevonden. Installeer python3: apt install python3 python3-venv"
fi

PY_VERSION=$("$PYTHON" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
info "Python versie: ${PY_VERSION}"

if [[ "${PY_VERSION%%.*}" -lt 3 ]] || [[ "${PY_VERSION#*.}" -lt 11 ]]; then
  error "Python 3.11 of hoger vereist (gevonden: ${PY_VERSION})"
fi

# ── .env check ─────────────────────────────────────────────────────────────────
if [[ ! -f "${APP_DIR}/.env" ]]; then
  if [[ -f "${APP_DIR}/.env.example" ]]; then
    cp "${APP_DIR}/.env.example" "${APP_DIR}/.env"
    warn ".env aangemaakt vanuit .env.example — vul HARDCOVER_API_TOKEN in voor gebruik."
  else
    error "Geen .env gevonden. Maak er een aan op basis van .env.example."
  fi
else
  info ".env gevonden"
fi

# ── Venv aanmaken ──────────────────────────────────────────────────────────────
if [[ -d "${VENV_DIR}" ]]; then
  warn "Bestaande venv gevonden in ${VENV_DIR}, wordt overgeslagen"
else
  info "Venv aanmaken in ${VENV_DIR}…"
  sudo -u "${APP_USER}" "$PYTHON" -m venv "${VENV_DIR}"
fi

# ── Dependencies installeren ───────────────────────────────────────────────────
info "Dependencies installeren vanuit requirements.txt…"
sudo -u "${APP_USER}" "${VENV_DIR}/bin/pip" install --upgrade pip --quiet
sudo -u "${APP_USER}" "${VENV_DIR}/bin/pip" install -r "${APP_DIR}/requirements.txt" --quiet
info "Dependencies geïnstalleerd"

# ── Eigenaarschap corrigeren ───────────────────────────────────────────────────
chown -R "${APP_USER}:${APP_USER}" "${APP_DIR}"

# ── systemd service aanmaken ───────────────────────────────────────────────────
info "systemd service schrijven naar ${SERVICE_FILE}…"
cat > "${SERVICE_FILE}" <<EOF
[Unit]
Description=Hardcover Sync — Hardcover ↔ CWA ↔ Shelfmark
Documentation=https://github.com/jouwrepo/hardcover-sync
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${APP_USER}
Group=${APP_USER}
WorkingDirectory=${APP_DIR}

# Laad .env als environment file (key=value, geen export nodig)
EnvironmentFile=${APP_DIR}/.env

ExecStart=${VENV_DIR}/bin/python ${APP_DIR}/app.py

# Automatisch herstarten bij crash, niet bij bewuste stop
Restart=on-failure
RestartSec=10
StartLimitIntervalSec=60
StartLimitBurst=3

# Logging via journald
StandardOutput=journal
StandardError=journal
SyslogIdentifier=${SERVICE_NAME}

# Beveiliging
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=${APP_DIR}
PrivateTmp=true

[Install]
WantedBy=multi-user.target
EOF

# ── systemd herladen en inschakelen ───────────────────────────────────────────
systemctl daemon-reload
systemctl enable "${SERVICE_NAME}"
info "Service '${SERVICE_NAME}' ingeschakeld (start automatisch bij boot)"

# ── Vragen of nu starten ───────────────────────────────────────────────────────
echo ""
read -r -p "  Service nu starten? [j/N] " START_NOW
if [[ "${START_NOW,,}" == "j" ]]; then
  systemctl start "${SERVICE_NAME}"
  sleep 2
  if systemctl is-active --quiet "${SERVICE_NAME}"; then
    info "Service actief — dashboard: http://localhost:$(grep '^WEB_PORT' "${APP_DIR}/.env" | cut -d= -f2 || echo 5055)"
  else
    warn "Service lijkt niet te draaien. Controleer logs:"
    echo "    journalctl -u ${SERVICE_NAME} -n 30"
  fi
else
  echo ""
  echo "  Start later met:"
  echo "    sudo systemctl start ${SERVICE_NAME}"
fi

echo ""
echo "  Handige commando's:"
echo "    sudo systemctl status ${SERVICE_NAME}"
echo "    sudo systemctl restart ${SERVICE_NAME}"
echo "    sudo systemctl stop ${SERVICE_NAME}"
echo "    journalctl -u ${SERVICE_NAME} -f"
echo ""
