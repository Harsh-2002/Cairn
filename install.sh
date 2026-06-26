#!/bin/sh
# Cairn installer and updater.
#
# Installs or updates Cairn either directly on the host (downloaded binary managed by a service)
# or with Docker (a compose project under /opt/cairn). Running it again updates an existing
# installation to the latest release. POSIX sh: works on any Unix shell (dash, ash/busybox, bash).
#
#   curl -fsSL https://raw.githubusercontent.com/Harsh-2002/Cairn/main/install.sh | sudo sh
#   sudo sh install.sh                 # interactive
#   sudo sh install.sh --docker --yes  # non-interactive Docker install
#   sudo sh install.sh --update        # update an existing installation
#   sudo sh install.sh --uninstall     # remove the service/compose project (keeps data)
#
# Flags: --host | --docker, --update, --uninstall, --yes (non-interactive), --version <tag>,
#        --data-dir <path>, --tls-cert <path>, --tls-key <path>, --no-color, --help.
set -eu

REPO="Harsh-2002/Cairn"
GHCR_IMAGE="ghcr.io/harsh-2002/cairn:latest"
BIN_PATH="/usr/local/bin/cairn"
HOST_DATA_DEFAULT="/var/lib/cairn"
HOST_ETC="/etc/cairn"
HOST_ENV="/etc/cairn/cairn.env"
DOCKER_DIR="/opt/cairn"
SVC_USER="cairn"
S3_PORT="7373"
UI_PORT="7374"

# Options (defaults; overridden by flags / prompts).
OPT_TARGET="auto"     # auto | host | docker
OPT_MODE="auto"       # auto | install | update | uninstall
OPT_YES="0"
OPT_VERSION="latest"
OPT_DATA_DIR="$HOST_DATA_DEFAULT"
OPT_TLS_CERT=""
OPT_TLS_KEY=""
USE_COLOR="1"

# ---- output -------------------------------------------------------------------------------------
setup_color() {
  if [ "$USE_COLOR" = "1" ] && [ -t 1 ] && [ -z "${NO_COLOR:-}" ]; then
    C_RESET=$(printf '\033[0m'); C_B=$(printf '\033[1m')
    C_RED=$(printf '\033[31m'); C_GRN=$(printf '\033[32m')
    C_YEL=$(printf '\033[33m'); C_BLU=$(printf '\033[34m'); C_DIM=$(printf '\033[2m')
  else
    C_RESET=''; C_B=''; C_RED=''; C_GRN=''; C_YEL=''; C_BLU=''; C_DIM=''
  fi
}
step() { printf '%s\n' "${C_BLU}${C_B}::${C_RESET} ${C_B}$*${C_RESET}"; }
info() { printf '%s\n' "   $*"; }
ok()   { printf '%s\n' "${C_GRN} ok${C_RESET} $*"; }
warn() { printf '%s\n' "${C_YEL}  !${C_RESET} $*" >&2; }
die()  { printf '%s\n' "${C_RED}error${C_RESET} $*" >&2; exit 1; }

have() { command -v "$1" >/dev/null 2>&1; }

# Ask a yes/no question; default is the second argument (y/n). Honors --yes.
confirm() {
  confirm_q="$1"; confirm_def="$2"
  if [ "$OPT_YES" = "1" ] || [ ! -t 0 ]; then
    [ "$confirm_def" = "y" ]; return $?
  fi
  if [ "$confirm_def" = "y" ]; then confirm_hint="[Y/n]"; else confirm_hint="[y/N]"; fi
  printf '%s %s ' "${C_BLU}?${C_RESET} $confirm_q" "$confirm_hint"
  read -r confirm_ans || confirm_ans=""
  [ -z "$confirm_ans" ] && confirm_ans="$confirm_def"
  case "$confirm_ans" in [Yy]*) return 0 ;; *) return 1 ;; esac
}

# Ask for a value with a default; honors --yes (uses the default).
ask() {
  ask_q="$1"; ask_def="$2"
  if [ "$OPT_YES" = "1" ] || [ ! -t 0 ]; then printf '%s' "$ask_def"; return; fi
  if [ -n "$ask_def" ]; then printf '%s [%s]: ' "${C_BLU}?${C_RESET} $ask_q" "$ask_def" >&2
  else printf '%s: ' "${C_BLU}?${C_RESET} $ask_q" >&2; fi
  read -r ask_ans || ask_ans=""
  [ -z "$ask_ans" ] && ask_ans="$ask_def"
  printf '%s' "$ask_ans"
}

usage() {
  cat <<EOF
Cairn installer / updater

Usage: sudo sh install.sh [options]

  --host             Install on the host (downloaded binary + service)
  --docker           Install with Docker (compose project in $DOCKER_DIR)
  --update           Update an existing installation to the latest release
  --uninstall        Stop and remove the installation (keeps your data)
  --version <tag>    Install a specific release tag (default: latest)
  --data-dir <path>  Host data directory (default: $HOST_DATA_DEFAULT)
  --tls-cert <path>  TLS certificate (enables HTTPS; requires --tls-key)
  --tls-key <path>   TLS private key
  --yes              Non-interactive; accept defaults
  --no-color         Disable coloured output
  --help             Show this help

With no target flag the script asks; if Docker is present it offers Docker.
EOF
}

# ---- helpers ------------------------------------------------------------------------------------
require_root() {
  [ "$(id -u)" = "0" ] || die "this script must run as root. Re-run with: sudo sh $0 $*"
}

detect_arch() {
  case "$(uname -m)" in
    x86_64 | amd64) printf 'amd64' ;;
    aarch64 | arm64) printf 'arm64' ;;
    *) die "unsupported architecture: $(uname -m) (released binaries are amd64 and arm64)" ;;
  esac
}

require_linux() {
  [ "$(uname -s)" = "Linux" ] || die "the prebuilt binaries are Linux-only (found $(uname -s)). Build from source instead."
}

gen_secret() {
  if have openssl; then openssl rand -hex 32
  elif [ -r /dev/urandom ]; then od -An -tx1 -N32 /dev/urandom | tr -d ' \n'
  else die "no openssl and no /dev/urandom to generate a secret key"; fi
}

# Fetch a URL to stdout (curl or wget).
fetch() {
  if have curl; then curl -fsSL "$1"
  elif have wget; then wget -qO- "$1"
  else die "need curl or wget to download"; fi
}
# Download a URL to a file.
fetch_to() {
  if have curl; then curl -fsSL -o "$2" "$1"
  elif have wget; then wget -qO "$2" "$1"
  else die "need curl or wget to download"; fi
}

# Resolve the release tag to install: honors --version, else the GitHub "latest" release.
resolve_version() {
  if [ "$OPT_VERSION" != "latest" ]; then printf '%s' "$OPT_VERSION"; return; fi
  rv_tag=$(fetch "https://api.github.com/repos/$REPO/releases/latest" \
    | grep -m1 '"tag_name"' | sed 's/.*: *"\([^"]*\)".*/\1/')
  [ -n "$rv_tag" ] || die "could not determine the latest release (none published yet, or network/API error)"
  printf '%s' "$rv_tag"
}

detect_init() {
  if [ -d /run/systemd/system ] && have systemctl; then printf 'systemd'
  elif have rc-service && have rc-update; then printf 'openrc'
  else printf 'none'; fi
}

# Prompt for optional TLS cert/key (sets OPT_TLS_CERT / OPT_TLS_KEY).
prompt_tls() {
  [ -n "$OPT_TLS_CERT" ] && return 0
  if confirm "Enable TLS (HTTPS) with your own certificate?" "n"; then
    OPT_TLS_CERT=$(ask "Path to the TLS certificate (PEM)" "")
    OPT_TLS_KEY=$(ask "Path to the TLS private key (PEM)" "")
  fi
}
validate_tls() {
  if [ -n "$OPT_TLS_CERT" ] || [ -n "$OPT_TLS_KEY" ]; then
    if [ -z "$OPT_TLS_CERT" ] || [ -z "$OPT_TLS_KEY" ]; then die "TLS needs both --tls-cert and --tls-key"; fi
    [ -r "$OPT_TLS_CERT" ] || die "TLS certificate not readable: $OPT_TLS_CERT"
    [ -r "$OPT_TLS_KEY" ]  || die "TLS key not readable: $OPT_TLS_KEY"
  fi
}

# Wait for the server to answer /healthz on the S3 port.
wait_healthy() {
  wh_i=0
  while [ "$wh_i" -lt 60 ]; do
    if fetch "http://127.0.0.1:$S3_PORT/healthz" >/dev/null 2>&1; then return 0; fi
    wh_i=$((wh_i + 1)); sleep 1
  done
  return 1
}

print_access() {
  pa_scheme="http"; [ -n "$OPT_TLS_CERT" ] && pa_scheme="https"
  printf '\n'
  step "Cairn is ready"
  info "S3 API     ${C_B}$pa_scheme://<host>:$S3_PORT${C_RESET}"
  info "Console    ${C_B}$pa_scheme://<host>:$UI_PORT${C_RESET}"
  info "Access key ${C_B}$ROOT_AK${C_RESET}"
  info "Secret key ${C_B}$ROOT_SK${C_RESET}"
  info "${C_DIM}Keep these and the master key safe. Update later by re-running this script.${C_RESET}"
}

# ---- host install / update ----------------------------------------------------------------------
download_binary() {
  db_arch=$(detect_arch); db_tag="$1"
  db_tmp=$(mktemp -d)
  db_url="https://github.com/$REPO/releases/download/$db_tag/cairn-linux-$db_arch"
  info "downloading cairn $db_tag ($db_arch)"
  fetch_to "$db_url" "$db_tmp/cairn" || die "download failed: $db_url"
  # Verify against SHA256SUMS when present and a checksum tool is available.
  if fetch_to "https://github.com/$REPO/releases/download/$db_tag/SHA256SUMS" "$db_tmp/SHA256SUMS" 2>/dev/null; then
    if have sha256sum || have shasum; then
      ( cd "$db_tmp"
        grep "cairn-linux-$db_arch" SHA256SUMS | sed "s#cairn-linux-$db_arch#cairn#" > sums.check
        if have sha256sum; then sha256sum -c sums.check >/dev/null 2>&1
        else shasum -a 256 -c sums.check >/dev/null 2>&1; fi
      ) || die "checksum verification failed for the downloaded binary"
      ok "checksum verified"
    else warn "no sha256sum/shasum available; skipping checksum verification"; fi
  else warn "SHA256SUMS not found for $db_tag; skipping checksum verification"; fi
  chmod 0755 "$db_tmp/cairn"
  install -m 0755 "$db_tmp/cairn" "$BIN_PATH"
  rm -rf "$db_tmp"
}

ensure_user() {
  id "$SVC_USER" >/dev/null 2>&1 && return 0
  if have useradd; then useradd --system --no-create-home --shell /usr/sbin/nologin "$SVC_USER" 2>/dev/null || useradd -r -s /bin/false "$SVC_USER"
  elif have adduser; then adduser -S -D -H -s /sbin/nologin "$SVC_USER" 2>/dev/null || adduser --system --no-create-home "$SVC_USER"
  else warn "cannot create a '$SVC_USER' user (no useradd/adduser); the service will run as root"; SVC_USER="root"; fi
}

write_env_file() {
  umask 077
  mkdir -p "$HOST_ETC"
  {
    printf 'CAIRN_DATA_DIR=%s\n' "$OPT_DATA_DIR"
    printf 'CAIRN_DB_PATH=%s/cairn.db\n' "$OPT_DATA_DIR"
    printf 'CAIRN_MASTER_KEY=%s\n' "$MASTER_KEY"
    printf 'CAIRN_ROOT_ACCESS_KEY=%s\n' "$ROOT_AK"
    printf 'CAIRN_ROOT_SECRET_KEY=%s\n' "$ROOT_SK"
    printf 'CAIRN_LISTEN_ADDR=0.0.0.0:%s\n' "$S3_PORT"
    printf 'CAIRN_UI_ADDR=0.0.0.0:%s\n' "$UI_PORT"
    if [ -n "$OPT_TLS_CERT" ]; then
      printf 'CAIRN_TLS_CERT_PATH=%s\n' "$OPT_TLS_CERT"
      printf 'CAIRN_TLS_KEY_PATH=%s\n' "$OPT_TLS_KEY"
    fi
  } > "$HOST_ENV"
  chmod 0600 "$HOST_ENV"
}

write_systemd_unit() {
  cat > /etc/systemd/system/cairn.service <<EOF
[Unit]
Description=Cairn S3-compatible object storage
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$SVC_USER
Group=$SVC_USER
EnvironmentFile=$HOST_ENV
ExecStart=$BIN_PATH serve
Restart=on-failure
RestartSec=3
NoNewPrivileges=true
ProtectSystem=full
ProtectHome=true
ReadWritePaths=$OPT_DATA_DIR

[Install]
WantedBy=multi-user.target
EOF
  systemctl daemon-reload
  systemctl enable cairn.service >/dev/null 2>&1 || true
  systemctl restart cairn.service
}

write_openrc_service() {
  cat > /etc/init.d/cairn <<EOF
#!/sbin/openrc-run
name="cairn"
description="Cairn S3-compatible object storage"
command="$BIN_PATH"
command_args="serve"
command_user="$SVC_USER:$SVC_USER"
command_background="yes"
pidfile="/run/cairn.pid"
output_log="/var/log/cairn.log"
error_log="/var/log/cairn.log"

start_pre() {
  set -a; . "$HOST_ENV"; set +a
}
EOF
  chmod 0755 /etc/init.d/cairn
  rc-update add cairn default >/dev/null 2>&1 || true
  rc-service cairn restart
}

install_host() {
  require_linux
  ih_init=$(detect_init)
  is_update="0"; [ -x "$BIN_PATH" ] && is_update="1"

  ih_tag=$(resolve_version)
  download_binary "$ih_tag"
  ok "installed binary to $BIN_PATH ($ih_tag)"

  if [ "$is_update" = "1" ] && [ -f "$HOST_ENV" ]; then
    step "Updating existing host installation"
    # Restart the running service onto the new binary; config and keys are preserved.
    case "$ih_init" in
      systemd) systemctl restart cairn.service 2>/dev/null || systemctl start cairn.service ;;
      openrc)  rc-service cairn restart 2>/dev/null || rc-service cairn start ;;
      *) warn "no service manager detected; restart 'cairn serve' yourself" ;;
    esac
    ROOT_AK=$(grep '^CAIRN_ROOT_ACCESS_KEY=' "$HOST_ENV" | cut -d= -f2-)
    ROOT_SK="(unchanged)"
    ok "updated to $ih_tag and restarted"
    return 0
  fi

  step "Fresh host installation"
  prompt_tls; validate_tls
  OPT_DATA_DIR=$(ask "Data directory" "$OPT_DATA_DIR")
  MASTER_KEY=$(gen_secret)
  ROOT_AK="cairn"
  ROOT_SK=$(gen_secret)
  ensure_user
  mkdir -p "$OPT_DATA_DIR"
  chown -R "$SVC_USER":"$SVC_USER" "$OPT_DATA_DIR" 2>/dev/null || true
  write_env_file
  ok "wrote config to $HOST_ENV (master key generated)"
  # Ensure the root admin on the fresh store (idempotent).
  (
    set -a
    # shellcheck source=/dev/null
    . "$HOST_ENV"
    set +a
    su -s /bin/sh "$SVC_USER" -c "$BIN_PATH bootstrap" >/dev/null 2>&1
  ) || true

  case "$ih_init" in
    systemd) write_systemd_unit; ok "installed and started the systemd service (cairn.service)" ;;
    openrc)  write_openrc_service; ok "installed and started the OpenRC service (cairn)" ;;
    *) warn "no systemd or OpenRC detected. Start it manually:"; info "set -a; . $HOST_ENV; set +a; $BIN_PATH serve" ;;
  esac
  if wait_healthy; then ok "health check passed"; else warn "did not pass health check in time; check the service logs"; fi
  print_access
}

# ---- docker install / update ---------------------------------------------------------------------
write_compose() {
  mkdir -p "$DOCKER_DIR"
  wc_tls=""
  if [ -n "$OPT_TLS_CERT" ]; then
    mkdir -p "$DOCKER_DIR/certs"
    install -m 0644 "$OPT_TLS_CERT" "$DOCKER_DIR/certs/cert.pem"
    install -m 0600 "$OPT_TLS_KEY"  "$DOCKER_DIR/certs/key.pem"
    wc_tls="1"
  fi
  cat > "$DOCKER_DIR/docker-compose.yml" <<EOF
# Cairn (generated by install.sh). Edit and re-run 'docker compose up -d' to apply changes.
services:
  cairn:
    image: $GHCR_IMAGE
    container_name: cairn
    restart: unless-stopped
    ports:
      - "$S3_PORT:7373"
      - "$UI_PORT:7374"
    environment:
      CAIRN_DATA_DIR: /data
      CAIRN_DB_PATH: /data/cairn.db
      CAIRN_MASTER_KEY: \${CAIRN_MASTER_KEY}
      CAIRN_ROOT_ACCESS_KEY: \${CAIRN_ROOT_ACCESS_KEY}
      CAIRN_ROOT_SECRET_KEY: \${CAIRN_ROOT_SECRET_KEY}
EOF
  if [ -n "$wc_tls" ]; then
    cat >> "$DOCKER_DIR/docker-compose.yml" <<EOF
      CAIRN_TLS_CERT_PATH: /certs/cert.pem
      CAIRN_TLS_KEY_PATH: /certs/key.pem
EOF
  fi
  cat >> "$DOCKER_DIR/docker-compose.yml" <<EOF
    volumes:
      - cairn_data:/data
EOF
  [ -n "$wc_tls" ] && printf '      - ./certs:/certs:ro\n' >> "$DOCKER_DIR/docker-compose.yml"
  cat >> "$DOCKER_DIR/docker-compose.yml" <<EOF

volumes:
  cairn_data:
EOF
}

compose() {
  if docker compose version >/dev/null 2>&1; then ( cd "$DOCKER_DIR" && docker compose "$@" )
  elif have docker-compose; then ( cd "$DOCKER_DIR" && docker-compose "$@" )
  else die "docker compose is not available"; fi
}

install_docker() {
  have docker || die "Docker is not installed"
  is_update="0"; [ -f "$DOCKER_DIR/docker-compose.yml" ] && is_update="1"

  if [ "$is_update" = "1" ]; then
    step "Updating the Docker installation"
    compose pull
    compose up -d
    ROOT_AK=$(grep '^CAIRN_ROOT_ACCESS_KEY=' "$DOCKER_DIR/.env" 2>/dev/null | cut -d= -f2-)
    ROOT_SK="(unchanged)"
    ok "pulled the latest image and recreated the container"
    return 0
  fi

  step "Fresh Docker installation in $DOCKER_DIR"
  prompt_tls; validate_tls
  MASTER_KEY=$(gen_secret)
  ROOT_AK="cairn"
  ROOT_SK=$(gen_secret)
  mkdir -p "$DOCKER_DIR"
  umask 077
  {
    printf 'CAIRN_MASTER_KEY=%s\n' "$MASTER_KEY"
    printf 'CAIRN_ROOT_ACCESS_KEY=%s\n' "$ROOT_AK"
    printf 'CAIRN_ROOT_SECRET_KEY=%s\n' "$ROOT_SK"
  } > "$DOCKER_DIR/.env"
  chmod 0600 "$DOCKER_DIR/.env"
  write_compose
  ok "wrote $DOCKER_DIR/docker-compose.yml and .env (master key generated)"
  compose pull
  compose up -d
  ok "started the cairn container (data in the 'cairn_data' volume)"
  if wait_healthy; then ok "health check passed"; else warn "did not pass health check in time; check 'docker logs cairn'"; fi
  print_access
}

# ---- uninstall ----------------------------------------------------------------------------------
do_uninstall() {
  step "Uninstalling (your data is kept)"
  if [ -f "$DOCKER_DIR/docker-compose.yml" ]; then
    compose down 2>/dev/null || true
    ok "stopped the Docker project (data volume 'cairn_data' and $DOCKER_DIR kept)"
  fi
  if [ -f /etc/systemd/system/cairn.service ]; then
    systemctl disable --now cairn.service 2>/dev/null || true
    rm -f /etc/systemd/system/cairn.service; systemctl daemon-reload 2>/dev/null || true
    ok "removed the systemd service"
  fi
  if [ -f /etc/init.d/cairn ]; then
    rc-service cairn stop 2>/dev/null || true; rc-update del cairn 2>/dev/null || true
    rm -f /etc/init.d/cairn; ok "removed the OpenRC service"
  fi
  info "binary $BIN_PATH, config $HOST_ETC, and data dirs were left in place; remove them by hand if you want them gone."
}

# ---- target selection ---------------------------------------------------------------------------
choose_target() {
  if [ "$OPT_TARGET" != "auto" ]; then return; fi
  # Existing installation wins (so re-running updates it).
  if [ -f "$DOCKER_DIR/docker-compose.yml" ]; then OPT_TARGET="docker"; return; fi
  if [ -x "$BIN_PATH" ] && [ -f "$HOST_ENV" ]; then OPT_TARGET="host"; return; fi
  if have docker && docker info >/dev/null 2>&1; then
    if confirm "Docker detected. Install with Docker (recommended)? Choose 'n' for a host install." "y"; then
      OPT_TARGET="docker"
    else OPT_TARGET="host"; fi
  else
    OPT_TARGET="host"
  fi
}

main() {
  step "Cairn installer"
  choose_target
  if [ "$OPT_MODE" = "uninstall" ]; then do_uninstall; exit 0; fi
  case "$OPT_TARGET" in
    docker) install_docker ;;
    host)   install_host ;;
    *) die "unknown target: $OPT_TARGET" ;;
  esac
}

# ---- argument parsing ---------------------------------------------------------------------------
while [ "$#" -gt 0 ]; do
  case "$1" in
    --host) OPT_TARGET="host" ;;
    --docker) OPT_TARGET="docker" ;;
    --update) OPT_MODE="update" ;;
    --uninstall) OPT_MODE="uninstall" ;;
    --yes | -y) OPT_YES="1" ;;
    --no-color) USE_COLOR="0" ;;
    --version) shift; OPT_VERSION="${1:-latest}" ;;
    --data-dir) shift; OPT_DATA_DIR="${1:-$HOST_DATA_DEFAULT}" ;;
    --tls-cert) shift; OPT_TLS_CERT="${1:-}" ;;
    --tls-key) shift; OPT_TLS_KEY="${1:-}" ;;
    --help | -h) usage; exit 0 ;;
    *) printf 'unknown option: %s\n\n' "$1" >&2; usage; exit 2 ;;
  esac
  shift
done

setup_color
require_root "$@"
main
