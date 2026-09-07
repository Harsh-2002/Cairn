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
#        --data-dir <path>, --tls-cert <path>, --tls-key <path>, --expose-s3,
#        --expose-console, --acknowledge-public-http, --no-color, --help.
set -eu

REPO="Harsh-2002/Cairn"
GHCR_IMAGE="ghcr.io/harsh-2002/cairn:latest"
RELEASE_IDENTITY="https://github.com/Harsh-2002/Cairn/.github/workflows/release.yml@refs/heads/main"
RELEASE_ISSUER="https://token.actions.githubusercontent.com"
BIN_PATH="/usr/local/bin/cairn"
HOST_DATA_DEFAULT="/var/lib/cairn"
HOST_ETC="/etc/cairn"
HOST_ENV="/etc/cairn/cairn.env"
DOCKER_DIR="/opt/cairn"
SVC_USER="cairn"
S3_PORT="7373"
WEB_PORT="7374"

# Options (defaults; overridden by flags / prompts).
OPT_TARGET="auto"     # auto | host | docker
OPT_MODE="auto"       # auto | install | update | uninstall
OPT_YES="0"
OPT_VERSION="latest"
OPT_DATA_DIR="$HOST_DATA_DEFAULT"
OPT_TLS_CERT=""
OPT_TLS_KEY=""
OPT_EXPOSE_S3="0"
OPT_EXPOSE_CONSOLE="0"
OPT_ACK_PUBLIC_HTTP="0"
S3_BIND_HOST="127.0.0.1"
WEB_BIND_HOST="127.0.0.1"
USE_COLOR="1"
C_RESET=''; C_B=''; C_RED=''; C_GRN=''; C_YEL=''; C_BLU=''; C_DIM=''

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
  --expose-s3        Bind/publish the S3 listener on all host interfaces
  --expose-console   Bind/publish the console on all host interfaces (independent of S3)
  --acknowledge-public-http
                     Required with either exposure flag when TLS is disabled
  --yes              Non-interactive; accept defaults
  --no-color         Disable coloured output
  --help             Show this help

With no target flag the script asks; if Docker is present it offers Docker.
Fresh host and Docker installs keep both listeners on 127.0.0.1 by default, including with TLS.
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

# Prompt independently for public S3 and console exposure. Non-interactive installs retain the
# loopback defaults unless the corresponding flags were supplied.
prompt_exposure() {
  if [ "$OPT_YES" = "1" ] || [ ! -t 0 ]; then return 0; fi
  if [ "$OPT_EXPOSE_S3" = "0" ] \
    && confirm "Expose the S3 listener outside this host?" "n"; then
    OPT_EXPOSE_S3="1"
  fi
  if [ "$OPT_EXPOSE_CONSOLE" = "0" ] \
    && confirm "Expose the web console and management API outside this host?" "n"; then
    OPT_EXPOSE_CONSOLE="1"
  fi
  if [ -z "$OPT_TLS_CERT" ] \
    && { [ "$OPT_EXPOSE_S3" = "1" ] || [ "$OPT_EXPOSE_CONSOLE" = "1" ]; } \
    && [ "$OPT_ACK_PUBLIC_HTTP" != "1" ]; then
    warn "Public HTTP sends credentials, object data, and management traffic without transport encryption."
    if confirm "I understand the risk and want public plaintext HTTP exposure." "n"; then
      OPT_ACK_PUBLIC_HTTP="1"
    else
      die "public plaintext exposure was not acknowledged; keeping it disabled"
    fi
  fi
}

# Resolve installer exposure choices to host-side addresses. Cairn's own raw configuration defaults
# remain unchanged; generated host and Compose deployments deliberately override them.
resolve_exposure() {
  if [ -z "$OPT_TLS_CERT" ] \
    && { [ "$OPT_EXPOSE_S3" = "1" ] || [ "$OPT_EXPOSE_CONSOLE" = "1" ]; } \
    && [ "$OPT_ACK_PUBLIC_HTTP" != "1" ]; then
    die "public plaintext exposure requires --acknowledge-public-http (or configure TLS)"
  fi

  S3_BIND_HOST="127.0.0.1"
  WEB_BIND_HOST="127.0.0.1"
  if [ "$OPT_EXPOSE_S3" = "1" ]; then S3_BIND_HOST="0.0.0.0"; fi
  if [ "$OPT_EXPOSE_CONSOLE" = "1" ]; then WEB_BIND_HOST="0.0.0.0"; fi
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
  pa_s3_host="127.0.0.1"
  pa_web_host="127.0.0.1"
  [ "$OPT_EXPOSE_S3" = "1" ] && pa_s3_host="<host>"
  [ "$OPT_EXPOSE_CONSOLE" = "1" ] && pa_web_host="<host>"
  printf '\n'
  step "Cairn is ready"
  info "S3 API     ${C_B}$pa_scheme://$pa_s3_host:$S3_PORT${C_RESET}"
  info "Console    ${C_B}$pa_scheme://$pa_web_host:$WEB_PORT${C_RESET}"
  info "Access key ${C_B}$ROOT_AK${C_RESET}"
  info "Secret key ${C_B}$ROOT_SK${C_RESET}"
  info "${C_DIM}Keep these and the master key safe. Update later by re-running this script.${C_RESET}"
}

# ---- host install / update ----------------------------------------------------------------------
# Only checksum-pinned verification tools may execute during installation. The script itself,
# installed system tools, HTTPS roots, and these embedded pins form the bootstrap trust root.
sha256_file() {
  if have sha256sum; then sha256sum "$1" | awk '{print $1}'
  elif have shasum; then shasum -a 256 "$1" | awk '{print $1}'
  else die "sha256sum or shasum is required; refusing unverified installation"; fi
}

verify_sha256() {
  vs_actual=$(sha256_file "$1") || die "could not hash $1"
  [ "$vs_actual" = "$2" ] || die "checksum verification failed: $1"
}

manifest_digest() {
  # Never pass a downloaded manifest to a tool that follows its arbitrary filesystem paths.
  # Require one exact filename, one lowercase SHA-256, and no duplicate selected entries.
  awk -v name="$2" '
    $2 == name { count++; if (NF != 2 || length($1) != 64 || $1 ~ /[^0-9a-f]/) bad=1; hash=$1 }
    END { if (count != 1 || bad) exit 1; print hash }
  ' "$1" || die "missing, duplicate, or malformed checksum entry: $2"
}

bootstrap_cosign() {
  bc_arch=$(detect_arch)
  case "$bc_arch" in
    amd64) bc_hash=c956e5dfcac53d52bcf058360d579472f0c1d2d9b69f55209e256fe7783f4c74 ;;
    arm64) bc_hash=bedac92e8c3729864e13d4a17048007cfafa79d5deca993a43a90ffe018ef2b8 ;;
    *) die "no pinned verification tool for $bc_arch" ;;
  esac
  COSIGN="$1/cosign"
  fetch_to "https://github.com/sigstore/cosign/releases/download/v3.0.6/cosign-linux-$bc_arch" "$COSIGN" \
    || die "could not download pinned Cosign verifier"
  verify_sha256 "$COSIGN" "$bc_hash"
  chmod 0700 "$COSIGN" || die "could not prepare verified Cosign"
}

verify_release_blob() {
  "$COSIGN" verify-blob --certificate-identity "$RELEASE_IDENTITY" \
    --certificate-oidc-issuer "$RELEASE_ISSUER" --bundle "$1.cosign.bundle" "$1" \
    >/dev/null || die "release signature verification failed: $1"
}

# Verify everything before replacing the installed executable. Subshell-local traps remove only
# this attempt's temporary files on every error, without disturbing the installer's caller.
download_binary() (
  db_arch=$(detect_arch); db_tag="$1"
  db_tmp=$(mktemp -d) || die "cannot allocate download directory"
  trap 'rm -rf "$db_tmp"' EXIT
  trap 'exit 1' HUP INT TERM
  bootstrap_cosign "$db_tmp"
  db_base="https://github.com/$REPO/releases/download/$db_tag"
  for db_asset in "cairn-linux-$db_arch" "cairn-linux-$db_arch.cosign.bundle" SHA256SUMS SHA256SUMS.cosign.bundle; do
    fetch_to "$db_base/$db_asset" "$db_tmp/$db_asset" \
      || die "required signed release artifact unavailable: $db_asset (older releases may be unsupported)"
  done
  verify_release_blob "$db_tmp/SHA256SUMS"
  db_hash=$(manifest_digest "$db_tmp/SHA256SUMS" "cairn-linux-$db_arch")
  verify_sha256 "$db_tmp/cairn-linux-$db_arch" "$db_hash"
  verify_release_blob "$db_tmp/cairn-linux-$db_arch"
  install -m 0755 "$db_tmp/cairn-linux-$db_arch" "$BIN_PATH" || die "could not install verified binary"
  ok "release signature and checksum verified"
)

bootstrap_gh() {
  bg_arch=$(detect_arch)
  case "$bg_arch" in
    amd64) bg_hash=83d5c2ccad5498f58bf6368acb1ab32588cf43ab3a4b1c301bf36328b1c8bd60 ;;
    arm64) bg_hash=06f86ec7103d41993b76cd78072f43595c34aaa56506d971d9860e67140bf909 ;;
    *) die "no pinned attestation verifier for $bg_arch" ;;
  esac
  bg_name="gh_2.96.0_linux_$bg_arch"
  fetch_to "https://github.com/cli/cli/releases/download/v2.96.0/$bg_name.tar.gz" "$1/gh.tar.gz" \
    || die "could not download pinned GitHub attestation verifier"
  verify_sha256 "$1/gh.tar.gz" "$bg_hash"
  tar -xzf "$1/gh.tar.gz" -C "$1" || die "could not unpack verified GitHub CLI"
  VERIFY_GH="$1/$bg_name/bin/gh"
}

# stdout contains only the verified immutable image reference, suitable for command substitution.
verified_image() (
  vi_tag="$1"
  vi_tmp=$(mktemp -d) || die "cannot allocate image verification directory"
  trap 'rm -rf "$vi_tmp"' EXIT
  trap 'exit 1' HUP INT TERM
  bootstrap_cosign "$vi_tmp"
  bootstrap_gh "$vi_tmp"
  fetch_to "https://github.com/$REPO/releases/download/$vi_tag/IMAGE-DIGEST" "$vi_tmp/IMAGE-DIGEST" \
    || die "release lacks an image digest; refusing an unbound container installation"
  vi_digest=$(cat "$vi_tmp/IMAGE-DIGEST")
  [ "${#vi_digest}" -eq 71 ] || die "invalid image digest"
  printf '%s\n' "$vi_digest" | grep -Eq '^sha256:[0-9a-f]{64}$' || die "invalid image digest"
  vi_image="ghcr.io/harsh-2002/cairn@$vi_digest"
  "$COSIGN" verify --certificate-identity "$RELEASE_IDENTITY" \
    --certificate-oidc-issuer "$RELEASE_ISSUER" "$vi_image" >/dev/null \
    || die "container signature verification failed"
  # Resolve annotated/lightweight tags through the commit API; the authenticated attestation
  # must bind both this exact commit and this exact release version to the immutable subject.
  vi_commit=$("$VERIFY_GH" api "repos/$REPO/commits/$vi_tag" --jq .sha) \
    || die "cannot resolve release commit for provenance verification"
  [ "${#vi_commit}" -eq 40 ] || die "invalid release commit"
  printf '%s\n' "$vi_commit" | grep -Eq '^[0-9a-f]{40}$' || die "invalid release commit"
  vi_versions=$("$VERIFY_GH" attestation verify "oci://$vi_image" --repo "$REPO" \
    --cert-identity "$RELEASE_IDENTITY" --cert-oidc-issuer "$RELEASE_ISSUER" \
    --source-digest "$vi_commit" --predicate-type https://slsa.dev/provenance/v1 \
    --format json --jq '.[].verificationResult.statement.predicate.buildDefinition.externalParameters.version') \
    || die "container provenance verification failed (GitHub/registry authentication may be required)"
  printf '%s\n' "$vi_versions" | grep -F -x -- "$vi_tag" >/dev/null \
    || die "container provenance does not name the requested release"
  printf '%s\n' "$vi_image"
)

ensure_user() {
  id "$SVC_USER" >/dev/null 2>&1 && return 0
  if have useradd; then useradd --system --no-create-home --shell /usr/sbin/nologin "$SVC_USER" 2>/dev/null || useradd -r -s /bin/false "$SVC_USER"
  elif have adduser; then adduser -S -D -H -s /sbin/nologin "$SVC_USER" 2>/dev/null || adduser --system --no-create-home "$SVC_USER"
  else warn "cannot create a '$SVC_USER' user (no useradd/adduser); the service will run as root"; SVC_USER="root"; fi
}

write_env_file() {
  resolve_exposure
  umask 077
  mkdir -p "$HOST_ETC"
  {
    printf 'CAIRN_DATA_DIR=%s\n' "$OPT_DATA_DIR"
    printf 'CAIRN_DB_PATH=%s/cairn.db\n' "$OPT_DATA_DIR"
    printf 'CAIRN_MASTER_KEY=%s\n' "$MASTER_KEY"
    printf 'CAIRN_ROOT_ACCESS_KEY=%s\n' "$ROOT_AK"
    printf 'CAIRN_ROOT_SECRET_KEY=%s\n' "$ROOT_SK"
    printf 'CAIRN_LISTEN_ADDR=%s:%s\n' "$S3_BIND_HOST" "$S3_PORT"
    printf 'CAIRN_WEB_ADDR=%s:%s\n' "$WEB_BIND_HOST" "$WEB_PORT"
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
  is_update="0"
  if [ -x "$BIN_PATH" ] && [ -f "$HOST_ENV" ]; then is_update="1"; fi

  if [ "$is_update" = "0" ]; then
    step "Fresh host installation"
    prompt_tls
    validate_tls
    prompt_exposure
    resolve_exposure
    OPT_DATA_DIR=$(ask "Data directory" "$OPT_DATA_DIR")
  fi

  ih_tag=$(resolve_version)
  download_binary "$ih_tag"
  ok "installed binary to $BIN_PATH ($ih_tag)"

  if [ "$is_update" = "1" ]; then
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
  resolve_exposure
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
      - "$S3_BIND_HOST:$S3_PORT:7373"
      - "$WEB_BIND_HOST:$WEB_PORT:7374"
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
  id_tag=$(resolve_version)
  GHCR_IMAGE=$(verified_image "$id_tag") || die "container verification failed; installation unchanged"
  is_update="0"; [ -f "$DOCKER_DIR/docker-compose.yml" ] && is_update="1"

  if [ "$is_update" = "1" ]; then
    step "Updating the Docker installation"
    # Preserve operator edits and exposure settings; change only the Cairn service image.
    id_candidate=$(mktemp "$DOCKER_DIR/.compose.XXXXXX") || die "cannot stage Compose update"
    awk -v image="$GHCR_IMAGE" '
      /^  cairn:$/ { in_cairn=1; print; next }
      /^  [^ ]/ { in_cairn=0 }
      in_cairn && /^    image:/ { print "    image: " image; count++; next }
      { print }
      END { if (count != 1) exit 1 }
    ' "$DOCKER_DIR/docker-compose.yml" > "$id_candidate" \
      || { rm -f "$id_candidate"; die "cannot identify exactly one Cairn service image"; }
    docker pull "$GHCR_IMAGE" || { rm -f "$id_candidate"; die "verified image pull failed"; }
    mv "$id_candidate" "$DOCKER_DIR/docker-compose.yml"
    compose up -d
    ROOT_AK=$(grep '^CAIRN_ROOT_ACCESS_KEY=' "$DOCKER_DIR/.env" 2>/dev/null | cut -d= -f2-)
    ROOT_SK="(unchanged)"
    ok "verified the release image and recreated the container"
    return 0
  fi

  step "Fresh Docker installation in $DOCKER_DIR"
  prompt_tls
  validate_tls
  prompt_exposure
  resolve_exposure
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
parse_args() {
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --host) OPT_TARGET="host" ;;
      --docker) OPT_TARGET="docker" ;;
      --update) OPT_MODE="update" ;;
      --uninstall) OPT_MODE="uninstall" ;;
      --yes | -y) OPT_YES="1" ;;
      --no-color) USE_COLOR="0" ;;
      --expose-s3) OPT_EXPOSE_S3="1" ;;
      --expose-console) OPT_EXPOSE_CONSOLE="1" ;;
      --acknowledge-public-http) OPT_ACK_PUBLIC_HTTP="1" ;;
      --version)
        [ "$#" -ge 2 ] || die "--version requires a value"
        shift
        OPT_VERSION="$1"
        ;;
      --data-dir)
        [ "$#" -ge 2 ] || die "--data-dir requires a value"
        shift
        OPT_DATA_DIR="$1"
        ;;
      --tls-cert)
        [ "$#" -ge 2 ] || die "--tls-cert requires a value"
        shift
        OPT_TLS_CERT="$1"
        ;;
      --tls-key)
        [ "$#" -ge 2 ] || die "--tls-key requires a value"
        shift
        OPT_TLS_KEY="$1"
        ;;
      --help | -h) usage; exit 0 ;;
      *) printf 'unknown option: %s\n\n' "$1" >&2; usage; exit 2 ;;
    esac
    shift
  done
}

run_installer() {
  parse_args "$@"
  setup_color
  require_root "$@"
  main
}

# The shell regression harness sources the renderer in-process; normal execution always takes this
# branch, including curl-to-sh installs.
if [ "${INSTALLER_SOURCE_ONLY:-0}" != "1" ]; then
  run_installer "$@"
fi
