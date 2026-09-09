#!/bin/sh
# Regression tests for install.sh argument parsing and generated listener exposure.
set -eu

TEST_DIR=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
REPO_DIR=$(CDPATH='' cd -- "$TEST_DIR/.." && pwd)
TEST_ROOT=$(mktemp -d)
trap 'rm -rf "$TEST_ROOT"' EXIT HUP INT TERM

TLS_CERT="$TEST_ROOT/cert.pem"
TLS_KEY="$TEST_ROOT/key.pem"
printf '%s\n' 'test certificate' > "$TLS_CERT"
printf '%s\n' 'test private key' > "$TLS_KEY"

fail() {
  printf 'FAIL: %s\n' "$*" >&2
  exit 1
}

assert_line() {
  assert_file="$1"
  assert_expected="$2"
  grep -F -x -- "$assert_expected" "$assert_file" >/dev/null \
    || fail "$assert_file does not contain line: $assert_expected"
}

assert_absent() {
  assert_file="$1"
  assert_unexpected="$2"
  if grep -F -- "$assert_unexpected" "$assert_file" >/dev/null; then
    fail "$assert_file unexpectedly contains: $assert_unexpected"
  fi
}

render_host() {
  render_name="$1"
  shift
  render_dir="$TEST_ROOT/$render_name"
  mkdir -p "$render_dir"
  REPO_DIR="$REPO_DIR" CASE_DIR="$render_dir" INSTALLER_SOURCE_ONLY=1 \
    sh -c '
      . "$REPO_DIR/install.sh"
      parse_args "$@"
      setup_color
      HOST_ETC="$CASE_DIR/etc"
      HOST_ENV="$HOST_ETC/cairn.env"
      OPT_DATA_DIR="$CASE_DIR/data"
      MASTER_KEY="installer-test-master"
      ROOT_AK="installer-test-access"
      ROOT_SK="installer-test-secret"
      validate_tls
      resolve_exposure
      write_env_file
    ' installer-test "$@" || return $?
  printf '%s\n' "$render_dir/etc/cairn.env"
}

render_compose() {
  render_name="$1"
  shift
  render_dir="$TEST_ROOT/$render_name"
  mkdir -p "$render_dir"
  REPO_DIR="$REPO_DIR" CASE_DIR="$render_dir" INSTALLER_SOURCE_ONLY=1 \
    sh -c '
      . "$REPO_DIR/install.sh"
      parse_args "$@"
      setup_color
      DOCKER_DIR="$CASE_DIR/docker"
      MASTER_KEY="installer-test-master"
      ROOT_AK="installer-test-access"
      ROOT_SK="installer-test-secret"
      validate_tls
      resolve_exposure
      write_compose
    ' installer-test "$@" || return $?
  printf '%s\n' "$render_dir/docker/docker-compose.yml"
}

assert_ack_required() {
  assert_mode="$1"
  shift
  assert_error="$TEST_ROOT/ack-$assert_mode.err"
  case "$assert_mode" in
    host)
      if render_host "ack-host-$*" "$@" >/dev/null 2>"$assert_error"; then
        fail "host public plaintext exposure succeeded without acknowledgement"
      fi
      ;;
    compose)
      if render_compose "ack-compose-$*" "$@" >/dev/null 2>"$assert_error"; then
        fail "Compose public plaintext exposure succeeded without acknowledgement"
      fi
      ;;
    *) fail "unknown acknowledgement test mode: $assert_mode" ;;
  esac
  grep -F -- '--acknowledge-public-http' "$assert_error" >/dev/null \
    || fail "missing acknowledgement error did not name --acknowledge-public-http"
}

# Host: unattended installs are safe for both plaintext and TLS. Exposure flags are independent.
host_plain=$(render_host host-plain --host --yes)
assert_line "$host_plain" 'CAIRN_LISTEN_ADDR=127.0.0.1:7373'
assert_line "$host_plain" 'CAIRN_WEB_ADDR=127.0.0.1:7374'
assert_absent "$host_plain" 'CAIRN_TLS_CERT_PATH='

host_tls=$(render_host host-tls --host --yes --tls-cert "$TLS_CERT" --tls-key "$TLS_KEY")
assert_line "$host_tls" 'CAIRN_LISTEN_ADDR=127.0.0.1:7373'
assert_line "$host_tls" 'CAIRN_WEB_ADDR=127.0.0.1:7374'
assert_line "$host_tls" "CAIRN_TLS_CERT_PATH=$TLS_CERT"

host_plain_s3=$(render_host host-plain-s3 --host --yes --expose-s3 \
  --acknowledge-public-http)
assert_line "$host_plain_s3" 'CAIRN_LISTEN_ADDR=0.0.0.0:7373'
assert_line "$host_plain_s3" 'CAIRN_WEB_ADDR=127.0.0.1:7374'

host_tls_s3=$(render_host host-tls-s3 --host --yes --tls-cert "$TLS_CERT" \
  --tls-key "$TLS_KEY" --expose-s3)
assert_line "$host_tls_s3" 'CAIRN_LISTEN_ADDR=0.0.0.0:7373'
assert_line "$host_tls_s3" 'CAIRN_WEB_ADDR=127.0.0.1:7374'

host_plain_console=$(render_host host-plain-console --host --yes --expose-console \
  --acknowledge-public-http)
assert_line "$host_plain_console" 'CAIRN_LISTEN_ADDR=127.0.0.1:7373'
assert_line "$host_plain_console" 'CAIRN_WEB_ADDR=0.0.0.0:7374'

# Compose: host-port publication is loopback-only by default, including when TLS is configured.
compose_plain=$(render_compose compose-plain --docker --yes)
assert_line "$compose_plain" '      - "127.0.0.1:7373:7373"'
assert_line "$compose_plain" '      - "127.0.0.1:7374:7374"'
assert_absent "$compose_plain" 'CAIRN_TLS_CERT_PATH:'

compose_tls=$(render_compose compose-tls --docker --yes --tls-cert "$TLS_CERT" \
  --tls-key "$TLS_KEY")
assert_line "$compose_tls" '      - "127.0.0.1:7373:7373"'
assert_line "$compose_tls" '      - "127.0.0.1:7374:7374"'
assert_line "$compose_tls" '      CAIRN_TLS_CERT_PATH: /certs/cert.pem'

compose_plain_s3=$(render_compose compose-plain-s3 --docker --yes --expose-s3 \
  --acknowledge-public-http)
assert_line "$compose_plain_s3" '      - "0.0.0.0:7373:7373"'
assert_line "$compose_plain_s3" '      - "127.0.0.1:7374:7374"'

compose_tls_console=$(render_compose compose-tls-console --docker --yes \
  --tls-cert "$TLS_CERT" --tls-key "$TLS_KEY" --expose-console)
assert_line "$compose_tls_console" '      - "127.0.0.1:7373:7373"'
assert_line "$compose_tls_console" '      - "0.0.0.0:7374:7374"'

# Argument validation: every public plaintext listener needs the explicit risk acknowledgement.
assert_ack_required host --host --yes --expose-s3
assert_ack_required host --host --yes --expose-console
assert_ack_required compose --docker --yes --expose-s3
assert_ack_required compose --docker --yes --expose-console

help_output=$(sh "$REPO_DIR/install.sh" --help)
for help_flag in --expose-s3 --expose-console --acknowledge-public-http; do
  printf '%s\n' "$help_output" | grep -F -- "$help_flag" >/dev/null \
    || fail "installer help omits $help_flag"
done

missing_value_error="$TEST_ROOT/missing-value.err"
if sh "$REPO_DIR/install.sh" --tls-cert >/dev/null 2>"$missing_value_error"; then
  fail "--tls-cert without a value succeeded"
fi
grep -F -- '--tls-cert requires a value' "$missing_value_error" >/dev/null \
  || fail "missing --tls-cert value did not produce the expected error"

printf '%s\n' 'installer exposure regression tests: PASS'

# Verify the trust boundary with real file hashing and mocked external signature tools. The
# separate live check uses the pinned real Cosign against published release artifacts.
cat > "$TEST_ROOT/verification-driver.sh" <<'DRIVER'
#!/bin/sh
set -eu
INSTALLER_SOURCE_ONLY=1
export INSTALLER_SOURCE_ONLY
. "$REPO_DIR/install.sh"
BIN_PATH="$CASE_DIR/installed"
printf '%s' old > "$BIN_PATH"
printf '%s' new > "$CASE_DIR/payload"
TEST_HASH=$(sha256_file "$CASE_DIR/payload")
export TEST_HASH
# Override only downloads and process boundaries, never the checksum/manifest implementation.
detect_arch() { printf '%s' "$TEST_ARCH"; }
bootstrap_cosign() { COSIGN="$CASE_DIR/mock-cosign"; }
bootstrap_gh() { VERIFY_GH="$CASE_DIR/mock-gh"; }
fetch_to() {
  asset=${1##*/}
  case "$TEST_CASE:$asset" in
    missing:SHA256SUMS|missing-signature:*.cosign.bundle) return 1 ;;
  esac
  case "$asset" in
    cairn-linux-amd64|cairn-linux-arm64)
      if [ "$TEST_CASE" = corrupt ]; then printf '%s' damaged > "$2"
      else cp "$CASE_DIR/payload" "$2"; fi ;;
    SHA256SUMS)
      printf '%s  cairn-linux-%s\n' "$TEST_HASH" "$TEST_ARCH" > "$2"
      case "$TEST_CASE" in
        duplicate) printf '%s  cairn-linux-%s\n' "$TEST_HASH" "$TEST_ARCH" >> "$2" ;;
        malformed) printf 'bad  cairn-linux-%s\n' "$TEST_ARCH" > "$2" ;;
        substring) printf '%s  cairn-linux-%s.attacker\n' "$TEST_HASH" "$TEST_ARCH" > "$2" ;;
      esac ;;
    *.cosign.bundle) printf '%s' bundle > "$2" ;;
    IMAGE-DIGEST)
      if [ "$TEST_CASE" = bad-image ]; then printf '%s' 'sha256:bad' > "$2"
      else printf 'sha256:%s\n' "$TEST_HASH" > "$2"; fi
      [ "$TEST_CASE" != multiline-image ] || printf 'extra\n' >> "$2" ;;
    *) return 1 ;;
  esac
}
cat > "$CASE_DIR/mock-cosign" <<'COSIGN'
#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$CASE_DIR/cosign-calls"
case " $* " in
  *' --certificate-identity https://github.com/Harsh-2002/Cairn/.github/workflows/release.yml@refs/heads/main '*) ;;
  *) exit 1 ;;
esac
case " $* " in
  *' --certificate-oidc-issuer https://token.actions.githubusercontent.com '*) ;;
  *) exit 1 ;;
esac
case "$TEST_CASE" in bad-signature|wrong-identity|wrong-issuer) exit 1 ;; esac
COSIGN
chmod +x "$CASE_DIR/mock-cosign"
cat > "$CASE_DIR/mock-gh" <<'GH'
#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$CASE_DIR/gh-calls"
if [ "$1" = api ]; then
  [ "$TEST_CASE" != multiline-commit ] || printf "extra\n"
  printf '%040d\n' 1; exit; fi
case " $* " in
  *' --source-digest 0000000000000000000000000000000000000001 '*) ;;
  *) exit 1 ;;
esac
for expected in \
  '--repo Harsh-2002/Cairn' \
  '--cert-identity https://github.com/Harsh-2002/Cairn/.github/workflows/release.yml@refs/heads/main' \
  '--cert-oidc-issuer https://token.actions.githubusercontent.com' \
  '--predicate-type https://slsa.dev/provenance/v1' \
  '--jq .[].verificationResult.statement.predicate.buildDefinition.internalParameters.cairn.parameters.version'; do
  case " $* " in *" $expected "*) ;; *) exit 1 ;; esac
done
case "$TEST_CASE" in bad-provenance) exit 1 ;; wrong-release) printf '%s\n' v2020.01.01 ;; *) printf '%s\n' v2026.09.06 ;; esac
GH
chmod +x "$CASE_DIR/mock-gh"
if [ "$TEST_CASE" = no-hasher ]; then
  have() { case "$1" in sha256sum|shasum) return 1 ;; *) command -v "$1" >/dev/null 2>&1 ;; esac; }
fi
case "$TEST_CASE" in
  image-success|bad-image|multiline-image|multiline-commit|bad-provenance|wrong-release)
    verified_image v2026.09.06 > "$CASE_DIR/image" ;;
  *) download_binary v2026.09.06 ;;
esac
DRIVER

for verify_arch in amd64 arm64; do
  for verify_case in success missing missing-signature corrupt duplicate malformed substring bad-signature wrong-identity wrong-issuer no-hasher image-success bad-image multiline-image multiline-commit bad-provenance wrong-release; do
    verify_dir="$TEST_ROOT/verify-$verify_arch-$verify_case"
    mkdir -p "$verify_dir"
    if REPO_DIR="$REPO_DIR" CASE_DIR="$verify_dir" TEST_ARCH="$verify_arch" TEST_CASE="$verify_case" \
      sh "$TEST_ROOT/verification-driver.sh" > "$verify_dir/log" 2>&1; then
      case "$verify_case" in
        success) [ "$(cat "$verify_dir/installed")" = new ] || fail "verified binary not installed" ;;
        image-success) grep -Eq '^ghcr.io/harsh-2002/cairn@sha256:[0-9a-f]{64}$' "$verify_dir/image" || fail "image not digest pinned" ;;
        *) fail "verification unexpectedly accepted $verify_arch/$verify_case" ;;
      esac
    else
      case "$verify_case" in success|image-success) cat "$verify_dir/log"; fail "valid verification rejected" ;; esac
      [ "$(cat "$verify_dir/installed")" = old ] || fail "failed verification replaced installed binary"
    fi
  done
done

# The bootstrap must reject a downloaded verifier before it is ever marked executable or run.
REPO_DIR="$REPO_DIR" CASE_DIR="$TEST_ROOT" INSTALLER_SOURCE_ONLY=1 sh -c '
  . "$REPO_DIR/install.sh"
  fetch_to() { printf "#!/bin/sh\ntouch %s/executed\n" "$CASE_DIR" > "$2"; }
  bootstrap_cosign "$CASE_DIR"
' > "$TEST_ROOT/bootstrap.log" 2>&1 && fail "unverified verifier accepted"
[ ! -e "$TEST_ROOT/executed" ] || fail "unverified verifier executed"

# The GitHub CLI archive is authenticated before extraction as well.
REPO_DIR="$REPO_DIR" CASE_DIR="$TEST_ROOT" INSTALLER_SOURCE_ONLY=1 sh -c '
  . "$REPO_DIR/install.sh"
  fetch_to() { printf corrupt-archive > "$2"; }
  tar() { touch "$CASE_DIR/archive-extracted"; }
  bootstrap_gh "$CASE_DIR"
' > "$TEST_ROOT/gh-bootstrap.log" 2>&1 && fail "unverified GitHub CLI accepted"
[ ! -e "$TEST_ROOT/archive-extracted" ] || fail "unverified archive extracted"

# No container mutation is allowed if image verification fails.
mkdir -p "$TEST_ROOT/compose-preserved"
printf '%s\n' 'original compose' > "$TEST_ROOT/compose-preserved/docker-compose.yml"
REPO_DIR="$REPO_DIR" CASE_DIR="$TEST_ROOT" INSTALLER_SOURCE_ONLY=1 sh -c '
  . "$REPO_DIR/install.sh"
  DOCKER_DIR="$CASE_DIR/compose-preserved"
  docker() { touch "$CASE_DIR/docker-called"; }
  resolve_version() { printf "%s" v2026.09.06; }
  verified_image() { return 1; }
  install_docker
' > "$TEST_ROOT/image-failure.log" 2>&1 && fail "invalid image accepted"
assert_line "$TEST_ROOT/compose-preserved/docker-compose.yml" 'original compose'
[ ! -e "$TEST_ROOT/docker-called" ] || fail "container changed before verification"
# Successful updates preserve custom settings and other services; failed pulls preserve the file.
for update_case in success pull-failure ambiguous; do
  update_dir="$TEST_ROOT/update-$update_case"
  mkdir -p "$update_dir"
  cat > "$update_dir/docker-compose.yml" <<'COMPOSE'
services:
  cairn:
    image: ghcr.io/harsh-2002/cairn:old
    restart: unless-stopped
    ports:
      - "127.0.0.1:9999:7373"
  custom:
    image: example/custom:retained
COMPOSE
  if [ "$update_case" = ambiguous ]; then
    printf '  cairn:\n    image: duplicate\n' >> "$update_dir/docker-compose.yml"
  fi
  cp "$update_dir/docker-compose.yml" "$update_dir/original"
  printf 'CAIRN_ROOT_ACCESS_KEY=retained-access\n' > "$update_dir/.env"
  if REPO_DIR="$REPO_DIR" CASE_DIR="$update_dir" UPDATE_CASE="$update_case" INSTALLER_SOURCE_ONLY=1 sh -c '
    . "$REPO_DIR/install.sh"
    DOCKER_DIR="$CASE_DIR"
    resolve_version() { printf "%s" v2026.09.06; }
    verified_image() { printf "ghcr.io/harsh-2002/cairn@sha256:%064d" 1; }
    docker() {
      printf "%s\n" "$*" >> "$CASE_DIR/docker-calls"
      [ "$UPDATE_CASE" != pull-failure ]
    }
    compose() { printf "%s\n" "$*" >> "$CASE_DIR/compose-calls"; }
    install_docker
  ' > "$update_dir/log" 2>&1; then
    [ "$update_case" = success ] || fail "invalid Compose update accepted"
    sed "s|ghcr.io/harsh-2002/cairn:old|ghcr.io/harsh-2002/cairn@sha256:$(printf '%064d' 1)|" \
      "$update_dir/original" > "$update_dir/expected"
    cmp "$update_dir/expected" "$update_dir/docker-compose.yml" || fail "custom Compose settings changed"
    assert_line "$update_dir/compose-calls" 'up -d'
  else
    [ "$update_case" != success ] || { cat "$update_dir/log"; fail "valid Compose update failed"; }
    cmp "$update_dir/original" "$update_dir/docker-compose.yml" || fail "failed update changed Compose"
    [ ! -e "$update_dir/compose-calls" ] || fail "failed update recreated containers"
  fi
done
printf '%s\n' 'installer verification regression tests: PASS'
