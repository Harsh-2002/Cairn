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
