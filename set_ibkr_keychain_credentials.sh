#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")" && pwd)"
runtime_dir="$repo_root/runtime"
config_file="$runtime_dir/ibkr-keychain.env"

usage() {
  cat <<'EOF'
Usage:
  ./set_ibkr_keychain_credentials.sh [--account=ACCOUNT] [--username-service=SERVICE] [--password-service=SERVICE]
  ./set_ibkr_keychain_credentials.sh --check
  ./set_ibkr_keychain_credentials.sh --delete

Behavior:
  - Stores IBKR username and password in macOS Keychain.
  - Reads defaults from runtime/ibkr-keychain.env if present.
  - Never writes secrets into the repository.

Examples:
  ./set_ibkr_keychain_credentials.sh
  ./set_ibkr_keychain_credentials.sh --check
  ./set_ibkr_keychain_credentials.sh --delete
EOF
}

load_config() {
  if [[ -f "$config_file" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "$config_file"
    set +a
  fi
}

get_keychain_value() {
  local service="$1"
  local account="$2"
  security find-generic-password -a "$account" -s "$service" -w 2>/dev/null || true
}

store_keychain_value() {
  local service="$1"
  local account="$2"
  local secret="$3"
  security add-generic-password -U -a "$account" -s "$service" -w "$secret" >/dev/null
}

delete_keychain_value() {
  local service="$1"
  local account="$2"
  security delete-generic-password -a "$account" -s "$service" >/dev/null 2>&1 || true
}

load_config

account="${IBKR_KEYCHAIN_ACCOUNT:-$USER}"
username_service="${IBKR_KEYCHAIN_USERNAME_SERVICE:-trading-agent.ibkr.username}"
password_service="${IBKR_KEYCHAIN_PASSWORD_SERVICE:-trading-agent.ibkr.password}"
check_only=0
delete_only=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --check)
      check_only=1
      ;;
    --delete)
      delete_only=1
      ;;
    --account=*)
      account="${1#--account=}"
      ;;
    --username-service=*)
      username_service="${1#--username-service=}"
      ;;
    --password-service=*)
      password_service="${1#--password-service=}"
      ;;
    *)
      echo "[IBKR-KEYCHAIN][ERROR] Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
  shift
done

if [[ "$delete_only" -eq 1 ]]; then
  delete_keychain_value "$username_service" "$account"
  delete_keychain_value "$password_service" "$account"
  echo "[IBKR-KEYCHAIN] Deleted Keychain items for account=$account"
  exit 0
fi

username_value="$(get_keychain_value "$username_service" "$account")"
password_value="$(get_keychain_value "$password_service" "$account")"

if [[ "$check_only" -eq 1 ]]; then
  [[ -n "$username_value" ]] && echo "[IBKR-KEYCHAIN] username present service=$username_service account=$account" || echo "[IBKR-KEYCHAIN] username missing service=$username_service account=$account"
  [[ -n "$password_value" ]] && echo "[IBKR-KEYCHAIN] password present service=$password_service account=$account" || echo "[IBKR-KEYCHAIN] password missing service=$password_service account=$account"
  if [[ -n "$username_value" && -n "$password_value" ]]; then
    exit 0
  fi
  exit 1
fi

read -r -p "IBKR username: " input_username
echo -n "IBKR password: "
stty -echo
IFS= read -r input_password
stty echo
echo

if [[ -z "$input_username" || -z "$input_password" ]]; then
  echo "[IBKR-KEYCHAIN][ERROR] Username and password are required." >&2
  exit 1
fi

store_keychain_value "$username_service" "$account" "$input_username"
store_keychain_value "$password_service" "$account" "$input_password"

echo "[IBKR-KEYCHAIN] Stored username in service=$username_service account=$account"
echo "[IBKR-KEYCHAIN] Stored password in service=$password_service account=$account"
echo "[IBKR-KEYCHAIN] Next step: set IBKR_LAUNCH_CMD to '$repo_root/ibkr_keychain_launch.sh' in runtime/ibkr-launch.env"

