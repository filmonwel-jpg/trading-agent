#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")" && pwd)"
runtime_dir="$repo_root/runtime"
config_file="$runtime_dir/ibkr-keychain.env"

usage() {
  cat <<'EOF'
Usage:
  ./ibkr_keychain_launch.sh [--check] [--dry-run] [--launch-only] [--prefill-only]

Behavior:
  - Reads non-secret launcher settings from runtime/ibkr-keychain.env.
  - Reads IBKR username/password from macOS Keychain.
  - Launches Trader Workstation / IB Gateway.
  - Optionally uses AppleScript UI scripting to type credentials into the login window.

Important:
  - This launcher is local-only. Do not commit runtime/ibkr-keychain.env.
  - IBKR 2FA / trusted-device prompts may still require manual approval.
  - For fully unattended login, an external launcher such as IBC is often more reliable.
EOF
}

log() {
  printf '[IBKR-KEYCHAIN][%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
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

launch_app() {
  if [[ -n "$app_path" ]]; then
    open "$app_path"
    return
  fi
  open -a "$app_name"
}

prefill_login() {
  local app_name="$1"
  local username="$2"
  local password="$3"
  local tab_to_username="$4"
  local tabs_between="$5"
  local tab_to_submit="$6"
  local submit_mode="$7"

  osascript - "$app_name" "$username" "$password" "$tab_to_username" "$tabs_between" "$tab_to_submit" "$submit_mode" <<'APPLESCRIPT'
on run argv
    set appName to item 1 of argv
    set userName to item 2 of argv
    set userPassword to item 3 of argv
    set tabToUsername to (item 4 of argv) as integer
    set tabsBetween to (item 5 of argv) as integer
    set tabsToSubmit to (item 6 of argv) as integer
    set submitMode to item 7 of argv

    tell application appName to activate
    delay 0.5
    tell application "System Events"
        repeat tabToUsername times
            key code 48
            delay 0.08
        end repeat
        keystroke userName
        repeat tabsBetween times
            key code 48
            delay 0.08
        end repeat
        keystroke userPassword
        repeat tabsToSubmit times
            key code 48
            delay 0.08
        end repeat
        if submitMode is "return" then
            key code 36
        end if
    end tell
end run
APPLESCRIPT
}

load_config

account="${IBKR_KEYCHAIN_ACCOUNT:-$USER}"
username_service="${IBKR_KEYCHAIN_USERNAME_SERVICE:-trading-agent.ibkr.username}"
password_service="${IBKR_KEYCHAIN_PASSWORD_SERVICE:-trading-agent.ibkr.password}"
app_name="${IBKR_KEYCHAIN_APP_NAME:-${IBKR_APP_NAME:-Trader Workstation}}"
app_path="${IBKR_KEYCHAIN_APP_PATH:-${IBKR_APP_PATH:-}}"
launch_delay_seconds="${IBKR_KEYCHAIN_LAUNCH_DELAY_SECONDS:-8}"
autofill_enabled="${IBKR_KEYCHAIN_AUTOFILL:-1}"
tab_to_username="${IBKR_KEYCHAIN_TAB_TO_USERNAME_COUNT:-0}"
tabs_between_fields="${IBKR_KEYCHAIN_TAB_BETWEEN_FIELDS:-1}"
tabs_to_submit="${IBKR_KEYCHAIN_TAB_TO_SUBMIT_COUNT:-0}"
submit_mode="${IBKR_KEYCHAIN_SUBMIT_MODE:-return}"

check_only=0
dry_run=0
launch_only=0
prefill_only=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --check)
      check_only=1
      ;;
    --dry-run)
      dry_run=1
      ;;
    --launch-only)
      launch_only=1
      ;;
    --prefill-only)
      prefill_only=1
      ;;
    *)
      echo "[IBKR-KEYCHAIN][ERROR] Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
  shift
done

if ! [[ "$launch_delay_seconds" =~ ^[0-9]+([.][0-9]+)?$ && "$tab_to_username" =~ ^[0-9]+$ && "$tabs_between_fields" =~ ^[0-9]+$ && "$tabs_to_submit" =~ ^[0-9]+$ ]]; then
  echo "[IBKR-KEYCHAIN][ERROR] launch delay and tab counts must be numeric." >&2
  exit 1
fi

username_value="$(get_keychain_value "$username_service" "$account")"
password_value="$(get_keychain_value "$password_service" "$account")"

if [[ -z "$username_value" || -z "$password_value" ]]; then
  echo "[IBKR-KEYCHAIN][ERROR] Missing Keychain credentials. Run ./set_ibkr_keychain_credentials.sh first." >&2
  exit 1
fi

log "Keychain credentials found for account=$account app=${app_path:-$app_name}"

if [[ "$check_only" -eq 1 ]]; then
  log "check-only passed"
  exit 0
fi

if [[ "$dry_run" -eq 1 ]]; then
  log "dry-run: would launch ${app_path:-$app_name} autofill=${autofill_enabled}"
  exit 0
fi

if [[ "$prefill_only" -ne 1 ]]; then
  log "launching ${app_path:-$app_name}"
  launch_app
fi

if [[ "$launch_only" -eq 1 ]]; then
  log "launch-only complete; skipping credential prefill"
  exit 0
fi

if [[ "$autofill_enabled" != "1" ]]; then
  log "autofill disabled; app launched without typing credentials"
  exit 0
fi

log "waiting ${launch_delay_seconds}s for the login window"
sleep "$launch_delay_seconds"
log "sending credentials through macOS UI scripting (2FA may still require manual approval)"
prefill_login "$app_name" "$username_value" "$password_value" "$tab_to_username" "$tabs_between_fields" "$tabs_to_submit" "$submit_mode"
log "credential prefill completed"


