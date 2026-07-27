#!/usr/bin/env bash
# Copyright(C) 2024-2026, HJD (https://github.com/hjdhjd). All rights reserved.
#
# dev-prismcast.sh: Launch PrismCast in a fully-isolated dev profile, structurally guaranteed not to interfere with the production instance.
#
# The script refuses to invoke node unless every isolation check below passes:
#   - PRISMCAST_DATA_DIR is set, absolute, not equal to or nested under the prod default ($HOME/.prismcast), and contains a dev/test/tmp marker.
#   - PRISMCAST_DEV_PORT is not the prod default (5589).
#   - The caller's extra arguments do not include --port or --data-dir (those would bypass the env-controlled values above).
#   - HDHR emulation is forced off (HDHR_ENABLED=false). Caller may opt in with ALLOW_HDHR=1,
#     but only when HDHR_PORT is also set to a non-prod value (default prod is 5004).
#
# Defaults when unset: PRISMCAST_DATA_DIR=/tmp/prismcast-dev, PRISMCAST_DEV_PORT=5590.
#
# Every refusal exits 1 BEFORE node is invoked. There is no path where a misconfigured invocation reaches the prod data directory or port.

set -euo pipefail

PROD_DATA_DIR="$HOME/.prismcast"
PROD_PORT=5589
PROD_HDHR_PORT=5004

DEV_DATA_DIR="${PRISMCAST_DATA_DIR:-/tmp/prismcast-dev}"
DEV_PORT="${PRISMCAST_DEV_PORT:-5590}"

refuse() {

  echo "[dev-prismcast] REFUSED: $1" >&2
  exit 1
}

# The script must run from the prismcast repo root so dist/index.js resolves correctly. The package.json check is the cheapest sanity gate.
if [ ! -f "package.json" ] || ! grep -q '"name": "prismcast"' package.json; then

  refuse "must be run from the prismcast repo root (cwd: $(pwd))."
fi

# The build artifact is required. Failing here gives a clearer message than node's MODULE_NOT_FOUND.
if [ ! -f "dist/index.js" ]; then

  refuse "dist/index.js not found. Run 'npm run build' first."
fi

# Data directory must be an absolute path. Relative paths resolve unpredictably and could land in the repo or elsewhere.
case "$DEV_DATA_DIR" in
  /*) ;;
  *) refuse "PRISMCAST_DATA_DIR must be absolute (got: $DEV_DATA_DIR)." ;;
esac

# Data directory must not equal the prod path or live under it. The case glob matches both the exact path and any nested subpath.
case "$DEV_DATA_DIR" in
  "$PROD_DATA_DIR"|"$PROD_DATA_DIR"/*)
    refuse "PRISMCAST_DATA_DIR ($DEV_DATA_DIR) points at production ($PROD_DATA_DIR) or a path under it." ;;
esac

# Data directory must contain a dev/test/tmp marker. This catches typos and accidental real-config-adjacent paths that would pass the prod-equality check but
# still feel production-ish (e.g., ~/.prismcast-backup, ~/Configs/prismcast).
if ! [[ "$DEV_DATA_DIR" =~ (dev|test|tmp) ]]; then

  refuse "PRISMCAST_DATA_DIR ($DEV_DATA_DIR) does not contain a 'dev', 'test', or 'tmp' marker. Set the path explicitly to something clearly non-prod."
fi

# Dev port must not be the prod default. This is the listening port the script passes to --port; we own it via the env, but we still verify.
if [ "$DEV_PORT" = "$PROD_PORT" ]; then

  refuse "PRISMCAST_DEV_PORT ($DEV_PORT) is the production default. Use a different port."
fi

# Reject any caller-supplied --port or --data-dir flags. Those would shadow the env-controlled values and bypass the checks above. The caller can adjust via env;
# the flags are reserved.
for arg in "$@"; do

  case "$arg" in
    --port|--port=*|-p|-p=*)
      refuse "do not pass --port; set PRISMCAST_DEV_PORT in the environment." ;;
    --data-dir|--data-dir=*)
      refuse "do not pass --data-dir; set PRISMCAST_DATA_DIR in the environment." ;;
  esac
done

# HDHR is forced off by default. Opt-in requires ALLOW_HDHR=1 AND a non-prod HDHR_PORT - both gates so a stale environment cannot accidentally enable broadcast on
# the prod port.
if [ "${ALLOW_HDHR:-0}" = "1" ]; then

  if [ -z "${HDHR_PORT:-}" ] || [ "$HDHR_PORT" = "$PROD_HDHR_PORT" ]; then

    refuse "ALLOW_HDHR=1 requires HDHR_PORT to be set to a non-prod value (prod default is $PROD_HDHR_PORT)."
  fi

  HDHR_ENABLED_VAL=true
  HDHR_PORT_FOR_LOG="$HDHR_PORT"
else

  HDHR_ENABLED_VAL=false
  HDHR_PORT_FOR_LOG="(disabled)"
fi

# Ensure the dev data directory exists before node tries to write into it. Safe to call more than once - existing dirs are left alone.
mkdir -p "$DEV_DATA_DIR"

# Default the debug filter to the cdp category so the Chrome DevTools Protocol proxy is enabled for dev runs out of the box. The ${VAR:=default} form assigns only
# when the variable is unset or empty, so a user-set PRISMCAST_DEBUG=tuning:hulu (or anything else) is preserved verbatim.
: "${PRISMCAST_DEBUG:=cdp}"
export PRISMCAST_DEBUG

# Echo the resolved environment so the operator can sanity-check before the process backgrounds (or before grepping logs after a foreground crash).
echo "[dev-prismcast] PRISMCAST_DATA_DIR=$DEV_DATA_DIR"
echo "[dev-prismcast] HTTP port=$DEV_PORT"
echo "[dev-prismcast] HDHR_ENABLED=$HDHR_ENABLED_VAL (port=$HDHR_PORT_FOR_LOG)"
echo "[dev-prismcast] PRISMCAST_DEBUG=$PRISMCAST_DEBUG"
echo "[dev-prismcast] PrismCast version: $(node -p "require('./package.json').version")"
echo "[dev-prismcast] Launching..."

PRISMCAST_DATA_DIR="$DEV_DATA_DIR" \
HDHR_ENABLED="$HDHR_ENABLED_VAL" \
exec node dist/index.js --port "$DEV_PORT" "$@"
