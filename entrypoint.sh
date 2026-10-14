#!/bin/sh
set -e
# CLI arg wins, then UID (compose/.env), then APPMAKER_UID. Under /bin/sh
# (dash) $UID has no builtin default, so every source must be checked.
EFFECTIVE_UID="${1:-${UID:-${APPMAKER_UID:-}}}"
if [ -z "${EFFECTIVE_UID}" ]; then
  echo "UID is not set. Provide it as arg, or set UID / APPMAKER_UID in .env" >&2
  exit 1
fi
echo "UID=${EFFECTIVE_UID} REDIS_HOST=${REDIS_HOST:-localhost} REDIS_PORT=${REDIS_PORT:-6379}"
exec python -u /appexecutor/src/appmakerexecutor/appmaker.py "${EFFECTIVE_UID}"
