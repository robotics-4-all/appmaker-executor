#!/bin/sh
set -e
exec python -u /appexecutor/src/appmakerexecutor/appmaker.py "${UID:?UID is not set in .env}"
