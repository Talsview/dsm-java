#!/bin/sh
# Uso: scripts/no.sh <idDoNo> [host] [porta]   Ex.: scripts/no.sh A
cd "$(dirname "$0")/.." || exit 1
java -Dfile.encoding=UTF-8 -cp out dsm.NodeMain "$@"
