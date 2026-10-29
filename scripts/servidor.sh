#!/bin/sh
# Uso: scripts/servidor.sh [porta]   (padrão: 5000)
cd "$(dirname "$0")/.." || exit 1
java -Dfile.encoding=UTF-8 -cp out dsm.ServerMain "$@"
