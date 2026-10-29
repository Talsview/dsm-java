#!/bin/sh
cd "$(dirname "$0")/.." || exit 1
mkdir -p out
javac -encoding UTF-8 -d out src/dsm/*.java && echo "Compilado com sucesso em ./out"
