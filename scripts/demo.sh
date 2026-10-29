#!/bin/sh
cd "$(dirname "$0")/.." || exit 1
java -Dfile.encoding=UTF-8 -cp out dsm.DemoAutomatica
