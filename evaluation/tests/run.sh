#!/usr/bin/env sh
set -eu

repository_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repository_root"
python3 -m unittest discover -s evaluation/tests -p 'test_*.py' -v
