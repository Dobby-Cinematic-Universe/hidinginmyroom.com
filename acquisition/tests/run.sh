#!/bin/sh
set -eu

repository_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repository_root"
exec python3 -m unittest discover -s acquisition/tests -p 'test_*.py' -v
