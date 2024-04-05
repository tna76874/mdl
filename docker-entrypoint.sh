#!/bin/bash
set -e

extension=${1##*.}
if [[ "$1" == "bash"  ]]; then
    /bin/bash
else
    # /usr/local/bin/mdl --config /config --download /download 
    "$@"
fi
