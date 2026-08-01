#!/usr/bin/env bash
# Copy this custom integration to the k2s Home Assistant host.
# Usage: scripts/deploy_k2s.sh [--restart]

set -euo pipefail

case "${1:-}" in
"")
  restart=false
  ;;
--restart)
  restart=true
  ;;
*)
  echo "Usage: $0 [--restart]" >&2
  exit 2
  ;;
esac

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$script_dir/.." && pwd)"
remote_host="k2s"
remote_config="/opt/hassio/homeassistant"
source_dir="$repo_root/custom_components/garmin_connect/"
remote_dir="$remote_config/custom_components/garmin_connect/"

ssh -o BatchMode=yes "$remote_host" "mkdir -p '$remote_dir'"
if ssh -o BatchMode=yes "$remote_host" "which rsync >/dev/null 2>&1"; then
  rsync -az --checksum --exclude='__pycache__/' --exclude='*.pyc' \
    "$source_dir" "$remote_host:$remote_dir"
else
  tar --exclude='__pycache__' --exclude='*.pyc' -C "$source_dir" -cf - . |
    ssh -o BatchMode=yes "$remote_host" "tar -C '$remote_dir' -xf -"
fi

ssh -o BatchMode=yes "$remote_host" \
  "python3 -m json.tool '$remote_dir/manifest.json' | grep '\"version\"'"

if [ "$restart" = true ]; then
  ssh -o BatchMode=yes "$remote_host" "docker restart homeassistant"
fi
