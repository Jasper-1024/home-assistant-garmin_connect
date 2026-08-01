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
source_dir="$repo_root/custom_components/garmin_connect/"
container_name="homeassistant"
container_dir="/config/custom_components/garmin_connect/"

ssh -o BatchMode=yes "$remote_host" \
  "docker exec '$container_name' mkdir -p '$container_dir'"
tar --exclude='__pycache__' --exclude='*.pyc' -C "$source_dir" -cf - . |
  ssh -o BatchMode=yes "$remote_host" \
    "docker exec -i '$container_name' tar --overwrite --no-same-owner -C '$container_dir' -xf -"

ssh -o BatchMode=yes "$remote_host" \
  "docker exec '$container_name' python3 -m json.tool '$container_dir/manifest.json' | grep '\"version\"'"

if [ "$restart" = true ]; then
  ssh -o BatchMode=yes "$remote_host" "docker restart '$container_name'"
fi
