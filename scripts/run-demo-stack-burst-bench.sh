#!/usr/bin/env bash
set -euo pipefail

host=""
remote_repo="/home/rui/fantasma"
remote_env_file=""
remote_helper_path="/tmp/fantasma-demo-stack-burst-bench.py"

usage() {
  cat <<'EOF'
Usage: scripts/run-demo-stack-burst-bench.sh --host <user@host> [--remote-repo <path>] [--remote-env-file <path>]

Runs the realistic 300-event burst benchmark against the existing Fantasma demo stack on a remote host.
The wrapper reads the remote `.env.demo`, derives the loopback demo ports, copies the checked-in Python helper
to the remote host, executes it there, prints the JSON result, and removes the remote temp helper.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host)
      host="${2:-}"
      shift 2
      ;;
    --remote-repo)
      remote_repo="${2:-}"
      shift 2
      ;;
    --remote-env-file)
      remote_env_file="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$host" ]]; then
  echo "--host is required" >&2
  usage >&2
  exit 1
fi

if [[ -z "$remote_env_file" ]]; then
  remote_env_file="${remote_repo}/.env.demo"
fi

scp "scripts/demo-stack-burst-bench.py" "${host}:${remote_helper_path}" >/dev/null

cleanup_remote() {
  ssh "$host" "rm -f '$remote_helper_path'" >/dev/null 2>&1 || true
}
trap cleanup_remote EXIT

ssh "$host" \
  "set -a && . '$remote_env_file' && set +a && \
   api_port=\${FANTASMA_API_PORTS#127.0.0.1:} && api_port=\${api_port%%:*} && \
   ingest_port=\${FANTASMA_INGEST_PORTS#127.0.0.1:} && ingest_port=\${ingest_port%%:*} && \
   python3 '$remote_helper_path' \
     --api-base-url 'http://127.0.0.1:'\"\$api_port\" \
     --ingest-base-url 'http://127.0.0.1:'\"\$ingest_port\" \
     --admin-token \"\$FANTASMA_ADMIN_TOKEN\""
