#!/usr/bin/env bash
set -euo pipefail

IMAGE_PATTERN='^(fantasma-(smoke|cli-smoke|bench|local|tests)(-| |$)|docker-fantasma-|docker-workspace-tests( |$)|fantasma-tests-review-workspace-tests( |$)|fantasma-workspace-tests( |$))'
VOLUME_PATTERN='^(fantasma-(smoke|cli-smoke|bench|local|tests)(-|_|$)|fantasma-tests-review_)'
CONTAINER_PATTERN='(fantasma-(smoke|cli-smoke|bench|local|tests)|workspace-tests|docker-fantasma)'

remove_matching_containers() {
  local containers=()
  while IFS= read -r container; do
    containers+=("$container")
  done < <(docker ps -a --format '{{.ID}} {{.Names}}' | rg "$CONTAINER_PATTERN" | awk '{print $1}')

  if (( ${#containers[@]} > 0 )); then
    docker rm -f "${containers[@]}" >/dev/null
  fi
}

remove_matching_images() {
  local images=()
  while IFS= read -r image; do
    images+=("$image")
  done < <(docker images --format '{{.Repository}} {{.ID}}' | rg "$IMAGE_PATTERN" | awk '{print $2}' | sort -u)

  if (( ${#images[@]} > 0 )); then
    docker image rm -f "${images[@]}" >/dev/null
  fi
}

remove_matching_volumes() {
  local volumes=()
  while IFS= read -r volume; do
    volumes+=("$volume")
  done < <(docker volume ls --format '{{.Name}}' | rg "$VOLUME_PATTERN")

  if (( ${#volumes[@]} > 0 )); then
    docker volume rm -f "${volumes[@]}" >/dev/null
  fi
}

main() {
  command -v docker >/dev/null 2>&1 || {
    echo "missing required command: docker" >&2
    exit 1
  }

  docker info >/dev/null 2>&1 || {
    echo "docker daemon is unavailable" >&2
    exit 1
  }

  remove_matching_containers
  remove_matching_images
  remove_matching_volumes

  echo "reclaimed Fantasma-owned Docker containers, images, and volumes"
}

main "$@"
