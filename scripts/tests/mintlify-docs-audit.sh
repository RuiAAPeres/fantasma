#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
docs_json="$repo_root/docs.json"
mintlify_dir="$repo_root/mintlify"

fail() {
  echo "mintlify-docs-audit: $*" >&2
  exit 1
}

while IFS= read -r page; do
  file_path="$repo_root/${page}.mdx"
  [[ -f "$file_path" ]] || fail "docs.json page is missing a file: ${page}.mdx"
done < <(jq -r '.navigation.tabs[].groups[].pages[]' "$docs_json")

while IFS= read -r link_target; do
  file_path="$repo_root/${link_target#/}.mdx"
  [[ -f "$file_path" ]] || fail "internal docs link points to a missing page: $link_target"
done < <(rg --no-filename -o '/mintlify/[a-z0-9-]+' "$mintlify_dir"/*.mdx | sort -u)

for landing_page in "$mintlify_dir/index.mdx" "$mintlify_dir/sdk-behavior.mdx"; do
  while IFS= read -r sdk_page; do
    link_target="/$sdk_page"
    grep -Fq "$link_target" "$landing_page" ||
      fail "$(basename "$landing_page") is missing SDK link: $link_target"
  done < <(
    jq -r '.navigation.tabs[].groups[] | select(.group == "SDKs") | .pages[]' "$docs_json" |
      grep -v '^mintlify/sdk-behavior$'
  )
done

echo "mintlify-docs-audit: ok"
