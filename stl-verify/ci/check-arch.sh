#!/usr/bin/env bash
# check-arch.sh — ratchet for three architecture invariants (C10/C5/C3 backstops).
#
# Only NEW/changed lines (added `+` lines in the diff vs the base rev, plus
# uncommitted worktree changes and untracked Go files) can fail. Old code
# passes untouched by construction.
#
# Rules (all on added lines only):
#   1. No new `env.Get("CHAIN_ID", "1")` mainnet default (F10.3: a missing var
#      must not silently index mainnet; CHAIN_ID is required).
#   2. No new UPDATE/DELETE/DO UPDATE in non-test Go files under
#      internal/adapters/outbound/postgres/ (append-only default, C5).
#      Test files are excluded: integration tests legitimately DELETE for
#      fixture cleanup.
#   3. No new `func Get...ABI() (*abi.ABI, error)` getters (ABIs are parsed
#      once at init and returned without error, C3).
#
# Usage: ci/check-arch.sh [--base REV]   (default: fork/main)
# Exit 0 when no new violations; exit 1 otherwise.
set -euo pipefail

BASE="fork/main"

cd "$(dirname "$0")/.." # stl-verify/

while [ $# -gt 0 ]; do
  case "$1" in
    --base) BASE="$2"; shift 2 ;;
    -h|--help) echo "Usage: $0 [--base REV]"; exit 0 ;;
    *) echo "error: unknown argument: $1" >&2; exit 2 ;;
  esac
done

if ! git rev-parse --verify --quiet "$BASE" >/dev/null; then
  echo "error: base rev '$BASE' not found (fetch it or pass --base)" >&2
  exit 2
fi

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

# git prints paths relative to the repo root; rules match stl-verify-relative
# paths, so strip the prefix (empty when stl-verify/ is the repo root).
PREFIX="$(git rev-parse --show-prefix)"

# unified=0 diffs. Emits path:newline<TAB>added-line per added line.
: > "$tmpdir/added.tsv"
scan_diff() {
  awk -v prefix="$PREFIX" '
    /^\+\+\+ b\// { file=substr($0, 7); sub("^" prefix, "", file); next }
    /^@@ / {
      start=$3; sub(/^\+/, "", start); sub(/,.*/, "", start); lineno=start-1; next
    }
    /^\+/ && file ~ /\.go$/ {
      lineno++; line=substr($0, 2)
      printf "%s:%d\t%s\n", file, lineno, line
    }
  ' >> "$tmpdir/added.tsv"
}

git diff --unified=0 "$BASE...HEAD" -- '*.go' | scan_diff
git diff --unified=0 -- '*.go' | scan_diff
git diff --unified=0 --cached -- '*.go' | scan_diff
while IFS= read -r f; do
  [ -z "$f" ] && continue
  f="${f#"$PREFIX"}"
  awk -v file="$f" '{ printf "%s:%d\t%s\n", file, NR, $0 }' "$f" >> "$tmpdir/added.tsv"
done < <(git status --porcelain --untracked-files=all | awk '$1 == "??" && $2 ~ /\.go$/ {print $2}')

fail=0
report() { # report <rule> <location> <line>
  echo "$2: $1 violation: $3"
  fail=1
}

while IFS=$'\t' read -r loc line; do
  file="${loc%:*}"
  if printf '%s' "$line" | grep -q -E 'env\.Get\("CHAIN_ID", *"1"\)'; then
    report "CHAIN_ID-default" "$loc" "$line"
  fi
  case "$file" in
    internal/adapters/outbound/postgres/*)
      case "$file" in *_test.go) ;; *)
        if printf '%s' "$line" | grep -q -E '\b(UPDATE|DELETE)\b'; then
          report "postgres-mutation" "$loc" "$line"
        fi ;;
      esac ;;
  esac
  if printf '%s' "$line" | grep -q -E 'func (\(.*\) )?Get[A-Za-z0-9_]*ABI\(\) \(\*abi\.ABI, error\)'; then
    report "abi-getter-error" "$loc" "$line"
  fi
done < "$tmpdir/added.tsv"

n="$(wc -l < "$tmpdir/added.tsv" | tr -d ' ')"
echo "arch check (base $BASE): scanned $n added line(s)."
if [ "$fail" -ne 0 ]; then
  echo "FAIL: new architecture violations found (old code is untouched and passes)." >&2
  exit 1
fi
echo "OK: no new architecture violations."
