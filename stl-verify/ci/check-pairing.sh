#!/usr/bin/env bash
# check-pairing.sh — enforce the foo_test.go <-> foo.go pairing rule (F11.11).
#
# Every *_test.go must have a sibling source file of the same basename,
# except the sanctioned exceptions:
#   - testhelpers_test.go (shared fixtures)
#   - *_integration_test.go (build-tagged cross-cutting scenarios)
#   - *_guardrail_test.go (convention tests, no source file to pair with)
#   - *_benchmark_test.go (benchmarks)
#
# Ratchet: files already unpaired on the baseline date are listed in
# ci/pairing-baseline.txt. A violation listed there passes (with a reminder
# to shrink the file); a violation NOT listed there fails. The baseline file
# only ever shrinks: fix the pairing (move/rename/split), then delete the
# line. Stale entries (fixed or deleted files) are warnings, not failures.
#
# Usage:
#   ci/check-pairing.sh [--base REV] [--baseline FILE] [--mode changed|all]
# Defaults: --base fork/main, --baseline ci/pairing-baseline.txt, --mode changed.
# Exit 0 when no NEW violations; exit 1 otherwise.
set -euo pipefail

BASE="fork/main"
MODE="changed"
BASELINE=""

cd "$(dirname "$0")/.." # stl-verify/

while [ $# -gt 0 ]; do
  case "$1" in
    --base) BASE="$2"; shift 2 ;;
    --baseline) BASELINE="$2"; shift 2 ;;
    --mode) MODE="$2"; shift 2 ;;
    -h|--help)
      sed -n '1,/^set /p' "$0" | sed -n '2,/^$/p'
      echo "Usage: $0 [--base REV] [--baseline FILE] [--mode changed|all]"
      exit 0 ;;
    *) echo "error: unknown argument: $1" >&2; exit 2 ;;
  esac
done

[ -z "$BASELINE" ] && BASELINE="ci/pairing-baseline.txt"
case "$MODE" in changed|all) ;; *) echo "error: --mode must be changed|all" >&2; exit 2 ;; esac

is_exception() {
  local base="$1"
  [ "$base" = "testhelpers_test.go" ] && return 0
  case "$base" in
    *_integration_test.go|*_guardrail_test.go|*_benchmark_test.go) return 0 ;;
  esac
  return 1
}

list_candidates() {
  # git prints paths relative to the repo root; the baseline and sibling
  # checks are relative to stl-verify/, so strip the prefix.
  local prefix
  prefix="$(git rev-parse --show-prefix)"
  if [ "$MODE" = "all" ]; then
    { git ls-files '*.go'
      git status --porcelain --untracked-files=all | awk '$1 == "??" && $2 ~ /\.go$/ {print $2}'; } \
      | sed "s|^$prefix||"
  else
    if ! git rev-parse --verify --quiet "$BASE" >/dev/null; then
      echo "error: base rev '$BASE' not found (fetch it or pass --base)" >&2
      exit 2
    fi
    { git diff --name-only --diff-filter=ACMR "$BASE...HEAD" -- '*.go'
      git diff --name-only --diff-filter=ACMR -- '*.go'
      git diff --name-only --diff-filter=ACMR --cached -- '*.go'
      git status --porcelain --untracked-files=all | awk '$1 == "??" && $2 ~ /\.go$/ {print $2}'; } \
      | sed "s|^$prefix||"
  fi
}

if [ ! -f "$BASELINE" ]; then
  echo "error: baseline file '$BASELINE' not found." >&2
  echo "Generate it (whole tree) with: $0 --mode all > /tmp/now.txt (then curate)," >&2
  echo "or list current unpaired files and save them to '$BASELINE'." >&2
  exit 2
fi

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

list_candidates | sort -u > "$tmpdir/candidates.txt"

: > "$tmpdir/violations.txt"
while IFS= read -r f; do
  [ -z "$f" ] && continue
  case "$f" in *_test.go) ;; *) continue ;; esac
  base="$(basename "$f")"
  if is_exception "$base"; then continue; fi
  sib="${f%_test.go}.go"
  if [ ! -f "$sib" ]; then
    echo "$f" >> "$tmpdir/violations.txt"
  fi
done < "$tmpdir/candidates.txt"

grep -v -e '^[[:space:]]*#' -e '^[[:space:]]*$' "$BASELINE" | sort -u > "$tmpdir/baseline.txt" || true

new_viol=0
if [ -s "$tmpdir/violations.txt" ]; then
  while IFS= read -r v; do
    if grep -qxF "$v" "$tmpdir/baseline.txt" 2>/dev/null; then
      echo "grandfathered (shrink $BASELINE when fixed): $v"
    else
      echo "NEW unpaired test file (no sibling ${v%_test.go}.go): $v"
      new_viol=1
    fi
  done < "$tmpdir/violations.txt"
fi

stale=0
# Stale entries are only knowable with whole-tree knowledge: in changed mode
# an untouched baseline entry is simply out of scope, not stale.
if [ "$MODE" = "all" ] && [ -s "$tmpdir/baseline.txt" ]; then
  while IFS= read -r b; do
    [ -z "$b" ] && continue
    if ! grep -qxF "$b" "$tmpdir/violations.txt" 2>/dev/null; then
      echo "stale baseline entry (please remove from $BASELINE): $b"
      stale=1
    fi
  done < "$tmpdir/baseline.txt"
fi

n_viol="$(wc -l < "$tmpdir/violations.txt" | tr -d ' ')"
echo "pairing check ($MODE): $n_viol unpaired file(s) outside the pattern allowlist."
if [ "$new_viol" -ne 0 ]; then
  echo "FAIL: new pairing violations found. Pair the test with its source file" >&2
  echo "or, for guardrail/convention tests, use the *_guardrail_test.go suffix." >&2
  exit 1
fi
[ "$stale" -ne 0 ] && echo "(warnings only: stale baseline entries above)"
echo "OK: no new pairing violations."
