#!/usr/bin/env bash
set -euo pipefail

BASE="release-1.16.3"
DIR="flink-state-backends/flink-statebackend-rocksdb"

# Sanity checks
git rev-parse --is-inside-work-tree >/dev/null 2>&1 || { echo "Not in a git repo"; exit 2; }
git rev-parse --verify --quiet "$BASE^{commit}" >/dev/null || {
  echo "Base '$BASE' not found. (Try: git fetch --tags)"; exit 3;
}

# Scope diff to the module and exclude build outputs
PATHSPEC=( "--" "$DIR/" \
  ':(exclude)**/target/**' ':(exclude)**/build/**' ':(exclude)**/build-target/**' \
  ':(exclude)**/.idea/**' ':(exclude)**/*.jar' ':(exclude)**/generated/**' )

# Collect: added \t removed \t path
mapfile -t NUMSTAT < <(git diff --numstat --find-renames=50% "$BASE"..HEAD "${PATHSPEC[@]}")

if (( ${#NUMSTAT[@]} == 0 )); then
  echo "No changes in '$DIR' vs '$BASE'."
  exit 0
fi

echo
echo "Per-file changes (touched = added + removed) in '$DIR' vs '$BASE':"
{
  printf "ADDED\tREMOVED\tTOUCHED\tPATH\n"
  printf "%s\n" "${NUMSTAT[@]}" \
  | awk '{
      add=$1; del=$2;
      $1=""; $2="";
      sub(/^[ \t]+/,"");
      touched=add+del;
      printf "%d\t%d\t%d\t%s\n", add, del, touched, $0
    }' \
  | sort -nr -k3,3 -k1,1
} | column -t -s $'\t'

echo
printf "Aggregate (touched lines): "
printf "%s\n" "${NUMSTAT[@]}" \
| awk '{A+=$1; D+=$2} END{printf("added %d + removed %d = total %d\n", A, D, A+D)}'