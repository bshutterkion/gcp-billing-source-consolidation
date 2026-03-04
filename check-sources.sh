#!/usr/bin/env bash
# Check usage_end_time availability across all source tables listed in a sources JSON file.
# Usage: ./check-sources.sh [sources-file] [--project PROJECT_ID]
#
# Defaults:
#   sources-file: my-sources.json
#   PROJECT_ID:   used for bq query execution (defaults to first source's project_id)

set -euo pipefail

SOURCES_FILE="${1:-my-sources.json}"
PROJECT_FLAG=""

# Parse optional --project flag
shift || true
while [[ $# -gt 0 ]]; do
  case "$1" in
    --project)
      PROJECT_FLAG="--project_id=$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done

if ! command -v jq &>/dev/null; then
  echo "Error: jq is required. Install with: brew install jq (mac) or apt install jq (linux)"
  exit 1
fi

if [ ! -f "$SOURCES_FILE" ]; then
  echo "Error: sources file not found: $SOURCES_FILE"
  exit 1
fi

COUNT=$(jq length "$SOURCES_FILE")
echo "Checking $COUNT sources from $SOURCES_FILE"
echo "=========================================="
printf "%-50s %-10s %-10s %-15s\n" "TABLE" "TOTAL" "NULL_END" "HAS_END_TIME"
echo "----------------------------------------------------------------------------------------------------------"

for i in $(seq 0 $((COUNT - 1))); do
  PROJECT=$(jq -r ".[$i].project_id" "$SOURCES_FILE")
  DATASET=$(jq -r ".[$i].dataset_id" "$SOURCES_FILE")
  TABLE=$(jq -r ".[$i].table_name" "$SOURCES_FILE")
  REF="${PROJECT}.${DATASET}.${TABLE}"

  RESULT=$(bq query --use_legacy_sql=false --format=json $PROJECT_FLAG \
    "SELECT COUNT(*) as total, COUNTIF(usage_end_time IS NULL) as null_end FROM \`${REF}\` LIMIT 1" 2>&1) || {
    printf "%-50s %-10s\n" "$REF" "SKIPPED (not found or no access)"
    continue
  }

  TOTAL=$(echo "$RESULT" | jq -r '.[0].total // "0"')
  NULL_END=$(echo "$RESULT" | jq -r '.[0].null_end // "0"')

  if [ "$TOTAL" = "0" ]; then
    HAS="EMPTY"
  elif [ "$NULL_END" = "$TOTAL" ]; then
    HAS="NO"
  elif [ "$NULL_END" = "0" ]; then
    HAS="YES (all)"
  else
    HAS="PARTIAL"
  fi

  printf "%-50s %-10s %-10s %-15s\n" "$REF" "$TOTAL" "$NULL_END" "$HAS"
done

echo ""
echo "Done."
