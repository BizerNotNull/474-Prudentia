#!/bin/sh
set -eu

: "${OUTPUT_DIR:?OUTPUT_DIR is required}"
: "${BACKUP_TIMEOUT_SECONDS:=1800}"
: "${RETENTION_COUNT:=14}"

case "$BACKUP_TIMEOUT_SECONDS:$RETENTION_COUNT" in
  *[!0-9:]*|0:*|*:0) echo "timeouts and retention must be positive integers" >&2; exit 64 ;;
esac

umask 077
mkdir -p "$OUTPUT_DIR"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
temporary="$OUTPUT_DIR/prudentia-$stamp.dump.partial"
final="$OUTPUT_DIR/prudentia-$stamp.dump"
trap 'rm -f "$temporary"' EXIT HUP INT TERM

timeout -s TERM -k 30 "$BACKUP_TIMEOUT_SECONDS" pg_dump \
  --format=custom --compress=9 --no-owner --no-privileges --file="$temporary"
pg_restore --list "$temporary" >/dev/null
mv "$temporary" "$final"
sha256sum "$final" >"$final.sha256"

# Retention is count-bounded and applies only to this script's exact filename pattern.
find "$OUTPUT_DIR" -maxdepth 1 -type f -name 'prudentia-*.dump' -print | sort -r | {
  count=0
  while IFS= read -r backup; do
    count=$((count + 1))
    if [ "$count" -gt "$RETENTION_COUNT" ]; then
      rm -f "$backup" "$backup.sha256"
    fi
  done
}
trap - EXIT HUP INT TERM
printf '%s\n' "$final"
