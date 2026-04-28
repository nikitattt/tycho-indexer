#!/usr/bin/env bash
set -Eeuo pipefail

log() {
  printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

run_pg_dump() {
  if [[ -n "${DATABASE_URL:-}" ]]; then
    pg_dump --dbname="$DATABASE_URL" "$@"
  else
    pg_dump "$@"
  fi
}

run_pg_dumpall() {
  if [[ -n "${DATABASE_URL:-}" ]]; then
    pg_dumpall --dbname="$DATABASE_URL" "$@"
  else
    pg_dumpall "$@"
  fi
}

bool_enabled() {
  case "${1:-}" in
    1 | true | TRUE | yes | YES | y | Y) return 0 ;;
    *) return 1 ;;
  esac
}

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
env_file="${1:-$script_dir/.env}"

if [[ ! -f "$env_file" ]]; then
  die "env file not found: $env_file (copy .env.example to .env first)"
fi

set -a
# shellcheck source=/dev/null
source "$env_file"
set +a

require_cmd pg_dump
require_cmd pg_dumpall
require_cmd rclone

STORAGE_BUCKET="${STORAGE_BUCKET:-${BUCKET:-}}"
STORAGE_ACCESS_KEY_ID="${STORAGE_ACCESS_KEY_ID:-${ACCESS_KEY_ID:-}}"
STORAGE_SECRET_ACCESS_KEY="${STORAGE_SECRET_ACCESS_KEY:-${SECRET_ACCESS_KEY:-}}"
STORAGE_REGION="${STORAGE_REGION:-${REGION:-auto}}"
STORAGE_ENDPOINT="${STORAGE_ENDPOINT:-${ENDPOINT:-}}"
STORAGE_FORCE_PATH_STYLE="${STORAGE_FORCE_PATH_STYLE:-false}"

: "${STORAGE_BUCKET:?set STORAGE_BUCKET or BUCKET in $env_file}"
: "${STORAGE_ACCESS_KEY_ID:?set STORAGE_ACCESS_KEY_ID or ACCESS_KEY_ID in $env_file}"
: "${STORAGE_SECRET_ACCESS_KEY:?set STORAGE_SECRET_ACCESS_KEY or SECRET_ACCESS_KEY in $env_file}"
: "${STORAGE_ENDPOINT:?set STORAGE_ENDPOINT or ENDPOINT in $env_file}"

BACKUP_PREFIX="${BACKUP_PREFIX:-postgres}"
BACKUP_NAME="${BACKUP_NAME:-tycho-indexer}"
PG_DUMP_COMPRESSION="${PG_DUMP_COMPRESSION:-zstd:9}"
INCLUDE_GLOBALS="${INCLUDE_GLOBALS:-true}"
GLOBALS_COMPRESSION="${GLOBALS_COMPRESSION:-zstd}"
GLOBALS_ZSTD_LEVEL="${GLOBALS_ZSTD_LEVEL:-9}"
GLOBALS_GZIP_LEVEL="${GLOBALS_GZIP_LEVEL:-9}"
VERIFY_UPLOAD="${VERIFY_UPLOAD:-true}"
RCLONE_REMOTE="${RCLONE_REMOTE:-railway}"
S3_CHUNK_SIZE="${S3_CHUNK_SIZE:-64M}"

if [[ -z "${DATABASE_URL:-}" && -z "${PGDATABASE:-}" ]]; then
  die "set DATABASE_URL or PGDATABASE/PGHOST/PGUSER/PGPASSWORD in $env_file"
fi

remote_env="$(printf '%s' "$RCLONE_REMOTE" | tr '[:lower:]-' '[:upper:]_')"
export "RCLONE_CONFIG_${remote_env}_TYPE=s3"
export "RCLONE_CONFIG_${remote_env}_PROVIDER=Other"
export "RCLONE_CONFIG_${remote_env}_ACCESS_KEY_ID=$STORAGE_ACCESS_KEY_ID"
export "RCLONE_CONFIG_${remote_env}_SECRET_ACCESS_KEY=$STORAGE_SECRET_ACCESS_KEY"
export "RCLONE_CONFIG_${remote_env}_REGION=$STORAGE_REGION"
export "RCLONE_CONFIG_${remote_env}_ENDPOINT=$STORAGE_ENDPOINT"
export "RCLONE_CONFIG_${remote_env}_FORCE_PATH_STYLE=$STORAGE_FORCE_PATH_STYLE"
export RCLONE_S3_CHUNK_SIZE="$S3_CHUNK_SIZE"

prefix="${BACKUP_PREFIX#/}"
prefix="${prefix%/}"
remote_dir="${RCLONE_REMOTE}:${STORAGE_BUCKET}"
if [[ -n "$prefix" ]]; then
  remote_dir="${remote_dir}/${prefix}"
fi

timestamp="${BACKUP_TIMESTAMP:-$(date -u +%Y%m%dT%H%M%SZ)}"
dump_file="${BACKUP_NAME}-${timestamp}.dump"
dump_remote_path="${remote_dir}/${dump_file}"

log "starting database dump upload to $dump_remote_path"
run_pg_dump -Fc -Z "$PG_DUMP_COMPRESSION" \
  | rclone rcat "$dump_remote_path"
log "database dump uploaded"

if bool_enabled "$VERIFY_UPLOAD"; then
  log "verifying uploaded dump exists"
  rclone lsl "$dump_remote_path" >/dev/null
fi

if bool_enabled "$INCLUDE_GLOBALS"; then
  case "$GLOBALS_COMPRESSION" in
    zstd)
      require_cmd zstd
      globals_file="${BACKUP_NAME}-globals-${timestamp}.sql.zst"
      globals_remote_path="${remote_dir}/${globals_file}"
      log "starting globals upload to $globals_remote_path"
      run_pg_dumpall --globals-only \
        | zstd "-${GLOBALS_ZSTD_LEVEL}" -T0 \
        | rclone rcat "$globals_remote_path"
      ;;
    gzip)
      require_cmd gzip
      globals_file="${BACKUP_NAME}-globals-${timestamp}.sql.gz"
      globals_remote_path="${remote_dir}/${globals_file}"
      log "starting globals upload to $globals_remote_path"
      run_pg_dumpall --globals-only \
        | gzip "-${GLOBALS_GZIP_LEVEL}" \
        | rclone rcat "$globals_remote_path"
      ;;
    none)
      globals_file="${BACKUP_NAME}-globals-${timestamp}.sql"
      globals_remote_path="${remote_dir}/${globals_file}"
      log "starting globals upload to $globals_remote_path"
      run_pg_dumpall --globals-only \
        | rclone rcat "$globals_remote_path"
      ;;
    *)
      die "GLOBALS_COMPRESSION must be one of: zstd, gzip, none"
      ;;
  esac

  log "globals uploaded"
  if bool_enabled "$VERIFY_UPLOAD"; then
    log "verifying uploaded globals exists"
    rclone lsl "$globals_remote_path" >/dev/null
  fi
fi

log "backup complete"
