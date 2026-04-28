# Tycho Indexer DB Backup

Linux-server script for streaming PostgreSQL backups to a Railway
S3-compatible bucket.

## Install On Linux

Debian/Ubuntu:

```bash
sudo apt-get update
sudo apt-get install -y postgresql-client rclone zstd ca-certificates
```

If your distro package for `rclone` is too old, install the official release
instead:

```bash
curl https://rclone.org/install.sh | sudo bash
```

RHEL/Fedora:

```bash
sudo dnf install -y postgresql rclone zstd ca-certificates
```

## Copy To The Server

From your local machine:

```bash
scp -r /Users/andrewflinch/Projects/tycho-indexer/tycho-indexer-db-backup user@server:/opt/
```

On the Linux server:

```bash
cd /opt/tycho-indexer-db-backup
chmod +x backup.sh
```

Copy the environment template and fill in the real values:

```bash
cp .env.example .env
nano .env
```

Use the values from the Railway bucket Credentials tab for `BUCKET`,
`ACCESS_KEY_ID`, `SECRET_ACCESS_KEY`, `REGION`, and `ENDPOINT`.

For the database connection:

```bash
DATABASE_URL="postgresql://postgres:mypassword@localhost:5432/tycho_indexer_0"
```

If PostgreSQL is running in Docker and this script runs on the host, use the
published host port. In this repo's `docker-compose.yaml`, that is `5431`:

```bash
DATABASE_URL="postgresql://postgres:mypassword@localhost:5431/tycho_indexer_0"
```

If this script runs from another Docker container on the same Compose network,
use the Compose service name and internal port:

```bash
DATABASE_URL="postgresql://postgres:mypassword@db:5432/tycho_indexer_0"
```

## Run A Backup

```bash
./backup.sh
```

The script streams the dump directly to Railway:

```text
pg_dump -> rclone rcat -> Railway bucket
```

That means you do not need enough local disk to hold the full compressed dump.
If the upload fails midway, rerun the script; streamed uploads are not
resumable as a complete PostgreSQL dump.

The script sets `S3_CHUNK_SIZE="64M"` by default. S3 multipart uploads allow up
to 10,000 parts, so this supports streamed backup objects up to about 625 GiB.
If your dump may exceed that, increase `S3_CHUNK_SIZE` in `.env`.

## Restore

`backup.sh` configures an in-memory rclone remote from `.env`. For manual
restore commands, either configure a persistent rclone remote named `railway`
with the same bucket credentials, or export the same `RCLONE_CONFIG_*` values
shown in `backup.sh`.

Download the dump first, replacing `BUCKET` with the actual bucket name:

```bash
rclone copy railway:BUCKET/postgres/tycho-indexer-YYYYMMDDTHHMMSSZ.dump .
```

Restore globals if you backed them up:

```bash
zstd -dc tycho-indexer-globals-YYYYMMDDTHHMMSSZ.sql.zst | psql postgres
```

Restore the database:

```bash
pg_restore --clean --if-exists --create --dbname=postgres tycho-indexer-YYYYMMDDTHHMMSSZ.dump
```

For production, periodically test a restore into a throwaway database or
cluster. A backup is only proven once restore has succeeded.
