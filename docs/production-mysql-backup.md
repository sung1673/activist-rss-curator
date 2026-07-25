# Production MySQL backup gate

The Production Alpha migration must not start until a new, completed backup
has been produced from the exact release SHA. `scripts/backup_mysql.py`
creates a streaming SQL gzip and a completion manifest without placing
credentials, endpoints, or source data in logs.

## Safety contract

- Database credentials are read from `DB_*` or `MYSQL_*` environment
  variables. Do not pass a password on a command line.
- When the database is reachable only through Gabia's private network, enable
  the SSH tunnel with environment variables and pin the server's SHA-256 host
  key. A changed or missing host key aborts before database authentication.
- InnoDB tables are read from one `REPEATABLE READ` consistent snapshot.
- If any non-InnoDB table exists, a separate connection holds a global read
  lock while the snapshot begins and those tables are streamed. The lock is
  released before the InnoDB portion continues.
- Generated columns are present in `CREATE TABLE` but omitted from `INSERT`
  column lists so MySQL recomputes them during restore.
- The dump and restore sessions are pinned to UTC while timestamp data is
  serialized, then the restore session's prior time zone is restored.
- Views, triggers, stored routines, or scheduled events cause a fail-closed
  result. The tool never labels a table-only dump as a full database backup.
- Existing output files are never overwritten. A failed or interrupted run
  has no completed manifest and must not be used for migration.

## Gabia legacy `ssh-rsa/SHA-1` exception

Paramiko 5 rejects the legacy `ssh-rsa/SHA-1` host-signature algorithm by
default. The backup and migration tools keep that default for every SSH host
unless all of these settings are present:

- explicit opt-in:
  `SSH_ALLOW_LEGACY_RSA_SHA1=true` or
  `--ssh-allow-legacy-rsa-sha1`;
- an exact target:
  `SSH_LEGACY_RSA_SHA1_HOST=alignpartnerscap.com` or
  `--ssh-legacy-rsa-sha1-host alignpartnerscap.com`;
- an exact SHA-256 host-key pin in `SSH_HOST_KEY_SHA256` or
  `--ssh-host-key-sha256`.

The target must match `SSH_HOST` after only case, trailing-dot, and IDNA
normalization. It is not a wildcard or a DNS suffix. Missing opt-in, a missing
target, a different target, an invalid pin, or a changed server key aborts
before SSH password authentication. The exception changes only the host-key
signature algorithm on that one Paramiko transport; it does not enable
legacy public-key user authentication, ciphers, MACs, or key exchanges.

The independently recorded Gabia identity is:

```text
SSH_HOST=alignpartnerscap.com
SSH_LEGACY_RSA_SHA1_HOST=alignpartnerscap.com
SSH_HOST_KEY_SHA256=SHA256:4Y2J13Nis0NOKupLJCOnr2w5X2UdBZH78TkZMVJCVLo
SSH_ALLOW_LEGACY_RSA_SHA1=true
```

Reconfirm the fingerprint through the previously approved out-of-band record
before each production operation. The fingerprint is not secret, but the SSH
and database passwords are: load passwords from the protected environment
file and never pass them as CLI arguments. CLI failure output intentionally
does not print endpoints, pins, usernames, passwords, or underlying exception
messages.

## Completion manifest

The manifest records:

- exact 40-character release SHA;
- start and completion timestamps;
- compressed file SHA-256 and byte count;
- uncompressed SQL SHA-256 and byte count;
- table count and exact streamed row count;
- per-table engine, row count, generated/inserted column counts, SQL byte
  count, and SQL SHA-256;
- whether a global read lock was required and which engines it protected.

The SQL itself ends with `-- BSIDE_BACKUP_COMPLETE`. Release operations must
verify the manifest status, both whole-file hashes, the completion marker,
and expected table count before applying migration 011.

## Operational order

1. Confirm no GitHub writer workflow is running and pause the legacy writer.
2. Create a new private output directory outside the repository.
3. Load the already-preserved local DB and SSH environment files without
   printing their values.
4. Run the backup with `--ssh-tunnel`, `--output`, and the exact release SHA.
5. Independently recompute compressed and uncompressed hashes and compare the
   manifest.
6. Record only the non-secret manifest and verification result in the private
   deployment evidence directory.
7. If any check fails, restore the writer setting and do not run migration.
