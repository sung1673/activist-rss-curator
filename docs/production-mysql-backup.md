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
- If any non-InnoDB table exists, a separate connection first requests
  `FLUSH TABLES WITH READ LOCK`. The lock brackets the consistent snapshot,
  definition capture, and streaming of every non-InnoDB table, and is
  released before the InnoDB portion continues.
- The only permitted fallback is MySQL numeric error `1227` from that exact
  global-lock request. In that case the same separate connection issues one
  deterministic `LOCK TABLES ... READ` statement for the complete preflight
  base-table inventory. Database and table identifiers are independently
  quoted and table names are sorted. Any other global-lock error, or any
  explicit-lock error, aborts without a completed backup.
- The complete table/engine inventory is compared after lock acquisition and
  again after all non-InnoDB tables are streamed, while the lock is still
  held. The final comparison uses a third, short-lived autocommit connection
  so the dump connection's consistent snapshot cannot hide current data
  dictionary changes. That verifier must close successfully before unlock.
  New, removed, renamed, or engine-changed tables abort the backup and remove
  the partial output. `UNLOCK TABLES` is attempted on every success and
  failure path, and closing the dedicated lock connection provides the final
  server-side release guarantee.
- Explicit table locks cannot name an object that does not exist in the
  preflight inventory. Scheduled writers, manual schema administration, and
  every other DDL path must therefore remain quiescent for the complete backup
  window. A transient object created and removed between inventory checks
  cannot be detected; without the no-DDL operational gate, the fallback must
  not be used and migration must not start.

These rules follow the MySQL 8.0 lock semantics:
[`FLUSH TABLES WITH READ LOCK`](https://dev.mysql.com/doc/refman/8.0/en/flush.html)
requires `FLUSH_TABLES` or `RELOAD`; the narrower
[`LOCK TABLES ... READ`](https://dev.mysql.com/doc/refman/8.0/en/lock-tables.html)
requires the per-table `LOCK TABLES` and `SELECT` privileges, permits
concurrent reads, and blocks writes to the locked tables. MySQL releases
session table locks when `START TRANSACTION` begins, so the explicit locks
must remain on the dedicated autocommit connection while the dump connection
starts its consistent snapshot. Moving them to the dump connection would
silently remove the protection and is prohibited.

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
- the exact read-lock strategy (`none`, `global_read_lock`, or
  `explicit_table_read_locks`), whether the global strategy succeeded, the
  total locked base-table count and engines, and the separate
  nontransactional table count and engines.

The SQL itself ends with `-- BSIDE_BACKUP_COMPLETE`. Release operations must
verify the manifest status, both whole-file hashes, the completion marker,
and expected table count before applying migration 011.

## Operational order

1. Confirm no GitHub writer workflow is running, pause the legacy writer, and
   establish a no-manual-DDL window through completion verification.
2. Create a new private output directory outside the repository.
3. Load the already-preserved local DB and SSH environment files without
   printing their values.
4. Run the backup with `--ssh-tunnel`, `--output`, and the exact release SHA.
5. Independently recompute compressed and uncompressed hashes and compare the
   manifest.
6. Record only the non-secret manifest and verification result in the private
   deployment evidence directory.
7. If any check fails, restore the writer setting and do not run migration.
