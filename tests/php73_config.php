<?php
declare(strict_types=1);

// Public, non-secret configuration for the isolated GitHub Actions MySQL service.
return array(
    'db_host' => '127.0.0.1',
    'db_port' => 3306,
    'db_name' => 'activist_ci',
    'db_user' => 'activist_ci',
    'db_password' => 'activist_ci_password',
    'db_charset' => 'utf8mb4',
    'table_prefix' => 'ci_',
    'api_secret' => 'php73-ci-only-hmac-key-00000000000000000000000000000000',
    'allowed_origin' => 'http://127.0.0.1:8787',
    'public_api_cors_origins' => array('http://127.0.0.1:8787'),
    'max_body_bytes' => 2097152,
    'telegram_signal_rebuild_lease_seconds' => 1,
);
