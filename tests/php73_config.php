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
    'role_token_hashes' => array(
        'admin' => array('c8e80d02ecd972e840698ade74adc37d485b9c2077fe5fc1d1fde57f97de0a74'),
        'editor' => array('957e0a84dd47002c3a093da30526279c213011fa06c606667b753ebe87f1c92b'),
        'ops' => array('27bc3fddd68fd0f3a042dae1dd472d0d3d5b615c8a86e93473375c4fe21eeae2'),
        'release_authorizer' => array('83a00f2797d3a214080e86809cb2eba45e0163581c1612ee7699055fa109ecb7'),
    ),
    'governance_preview_token_hash' => '39e7a8a5d3c11b6b631e0bb1bce952feca2e6960bae9aef6a409f4086c00fa93',
    'max_body_bytes' => 2097152,
    'telegram_signal_rebuild_lease_seconds' => 1,
);
