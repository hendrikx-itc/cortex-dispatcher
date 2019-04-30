CREATE SCHEMA sftp_scanner;

CREATE FUNCTION sftp_scanner.version()
    RETURNS text
AS $$
    SELECT '0.1.2';
$$ LANGUAGE sql IMMUTABLE;

CREATE TABLE sftp_scanner.scan (
    id serial,
    created timestamptz not null default now(),
    remote text not null,
    path text not null,
    size bigint not null,
    UNIQUE (remote, path)
);
