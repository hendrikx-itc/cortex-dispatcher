CREATE SCHEMA dispatcher;

CREATE FUNCTION dispatcher.version()
    RETURNS text
AS $$
    SELECT '0.1.2';
$$ LANGUAGE sql IMMUTABLE;

CREATE TABLE dispatcher.sftp_download (
    id serial,
    created timestamptz not null default now(),
    remote text not null,
    path text not null,
    hash text not null,
    UNIQUE (remote, path)
);
