create table sftp_download (
    id serial,
    created timestamptz not null default now(),
    remote text not null,
    path text not null,
    hash text not null,
    unique (remote, path)
);
