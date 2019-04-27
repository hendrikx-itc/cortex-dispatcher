create table sftp_scan (
    id serial,
    created timestamptz not null default now(),
    remote text not null,
    path text not null,
    unique (remote, path)
);
