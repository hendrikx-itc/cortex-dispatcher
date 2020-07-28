

CREATE SCHEMA IF NOT EXISTS "dispatcher";


CREATE TYPE "dispatcher"."version_tuple" AS (
  "major" smallint,
  "minor" smallint,
  "patch" smallint
);



CREATE FUNCTION "dispatcher"."version"()
    RETURNS dispatcher.version_tuple
AS $$
SELECT (1,0,1)::dispatcher.version_tuple;
$$ LANGUAGE sql IMMUTABLE;


CREATE TABLE "dispatcher"."file"
(
  "id" bigint NOT NULL GENERATED BY DEFAULT AS IDENTITY,
  "timestamp" timestamptz NOT NULL DEFAULT now(),
  "source" text NOT NULL,
  "path" text NOT NULL,
  "modified" timestamptz NOT NULL,
  "size" bigint NOT NULL,
  "hash" text,
  PRIMARY KEY (id)
);

COMMENT ON TABLE "dispatcher"."file" IS 'All files in the internal storage area of Cortex are registered here.';

CREATE INDEX "sftp_download_file_index" ON "dispatcher"."file" USING btree (source, path);



CREATE TABLE "dispatcher"."sftp_download"
(
  "id" bigint NOT NULL GENERATED BY DEFAULT AS IDENTITY,
  "timestamp" timestamptz NOT NULL DEFAULT now(),
  "source" text NOT NULL,
  "path" text NOT NULL,
  "size" bigint,
  "file_id" bigint,
  PRIMARY KEY (id)
);

COMMENT ON TABLE "dispatcher"."sftp_download" IS 'Contains records of files that need to be downloaded from a remote SFTP location.';

CREATE INDEX "sftp_download_file_index" ON "dispatcher"."sftp_download" USING btree (source, path);



CREATE TABLE "dispatcher"."directory_source"
(
  "id" bigint NOT NULL GENERATED BY DEFAULT AS IDENTITY,
  "timestamp" timestamptz NOT NULL DEFAULT now(),
  "source" text NOT NULL,
  "path" text NOT NULL,
  "modified" timestamptz NOT NULL,
  "size" bigint NOT NULL,
  "file_id" bigint
);

COMMENT ON TABLE "dispatcher"."directory_source" IS 'Contains records of files delivered on a local filesystem that can be
monitored with mechanisms like inotify. All files recorded here must be
present on the filesystem, and when the file is copied/hardlinked to the
internal Cortex storage, a reference to the file table is added.';

CREATE INDEX "sftp_download_file_index" ON "dispatcher"."directory_source" USING btree (source, path);



CREATE TABLE "dispatcher"."dispatched"
(
  "file_id" bigint NOT NULL,
  "target" text NOT NULL,
  "timestamp" timestamptz NOT NULL DEFAULT now()
);



CREATE FUNCTION "dispatcher"."undispatched_files"("source" text, "target" text, timestamptz)
    RETURNS SETOF bigint
AS $$
SELECT file.id
FROM dispatcher.file LEFT JOIN dispatcher.dispatched
ON dispatched.file_id = file.id
  AND file.source = $1
  AND dispatched.target = $2
WHERE dispatched IS NULL
  AND file.timestamp < $3;
$$ LANGUAGE sql IMMUTABLE;

COMMENT ON FUNCTION "dispatcher"."undispatched_files"("source" text, "target" text, timestamptz) IS 'Provide the ids of files from source that have not been sent to
the target yet. Only files inserted before the given timestamp
are checked.';


CREATE FUNCTION "dispatcher"."undispatched_files"("source" text, "target" text, interval)
    RETURNS SETOF bigint
AS $$
SELECT dispatcher.undispatched_files($1, $2, now()-$3);
$$ LANGUAGE sql IMMUTABLE;

COMMENT ON FUNCTION "dispatcher"."undispatched_files"("source" text, "target" text, interval) IS 'Provide the ids of files from source that have not been sent to
the target yet. Only files that have been in the database for
at least the given interval are checked.';


CREATE FUNCTION "dispatcher"."undispatched_files"("source" text, "target" text)
    RETURNS SETOF bigint
AS $$
SELECT dispatcher.undispatched_files($1, $2, now());
$$ LANGUAGE sql IMMUTABLE;

COMMENT ON FUNCTION "dispatcher"."undispatched_files"("source" text, "target" text) IS 'Provide the ids of files from source that have not been sent to
the target yet.';


ALTER TABLE "dispatcher"."sftp_download"
  ADD CONSTRAINT "sftp_download_file_id_fkey"
  FOREIGN KEY (file_id)
  REFERENCES "dispatcher"."file" (id) ON DELETE CASCADE;

ALTER TABLE "dispatcher"."directory_source"
  ADD CONSTRAINT "directory_source_file_id_fkey"
  FOREIGN KEY (file_id)
  REFERENCES "dispatcher"."file" (id) ON DELETE CASCADE;

ALTER TABLE "dispatcher"."dispatched"
  ADD CONSTRAINT "dispatched_file_id_fkey"
  FOREIGN KEY (file_id)
  REFERENCES "dispatcher"."file" (id) ON DELETE CASCADE;
