createdb cortex
psql -d cortex --variable=VERBOSE=verbose -f /schema.sql
