createdb cortex
psql -d cortex --variable=VERBOSE=verbose -f /dispatcher.sql
psql -d cortex --variable=VERBOSE=verbose -f /sftp-scanner.sql
