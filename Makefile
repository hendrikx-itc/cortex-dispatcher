ECONDEXPANSION:

all: db/schema.sql

db/schema.sql: db/schema.yml
	db-schema compile sql -o db/$(@F) db/schema.yml

