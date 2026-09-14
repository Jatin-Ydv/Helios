#!/bin/sh
set -eu
# Creates one logical database per bounded context. Physical isolation in prod
# is a topology choice; locally a single Postgres with multiple DBs is enough
# to keep schemas from leaking across services.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
  CREATE DATABASE identity;
  CREATE DATABASE catalog;
  CREATE DATABASE inventory;
  CREATE DATABASE orders;
  CREATE DATABASE payment;
  CREATE DATABASE fulfillment;
  CREATE DATABASE orchestrator;
  CREATE DATABASE notification;
EOSQL
