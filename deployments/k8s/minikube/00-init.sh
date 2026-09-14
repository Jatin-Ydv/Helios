#!/bin/sh
set -eu
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
