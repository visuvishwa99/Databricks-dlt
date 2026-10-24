-- Run with psql as a PostgreSQL administrator, connected to the postgres database.
-- Never replace an existing database.
\set ON_ERROR_STOP on
SELECT format('CREATE DATABASE %I', 'dbx_db')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'dbx_db')
\gexec
SELECT datname FROM pg_database WHERE datname = 'dbx_db';
