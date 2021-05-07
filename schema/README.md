Folder created: 03/01/2021.

Purpose:
for sb_obs.sql
uploading a sample partially populated database

To kill any connections to a database instance(so you can DROP then CREATE
a new instance of the database)
SELECT pg_terminate_backend(pid) FROM pg_stat_activity
WHERE datname = 'dr_shellbase'
AND pid <> pg_backend_pid()
AND state in ('idle');