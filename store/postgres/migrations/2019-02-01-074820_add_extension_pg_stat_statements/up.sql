/**************************************************************
* INSTALL pg_stats_statements
*
* Provides tools to track statement execution statistics.
*
* Note: In order to begin collecting stats the module must be loaded
* by adding 'pg_stat_statements' to `shared_preload_libraries` in postgresql.conf.
**************************************************************/
CREATE EXTENSION pg_stat_statements;
