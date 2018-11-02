@@PY_STATS_TABLES.sql

@@PY_STATS_MONITOR.pkh

@@PY_STATS_MONITOR.pkb

exec py_stats_monitor.initialize

exec py_stats_monitor.enable_monitoring

exec py_stats_monitor.load_old_data

connect / as sysdba

create directory log_files_dir as '/appl/oracle/scripts';

grant read,write on directory log_files_dir to availng;

exec availng.py_stats_monitor.write_logs(TRUNC(SYSDATE-1))

exit

