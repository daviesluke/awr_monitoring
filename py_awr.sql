i@@PY_AWR_TABLES.sql

@@PY_AWR_MONITOR.pkh

@@PY_AWR_MONITOR.pkb

exec py_awr_monitor.initialize

exec py_awr_monitor.enable_monitoring

exec py_awr_monitor.load_old_data

exec py_awr_monitor.enable_metric('enq: TX - row lock contention','EVENT',0,0,0)

connect / as sysdba

create directory log_files_dir as '/home/oracle/scripts';

grant read,write on directory log_files_dir to avail;

GRANT EXECUTE ON utl_mail TO AVAIL;
GRANT EXECUTE ON utl_smtp TO AVAIL;

-- Need to set this properly if need to e-mail it out
-- change -> mail.server.com to the mail server 

-- alter system set smtp_out_server='mail.server.com' scope=both;

--begin
--    dbms_network_acl_admin.create_acl (
--	acl         => 'utl_mail.xml',
--	description => 'Allow mail to be send',
--	principal   => 'AVAIL',
--	is_grant    => TRUE,
--	privilege   => 'connect'
--    );
--    dbms_network_acl_admin.add_privilege (
--	acl       => 'utl_mail.xml',
--	principal => 'AVAIL',
--	is_grant  => TRUE,
--	privilege => 'resolve'
--    );
--    dbms_network_acl_admin.assign_acl(
--	acl  => 'utl_mail.xml',
--	host => 'mail.server.com',
--      lower_port => 25,
--      upper_port => 25
--    );
--    commit;
--end;
--/

exec avail.py_awr_monitor.write_logs(TRUNC(SYSDATE-1))

exit
