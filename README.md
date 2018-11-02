# AWR Monitoring

This is to introduce a set up that does some capacity analysis of the data held in AWR and then reports on a daily basis any monitored event or statistic that exceeds threshholds based on normal throughput.  The schema objects are now RAC aware.

## Installation

The install involves 4 tables, 1 view and a package installed into the selected schema. Once installed and implemented the DBMS_JOB produces a report in /home/oracle/scripts. This report can be examined using a daily check (basic_log_check). The report may be e-mailed out to selected recipients using Oracle's SMTP routines but some configuration must be done to achieve this. 

### Tables

1. Table PY_AWR_METRICS_PARAMETERS hold all possible events and statistics that can be monitored. 
2. Table PY_AWR_METRICS_HIST holds the historical AWR averages and data 
3. Table PY_AWR_METRICS_DAILY holds the calculated daily totals for the latest run 
4. Table PY_AWR_WARNINGS_HIST is the table containing the historical warnings that have been generated 
5. Table PY_AWR_EMAIL_RECIPIENTS is the table containing the e-mail addresses that to whom you wish to send the report 

### View 

6. View PY_AWR_AVGS is a convenient mechanism to check the daily analysis against the historical data 

### Package 

7. Install package PY_AWR_MONITOR 

## Implementation Details 

### E-mail Implementation 

1. Create UTL_MAIL package 

@?/rdbms/admin/utlmail
@?/rdbms/admin/prvtmail

2. Set the init.ora parameter SMTP_OUT_SERVER - usually to localhost 3. Set up the ACL Example 

begin
    dbms_network_acl_admin.create_acl (
    acl         => 'utl_mail.xml',
    description => 'Allow mail to be send',
    principal   => '<Schema>',
    is_grant    => TRUE,
    privilege   => 'connect'
    );
    dbms_network_acl_admin.add_privilege (
    acl       => 'utl_mail.xml',
    principal => '<Schema>',
    is_grant  => TRUE,
    privilege => 'resolve'
    );
    dbms_network_acl_admin.assign_acl(
    acl  => 'utl_mail.xml',
    host => 'mail.server.com',
        lower_port => 25,
        upper_port => 25
    );
    commit;
end;
/

## Implementation 

1.Create the tables 

2. Create the view 

3. Create the package and package body 

4. Initialise the parameter data 

exec py_awr_monitor.initialize

5. Create the monitoring DBMS_JOB 

exec py_awr_monitor.enable_monitoring

6. Load historic AWR data 

exec py_awr_monitor.load_old_data

7. Create any extra moinitoring that you require Example 

exec py_awr_monitor.enable_metric('enq: TX - row lock contention','EVENT',0,0,0)

8. Create the appropriate directory and grants 

connect / as sysdba
create directory log_files_dir as '/home/oracle/scripts';
grant read,write on directory log_files_dir to <schema>;

9. Run off the latest report 

exec py_awr_monitor.write_logs(TRUNC(SYSDATE-1))

## Oracle Enterprise Manager Installation 

The details may be obtained using Oracle Enterprise Manager by running a job and a report after the DBMS_JOB has been run. 

#### Oracle Enterprise Manager Job Setup 

Set up an Oracle Enterprise Manager Job to run an SQL script within the Oracle Enterprise Manager application. 
Ensure that the parameters to sqlplus contain '-s'. 
Set up the named SQL file with these contents 

```sql
SET HEAD OFF
SET PAGESIZE 9999
SET LINESIZE 999
SET TRIMOUT ON
SET TRIMSPOOL ON
SET FEEDBACK OFF
SET COLSEP ' '
COL MARKER FORMAT A2
COL WARNING_TYPE FORMAT A30
COL METRIC_TYPE FORMAT A20
COL NAME FORMAT A65
COL INSTANCE FORMAT A4
COL ERROR_VALUE FORMAT A20
COL PERCENT_EXCEED FORMAT A20
COL THRESHOLD FORMAT A20
COL PRECENT_THRESHOLD FORMAT A20

WHENEVER SQLERROR EXIT FAILURE
SELECT  '@|' marker
        , warning_type||'|'         warning_type
        , pamp.metric_type||'|'     metric_type
        , pawh.name||'|'            name
        , pawh.instance_number||'|' instance
        , CASE WHEN warning_type = 'TOTAL THRESHOLD EXCEEDED'
                    THEN ROUND(pawh.value, 2)
                    WHEN warning_type = 'DAILY MAX THRESHOLD EXCEEDED'
                    THEN ROUND(pawh.daily_max, 2)
                    WHEN warning_type = 'SPIKE THRESHOLD EXCEEDED'
                    THEN ROUND(pawh.spike_value, 2)
                    ELSE 0  END  ||'|' error_value
        , CASE WHEN warning_type = 'TOTAL THRESHOLD EXCEEDED'
                    THEN DECODE(avg_value,0,' ',ROUND(((pawh.value/pawh.avg_value)-1)*100)||'% above avg')
                    WHEN warning_type = 'DAILY MAX THRESHOLD EXCEEDED'
                    THEN DECODE(daily_max,0,' ',ROUND(((pawh.daily_max/pawh.max_daily_max)-1)*100)||'% above max')
                    WHEN warning_type = 'SPIKE THRESHOLD EXCEEDED'
                    THEN ' '
                    ELSE ' '     END||'|'  percent_exceed
        , CASE WHEN warning_type = 'TOTAL THRESHOLD EXCEEDED'
                    THEN ROUND(pawh.high_threshold, 2)
                    WHEN warning_type = 'DAILY MAX THRESHOLD EXCEEDED'
                    THEN ROUND(pawh.daily_max_threshold, 2)
                    WHEN warning_type = 'SPIKE THRESHOLD EXCEEDED'
                    THEN ROUND(pawh.spike_threshold, 2)
                    ELSE 0   END||'|'   threshold
        , CASE WHEN warning_type = 'TOTAL THRESHOLD EXCEEDED'
                    THEN DECODE(avg_value,0,' ',ROUND(((pawh.high_threshold/pawh.avg_value)-1)*100)||'% above avg')
                    WHEN warning_type = 'DAILY MAX THRESHOLD EXCEEDED'
                    THEN DECODE(daily_max,0,' ',ROUND(((pawh.daily_max_threshold/pawh.max_daily_max)-1)*100)||'% above max')
                    WHEN warning_type = 'SPIKE THRESHOLD EXCEEDED'
                    THEN ' '
                    ELSE ' '   END||'|'  percent_threshold
FROM    py_awr_warnings_hist      pawh
        py_awr_metrics_parameters pamp
WHERE   pawh.log_time = trunc(sysdate - 1)
AND     pawh.name     = pamp.name;
```

Set the job to run daily sometime after the DBMS_JOB run. 

#### Oracle Enterprise Manager Report Setup 

Set up the report to run some SQL and run something similar to the script below 

```sql
SELECT TARGET_NAME
     , TO_CHAR(END_TIME,'DD-MON-RRRR HH24:MI:SS') JOB_TIME
     , TRIM(SUBSTR(AWR_EXCEPTIONS,INSTR(AWR_EXCEPTIONS,'|')+1,INSTR(AWR_EXCEPTIONS,'|',1,2)-(INSTR(AWR_EXCEPTIONS,'|')+1))) EXCEPTION
     , TRIM(SUBSTR(AWR_EXCEPTIONS,INSTR(AWR_EXCEPTIONS,'|',1,2)+1,INSTR(AWR_EXCEPTIONS,'|',1,3)-(INSTR(AWR_EXCEPTIONS,'|',1,2)+1))) STATISTIC_TYPE
     , TRIM(SUBSTR(AWR_EXCEPTIONS,INSTR(AWR_EXCEPTIONS,'|',1,3)+1,INSTR(AWR_EXCEPTIONS,'|',1,4)-(INSTR(AWR_EXCEPTIONS,'|',1,3)+1))) STATISTIC_NAME
     , TRIM(SUBSTR(AWR_EXCEPTIONS,INSTR(AWR_EXCEPTIONS,'|',1,4)+1,INSTR(AWR_EXCEPTIONS,'|',1,5)-(INSTR(AWR_EXCEPTIONS,'|',1,4)+1))) INSTANCE
     , TRIM(SUBSTR(AWR_EXCEPTIONS,INSTR(AWR_EXCEPTIONS,'|',1,5)+1,INSTR(AWR_EXCEPTIONS,'|',1,6)-(INSTR(AWR_EXCEPTIONS,'|',1,5)+1))) ERROR_VALUE
     , TRIM(SUBSTR(AWR_EXCEPTIONS,INSTR(AWR_EXCEPTIONS,'|',1,6)+1,INSTR(AWR_EXCEPTIONS,'|',1,7)-(INSTR(AWR_EXCEPTIONS,'|',1,6)+1))) ERROR_PERCENT
     , TRIM(SUBSTR(AWR_EXCEPTIONS,INSTR(AWR_EXCEPTIONS,'|',1,7)+1,INSTR(AWR_EXCEPTIONS,'|',1,8)-(INSTR(AWR_EXCEPTIONS,'|',1,7)+1))) THRESHOLD_VALUE
     , TRIM(SUBSTR(AWR_EXCEPTIONS,INSTR(AWR_EXCEPTIONS,'|',1,8)+1,INSTR(AWR_EXCEPTIONS,'|',1,9)-(INSTR(AWR_EXCEPTIONS,'|',1,8)+1))) THRESHOLD_PERCENT         
FROM (
     SELECT mh.TARGET_NAME
          , mh.END_TIME
         , TO_CHAR(REGEXP_SUBSTR(mh.OUTPUT,'[^@]+', 1,ct.x)) AWR_EXCEPTIONS 
        FROM  MGMT$JOB_STEP_HISTORY mh
            , (SELECT rownum x FROM ALL_OBJECTS WHERE rownum<=5000) ct 
       WHERE mh.TARGET_TYPE='oracle_database'
        AND   mh.JOB_OWNER='SYSMAN'
        AND   mh.JOB_NAME LIKE 'PYTHIAN_GET_AWR_ANALYSIS.%'
        AND   mh.STEP_NAME='Command'
     )
WHERE AWR_EXCEPTIONS LIKE '|%'
ORDER BY TARGET_NAME, END_TIME desc
```

Schedule this report to run sometime after the Oracle Enterprise Manager job has completed. 

## Example Report 

```
################################################################################
#
# Executable   :- dba_jobs job = 360, py_awr_monitor.collect_metrics(sysdate - 1);
#
# Run from     :- AWRMON@prod01 dba jobs
#
# Run schedule :- TRUNC(SYSDATE + 1) + 1/24
#
# Log File     :- /home/oracle/scripts/awr_hist_chdb02.log
#
# Contact      :- The Pythian Group
#
# Checked      :- Daily Monitoring
#
# Version      :- 1.0
#
# Note         :-
# Please check PY_AWR_METRICS_DAILY for system and database statistics for the day
#          and PY_AWR_METRICS_HIST  for historical data.
# You can also query and update the thresholds in the table
#              PY_AWR_METRICS_PARAMETERS (default is 50(%)).
#
################################################################################
TOTAL THRESHOLD EXCEEDED      :  # Type - STAT CLASS # Name - OS # Value - 203608729 (71% above avg) # Threshold - 178155338.7 (50% above avg)
TOTAL THRESHOLD EXCEEDED      :  # Type - STAT CLASS # Name - SQL # Value - 192994645325 (55% above avg) # Threshold - 186318379306.65 (50% above avg)
TOTAL THRESHOLD EXCEEDED      :  # Type - STATISTIC # Name - table scans (short tables) # Value - 28618254 (61% above avg) # Threshold - 26667394.6 (50% above avg)
TOTAL THRESHOLD EXCEEDED      :  # Type - STATISTIC # Name - index fast full scans (full) # Value - 720213 (66% above avg) # Threshold - 651395.9 (50% above avg)
TOTAL THRESHOLD EXCEEDED      :  # Type - WAIT CLASS # Name - Commit # Value - 1415.82 (63% above avg) # Threshold - 1302.45 (50% above avg)
TOTAL THRESHOLD EXCEEDED      :  # Type - WAIT CLASS # Name - Network # Value - 3715.92 (55% above avg) # Threshold - 3605.34 (50% above avg)
```

## Configuration 
All configuration is done by updating rows in the PY_AWR_METRICS_PARAMETERS.   By default the following statistics and wait events are monitored (as indicated by the flag PY_AWR_METRICS_PARAMETERS.MONITORED = 'Y') 
 
```
Type        Name 

WAIT CLASS 	Administrative 
WAIT CLASS 	Application 
WAIT CLASS 	Cluster 
WAIT CLASS 	Commit 
WAIT CLASS 	Concurrency 
WAIT CLASS 	Configuration 
WAIT CLASS 	Network 
WAIT CLASS 	Other 
WAIT CLASS 	Scheduler 
WAIT CLASS 	System I/O 
WAIT CLASS 	User I/O 
STAT CLASS 	Cache 
STAT CLASS 	Debug 
STAT CLASS 	Enqueue 
STAT CLASS 	OS 
STAT CLASS 	Parallel Server 
STAT CLASS 	Redo 
STAT CLASS 	SQL 
STAT CLASS 	User 
OS STAT 	CPU Used 
STATISTIC 	index fast full scans (full) 
STATISTIC 	sorts (disk) 
STATISTIC 	table scans (long tables) 
STATISTIC 	table scans (short tables) 
```

### Enable New Check 

1. Run the procedure PY_AWR_MONITOR.ENABLE_METRIC 
```sql
BEGIN
   PY_AWR_MONITOR.ENABLE_METRIC ( metric_name_lookup => '<Metric Name>'
                                , metric_type_lookup => '<Metric Type>'
                                , amt_threshold_IN   => <Amount Threshold>
                                , pct_threshold_IN   => <Percent Threshold>
                                , spike_threshold_IN => <Spike Threshold>
   );
END;
/ 
```

Where 
    <Metric Name> is the name of the metric(may contain wildcards) 
    <Metric Type> is the type of the metric e.g. WAIT CLASS, EVENT, STATISTIC etc (may contain wildcards) 
    <Amount Threshold> is the threshold set so that only values above a certain threshold are reported to avoid unnecessary alerts 
    <Percent Threshold> is the percentage increase on the average that must occur before an alert is generated for a specific metric 
    <Spike Threshold> is the percentage increase on the maximum that must occur before an alert is generated for a specific metric 

#### Example 

```sql
BEGIN
   PY_AWR_MONITOR.ENABLE_METRIC ( metric_name_lookup => 'db file sequential read'
                                , metric_type_lookup => 'EVENT'
                                , amt_threshold_IN   => 10000
                                , pct_threshold_IN   => 50
                                , spike_threshold_IN => 20
   );
END;
/
```

### Disable Check 

1. Run the procedure PY_AWR_MONITOR.DISABLE_METRIC 

```sql
BEGIN
   PY_AWR_MONITOR.DISABLE_METRIC ( metric_name_lookup => '<Metric Name>'
                                 , metric_type_lookup => '<Metric Type>'
   );
END;
/ 
```

Where 
    <Metric Name> is the name of the metric (may contain wildcards) 
    <Metric Type> is the type of the metric e.g. WAIT CLASS, EVENT, STATISTIC etc (may contain wildcards) 

#### Example 

```sql
BEGIN
   PY_AWR_MONITOR.DISABLE_METRIC ( metric_name_lookup => 'db file sequential read'
                                 , metric_type_lookup => 'EVENT'
   );
END;
/ 
```

### Disable DBMS_JOB job 

exec PY_AWR_MONITOR.DISABLE_MONITORING

### Enable DBMS_JOB job 

exec PY_AWR_MONITOR.ENABLE_MONITORING

### Excluding stats 

If the stats for a day are anomolous and you do not wish them to be included in the average calculation then you may choose to set them to be ignored. 

exec PY_AWR_MONITOR.EXCLUDE_STATS(<Exclude Date>,'<Metric Name>','<Metric Type>')

Where 
    <Exclude Date> is the date of the day that you wish to exclude 
    <Metric Name> is the name of the metric (may contain wildcards) 
    <Metric Type> is the type of the metric e.g. WAIT CLASS, EVENT, STATISTIC etc (may contain wildcards) 

### Including stats 

The stats may be excluded automatically due to a database bounce or they have been manually excluded and you may wish to included them as valid stats to be included in the average calculations 

exec PY_AWR_MONITOR.INCLUDE_STATS(<Include Date>,'<Metric Name>','<Metric Type>')

Where 
    <Include Date> is the date of the day that you wish to include 
    <Metric Name> is the name of the metric (may contain wildcards) 
    <Metric Type> is the type of the metric e.g. WAIT CLASS, EVENT, STATISTIC etc (may contain wildcards) 

### Delete stats 

If you do not want the stats for a specific day then you may delete them 

exec PY_AWR_MONITOR.DELETE_STATS(<Delete Date>,'<Metric Name>','<Metric Type>')

Where 
    <Delete Date> is the date of the day that you wish to include 
    <Metric Name> is the name of the metric (may contain wildcards)
    <Metric Type> is the type of the metric e.g. WAIT CLASS, EVENT, STATISTIC etc (may contain wildcards) 

### Examining the output 

Once a report is produced and you may want to drill down into the data to isolate the issues. Here are some scripts that I have found useful for this purpose 

