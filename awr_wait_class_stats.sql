accept ClassName prompt 'Enter Value for the Wait Class : '
accept Instance prompt  'Enter instance number    : '

col time_waited_sec format 9999999999.99
col name format a45

set lines 140
set pages 500
set verify off

break on wait_class skip 2
compute avg of time_waited_sec on wait_class

set trims off

spool awr_wait_class_stats

SELECT wait_class
     , snap_time
     , ROUND(SUM(time_waited_usecs)/1000000,2) time_waited_sec
FROM (
        SELECT
               '&ClassName'                               wait_class
             , NVL(min_snap.snap_time,max_snap.snap_time) snap_time
             , CASE WHEN NVL(max_snap.time_waited_micro,0)-NVL(min_snap.time_waited_micro,0) < 0
                    THEN NVL(max_snap.time_waited_micro,0)
                    ELSE NVL(max_snap.time_waited_micro,0)-NVL(min_snap.time_waited_micro,0)
               END                                       time_waited_usecs
        FROM
             (
               SELECT snap.snap_time
                    , sysev.event_name
                    , sysev.time_waited_micro
               FROM (
                      SELECT   TRUNC(begin_interval_time) snap_time
                             , MIN(snap_id)     min_snap_id
                      FROM     dba_hist_snapshot
                      WHERE    instance_number  = '&Instance'
                      GROUP BY TRUNC(begin_interval_time)
                    )                       snap
                  , dba_hist_system_event   sysev
               WHERE
                      instance_number  = '&Instance'
               AND    sysev.snap_id    = snap.min_snap_id
               AND    sysev.wait_class = '&ClassName'
             )                                                 min_snap
                full outer join
             (
               SELECT snap.snap_time
                    , sysev.event_name
                    , sysev.time_waited_micro
               FROM (
                      SELECT   TRUNC(begin_interval_time) snap_time
                             , MAX(snap_id)     min_snap_id
                      FROM     dba_hist_snapshot
                      WHERE    instance_number  = '&Instance'
                      GROUP BY TRUNC(begin_interval_time)
                    )                          snap
                  , dba_hist_system_event      sysev
               WHERE
                      instance_number  = '&Instance'
               AND    sysev.snap_id    = snap.min_snap_id
               AND    sysev.wait_class = '&ClassName'
             )                                                 max_snap
                on     max_snap.snap_time  = min_snap.snap_time
                   and max_snap.event_name = min_snap.event_name
      )
GROUP BY snap_time
       , wait_class
ORDER BY snap_time
/


break on name skip 2
compute avg of time_waited_sec on name

SELECT name
     , snap_time
     , ROUND(SUM(time_waited_usecs)/1000000,2) time_waited_sec
FROM (
        SELECT
               NVL(min_snap.event_name,max_snap.event_name) name
             , NVL(min_snap.snap_time,max_snap.snap_time) snap_time
             , CASE WHEN NVL(max_snap.time_waited_micro,0)-NVL(min_snap.time_waited_micro,0) < 0
                    THEN NVL(max_snap.time_waited_micro,0)
                    ELSE NVL(max_snap.time_waited_micro,0)-NVL(min_snap.time_waited_micro,0)
               END                                       time_waited_usecs
        FROM
             (
               SELECT snap.snap_time
                    , sysev.event_name
                    , sysev.time_waited_micro
               FROM (
                      SELECT   TRUNC(begin_interval_time) snap_time
                             , MIN(snap_id)     min_snap_id
                      FROM     dba_hist_snapshot
                      WHERE    instance_number  = '&Instance'
                      GROUP BY TRUNC(begin_interval_time)
                    )                       snap
                  , dba_hist_system_event   sysev
               WHERE
                      instance_number  = '&Instance'
               AND    sysev.snap_id    = snap.min_snap_id
               AND    sysev.wait_class = '&ClassName'
             )                                                 min_snap
                full outer join
             (
               SELECT snap.snap_time
                    , sysev.event_name
                    , sysev.time_waited_micro
               FROM (
                      SELECT   TRUNC(begin_interval_time) snap_time
                             , MAX(snap_id)     min_snap_id
                      FROM     dba_hist_snapshot
                      WHERE    instance_number  = '&Instance'
                      GROUP BY TRUNC(begin_interval_time)
                    )                          snap
                  , dba_hist_system_event      sysev
               WHERE
                      instance_number  = '&Instance'
               AND    sysev.snap_id    = snap.min_snap_id
               AND    sysev.wait_class = '&ClassName'
             )                                                 max_snap
                on     max_snap.snap_time  = min_snap.snap_time
                   and max_snap.event_name = min_snap.event_name
      )
GROUP BY snap_time
       , name
ORDER BY name
       , snap_time
/
spool off