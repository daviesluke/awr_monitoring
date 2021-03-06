accept EventName prompt 'Enter Event Name         : '
accept Instance  prompt 'Enter Instance           : '
accept Day       prompt 'Enter date (DD-MON-YYYY) : '

col time_waited_sec format 9999999999.99
col name format a45
col avg_time_waited_ms format 9999999.999
col snap_time for a20

set lines 140
set pages 500
set verify off

break on name skip 2
compute avg of time_waited_sec on name

SELECT
        name
      , snap_time
      , min_snap_id
      , max_snap_id
      , ROUND(time_waited_us/1000000,2) time_waited_sec
      , CASE total_waits
             WHEN 0 THEN 0
             ELSE ROUND((time_waited_us/total_waits)/1000,3)
        END  avg_time_waited_ms
FROM (
        SELECT name
             , to_char(snap_time,'DD-MON-YYYY HH24:MI:SS') snap_time
             , min_snap_id
             , max_snap_id
             , SUM(time_waited_usecs)          time_waited_us
             , SUM(total_waits)                total_waits
        FROM (
                SELECT
                       '&EventName'        name
                     , NVL(min_snap.snap_time,max_snap.snap_time) snap_time
                     , min_snap.min_snap_id
                     , max_snap.max_snap_id
                     , CASE WHEN NVL(max_snap.time_waited_micro,0)-NVL(min_snap.time_waited_micro,0) < 0
                            THEN NVL(max_snap.time_waited_micro,0)
                            ELSE NVL(max_snap.time_waited_micro,0)-NVL(min_snap.time_waited_micro,0)
                       END  time_waited_usecs
                     , CASE WHEN NVL(max_snap.total_waits,0)-NVL(min_snap.total_waits,0) < 0
                            THEN NVL(max_snap.total_waits,0)
                            ELSE NVL(max_snap.total_waits,0)-NVL(min_snap.total_waits,0)
                       END  total_waits
                FROM
                     (
                       SELECT snap.snap_time
                            , snap.min_snap_id
                            , histstat.time_waited_micro
                            , histstat.total_waits
                       FROM (
                              SELECT   TRUNC(begin_interval_time,'HH24') snap_time
                                     , MIN(snap_id)-1                    min_snap_id
                                     , instance_number
                              FROM     dba_hist_snapshot
                              WHERE    TRUNC(begin_interval_time) = '&Day'
                              AND      instance_number = &Instance
                              GROUP BY TRUNC(begin_interval_time,'HH24'),instance_number
                            )                       snap
                          , dba_hist_system_event   histstat
                       WHERE
                              histstat.snap_id    = snap.min_snap_id
                       AND    histstat.event_name = '&EventName'
                       AND    histstat.instance_number = &Instance
                       AND    histstat.instance_number = snap.instance_number
                     )                                                  min_snap
                        full outer join
                     (
                       SELECT snap.snap_time
                            , snap.max_snap_id
                            , histstat.time_waited_micro
                            , histstat.total_waits
                       FROM (
                              SELECT   TRUNC(begin_interval_time,'HH24') snap_time
                                     , MAX(snap_id)                      max_snap_id
                                     , instance_number
                              FROM     dba_hist_snapshot
                              WHERE    TRUNC(begin_interval_time) = '&Day'
                              AND      instance_number = &Instance
                              GROUP BY TRUNC(begin_interval_time,'HH24'),instance_number
                            )                       snap
                          , dba_hist_system_event   histstat
                       WHERE
                              histstat.snap_id    = snap.max_snap_id
                       AND    histstat.event_name = '&EventName'
                       AND    histstat.instance_number = &Instance
                       AND    histstat.instance_number = snap.instance_number
                     )                                                  max_snap
                        on     max_snap.snap_time = min_snap.snap_time
               )
        GROUP BY snap_time
               , name
               , min_snap_id
               , max_snap_id
        ORDER BY name
               , snap_time
      )
/