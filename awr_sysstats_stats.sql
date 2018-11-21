accept StatName prompt 'Enter Stats Name         : ' 
accept Instance prompt 'Enter instance number    : ' 
 
col name format a45 
col snap_time for a20 
 
set lines 140 
set pages 500 
set verify off 
 
break on name skip 2 on report 
compute sum of total_count on report 
 
SELECT name 
     , to_char(snap_time,'DD-MON-YYYY HH24:MI:SS') snap_time 
     , min_snap_id 
     , max_snap_id 
     , SUM(stat_count) total_count 
FROM ( 
        SELECT 
               '&StatName'        name 
             , NVL(min_snap.snap_time,max_snap.snap_time) snap_time 
             , min_snap.min_snap_id 
             , max_snap.max_snap_id 
             , CASE WHEN NVL(max_snap.value,0)-NVL(min_snap.value,0) < 0 
                    THEN NVL(max_snap.value,0) 
                    ELSE NVL(max_snap.value,0)-NVL(min_snap.value,0) 
               END  stat_count 
        FROM 
             ( 
               SELECT snap.snap_time 
                    , snap.min_snap_id 
                    , histstat.value 
               FROM ( 
                      SELECT   TRUNC(begin_interval_time) snap_time 
                             , MIN(snap_id) min_snap_id 
                      FROM     dba_hist_snapshot 
                      WHERE    instance_number = '&Instance' 
                      GROUP BY TRUNC(begin_interval_time) 
                    )                       snap 
                  , dba_hist_sysstat   histstat 
               WHERE 
                      histstat.snap_id    = snap.min_snap_id 
               AND    histstat.stat_name = '&StatName' 
               AND    histstat.instance_number = '&Instance' 
             ) min_snap 
                full outer join 
             ( 
               SELECT snap.snap_time 
                    , snap.max_snap_id 
                    , histstat.value 
               FROM ( 
                      SELECT   TRUNC(begin_interval_time) snap_time 
                             , MAX(snap_id) max_snap_id 
                      FROM     dba_hist_snapshot 
                      WHERE    instance_number = '&Instance' 
                      GROUP BY TRUNC(begin_interval_time) 
                    )                       snap 
                  , dba_hist_sysstat   histstat 
               WHERE 
                      histstat.snap_id    = snap.max_snap_id 
               AND    histstat.stat_name = '&StatName' 
               AND    histstat.instance_number = '&Instance' 
             ) max_snap 
                on     max_snap.snap_time = min_snap.snap_time 
       ) 
GROUP BY snap_time 
       , name 
       , min_snap_id 
       , max_snap_id 
ORDER BY name 
       , min_snap_id 
/