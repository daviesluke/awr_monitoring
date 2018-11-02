CREATE OR REPLACE PACKAGE BODY py_awr_monitor
AS

        /* Truncate the tables and set up the parameters table with default values */

        PROCEDURE initialize
        AS
        BEGIN
          EXECUTE IMMEDIATE 'TRUNCATE TABLE py_awr_metrics_daily';
          EXECUTE IMMEDIATE 'TRUNCATE TABLE py_awr_metrics_hist';
          EXECUTE IMMEDIATE 'TRUNCATE TABLE py_awr_warnings_hist';
          EXECUTE IMMEDIATE 'DELETE py_awr_metrics_parameters';

          INSERT INTO py_awr_metrics_parameters (
                  name
                , metric_type
                , monitored
                , pct_threshold
                , who
                , updated
                , amt_threshold
                , spike_threshold
          ) SELECT
                  DISTINCT stat_name
                  , 'STATISTIC'
                  , CASE stat_name
                        WHEN 'table scans (long tables)'    THEN 'Y'
                        WHEN 'table scans (short tables)'   THEN 'Y'
                        WHEN 'index fast full scans (full)' THEN 'Y'
                        WHEN 'sorts (disk)'                 THEN 'Y'
                        ELSE                                     'N'
                    END
                  , CASE stat_name
                        WHEN 'table scans (long tables)'    THEN 50
                        WHEN 'table scans (short tables)'   THEN 50
                        WHEN 'index fast full scans (full)' THEN 50
                        WHEN 'sorts (disk)'                 THEN 50
                        ELSE                                     NULL
                    END
                  , 'auto'
                  , SYSDATE
                  , CASE stat_name
                        WHEN 'table scans (long tables)'    THEN 50
                        WHEN 'table scans (short tables)'   THEN 50
                        WHEN 'index fast full scans (full)' THEN 50
                        WHEN 'sorts (disk)'                 THEN 50
                        ELSE                                     NULL
                    END
                  , CASE stat_name
                        WHEN 'table scans (long tables)'    THEN 20
                        WHEN 'table scans (short tables)'   THEN 20
                        WHEN 'index fast full scans (full)' THEN 20
                        WHEN 'sorts (disk)'                 THEN 20
                        ELSE                                     NULL
                    END
            FROM dba_hist_sysstat;

          INSERT INTO py_awr_metrics_parameters (
                  name
                , metric_type
                , monitored
                , pct_threshold
                , who
                , updated
                , amt_threshold
                , spike_threshold
          ) SELECT
                  CASE POWER(2,ROWNUM-1)
                       WHEN   1   THEN 'User'
                       WHEN   2   THEN 'Redo'
                       WHEN   4   THEN 'Enqueue'
                       WHEN   8   THEN 'Cache'
                       WHEN  16   THEN 'Parallel Server'
                       WHEN  32   THEN 'OS'
                       WHEN  64   THEN 'SQL'
                       WHEN 128   THEN 'Debug'
                  END
                  , 'STAT CLASS'
                  , 'Y'
                  , 50
                  , 'auto'
                  , SYSDATE
                  , 10000
                  , 20
            FROM dual
            CONNECT BY LEVEL <= 8;

          INSERT INTO py_awr_metrics_parameters (
                  name
                , metric_type
                , monitored
                , who
                , updated
          ) SELECT
                  DISTINCT name
                , 'EVENT'
                , 'N'
                , 'auto'
                , SYSDATE
            FROM v$event_name
            ORDER BY 1;

          INSERT INTO py_awr_metrics_parameters (
                  name
                , metric_type
                , monitored
                , pct_threshold
                , who
                , updated
                , amt_threshold
                , spike_threshold
          ) SELECT
                  DISTINCT wait_class
                , 'WAIT CLASS'
                , 'Y'
                , 50
                , 'auto'
                , SYSDATE
                , 500
                , 20
            FROM v$event_name
            WHERE wait_class <> 'Idle'
            ORDER BY 1;

          INSERT INTO py_awr_metrics_parameters (
                  name
                , metric_type
                , monitored
                , pct_threshold
                , who
                , updated
                , amt_threshold
                , spike_threshold
          ) VALUES (
                  'CPU Used'
                , 'OS STAT'
                , 'Y'
                , 50
                , 'auto'
                , SYSDATE
                , 20
                , 20
          );

          COMMIT;

        END initialize;


        /* This procedure loads any old data that we currently do not have */

        PROCEDURE load_old_data (
                  metric_name_lookup  VARCHAR2 DEFAULT '%'
                , metric_type_lookup  VARCHAR2 DEFAULT '%'
        ) AS
        BEGIN
                FOR DateCurs IN (
                        SELECT DISTINCT TRUNC(begin_interval_time) snap_date
                        FROM   dba_hist_snapshot
                        MINUS
                        SELECT DISTINCT TRUNC(pamh.log_time)
                        FROM   py_awr_metrics_hist       pamh
                             , py_awr_metrics_parameters pamp
                        WHERE  pamh.name        like metric_name_lookup
                        AND    pamh.name        =    pamp.name
                        AND    pamp.metric_type like metric_type_lookup
                ) LOOP
                        collect_metrics(
                                  snap_date          => DateCurs.snap_date
                                , do_report          => FALSE
                                , do_email           => FALSE
                                , metric_name_lookup => metric_name_lookup
                                , metric_type_lookup => metric_type_lookup
                        );
                END LOOP;
        END load_old_data;



        /* Procedure to check the daily stats against the threshold and write results to the warnings table */

        PROCEDURE check_daily_stats (
                  snap_date       DATE
                , instance        NUMBER DEFAULT 1
        ) AS
        BEGIN
                DELETE py_awr_warnings_hist
                WHERE  log_time = TRUNC(snap_date)
                AND    instance_number = Instance;

                INSERT INTO py_awr_warnings_hist (
                          log_time
                        , name
                        , instance_number
                        , value
                        , min_value
                        , avg_value
                        , max_value
                        , daily_min
                        , daily_avg
                        , daily_max
                        , min_daily_min
                        , avg_daily_avg
                        , max_daily_max
                        , high_threshold
                        , daily_max_threshold
                        , spike_value
                        , spike_threshold
                        , warning_type
                ) SELECT
                          hist.log_time
                        , hist.name
                        , hist.instance_number
                        , hist.value
                        , avgs.min_value
                        , avgs.avg_value
                        , avgs.max_value
                        , hist.daily_min
                        , hist.daily_avg
                        , hist.daily_max
                        , avgs.min_daily_min
                        , avgs.avg_daily_avg
                        , avgs.max_daily_max
                        , avgs.high_threshold
                        , avgs.daily_max_threshold
                        , CASE WHEN NVL(hist.daily_avg,0) - NVL(hist.daily_min,0) <= 0
                               THEN 0
                               ELSE (hist.daily_max - hist.daily_avg)/(hist.daily_avg - hist.daily_min)
                          END
                        , avgs.spike_threshold
                        , CASE WHEN hist.value > avgs.high_threshold AND hist.value > avgs.amt_threshold
                               THEN 'TOTAL THRESHOLD EXCEEDED'
                               WHEN hist.daily_max > avgs.daily_max_threshold AND hist.daily_max > avgs.amt_threshold
                               THEN 'DAILY MAX THRESHOLD EXCEEDED'
                               WHEN     NVL(hist.daily_avg,0) - NVL(hist.daily_min,0) > 0
                                    AND (hist.daily_max - hist.daily_avg)/(hist.daily_avg - hist.daily_min) > avgs.spike_threshold
                                    AND hist.daily_max > avgs.amt_threshold
                               THEN 'SPIKE THRESHOLD EXCEEDED'
                               ELSE 'UNKNOWN'
                          END
                  FROM    py_awr_metrics_hist  hist
                        , py_awr_avgs          avgs
                  WHERE   hist.name            = avgs.name
                  AND     hist.instance_number = avgs.instance_number
                  AND     TRUNC(hist.log_time) = trunc(snap_date)
                  AND     hist.instance_number = Instance
                  AND     hist.counted         = 'Y'
                  AND     (
                           (    hist.value     > avgs.high_threshold
                            AND hist.value     > avgs.amt_threshold
                           )
                           OR
                           (    hist.daily_max > avgs.daily_max_threshold
                            AND hist.daily_max > avgs.amt_threshold
                           )
                           OR
                           (    NVL(hist.daily_avg,0) - NVL(hist.daily_min,0) <= 0
                            AND (hist.daily_max - hist.daily_avg)/LEAST((hist.daily_avg - hist.daily_min),-1) > avgs.spike_threshold
                            AND hist.daily_max > avgs.amt_threshold
                           )
                          );

        END check_daily_stats;


        /* This procedure is designed to be run once a day */

        PROCEDURE collect_metrics (
                  snap_date          DATE
                , do_report          BOOLEAN  DEFAULT TRUE
                , do_email           BOOLEAN  DEFAULT TRUE
                , metric_name_lookup VARCHAR2 DEFAULT '%'
                , metric_type_lookup VARCHAR2 DEFAULT '%'
        ) AS
                MinSnapID      NUMBER;
                MaxSnapID      NUMBER;
                NextMinSnapID  NUMBER;
                Counter        NUMBER;
                Instances      NUMBER;
                Counter2       NUMBER;
        BEGIN
                /* Get the snapshot counts and limits for the date specified */

                SELECT COUNT(DISTINCT startup_time)
                     , COUNT(DISTINCT instance_number)
                     , MIN(snap_id)
                     , MAX(snap_id)
                INTO   Counter
                     , Instances
                     , MinSnapID
                     , MaxSnapID
                FROM   dba_hist_snapshot
                WHERE  TRUNC(begin_interval_time) = TRUNC(snap_date);

                /* If no count then no snapshots have occurred if this is the case then exit */

                IF Counter = 0
                THEN
                        RETURN;
                END IF;

                /* Get the first snap shot of the next day */

                SELECT NVL(MIN(snap_id), -1)
                INTO   NextMinSnapID
                FROM   dba_hist_snapshot
                WHERE  TRUNC(begin_interval_time) = TRUNC(snap_date + 1);

                /* Get rid of the current collections in daily table */

                DELETE py_awr_metrics_daily;

                /* Let's loop through any instances */

                FOR InstanceCurs IN ( SELECT DISTINCT instance_number
                                      FROM   dba_hist_snapshot
                                      WHERE  TRUNC(begin_interval_time) = TRUNC(snap_date)
                ) LOOP
                        /* Check there is a next day snap */

                        IF NextMinSnapID <> -1
                        THEN
                                /* Check there has not been a bounce of the DB between the max snap id obtained and the next one */

                                SELECT COUNT(DISTINCT startup_time)
                                INTO   Counter2
                                FROM   dba_hist_snapshot
                                WHERE  snap_id BETWEEN MaxSnapID AND NextMinSnapID
                                AND    instance_number = InstanceCurs.instance_number;

                                /* If no bounce then amend the max snap id */

                                IF counter2 = 1
                                THEN
                                        MaxSnapID := NextMinSnapID;
                                END IF;
                        END IF;

                        IF Counter - Instances = 0   /* No instance bounce during the day (unique startup_time for the day) */
                        THEN
                                populate_daily(
                                          snap_date          => snap_date
                                        , min_snap_id        => MinSnapID
                                        , max_snap_id        => MaxSnapID
                                        , instance           => InstanceCurs.instance_number
                                        , metric_name_lookup => metric_name_lookup
                                        , metric_type_lookup => metric_type_lookup
                                );
                        ELSE            /* Instance bounced during the day */
                                FOR SnapCurs IN (
                                        SELECT MIN(snap_id) min_snap_id
                                             , MAX(snap_id) max_snap_id
                                        FROM   dba_hist_snapshot
                                        WHERE  TRUNC(begin_interval_time) = TRUNC(snap_date)
                                        GROUP BY startup_time
                                        ORDER BY 1
                                ) LOOP
                                        IF MaxSnapID = SnapCurs.max_snap_id + 1
                                        THEN
                                                populate_daily(
                                                          snap_date          => snap_date
                                                        , min_snap_id        => SnapCurs.min_snap_id
                                                        , max_snap_id        => MaxSnapID
                                                        , instance           => InstanceCurs.instance_number
                                                        , metric_name_lookup => metric_name_lookup
                                                        , metric_type_lookup => metric_type_lookup
                                                );
                                        ELSE
                                                populate_daily(
                                                          snap_date          => snap_date
                                                        , min_snap_id        => SnapCurs.min_snap_id
                                                        , max_snap_id        => SnapCurs.max_snap_id
                                                        , instance           => InstanceCurs.instance_number
                                                        , metric_name_lookup => metric_name_lookup
                                                        , metric_type_lookup => metric_type_lookup
                                                );
                                        END IF;
                                END LOOP;
                        END IF;
                END LOOP;

                /* Move the stats to the history table */

                populate_hist(
                          snap_date          => snap_date
                        , metric_name_lookup => metric_name_lookup
                        , metric_type_lookup => metric_type_lookup
                );

                FOR HistCurs IN ( SELECT COUNT (DISTINCT log_time) Counter
                                       , instance_number
                                  FROM   py_awr_metrics_hist
                                  WHERE  counted = 'Y'
                                  GROUP BY instance_number
                ) LOOP
                        /* Only report if there have been at least 5 samples otherwise no decent average can be obtained */

                        IF HistCurs.Counter >= 5
                        THEN
                                check_daily_stats(snap_date,HistCurs.instance_number);
                        END IF;
                END LOOP;

                IF do_report
                THEN
                        IF write_logs(snap_date) > 0
                        THEN
                                IF do_email
                                THEN
                                        email_logs;
                                END IF;
                        END IF;
                END IF;

                /* All this is done in one big transaction to maintain data integrity */

                COMMIT;
        END collect_metrics;



        /* Procedure to enable gathering of history of a metric */

        PROCEDURE enable_metric (
                  metric_name_lookup VARCHAR2
                , metric_type_lookup VARCHAR2
                , amt_threshold_IN   NUMBER   DEFAULT 10000
                , pct_threshold_IN   NUMBER   DEFAULT 50
                , spike_threshold_IN NUMBER   DEFAULT 20
        ) AS
        BEGIN
                UPDATE py_awr_metrics_parameters
                SET    monitored       = 'Y'
                     , updated         = SYSDATE
                     , who             = USER
                     , amt_threshold   = amt_threshold_IN
                     , pct_threshold   = pct_threshold_IN
                     , spike_threshold = spike_threshold_IN
                WHERE  name        like metric_name_lookup
                AND    metric_type like metric_type_lookup;

                COMMIT;

                /* Now load the old data */

                load_old_data(
                          metric_name_lookup => metric_name_lookup
                        , metric_type_lookup => metric_type_lookup
                );
        END enable_metric;


        /* Procedure to disable gathering of history of a metric */

        PROCEDURE disable_metric (
                  metric_name_lookup  VARCHAR2
                , metric_type_lookup  VARCHAR2
        ) AS
        BEGIN
                UPDATE py_awr_metrics_parameters
                SET    monitored     = 'N'
                     , updated       = SYSDATE
                     , who           = USER
                     , pct_threshold = NULL
                WHERE  name        like metric_name_lookup
                AND    metric_type like metric_type_lookup;

                COMMIT;
        END disable_metric;


        /* Procedure to submit a job to monitor the stats */

        PROCEDURE enable_monitoring (
                  instance    NUMBER DEFAULT 1
        ) AS
                JobCount NUMBER;
                JobNo    BINARY_INTEGER;
        BEGIN
                SELECT COUNT(*)
                INTO   JobCount
                FROM   user_jobs
                WHERE  what like 'py_awr_monitor.collect_metrics%';

                IF jobcount >= 1
                THEN
                        raise_application_error(-20101, 'Job already exists, remove it first.');
                END IF;

                dbms_job.submit(
                          job       => JobNo
                        , what      => 'py_awr_monitor.collect_metrics(sysdate - 1);'
                        , next_date => TRUNC(SYSDATE + 1) + 5/24
                        , interval  => 'TRUNC(SYSDATE + 1) + 1/24'
                        , instance  => instance
                );

                COMMIT;
        END enable_monitoring;

        /* Procedure to remove the job for monitoring the stats */

        PROCEDURE disable_monitoring
        AS
                JobNo    BINARY_INTEGER;
        BEGIN
                FOR JobCurs IN (
                        SELECT job
                        FROM   user_jobs
                        WHERE  what like 'py_awr_monitor.collect_metrics%'
                ) LOOP
                        dbms_job.remove(JobCurs.job);

                        COMMIT;
                END LOOP;
        END disable_monitoring;



        /* Procedure to delete historical stats */

        PROCEDURE delete_stats (
                  snap_date DATE
                , metric_name_lookup  VARCHAR2 DEFAULT '%'
                , metric_type_lookup  VARCHAR2 DEFAULT '%'
        ) AS
        BEGIN
                DELETE py_awr_metrics_hist
                WHERE  log_time    =    TRUNC(snap_date)
                AND    name        like metric_name_lookup
                AND    name in   ( SELECT name
                                   FROM   py_awr_metrics_parameters
                                   WHERE  metric_type like metric_type_lookup );

                COMMIT;
        END delete_stats;


        /* Stats are by default included in the calculations
           This procedure is to re-include stats if they have been previously excluded
        */

        PROCEDURE include_stats (
                  snap_date           DATE
                , metric_name_lookup  VARCHAR2 DEFAULT '%'
                , metric_type_lookup  VARCHAR2 DEFAULT '%'
        ) AS
        BEGIN
                UPDATE py_awr_metrics_hist
                SET    counted = 'Y'
                WHERE  log_time    =    TRUNC(snap_date)
                AND    name        like metric_name_lookup
                AND    name in   ( SELECT name
                                   FROM   py_awr_metrics_parameters
                                   WHERE  metric_type like metric_type_lookup );

                COMMIT;
        END include_stats;


        /* Stats are by default included in the calculations
           This procedure is to exclude stats due to skewed results
        */

        PROCEDURE exclude_stats (
                  snap_date           DATE
                , metric_name_lookup  VARCHAR2 DEFAULT '%'
                , metric_type_lookup  VARCHAR2 DEFAULT '%'
        ) AS
        BEGIN
                UPDATE py_awr_metrics_hist
                SET    counted = 'N'
                WHERE  log_time    =    TRUNC(snap_date)
                AND    name        like metric_name_lookup
                AND    name in   ( SELECT name
                                   FROM   py_awr_metrics_parameters
                                   WHERE  metric_type like metric_type_lookup );

                COMMIT;
        END exclude_stats;


        /* This procedure populates the daily stats table with the metrics required */

        PROCEDURE populate_daily (
                  snap_date            DATE
                , min_snap_id          NUMBER
                , max_snap_id          NUMBER
                , instance             NUMBER   DEFAULT 1
                , metric_name_lookup   VARCHAR2 DEFAULT '%'
                , metric_type_lookup   VARCHAR2 DEFAULT '%'
        ) AS
        BEGIN
                MERGE INTO py_awr_metrics_daily daily
                USING (
                        SELECT log_time
                             , begin_snap_id
                             , end_snap_id
                             , name
                             , instance_number
                             , max(real_value) - first_value value
                             , min(delta_value)              daily_min
                             , avg(delta_value)              daily_avg
                             , max(delta_value)              daily_max
                        FROM (
                                SELECT /* Get System Stats */
                                       TRUNC(snap_date)    log_time
                                     , min(stat.snap_id) over (partition by stat.stat_name) begin_snap_id
                                     , max(stat.snap_id) over (partition by stat.stat_name) end_snap_id
                                     , par.name          name
                                     , stat.instance_number
                                     , first_value(stat.value) over (partition by stat.stat_name order by stat.snap_id) first_value
                                     , stat.value - lag(stat.value) over (partition by stat.stat_name order by stat.snap_id) delta_value
                                     , stat.value real_value
                                FROM   py_awr_metrics_parameters                       par
                                     , dba_hist_sysstat                                stat
                                WHERE  par.name             =       stat.stat_name
                                AND    par.monitored        =       'Y'
                                AND    par.metric_type      =       'STATISTIC'
                                AND    par.name             like    metric_name_lookup
                                AND    par.metric_type      like    metric_type_lookup
                                AND    stat.snap_id         between min_snap_id AND max_snap_id
                                AND    stat.instance_number = instance
                                ORDER BY par.name
                                       , stat.snap_id
                                       , stat.instance_number
                             )
                        WHERE delta_value IS NOT NULL
                        GROUP BY log_time
                             , begin_snap_id
                             , end_snap_id
                             , name
                             , instance_number
                             , first_value
                        UNION ALL
                        SELECT log_time
                             , begin_snap_id
                             , end_snap_id
                             , name
                             , instance_number
                             , max(real_value) - first_value value
                             , min(delta_value)              daily_min
                             , avg(delta_value)              daily_avg
                             , max(delta_value)              daily_max
                        FROM (
                                SELECT /* Get System Stat Class */
                                       TRUNC(snap_date) log_time
                                     , min(stat.snap_id) over (partition by stat.stat_name) begin_snap_id
                                     , max(stat.snap_id) over (partition by stat.stat_name) end_snap_id
                                     , par.name               name
                                     , stat.instance_number
                                     , first_value(stat.value) over (partition by stat.stat_name order by stat.snap_id) first_value
                                     , stat.value - lag(stat.value) over (partition by stat.stat_name order by stat.snap_id) delta_value
                                     , stat.value real_value
                                FROM   py_awr_metrics_parameters                       par
                                     , (
                                         SELECT dhs.snap_id
                                              , CASE BITAND( class.bit_mask, vs.class)
                                                     WHEN   1   THEN 'User'
                                                     WHEN   2   THEN 'Redo'
                                                     WHEN   4   THEN 'Enqueue'
                                                     WHEN   8   THEN 'Cache'
                                                     WHEN  16   THEN 'Parallel Server'
                                                     WHEN  32   THEN 'OS'
                                                     WHEN  64   THEN 'SQL'
                                                     WHEN 128   THEN 'Debug'
                                                END                                    stat_name
                                              , dhs.instance_number
                                              , SUM(NVL(dhs.value,0))                  value
                                         FROM   dba_hist_sysstat                       dhs
                                              , v$statname                             vs
                                              , ( SELECT POWER(2,ROWNUM-1)             bit_mask
                                                  FROM   DUAL
                                                  CONNECT BY LEVEL <= 8 )              class
                                         WHERE  dhs.stat_id                       = vs.stat_id
                                         AND    BITAND( class.bit_mask, vs.class) != 0
                                         AND    dhs.snap_id         BETWEEN min_snap_id AND max_snap_id
                                         AND    dhs.stat_name       NOT LIKE '%memory%'
                                         AND    dhs.instance_number = instance
                                         GROUP BY dhs.snap_id
                                                , dhs.instance_number
                                                , BITAND( class.bit_mask, vs.class)
                                       )                                               stat
                                WHERE  par.name        =    stat.stat_name
                                AND    par.monitored   =    'Y'
                                AND    par.metric_type = 'STAT CLASS'
                                AND    par.name        like metric_name_lookup
                                AND    par.metric_type like metric_type_lookup
                                ORDER BY par.name
                                       , stat.snap_id
                                       , stat.instance_number
                              )
                        WHERE delta_value IS NOT NULL
                        GROUP BY log_time
                             , begin_snap_id
                             , end_snap_id
                             , name
                             , instance_number
                             , first_value
                        UNION ALL
                        SELECT log_time
                             , begin_snap_id
                             , end_snap_id
                             , name
                             , instance_number
                             , max(real_value) - first_value value
                             , min(delta_value)              daily_min
                             , avg(delta_value)              daily_avg
                             , max(delta_value)              daily_max
                        FROM (
                                SELECT /* Get System Events */
                                       TRUNC(snap_date) log_time
                                     , min(stat.snap_id) over (partition by stat.event_name) begin_snap_id
                                     , max(stat.snap_id) over (partition by stat.event_name) end_snap_id
                                     , par.name name
                                     , stat.instance_number
                                     , first_value(stat.time_waited_micro) over (partition by stat.event_name order by stat.snap_id) first_value
                                     , stat.time_waited_micro - lag(stat.time_waited_micro) over (partition by stat.event_name order by stat.snap_id) delta_value
                                     , stat.time_waited_micro real_value
                                FROM   py_awr_metrics_parameters                       par
                                     , dba_hist_system_event                           stat
                                WHERE  par.name             =    stat.event_name
                                AND    par.monitored        =    'Y'
                                AND    par.metric_type      = 'EVENT'
                                AND    par.name             like metric_name_lookup
                                AND    par.metric_type      like metric_type_lookup
                                AND    stat.snap_id         between min_snap_id AND max_snap_id
                                AND    stat.instance_number = instance
                                ORDER BY par.name
                                       , stat.snap_id
                                       , stat.instance_number
                             )
                        WHERE delta_value IS NOT NULL
                        GROUP BY log_time
                             , begin_snap_id
                             , end_snap_id
                             , name
                             , instance_number
                             , first_value
                        UNION ALL
                        SELECT log_time
                             , begin_snap_id
                             , end_snap_id
                             , name
                             , instance_number
                             , max(real_value) - first_value value
                             , min(delta_value)              daily_min
                             , avg(delta_value)              daily_avg
                             , max(delta_value)              daily_max
                        FROM (
                                SELECT /* Get System Event Wait Classes */
                                       TRUNC(snap_date) log_time
                                     , min(stat.snap_id) over (partition by stat.stat_name) begin_snap_id
                                     , max(stat.snap_id) over (partition by stat.stat_name) end_snap_id
                                     , par.name name
                                     , stat.instance_number
                                     , first_value(stat.value) over (partition by stat.stat_name order by stat.snap_id) first_value
                                     , stat.value - lag(stat.value) over (partition by stat.stat_name order by stat.snap_id) delta_value
                                     , stat.value real_value
                                FROM   py_awr_metrics_parameters                       par
                                     , (
                                         SELECT snap_id
                                              , wait_class                                        stat_name
                                              , instance_number
                                              , ROUND(SUM(NVL(time_waited_micro,0)) / 1000000, 2) value
                                         FROM   dba_hist_system_event
                                         WHERE  wait_class <> 'Idle'
                                         AND    snap_id BETWEEN min_snap_id AND max_snap_id
                                         AND    instance_number = instance
                                         GROUP BY snap_id
                                                , instance_number
                                                , wait_class
                                       )                                               stat
                                WHERE  par.name        =    stat.stat_name
                                AND    par.monitored   =    'Y'
                                AND    par.metric_type = 'WAIT CLASS'
                                AND    par.name        like metric_name_lookup
                                AND    par.metric_type like metric_type_lookup
                                ORDER BY par.name
                                       , stat.snap_id
                                       , stat.instance_number
                             )
                        WHERE delta_value IS NOT NULL
                        GROUP BY log_time
                             , begin_snap_id
                             , end_snap_id
                             , name
                             , instance_number
                             , first_value
                        UNION ALL
                        SELECT log_time
                             , begin_snap_id
                             , end_snap_id
                             , name
                             , instance_number
                             , max(real_value) - first_value value
                             , min(delta_value) daily_min
                             , avg(delta_value) daily_avg
                             , max(delta_value) daily_max
                        FROM (
                                SELECT /* Get CPU used */
                                       TRUNC(snap_date) log_time
                                     , min(stat.snap_id) over (partition by stat.stat_name) begin_snap_id
                                     , max(stat.snap_id) over (partition by stat.stat_name) end_snap_id
                                     , par.name name
                                     , stat.instance_number
                                     , first_value(stat.value) over (partition by stat.stat_name order by stat.snap_id) first_value
                                     , stat.value - lag(stat.value) over (partition by stat.stat_name order by stat.snap_id) delta_value
                                     , stat.value real_value
                                FROM   py_awr_metrics_parameters                       par
                                     , (
                                         SELECT snap_id
                                              , 'CPU Used'                             stat_name
                                              , instance_number
                                              , ROUND(SUM(NVL(value,0)) / 1000000, 2)  value
                                         FROM   dba_hist_sys_time_model
                                         WHERE  snap_id BETWEEN min_snap_id AND max_snap_id
                                         AND    instance_number = instance
                                         AND    LOWER(stat_name) like '%cpu%'
                                         GROUP BY snap_id
                                                , instance_number
                                       )                                               stat
                                WHERE  par.name        =    stat.stat_name
                                AND    par.monitored   =    'Y'
                                AND    par.metric_type = 'OS STAT'
                                AND    par.name        like metric_name_lookup
                                AND    par.metric_type like metric_type_lookup
                                ORDER BY par.name
                                       , stat.snap_id
                                       , stat.instance_number
                             )
                        WHERE delta_value IS NOT NULL
                        GROUP BY log_time
                             , begin_snap_id
                             , end_snap_id
                             , name
                             , instance_number
                             , first_value
                      )                                                        newdata
                ON (
                         newdata.log_time        = daily.log_time
                     and newdata.name            = daily.name
                     and newdata.instance_number = daily.instance_number
                   )
                WHEN NOT MATCHED THEN INSERT (
                          log_time
                        , begin_snap_id
                        , end_snap_id
                        , name
                        , instance_number
                        , counted
                        , value
                        , daily_min
                        , daily_avg
                        , daily_max
                ) VALUES (
                          newdata.log_time
                        , newdata.begin_snap_id
                        , newdata.end_snap_id
                        , newdata.name
                        , newdata.instance_number
                        , 'Y'
                        , newdata.value
                        , newdata.daily_min
                        , newdata.daily_avg
                        , newdata.daily_max
                )
                WHEN MATCHED THEN UPDATE
                SET end_snap_id = newdata.end_snap_id
                  , value       = NVL(daily.value,0) + NVL(newdata.value,0)
                  , daily_min   = LEAST(NVL(daily.value,0),NVL(newdata.value,0))
                  , daily_avg   = (NVL(daily.value,0)+NVL(newdata.value,0))/2
                  , daily_max   = GREATEST(NVL(daily.value,0),NVL(newdata.value,0))
                  , counted     = 'N'
                  , comments    = 'Instance bounced - metric values may be inaccurate';

                UPDATE py_awr_metrics_daily
                SET    startup_time = ( SELECT startup_time
                                        FROM   dba_hist_snapshot
                                        WHERE  snap_id         = max_snap_id
                                        AND    instance_number = instance
                                      )
                WHERE  end_snap_id = max_snap_id;

        END populate_daily;



        /* Procedure to clear down old history and move daily metrics to history */

        PROCEDURE populate_hist(
                  snap_date          DATE
                , metric_name_lookup VARCHAR2 DEFAULT '%'
                , metric_type_lookup VARCHAR2 DEFAULT '%'
        ) AS
        BEGIN
                /* Delete old data */
                DELETE py_awr_metrics_hist
                WHERE  log_time < sysdate - py_awr_monitor.DaysToKeep;

                DELETE py_awr_warnings_hist
                WHERE  log_time < sysdate - py_awr_monitor.DaysToKeep;

                /* Delete the specific data that is about to be inserted */

                DELETE py_awr_metrics_hist
                WHERE  TRUNC(log_time) =    TRUNC(snap_date)
                AND    name like metric_name_lookup
                AND    name in  ( SELECT name
                                  FROM   py_awr_metrics_parameters
                                  WHERE  metric_type like metric_type_lookup );

                /* Insert the data */

                INSERT INTO py_awr_metrics_hist
                SELECT *
                FROM   py_awr_metrics_daily
                WHERE  TRUNC(log_time) = TRUNC(snap_date);
        END populate_hist;


        /* Write the log file for checking by avail */

        PROCEDURE write_logs (
                  snap_date DATE
        ) AS
                LogFile      utl_file.file_type;
                Instance     VARCHAR2(16);
                JobNo        NUMBER;
                JobInterval  VARCHAR2(200);
                JobProg      VARCHAR2(4000);
                Location     VARCHAR2(4000);
        BEGIN
                SELECT LOWER(instance_name)
                INTO   Instance
                FROM   v$instance;

                SELECT directory_path
                INTO   Location
                FROM   all_directories
                WHERE  directory_name = LogDirectory
                AND    ROWNUM = 1;

                SELECT job
                     , what
                     , interval
                INTO   JobNo
                     , JobProg
                     , JobInterval
                FROM   user_jobs
                WHERE  what = 'py_awr_monitor.collect_metrics(sysdate - 1);'
                AND    rownum = 1;

                LogFile := utl_file.fopen(
                          LogDirectory
                        , 'awr_hist_'||Instance||'.log'
                        , 'w');

                -- write banner

                utl_file.put_line(LogFile, '################################################################################');
                utl_file.put_line(LogFile, '#');
                utl_file.put_line(LogFile, '# Executable   :- dba_jobs job = '||JobNo||', '||JobProg);
                utl_file.put_line(LogFile, '#');
                utl_file.put_line(LogFile, '# Run from     :- AVAIL@'||Instance||' dba jobs');
                utl_file.put_line(LogFile, '#');
                utl_file.put_line(LogFile, '# Run schedule :- '||JobInterval);
                utl_file.put_line(LogFile, '#');
                utl_file.put_line(LogFile, '# Log File     :- '||Location||'/awr_hist_'||Instance||'.log');
                utl_file.put_line(LogFile, '#');
                utl_file.put_line(LogFile, '# Contact      :- The Pythian Group');
                utl_file.put_line(LogFile, '#');
                utl_file.put_line(LogFile, '# Checked      :- Avail Daily Monitoring');
                utl_file.put_line(LogFile, '#');
                utl_file.put_line(LogFile, '# Version      :- 1.0');
                utl_file.put_line(LogFile, '#');
                utl_file.put_line(LogFile, '# Note         :-');
                utl_file.put_line(LogFile, '# Please check AVAIL.PY_AWR_METRICS_DAILY for system and database statistics for the day');
                utl_file.put_line(LogFile, '#          and AVAIL.PY_AWR_METRICS_HIST  for historical data.');
                utl_file.put_line(LogFile, '# You can also query and update the thresholds in the table');
                utl_file.put_line(LogFile, '#              AVAIL.PY_AWR_METRICS_PARAMETERS (default is 50(%)).');
                utl_file.put_line(LogFile, '#');
                utl_file.put_line(LogFile, '################################################################################');

                FOR WarnCurs IN (
                        SELECT  RPAD(warning_type,30,' ')
                              ||':  # Type - '
                              ||pamp.metric_type
                              ||' # Name(Instance) - '
                              ||pawh.name||'('||pawh.instance_number||')'
                              ||' # Value - '
                              ||CASE WHEN warning_type = 'TOTAL THRESHOLD EXCEEDED'
                                     THEN ROUND(pawh.value, 2)
                                     WHEN warning_type = 'DAILY MAX THRESHOLD EXCEEDED'
                                     THEN ROUND(pawh.daily_max, 2)
                                     WHEN warning_type = 'SPIKE THRESHOLD EXCEEDED'
                                     THEN ROUND(pawh.spike_value, 2)
                                     ELSE 0
                                END
                              ||CASE WHEN warning_type = 'TOTAL THRESHOLD EXCEEDED'
                                     THEN DECODE(avg_value,0,' ',' ('||ROUND(((pawh.value/pawh.avg_value)-1)*100)||'% above avg)')
                                     WHEN warning_type = 'DAILY MAX THRESHOLD EXCEEDED'
                                     THEN DECODE(daily_max,0,' ',' ('||ROUND(((pawh.daily_max/pawh.max_daily_max)-1)*100)||'% above max)')
                                     WHEN warning_type = 'SPIKE THRESHOLD EXCEEDED'
                                     THEN ''
                                     ELSE ''
                                END
                              ||' # Threshold - '
                              ||CASE WHEN warning_type = 'TOTAL THRESHOLD EXCEEDED'
                                     THEN ROUND(pawh.high_threshold, 2)
                                     WHEN warning_type = 'DAILY MAX THRESHOLD EXCEEDED'
                                     THEN ROUND(pawh.daily_max_threshold, 2)
                                     WHEN warning_type = 'SPIKE THRESHOLD EXCEEDED'
                                     THEN ROUND(pawh.spike_threshold, 2)
                                     ELSE 0
                                END
                              ||CASE WHEN warning_type = 'TOTAL THRESHOLD EXCEEDED'
                                     THEN DECODE(avg_value,0,' ',  ' ('||ROUND(((pawh.high_threshold/pawh.avg_value)-1)*100)||'% above avg)')
                                     WHEN warning_type = 'DAILY MAX THRESHOLD EXCEEDED'
                                     THEN DECODE(daily_max,0,' ',' ('||ROUND(((pawh.daily_max_threshold/pawh.max_daily_max)-1)*100)||'% above max)')
                                     WHEN warning_type = 'SPIKE THRESHOLD EXCEEDED'
                                     THEN ''
                                     ELSE ''
                                END
                                message
                        FROM    py_awr_warnings_hist      pawh
                              , py_awr_metrics_parameters pamp
                        WHERE   pawh.log_time = trunc(snap_date)
                        AND     pawh.name     = pamp.name
                ) LOOP
                        utl_file.put_line(LogFile,WarnCurs.message);
                END LOOP;

                -- close file
                utl_file.fflush(LogFile);
                utl_file.fclose(LogFile);
        END write_logs;

        /* Write the log file for checking by avail */

        FUNCTION write_logs (
                  snap_date DATE
        ) RETURN NUMBER 
        AS
                LogFile      utl_file.file_type;
                Instance     VARCHAR2(8);
                JobNo        NUMBER;
                JobInterval  VARCHAR2(200);
                JobProg      VARCHAR2(4000);
                Location     VARCHAR2(4000);
                LineCount    NUMBER := 0;
        BEGIN
                SELECT LOWER(instance_name)
                INTO   Instance
                FROM   v$instance;

                SELECT directory_path
                INTO   Location
                FROM   all_directories
                WHERE  directory_name = LogDirectory
                AND    ROWNUM = 1;

                SELECT job
                     , what
                     , interval
                INTO   JobNo
                     , JobProg
                     , JobInterval
                FROM   user_jobs
                WHERE  what = 'py_awr_monitor.collect_metrics(sysdate - 1);'
                AND    rownum = 1;

                LogFile := utl_file.fopen(
                          LogDirectory
                        , 'awr_hist_'||Instance||'.log'
                        , 'W');

                -- write banner

                utl_file.put_line(LogFile, '################################################################################');
                utl_file.put_line(LogFile, '#');
                utl_file.put_line(LogFile, '# Executable   :- dba_jobs job = '||JobNo||', '||JobProg);
                utl_file.put_line(LogFile, '#');
                utl_file.put_line(LogFile, '# Run from     :- AVAIL@'||Instance||' dba jobs');
                utl_file.put_line(LogFile, '#');
                utl_file.put_line(LogFile, '# Run schedule :- '||JobInterval);
                utl_file.put_line(LogFile, '#');
                utl_file.put_line(LogFile, '# Log File     :- '||Location||'/awr_hist_'||Instance||'.log');
                utl_file.put_line(LogFile, '#');
                utl_file.put_line(LogFile, '# Contact      :- The Pythian Group');
                utl_file.put_line(LogFile, '#');
                utl_file.put_line(LogFile, '# Checked      :- Avail Daily Monitoring');
                utl_file.put_line(LogFile, '#');
                utl_file.put_line(LogFile, '# Version      :- 1.0');
                utl_file.put_line(LogFile, '#');
                utl_file.put_line(LogFile, '# Note         :-');
                utl_file.put_line(LogFile, '# Please check AVAIL.PY_AWR_METRICS_DAILY for system and database statistics for the day');
                utl_file.put_line(LogFile, '#          and AVAIL.PY_AWR_METRICS_HIST  for historical data.');
                utl_file.put_line(LogFile, '# You can also query and update the thresholds in the table');
                utl_file.put_line(LogFile, '#              AVAIL.PY_AWR_METRICS_PARAMETERS (default is 50(%)).');
                utl_file.put_line(LogFile, '#');
                utl_file.put_line(LogFile, '################################################################################');

                FOR WarnCurs IN (
                        SELECT  RPAD(warning_type,30,' ')
                              ||':  # Type - '
                              ||pamp.metric_type
                              ||' # Name(Instance) - '
                              ||pawh.name||'('||pawh.instance_number||')'
                              ||' # Value - '
                              ||CASE WHEN warning_type = 'TOTAL THRESHOLD EXCEEDED'
                                     THEN ROUND(pawh.value, 2)
                                     WHEN warning_type = 'DAILY MAX THRESHOLD EXCEEDED'
                                     THEN ROUND(pawh.daily_max, 2)
                                     WHEN warning_type = 'SPIKE THRESHOLD EXCEEDED'
                                     THEN ROUND(pawh.spike_value, 2)
                                     ELSE 0
                                END
                              ||CASE WHEN warning_type = 'TOTAL THRESHOLD EXCEEDED'
                                     THEN DECODE(avg_value,0,' ',' ('||ROUND(((pawh.value/pawh.avg_value)-1)*100)||'% above avg)')
                                     WHEN warning_type = 'DAILY MAX THRESHOLD EXCEEDED'
                                     THEN DECODE(daily_max,0,' ',' ('||ROUND(((pawh.daily_max/pawh.max_daily_max)-1)*100)||'% above max)')
                                     WHEN warning_type = 'SPIKE THRESHOLD EXCEEDED'
                                     THEN ''
                                     ELSE ''
                                END
                              ||' # Threshold - '
                              ||CASE WHEN warning_type = 'TOTAL THRESHOLD EXCEEDED'
                                     THEN ROUND(pawh.high_threshold, 2)
                                     WHEN warning_type = 'DAILY MAX THRESHOLD EXCEEDED'
                                     THEN ROUND(pawh.daily_max_threshold, 2)
                                     WHEN warning_type = 'SPIKE THRESHOLD EXCEEDED'
                                     THEN ROUND(pawh.spike_threshold, 2)
                                     ELSE 0
                                END
                              ||CASE WHEN warning_type = 'TOTAL THRESHOLD EXCEEDED'
                                     THEN DECODE(avg_value,0,' ',  ' ('||ROUND(((pawh.high_threshold/pawh.avg_value)-1)*100)||'% above avg)')
                                     WHEN warning_type = 'DAILY MAX THRESHOLD EXCEEDED'
                                     THEN DECODE(daily_max,0,' ',' ('||ROUND(((pawh.daily_max_threshold/pawh.max_daily_max)-1)*100)||'% above max)')
                                     WHEN warning_type = 'SPIKE THRESHOLD EXCEEDED'
                                     THEN ''
                                     ELSE ''
                                END
                                message
                        FROM    py_awr_warnings_hist      pawh
                              , py_awr_metrics_parameters pamp
                        WHERE   pawh.log_time = trunc(snap_date)
                        AND     pawh.name     = pamp.name
                ) LOOP
                        utl_file.put_line(LogFile,WarnCurs.message);
                        LineCount := LineCount + 1;
                END LOOP;

                -- close file
                utl_file.fflush(LogFile);
                utl_file.fclose(LogFile);

                RETURN LineCount;
        END write_logs;



        PROCEDURE email_logs
        AS
                LogFile        utl_file.file_type;
                Instance       VARCHAR2(8);
                HostName       VARCHAR2(64);
                TextOut        VARCHAR2(32000);
                EmailText      VARCHAR2(32000);
                EmailAddresses VARCHAR2(2000) := NULL ;
        BEGIN 
                /* Get e-mail addresses */

                FOR EMailRecipientsRec IN (
                        SELECT email_address
                        FROM   py_awr_email_recipients )
                LOOP
                        IF EmailAddresses IS NULL 
                        THEN
                                EmailAddresses := EMailRecipientsRec.email_address;
                        ELSE
                                EmailAddresses := EmailAddresses||','||EMailRecipientsRec.email_address;
                        END IF;
                END LOOP;


                /* If there are no e-mail addresses then do nothing */

                IF EmailAddresses IS NOT NULL
                THEN
                        SELECT LOWER(instance_name)
                             , host_name
                        INTO   Instance
                             , HostName
                        FROM   v$instance;

                        LogFile := UTL_FILE.FOPEN(
                                  LogDirectory
                                , 'awr_hist_'||Instance||'.log'
                                , 'r');

                        /* Get contents of file */

                        LOOP
                                BEGIN
                                        UTL_FILE.GET_LINE(LogFile,TextOut);

                                        EmailText:=EmailText||TextOut||CHR(10);
                                EXCEPTION
                                        WHEN NO_DATA_FOUND THEN EXIT;
                                END;
                        END LOOP;    

                        UTL_FILE.FCLOSE(LogFile);

                        /* Send the e-mail */

                        UTL_MAIL.SEND( 
                                sender     => Instance||'@'||HostName
                              , recipients => EmailAddresses
                              , subject    => 'AWR Monitor Report - '||Instance 
                              , message    => EmailText
                        ); 
                END IF;
        END email_logs;

END py_awr_monitor;
/

