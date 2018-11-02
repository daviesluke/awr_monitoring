CREATE OR REPLACE PACKAGE BODY py_stats_monitor
AS

	/* Truncate the tables and set up the parameters table with default values */

	PROCEDURE initialize
	AS
	BEGIN
		EXECUTE IMMEDIATE 'TRUNCATE TABLE py_stats_metrics_daily';
		EXECUTE IMMEDIATE 'TRUNCATE TABLE py_stats_metrics_hist';
		EXECUTE IMMEDIATE 'TRUNCATE TABLE py_stats_warnings_hist';
		EXECUTE IMMEDIATE 'DELETE py_stats_metrics_parameters';

		INSERT INTO py_stats_metrics_parameters (
			  name
			, metric_type
			, monitored
			, pct_threshold
			, who
			, updated
		) SELECT 
			  DISTINCT name
			, 'STATISTIC'
			, DECODE(  name
			         , 'table scans (long tables)'    , 'Y'
				 , 'table scans (short tables)'   , 'Y'
				 , 'index fast full scans (full)' , 'Y'
				 , 'sorts (disk)'                 , 'Y'
				                                  , 'N'
				)
			, DECODE(  name
			         , 'table scans (long tables)'    , 50
				 , 'table scans (short tables)'   , 50
				 , 'index fast full scans (full)' , 50
				 , 'sorts (disk)'                 , 50
				                                  , NULL
			        )
			, 'auto'
			, SYSDATE
		  FROM    stats$sysstat;

		  INSERT INTO py_stats_metrics_parameters (
			  name
			, metric_type
			, monitored
			, pct_threshold
			, who
			, updated
		  ) SELECT 
			  DECODE(  POWER(2,ROWNUM-1)
			         , 1   , 'User'
			         , 2   , 'Redo'
			         , 4   , 'Enqueue'
			         , 8   , 'Cache'
			         , 16  , 'Parallel Server'
			         , 32  , 'OS'
			         , 64  , 'SQL'
			         , 128 , 'Debug'
			         ,       'Unknown'
			        )
			  END
			  , 'STAT CLASS'
			  , 'Y'
			  , 50
			  , 'auto'
			  , SYSDATE
		    FROM dual
		    CONNECT BY LEVEL <= 8;

		INSERT INTO py_stats_metrics_parameters (
			  name
			, metric_type
			, monitored
			, who
			, updated
		) SELECT 
			  DISTINCT event
			, 'EVENT'
			, 'N'
			, 'auto'
			, SYSDATE
		  FROM   stats$system_event;

		INSERT INTO py_stats_metrics_parameters (
			  name
			, metric_type
			, monitored
			, pct_threshold
			, who
			, updated
		) SELECT 
			  DISTINCT wait_class
			, 'WAIT CLASS'
			, 'Y'
			, 50
			, 'auto'
			, SYSDATE
		  FROM    v$event_name
		  WHERE   wait_class <> 'Idle';

		INSERT INTO py_stats_metrics_parameters (
			  name
			, metric_type
			, monitored
			, pct_threshold
			, who
			, updated
		) VALUES (
			  'CPU Used'
			, 'OS STAT'
			, 'Y'
			, 50
			, 'auto'
			, SYSDATE
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
			SELECT DISTINCT TRUNC(snap_time) snap_date
			FROM   stats$snapshot
			MINUS
			SELECT DISTINCT TRUNC(psmh.log_time)
			FROM   py_stats_metrics_hist       psmh
			     , py_stats_metrics_parameters psmp
			WHERE  psmh.name        like metric_name_lookup
			AND    psmh.name        =    psmp.name
			AND    psmp.metric_type like metric_type_lookup
		) LOOP
			collect_metrics(
				  snap_date          => DateCurs.snap_date
				, do_report          => FALSE
				, metric_name_lookup => metric_name_lookup
				, metric_type_lookup => metric_type_lookup
			);
		END LOOP;
	END load_old_data;


	/* Procedure to check the daily stats against the threshold and write results to the warnings table */

	PROCEDURE check_daily_stats (
		  snap_date DATE
		, instance  NUMBER DEFAULT 1
	) AS
	BEGIN
		DELETE py_stats_warnings_hist
		WHERE  log_time = TRUNC(snap_date)
		AND    instance_number = Instance;

		INSERT INTO py_stats_warnings_hist (
			  log_time
			, name
			, instance_number
			, value
			, min_value
			, avg_value
			, max_value
			, high_threshold
		) SELECT 
			  hist.log_time
			, hist.name
			, hist.instance_number
			, hist.value
			, avgs.min_value
			, avgs.avg_value
			, avgs.max_value
			, avgs.high_threshold
		  FROM    py_stats_metrics_hist  hist
		        , py_stats_avgs          avgs
		  WHERE   hist.name             = avgs.name
		  AND     hist.instance_number  = avgs.instance_number
		  AND     hist.instance_number  = Instance
		  AND     TRUNC(hist.log_time)  = trunc(snap_date)
		  AND     hist.value            > avgs.high_threshold;
	  
	END check_daily_stats;


	/* This procedure is designed to be run once a day */

	PROCEDURE collect_metrics (
		  snap_date          DATE
		, do_report          BOOLEAN  DEFAULT TRUE
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
		FROM   stats$snapshot
		WHERE  TRUNC(snap_time) = TRUNC(snap_date);

		/* If no count then no snapshots have occurred if this is the case then exit */
		
		IF Counter = 0 
		THEN
	  		RETURN;
		END IF;

		/* Get the first snap shot of the next day */
		
		SELECT NVL(MIN(snap_id), -1) 
		INTO   NextMinSnapID
		FROM   stats$snapshot
		WHERE  TRUNC(snap_time) = TRUNC(snap_date + 1);

		/* Get rid of the current collections in daily table */
		
		DELETE py_stats_metrics_daily;

		/* Let's loop through any instances */
                
		FOR InstanceCurs IN ( SELECT DISTINCT instance_number
				      FROM   stats$snapshot
				      WHERE  TRUNC(snap_time) = TRUNC(snap_date)
		) LOOP        
			/* Check there is a next day snap */
			
			IF NextMinSnapID <> -1 
			THEN
				/* Check there has not been a bounce of the DB between the max snap id obtained and the next one */
				
				SELECT COUNT(DISTINCT startup_time) 
				INTO   Counter2
				FROM   stats$snapshot
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
					FROM   stats$snapshot
					WHERE  TRUNC(snap_time) = TRUNC(snap_date)
					AND    instance_number = InstanceCurs.instance_number 
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
				  FROM   py_stats_metrics_hist
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
			write_logs(snap_date);
		END IF;
	  	
	  	/* All this is done in one big transaction to maintain data integrity */
	  	
	  	COMMIT;
	END collect_metrics;
	

	/* Procedure to enable gathering of history of a metric */
	
	PROCEDURE enable_metric (
		  metric_name_lookup VARCHAR2 
		, metric_type_lookup VARCHAR2 
		, pct_threshold_IN   NUMBER   DEFAULT 50
	) AS
	BEGIN
		UPDATE py_stats_metrics_parameters
		SET    monitored      = 'Y'
		     , updated        = SYSDATE
		     , who            = USER
		     , pct_threshold  = pct_threshold_IN
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
		UPDATE py_stats_metrics_parameters
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
		WHERE  what like 'py_stats_monitor.collect_metrics%';

		IF jobcount >= 1 
		THEN
			raise_application_error(-20101, 'Job already exists, remove it first.');
		END IF;
		
		dbms_job.submit(
			  job       => JobNo
			, what      => 'py_stats_monitor.collect_metrics(sysdate - 1);'
			, next_date => TRUNC(SYSDATE + 1) + 1/24
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
			WHERE  what like 'py_stats_monitor.collect_metrics%'
		) LOOP
			dbms_job.remove(JobCurs.job);
			
			COMMIT;
		END LOOP;
	END disable_monitoring;


	/* Procedure to delete historical stats */
	
	PROCEDURE delete_stats (
		  snap_date           DATE
		, metric_name_lookup  VARCHAR2 DEFAULT '%'
		, metric_type_lookup  VARCHAR2 DEFAULT '%'
	) AS
	BEGIN
		DELETE py_stats_metrics_hist
		WHERE  log_time    =    TRUNC(snap_date)
		AND    name        LIKE metric_name_lookup
		AND    name in   ( SELECT name
		                   FROM   py_stats_metrics_parameters
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
		UPDATE py_stats_metrics_hist
		SET    counted = 'Y'
		WHERE  log_time    =    TRUNC(snap_date)
		AND    name        like metric_name_lookup
		AND    name in   ( SELECT name
		                   FROM   py_stats_metrics_parameters
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
		UPDATE py_stats_metrics_hist
		SET    counted = 'N'
		WHERE  log_time    =    TRUNC(snap_date)
		AND    name        like metric_name_lookup
		AND    name in   ( SELECT name
		                   FROM   py_stats_metrics_parameters
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
		MERGE INTO py_stats_metrics_daily daily
		USING (
			SELECT /* Get System Stats */
			       TRUNC(snap_date)                                log_time
			     , min_snap_id                                     begin_snap_id
			     , max_snap_id                                     end_snap_id
			     , par.name                                        name
			     , instance                                        instance_number
			     , 'Y'                                             counted
			     , maxstat.value - minstat.value                   value
			FROM   py_stats_metrics_parameters                     par
			     , ( 
			         SELECT name                                   stat_name
			              , NVL(value,0)                           value
			         FROM   stats$sysstat
			         WHERE  snap_id = min_snap_id 
			         AND    instance_number = instance
			       )                                               minstat
			     , ( 
			         SELECT name                                   stat_name
			              , NVL(value,0)                           value
			         FROM   stats$sysstat
			         WHERE  snap_id = max_snap_id 
			         AND    instance_number = instance
			       )                                               maxstat
			WHERE  par.name        =    maxstat.stat_name
			AND    par.name        =    minstat.stat_name
			AND    par.monitored   =    'Y'
			AND    par.metric_type =    'STATISTIC'
			AND    par.name        like metric_name_lookup
			AND    par.metric_type like metric_type_lookup
			UNION ALL
			SELECT /* Get System Stat Class */
			       TRUNC(snap_date)
			     , min_snap_id
			     , max_snap_id
			     , par.name
			     , instance
			     , 'Y'
			     , maxstat.value - minstat.value
			FROM   py_stats_metrics_parameters                       par
			     , ( 
			         SELECT CASE BITAND( class.bit_mask, vs.class)
			                     WHEN   1   THEN 'User'
			                     WHEN   2   THEN 'Redo'
			                     WHEN   4   THEN 'Enqueue'
			                     WHEN   8   THEN 'Cache'
			                     WHEN  16   THEN 'Parallel Server'
			                     WHEN  32   THEN 'OS'
			                     WHEN  64   THEN 'SQL'
			                     WHEN 128   THEN 'Debug'
			                END                                    stat_name
			              , SUM(NVL(stat.value,0))                 value
			         FROM   stats$sysstat                          stat
			              , v$statname                             vs
			              , ( SELECT POWER(2,ROWNUM-1)             bit_mask
			                  FROM   DUAL
			                  CONNECT BY LEVEL <= 8 )              class
			         WHERE  stat.snap_id                      =  min_snap_id
			         AND    stat.instance_number              =  instance
			         AND    stat.statistic#                   =  vs.statistic#
			         AND    BITAND( class.bit_mask, vs.class) != 0
			         GROUP BY BITAND( class.bit_mask, vs.class)
			       )                                               minstat
			     , ( 
			         SELECT CASE BITAND( class.bit_mask, vs.class)
			                     WHEN   1   THEN 'User'
			                     WHEN   2   THEN 'Redo'
			                     WHEN   4   THEN 'Enqueue'
			                     WHEN   8   THEN 'Cache'
			                     WHEN  16   THEN 'Parallel Server'
			                     WHEN  32   THEN 'OS'
			                     WHEN  64   THEN 'SQL'
			                     WHEN 128   THEN 'Debug'
			                END                                    stat_name
			              , SUM(NVL(stat.value,0))                 value
			         FROM   stats$sysstat                          stat
			              , v$statname                             vs
			              , ( SELECT POWER(2,ROWNUM-1)             bit_mask
			                  FROM   DUAL
			                  CONNECT BY LEVEL <= 8 )              class
			         WHERE  stat.snap_id                      =  min_snap_id
			         AND    stat.instance_number              =  instance
			         AND    stat.statistic#                   =  vs.statistic#
			         AND    BITAND( class.bit_mask, vs.class) != 0
			         GROUP BY BITAND( class.bit_mask, vs.class)
			       )                                               maxstat
			WHERE  par.name        =    maxstat.stat_name
			AND    par.name        =    minstat.stat_name
			AND    par.monitored   =    'Y'
			AND    par.metric_type =    'STAT CLASS'
			AND    par.name        like metric_name_lookup
			AND    par.metric_type like metric_type_lookup
			UNION ALL
			SELECT /* Get System Events */
			       TRUNC(snap_date)
			     , min_snap_id
			     , max_snap_id
			     , par.name
			     , instance
			     , 'Y'
			     , maxstat.value - minstat.value
			FROM   py_stats_metrics_parameters                       par
			     , ( 
			         SELECT event                                  stat_name
			              , NVL(time_waited_micro,0)               value
			         FROM   stats$system_event
			         WHERE  snap_id = min_snap_id
			         AND    instance_number = instance
			       )                                               minstat
			     , ( 
			         SELECT event                                  stat_name
			              , NVL(time_waited_micro,0)               value
			         FROM   stats$system_event
			         WHERE  snap_id = max_snap_id
			         AND    instance_number = instance
			       )                                               maxstat
			WHERE  par.name        =    maxstat.stat_name
			AND    par.name        =    minstat.stat_name
			AND    par.monitored   =    'Y'
			AND    par.metric_type =    'EVENT'
			AND    par.name        like metric_name_lookup
			AND    par.metric_type like metric_type_lookup
			UNION ALL
			SELECT TRUNC(snap_date) log_time
			     , min_snap_id
			     , max_snap_id
			     , par.name
			     , instance
			     , 'Y'
			     , maxstat.value - minstat.value
			FROM   py_stats_metrics_parameters                       par
			     , (
			         SELECT vev.wait_class                                        stat_name
			              , ROUND(SUM(NVL(sev.time_waited_micro,0)) / 1000000, 2) value
			         FROM   stats$system_event                                    sev
			              , v$event_name                                          vev
				 WHERE  sev.snap_id         = min_snap_id
				 AND    sev.event           = vev.name
			         AND    sev.instance_number = instance
				 AND    vev.wait_class     <> 'Idle'
				 GROUP BY vev.wait_class
			       )                                               minstat
			     , (
			         SELECT vev.wait_class                                        stat_name
			              , ROUND(SUM(NVL(sev.time_waited_micro,0)) / 1000000, 2) value
			         FROM   stats$system_event                                    sev
			              , v$event_name                                          vev
				 WHERE  sev.snap_id         = max_snap_id
				 AND    sev.event           = vev.name
			         AND    sev.instance_number = instance
				 AND    vev.wait_class     <> 'Idle'
				 GROUP BY vev.wait_class
			       )                                               maxstat
			WHERE  par.name        =    maxstat.stat_name
			AND    par.name        =    minstat.stat_name
			AND    par.monitored   =    'Y'
			AND    par.metric_type =    'WAIT CLASS'
			AND    par.name        like metric_name_lookup
			AND    par.metric_type like metric_type_lookup
			UNION ALL
			SELECT TRUNC(snap_date) log_time
			     , min_snap_id
			     , max_snap_id
			     , par.name
			     , instance
			     , 'Y'
			     , maxstat.value - minstat.value
			FROM   py_stats_metrics_parameters                       par
			     , (
			         SELECT 'CPU Used'                                   stat_name
			              , ROUND(SUM(NVL(stmod.value,0)) / 1000000, 2)  value
			         FROM   stats$sys_time_model                   stmod
			              , stats$time_model_statname              tmodstat
				 WHERE  stmod.snap_id             =    min_snap_id
				 AND    stmod.stat_id             =    tmodstat.stat_id
			         AND    stmod.instance_number     =    instance
				 AND    LOWER(tmodstat.stat_name) like '%cpu%'
			       )                                               minstat
			     , (
			         SELECT 'CPU Used'                                   stat_name
			              , ROUND(SUM(NVL(stmod.value,0)) / 1000000, 2)  value
			         FROM   stats$sys_time_model                   stmod
			              , stats$time_model_statname              tmodstat
				 WHERE  stmod.snap_id             =    max_snap_id
				 AND    stmod.stat_id             =    tmodstat.stat_id
			         AND    stmod.instance_number     =    instance
				 AND    LOWER(tmodstat.stat_name) like '%cpu%'
			       )                                               maxstat
			WHERE  par.name        =    maxstat.stat_name
			AND    par.name        =    minstat.stat_name
			AND    par.monitored   =    'Y'
			AND    par.metric_type =    'OS STAT'
			AND    par.name        like metric_name_lookup
			AND    par.metric_type like metric_type_lookup
		      )                                                        newdata
		ON ( 
		         newdata.log_time        = daily.log_time 
		     AND newdata.name            = daily.name
		     AND newdata.instance_number = daily.instance_number
		   )
		WHEN NOT MATCHED THEN INSERT ( 
			  log_time
			, begin_snap_id
			, end_snap_id
			, name
			, instance_number
			, counted
			, value
		) VALUES ( 
		            newdata.log_time
		          , newdata.begin_snap_id
		          , newdata.end_snap_id
		          , newdata.name
		          , newdata.instance_number
		          , newdata.counted
		          , newdata.value
		)
		WHEN MATCHED THEN UPDATE
		SET end_snap_id = newdata.end_snap_id
		  , value       = NVL(daily.value,0) + NVL(newdata.value,0)
		  , counted     = 'N'
		  , comments    = 'Instance bounced - metric values may be inaccurate';

		UPDATE py_stats_metrics_daily
		SET    startup_time = ( SELECT startup_time 
		                        FROM   stats$snapshot
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
		DELETE py_stats_metrics_hist
		WHERE  log_time < sysdate - py_stats_monitor.DaysToKeep;
		
		DELETE py_stats_warnings_hist
		WHERE  log_time < sysdate - py_stats_monitor.DaysToKeep;

		/* Delete the specific data that is about to be inserted */
		
		DELETE py_stats_metrics_hist
		WHERE  TRUNC(log_time) =    TRUNC(snap_date)
		AND    name like metric_name_lookup
		AND    name in  ( SELECT name
		                  FROM   py_stats_metrics_parameters
		                  WHERE  metric_type like metric_type_lookup );
		
		/* Insert the data */
		
		INSERT INTO py_stats_metrics_hist
		SELECT * 
		FROM   py_stats_metrics_daily
		WHERE  TRUNC(log_time) = TRUNC(snap_date);		
	END populate_hist;

	/* Write the log file for checking by avail */
	
	PROCEDURE write_logs (
		  snap_date DATE
	) AS
		LogFile      utl_file.file_type;
		Instance     VARCHAR2(12);
		UserName     VARCHAR2(30);
		JobNo        NUMBER;
		JobInterval  VARCHAR2(200);
		JobProg      VARCHAR2(4000);
		Location     VARCHAR2(4000);
	BEGIN
		SELECT LOWER(instance_name)
		     , user 
		INTO   Instance 
		     , UserName
		FROM   v$instance;

		SELECT directory_path 
		INTO   Location
		FROM   all_directories
		WHERE  directory_name = 'LOG_FILES_DIR'
		AND    ROWNUM = 1;

		SELECT job
		     , what
		     , interval 
		INTO   JobNo
		     , JobProg
		     , JobInterval
		FROM   user_jobs
		WHERE  what = 'py_stats_monitor.collect_metrics(sysdate - 1);'
		AND    rownum = 1;

		LogFile := utl_file.fopen(
			  'LOG_FILES_DIR'
			, 'stats_hist_'||Instance||'.log'
			, 'W');
			
		-- write banner
		
		utl_file.put_line(LogFile, '################################################################################');
		utl_file.put_line(LogFile, '#');
		utl_file.put_line(LogFile, '# Executable   :- dba_jobs job = '||JobNo||', '||JobProg);
		utl_file.put_line(LogFile, '#');
		utl_file.put_line(LogFile, '# Run from     :- '||UserName||'@'||Instance||' dba jobs');
		utl_file.put_line(LogFile, '#');
		utl_file.put_line(LogFile, '# Run schedule :- '||JobInterval);
		utl_file.put_line(LogFile, '#');
		utl_file.put_line(LogFile, '# Log File     :- '||Location||'/stats_hist_'||Instance||'.log');
		utl_file.put_line(LogFile, '#');
		utl_file.put_line(LogFile, '# Contact      :- The Pythian Group');
		utl_file.put_line(LogFile, '#');
		utl_file.put_line(LogFile, '# Checked      :- Avail Daily Monitoring');
		utl_file.put_line(LogFile, '#');
		utl_file.put_line(LogFile, '# Version      :- 1.0');
		utl_file.put_line(LogFile, '#');
		utl_file.put_line(LogFile, '# Note         :-');
		utl_file.put_line(LogFile, '# Please check '||UserName||'.PY_STATS_METRICS_DAILY for system and database statistics for the day');
		utl_file.put_line(LogFile, '#          and '||UserName||'.PY_STATS_METRICS_HIST  for historical data.');
		utl_file.put_line(LogFile, '# You can also query and update the thresholds in the table');
		utl_file.put_line(LogFile, '#              '||UserName||'.PY_STATS_METRICS_PARAMETERS (default is 50(%)).');
		utl_file.put_line(LogFile, '#');
		utl_file.put_line(LogFile, '################################################################################');

		FOR WarnCurs IN ( 
			SELECT  'WARNING:  # Type - '
			      ||par.metric_type
			      ||' # Name(Instance) - '
			      ||warn.name||'('||warn.instance_number||')'
			      ||' # Value - '
			      ||ROUND(warn.value, 2)
			      ||DECODE(warn.avg_value,0,' ',  ' ('
			                               ||ROUND(((warn.value/warn.avg_value)-1)*100)
			                               ||'% above avg)')
			      ||' # Threshold - '
			      ||ROUND(warn.high_threshold, 2)
			      ||DECODE(warn.avg_value,0,' ',  ' ('
			                               ||ROUND(((warn.high_threshold/warn.avg_value)-1)*100) 
			                               ||'% above avg)')
			        message
			FROM    py_stats_warnings_hist      warn
			      , py_stats_metrics_parameters par
			WHERE   warn.log_time = trunc(snap_date)
			AND     warn.name     = par.name
		) LOOP
			utl_file.put_line(LogFile,WarnCurs.message);
		END LOOP;
		
		-- close file
		utl_file.fflush(LogFile);
		utl_file.fclose(LogFile);
	END write_logs;

end PY_STATS_MONITOR;
/

