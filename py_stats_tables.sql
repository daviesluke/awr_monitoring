/* Main Metrics table */

CREATE TABLE py_stats_metrics_parameters (
	  name          VARCHAR2(64 BYTE) NOT NULL
	, metric_type   VARCHAR2(10 BYTE) NOT NULL
	, monitored     CHAR(1 BYTE)      NOT NULL
	, pct_threshold NUMBER
	, updated	DATE
	, who           VARCHAR2(30 BYTE) NOT NULL
)
/

ALTER TABLE py_stats_metrics_parameters
  ADD CHECK (monitored IN ('Y', 'N'))
/
  
ALTER TABLE py_stats_metrics_parameters
  ADD PRIMARY KEY (name)
/


/* Metrics history table */

CREATE TABLE py_stats_metrics_hist (
	  log_time        DATE              NOT NULL
	, begin_snap_id   NUMBER            NOT NULL
	, end_snap_id     NUMBER            NOT NULL
	, name            VARCHAR2(64 BYTE) NOT NULL
        , instance_number NUMBER            NOT NULL
	, counted         CHAR(1 BYTE)      NOT NULL
	, value           NUMBER
	, startup_time    DATE 
	, comments        VARCHAR2(4000 BYTE)
)
/
 
ALTER TABLE py_stats_metrics_hist 
  ADD CHECK (counted IN ('Y', 'N'))
/
  
ALTER TABLE py_stats_metrics_hist 
  ADD PRIMARY KEY (log_time, name, instance_number)
/

ALTER TABLE py_stats_metrics_hist
  ADD FOREIGN KEY (name) REFERENCES py_stats_metrics_parameters (name)
/


/* Metrics daily table */

CREATE TABLE py_stats_metrics_daily (
	  log_time        DATE               NOT NULL 
	, begin_snap_id   NUMBER             NOT NULL 
	, end_snap_id     NUMBER             NOT NULL 
	, name            VARCHAR2(64 BYTE)  NOT NULL 
	, instance_number NUMBER             NOT NULL
	, counted         CHAR(1 BYTE)       NOT NULL 
	, value           NUMBER
	, startup_time    DATE
	, comments        VARCHAR2(4000 BYTE)
)
/

ALTER TABLE py_stats_metrics_daily 
  ADD CHECK (counted IN ('Y', 'N'))
/

ALTER TABLE py_stats_metrics_daily 
  ADD PRIMARY KEY (log_time, name, instance_number)
/

ALTER TABLE py_stats_metrics_daily
  ADD FOREIGN KEY (name) REFERENCES py_stats_metrics_parameters (name)
/


/* Metrics warning table */

CREATE TABLE py_stats_warnings_hist (
	  log_time        DATE                NOT NULL
	, name            VARCHAR2(64 BYTE)   NOT NULL
	, instance_number NUMBER              NOT NULL
	, value           NUMBER
	, min_value       NUMBER
	, avg_value       NUMBER
	, max_value       NUMBER
	, high_threshold  NUMBER
	, comments        VARCHAR2(4000 BYTE)
)
/

ALTER TABLE py_stats_warnings_hist
  ADD PRIMARY KEY (log_time, name, instance_number)
/

ALTER TABLE py_stats_warnings_hist
  ADD FOREIGN KEY (name) REFERENCES py_stats_metrics_parameters (name)
/
  
-- View

CREATE OR REPLACE FORCE VIEW py_stats_avgs (
	   name
	 , instance_number
	 , updated
	 , pct_threshold
	 , min_value
	 , avg_value
	 , max_value
	 , high_threshold
) AS SELECT
	   par.name
	 , hist.instance_number
	 , par.updated
	 , par.pct_threshold
	 , min(hist.value)
	 , avg(hist.value)                                           
	 , max(hist.value)
	 , avg(hist.value) * ( 1 + nvl(par.pct_threshold, 0) / 100 ) 
     FROM  py_stats_metrics_parameters par
         , py_stats_metrics_hist       hist
     WHERE par.name        = hist.name(+)
     AND   par.monitored   = 'Y'
     AND   hist.counted(+) = 'Y'
     GROUP BY par.name
            , hist.instance_number
            , par.updated
            , par.pct_threshold
     ORDER BY par.name
            , hist.instance_number
/

