/* Main Metrics table */

CREATE TABLE py_awr_metrics_parameters (
          name            VARCHAR2(64 BYTE) NOT NULL
        , metric_type     VARCHAR2(10 BYTE) NOT NULL
        , monitored       CHAR(1 BYTE)      NOT NULL
        , who             VARCHAR2(30 BYTE) NOT NULL
        , pct_threshold   NUMBER
        , amt_threshold   NUMBER
        , spike_threshold NUMBER
        , updated         DATE
)
/

ALTER TABLE py_awr_metrics_parameters
  ADD CHECK (monitored IN ('Y', 'N'))
/

ALTER TABLE py_awr_metrics_parameters
  ADD PRIMARY KEY (name)
/


/* Metrics history table */

CREATE TABLE py_awr_metrics_hist (
          log_time         DATE                NOT NULL
        , begin_snap_id    NUMBER              NOT NULL
        , end_snap_id      NUMBER              NOT NULL
        , name             VARCHAR2(64 BYTE)   NOT NULL
        , instance_number  NUMBER              NOT NULL
        , counted          CHAR(1 BYTE)        NOT NULL
        , value            NUMBER
        , startup_time     DATE
        , daily_min        NUMBER
        , daily_avg        NUMBER
        , daily_max        NUMBER
        , comments         VARCHAR2(4000 BYTE)
)
/

ALTER TABLE py_awr_metrics_hist
  ADD CHECK (counted IN ('Y', 'N'))
/

ALTER TABLE py_awr_metrics_hist
  ADD PRIMARY KEY (log_time, name, instance_number)
/

ALTER TABLE py_awr_metrics_hist
  ADD FOREIGN KEY (name) REFERENCES py_awr_metrics_parameters (name)
/


/* Metrics daily table */

CREATE TABLE py_awr_metrics_daily (
          log_time        DATE               NOT NULL
        , begin_snap_id   NUMBER             NOT NULL
        , end_snap_id     NUMBER             NOT NULL
        , name            VARCHAR2(64 BYTE)  NOT NULL
        , instance_number NUMBER
        , counted         CHAR(1 BYTE)       NOT NULL
        , value           NUMBER
        , startup_time    DATE
        , daily_min       NUMBER
        , daily_avg       NUMBER
        , daily_max       NUMBER
        , comments        VARCHAR2(4000 BYTE)
)
/

ALTER TABLE py_awr_metrics_daily
  ADD CHECK (counted IN ('Y', 'N'))
/

ALTER TABLE py_awr_metrics_daily
  ADD PRIMARY KEY (log_time, name, instance_number)
/

ALTER TABLE py_awr_metrics_daily
  ADD FOREIGN KEY (name) REFERENCES py_awr_metrics_parameters (name)
/


/* Metrics warning table */

CREATE TABLE py_awr_warnings_hist (
          log_time            DATE                NOT NULL
        , name                VARCHAR2(64 BYTE)   NOT NULL
        , instance_number     NUMBER              NOT NULL
        , value               NUMBER
        , min_value           NUMBER
        , avg_value           NUMBER
        , max_value           NUMBER
        , high_threshold      NUMBER
        , daily_min           NUMBER
        , daily_avg           NUMBER
        , daily_max           NUMBER
        , min_daily_min       NUMBER
        , avg_daily_avg       NUMBER
        , max_daily_max       NUMBER
        , daily_max_threshold NUMBER
        , spike_value         NUMBER
        , spike_threshold     NUMBER
        , warning_type        VARCHAR2(30)
        , comments            VARCHAR2(4000 BYTE)
)
/

ALTER TABLE py_awr_warnings_hist
  ADD PRIMARY KEY (log_time, name, instance_number)
/

ALTER TABLE py_awr_warnings_hist
  ADD FOREIGN KEY (name) REFERENCES py_awr_metrics_parameters (name)
/


CREATE OR REPLACE FORCE VIEW py_awr_avgs (
           name
         , instance_number
         , updated
         , pct_threshold
         , amt_threshold
         , spike_threshold
         , min_value
         , avg_value
         , max_value
         , min_daily_min
         , avg_daily_avg
         , max_daily_max
         , high_threshold
         , daily_max_threshold
) AS SELECT
           par.name
         , hist.instance_number
         , par.updated
         , par.pct_threshold
         , par.amt_threshold
         , par.spike_threshold
         , min(hist.value)
         , avg(hist.value)
         , max(hist.value)
         , min(hist.daily_min)
         , avg(hist.daily_avg)
         , max(hist.daily_max)
         , avg(hist.value)     * ( 1 + nvl(par.pct_threshold, 0) / 100 )
         , max(hist.daily_max) * ( 1 + nvl(par.pct_threshold, 0) / 100 )
     FROM  py_awr_metrics_parameters par
         , py_awr_metrics_hist       hist
     WHERE par.name        = hist.name(+)
     AND   hist.counted(+) = 'Y'
     AND   par.monitored   = 'Y'
     AND   hist.log_time   < trunc(sysdate)
     GROUP BY par.name
            , hist.instance_number
            , par.updated
            , par.pct_threshold
            , par.amt_threshold
            , par.spike_threshold
     ORDER BY par.name
            , hist.instance_number
/


CREATE TABLE py_awr_email_recipients (
	   email_address VARCHAR2(100)
)
/

