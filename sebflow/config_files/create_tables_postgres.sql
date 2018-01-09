DROP TABLE IF EXISTS dag;

CREATE TABLE dag (
  dag_id VARCHAR(50)
  , is_paused BOOLEAN DEFAULT FALSE
  , schedule_interval VARCHAR(10)
  , last_run_date TIMESTAMP WITH TIME ZONE DEFAULT NULL
  , last_run_result VARCHAR(50) DEFAULT NULL
  , PRIMARY KEY (dag_id)
);

DROP TABLE IF EXISTS dag_run;

CREATE TABLE dag_run (
  dag_run_id SERIAL
  , dag_id VARCHAR(50)
  , start_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
  , end_date TIMESTAMP WITH TIME ZONE DEFAULT NULL
  , state VARCHAR(50)
);

DROP TABLE IF EXISTS task;

CREATE TABLE task (
  id Serial
  , task_id VARCHAR(50)
  , dag_run_id INTEGER
  , state VARCHAR(20)
  , start_date TIMESTAMP WITH TIME ZONE DEFAULT NULL
  , end_date TIMESTAMP WITH TIME ZONE DEFAULT NULL
  , hostname VARCHAR(50)
  , unixname VARCHAR(50)
  , UNIQUE (task_id, dag_run_id)
);
