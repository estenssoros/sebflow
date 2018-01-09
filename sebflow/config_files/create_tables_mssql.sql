IF OBJECT_ID('sebflow.dbo.dag') IS NOT NULL DROP TABLE sebflow.dbo.dag;

CREATE TABLE sebflow.dbo.dag (
  dag_id NVARCHAR(50)
  , is_paused BIT DEFAULT 0
  , schedule_interval NVARCHAR(20)
  , last_run_date DATETIME DEFAULT NULL
  , last_run_result NVARCHAR(50) DEFAULT NULL
  , PRIMARY KEY (dag_id)
);

IF OBJECT_ID('sebflow.dbo.dag_run') IS NOT NULL DROP TABLE sebflow.dbo.dag_run;

CREATE TABLE sebflow.dbo.dag_run (
  dag_run_id INT IDENTITY(1,1)
  , dag_id NVARCHAR(50)
  , start_date DATETIME DEFAULT GETDATE()
  , end_date DATETIME DEFAULT NULL
  , state NVARCHAR(50)
  , PRIMARY KEY (dag_run_id)
);

IF OBJECT_ID('sebflow.dbo.task') IS NOT NULL DROP TABLE sebflow.dbo.task;

CREATE TABLE sebflow.dbo.task (
  id INT IDENTITY(1,1)
  , task_id NVARCHAR(50)
  , dag_run_id INTEGER
  , state NVARCHAR(20)
  , start_date DATETIME DEFAULT NULL
  , end_date DATETIME DEFAULT NULL
  , hostname NVARCHAR(50)
  , unixname NVARCHAR(50)
  , PRIMARY KEY (id)
  , UNIQUE (task_id, dag_run_id)
);
