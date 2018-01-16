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
  , message TEXT DEFAULT NULL
  , PRIMARY KEY (id)
  , UNIQUE (task_id, dag_run_id)
);


IF OBJECT_ID('sebflow.dbo.connection') IS NOT NULL DROP TABLE sebflow.dbo.connection;

CREATE TABLE sebflow.dbo.connection(
  id INT IDENTITY(1,1)
  , conn_id NVARCHAR(50)
  , conn_type NVARCHAR(100)
  , host NVARCHAR(100)
  , _schema NVARCHAR(100)
  , login NVARCHAR(100)
  , password NVARCHAR(100)
  , port INTEGER
  , is_encrypted BIT DEFAULT 0
  , is_extra_encrypted BIT DEFAULT 0
  , extra NVARCHAR(500)
  , PRIMARY KEY (id)
)
