-- Create table first
create or replace table my_first_db.public.fpl_standings (
    league_id INT,
    league_name STRING,
    date_created DATE,
    gameweek STRING,
    is_close_flag STRING,
    has_cup_flag STRING,
    manager_name STRING,
    manager_id INT,
    ranking INT,
    total_points INT,
    team_name STRING,
    insert_date_id INT
  )

;

select * from my_first_db.public.fpl_standings

;

-- Forgot about PK
alter table my_first_db.public.fpl_standings ADD PRIMARY KEY (manager_id, insert_date_id)

;

-- Create file format 
create or replace file format my_first_db.file_formats.csv_fileformat
    type = csv
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL','null')
    empty_field_as_null = TRUE

;


create or replace storage integration s3_integration_fpl
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE --to make a connection
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::654654296185:role/FPL_Rola'
    STORAGE_ALLOWED_LOCATIONS = ('s3://fpl-2024-2025')
    COMMENT = 'Creating connection to S3 bucket for FPL data' 

;

-- Copy STORAGE AWS EXTERNAL ID and STORAGE AWS IAM USER ARN in to IAM role
desc integration s3_integration_fpl

;

-- Create stage object with integration object
CREATE OR REPLACE stage my_first_db.external_stages.fpl_standings_stage
    URL = 's3://fpl-2024-2025/transformed_data/fpl_standings/'
    STORAGE_INTEGRATION = s3_integration_fpl
    FILE_FORMAT = my_first_db.file_formats.csv_fileformat

;

-- Verify that the stage is created well
LIST @my_first_db.external_stages.fpl_standings_stage

;

-- Load data 

COPY INTO my_first_db.public.fpl_standings
FROM @my_first_db.external_stages.fpl_standings_stage

;

SELECT * FROM my_first_db.public.fpl_standings
ORDER BY INSERT_DATE_ID DESC, RANKING 

;

TRUNCATE TABLE my_first_db.public.fpl_standings

;

SELECT * FROM my_first_db.public.fpl_standings

;

-- Create snowpipe
CREATE OR REPLACE pipe my_first_db.pipes.fpl_standings_pipe
auto_ingest = TRUE
AS
COPY INTO my_first_db.public.fpl_standings
FROM @my_first_db.external_stages.fpl_standings_stage

;

-- Describe the pipe to get the notification_channel and copy it to S3 event 
DESC pipe my_first_db.pipes.fpl_standings_pipe

;

SELECT SYSTEM$PIPE_STATUS('my_first_db.pipes.fpl_standings_pipe')