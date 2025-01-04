create database dnb_database;

create schema dnb_schema;

create or replace storage integration dnb_integration
type = external_stage,
enabled = true,
storage_allowed_locations = ('s3://dnb1/csv/'),
storage_provider = S3,
storage_aws_role_arn = 'arn:aws:iam::759489707541:role/dnb1';

desc integration dnb_integration;
show integrations;
select get_ddl ('integration', 'dnb_integration');

create or replace file format dnb_format
type = csv
skip_header = 1
field_delimiter = ','
field_optionally_enclosed_by = '"';

desc file format dnb_format;
show file formats;
select get_ddl ('format', 'dnb_formart');   ----  not working

create or replace stage dnb_stage
url = 's3://dnb1/csv/'
storage_integration = dnb_integration
file_format = dnb_format;

show stages;
desc stage dnb_stage;
list  @dnb_stage;

select $1, $2,$3,$4,$5,$6,$7,$8 from @dnb_stage;

create or replace transient table source_table (
cusid varchar,
name varchar,
accno number,
transcation_amount number,
transcation_type varchar,
transcation_date date );

copy into source_table from @dnb_stage;
validation_mode = return_all_errors;

select * from source_table;
select count(*) from source_table;


create or replace  table target_table (
cusid varchar,
name varchar,
accno number,
transcation_amount number,
transcation_type varchar,
transcation_date date,
action varchar
);

select * from target_table;
select count(*) from target_table;

create or replace pipe dnb_pipe
auto_ingest = true
as
copy into source_table from @dnb_stage;

desc pipe dnb_pipe;

show pipes;

create or replace stream dnb_stream on table source_table;

insert into target_table (CUSID, NAME, ACCNO, TRANSCATION_AMOUNT, TRANSCATION_TYPE, TRANSCATION_DATE, ACTION)
select CUSID, NAME, ACCNO, TRANSCATION_AMOUNT, TRANSCATION_TYPE, TRANSCATION_DATE, 'H' from source_table;

select * from target_table;
select * from source_table;

select count(*) from target_table;
select count(*) from source_table;

create or replace table aggregate (
cusid varchar,
name varchar,
accno number,
transcation_debit_amount number,
transcation_credit_amount number,
transcation_date date,
action varchar );

select * from aggregate;
select count(*) from aggregate;

create or replace procedure versioning()
returns string
language sql
AS
$$
begin
    insert into target_table (CUSID, NAME, ACCNO, TRANSCATION_AMOUNT, TRANSCATION_TYPE, TRANSCATION_DATE, ACTION)
    select s.CUSID, s.NAME, s.ACCNO, s.TRANSCATION_AMOUNT, s.TRANSCATION_TYPE, s.TRANSCATION_DATE, 'N'
    from source_table s 
    left join target_table t 
    on s.cusid = t.cusid and s.accno = t.accno 
    where t.cusid is not null;

    insert into target_table (CUSID, NAME, ACCNO, TRANSCATION_AMOUNT, TRANSCATION_TYPE, TRANSCATION_DATE, ACTION)
    select s.CUSID, s.NAME, s.ACCNO, s.TRANSCATION_AMOUNT, s.TRANSCATION_TYPE, s.TRANSCATION_DATE, 'H'
    from source_table s 
    left join target_table t 
    on s.cusid = t.cusid and s.accno = t.accno 
    where t.cusid is null;

    
    return 'changes made';
end;
$$;



create or replace procedure aggregate_proc()
    RETURNS STRING
    LANGUAGE SQL
AS
$$
BEGIN

    -- Update existing records (Action = 'Update')
    INSERT INTO aggregate (cusid, name, accno, transcation_debit_amount, transcation_credit_amount, transcation_date, action)
    SELECT t.cusid, t.name, t.accno,
           SUM(CASE WHEN t.transcation_type = 'debit' THEN t.transcation_amount ELSE 0 END) AS transcation_debit_amount,
           SUM(CASE WHEN t.transcation_type = 'credit' THEN t.transcation_amount ELSE 0 END) AS transcation_credit_amount,
           t.transcation_date, 'Update' AS action
    FROM target_table t
    LEFT JOIN aggregate a
    ON t.cusid = a.cusid AND t.accno = a.accno AND t.transcation_date = a.transcation_date
    WHERE a.cusid IS NOT NULL
    GROUP BY t.cusid, t.name, t.accno, t.transcation_date;

    -- Insert new records (Action = 'New')
    INSERT INTO aggregate (cusid, name, accno, transcation_debit_amount, transcation_credit_amount, transcation_date, action)
    SELECT t.cusid, t.name, t.accno,
           SUM(CASE WHEN t.transcation_type = 'debit' THEN t.transcation_amount ELSE 0 END) AS transcation_debit_amount,
           SUM(CASE WHEN t.transcation_type = 'credit' THEN t.transcation_amount ELSE 0 END) AS transcation_credit_amount,
           t.transcation_date, 'New' AS action
    FROM target_table t
    LEFT JOIN aggregate a
    ON t.cusid = a.cusid AND t.accno = a.accno AND t.transcation_date = a.transcation_date
    WHERE a.cusid IS NULL
    GROUP BY t.cusid, t.name, t.accno, t.transcation_date;

    RETURN 'Aggregation completed';
END;
$$;

CREATE OR REPLACE TASK PARENT_TASK
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON 0 7 * * * UTC'
AS 
CALL versioning();

CREATE OR REPLACE TASK CHILD_TASK
WAREHOUSE = COMPUTE_WH
AFTER PARENT_TASK
AS
call aggregate_proc();

alter task child_task resume;    ----- bottom to top
alter task parent_task resume;


alter task parent_task suspend;      -------- top to bottom
alter task child_task suspend;


create or replace secure materialized view sm_view as
select * from source_table 
where accno in (12378, 78965, 12345) and name in ('venkat', 'james');


select * from standard_view;
select * from secure_view;
select * from materialized_view;
select * from sm_view;
select * from source_table;
select * from target_table;
select * from aggregate;


--