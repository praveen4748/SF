desc integration bank_integration;

show integrations;

CREATE OR REPLACE FILE FORMAT BANK_FORMAT
type = csv,
field_delimiter = ',',
skip_header = 1,
field_optionally_enclosed_by = '"',
null_if = ('');

show file formats;

create or replace stage bank_stage
storage_integration = bank_integration,
url = 's3://utb1/csv/',
file_format = bank_format;

show stages;

list @bank_stage;

select metadata$filename from @bank_stage;

select $1, $2, $3, $4, $5, $6, $7, $8,$9, $10, $11, $12, $13, $14, $15, $16 from @bank_stage;

create or replace transient  table source_table (
id varchar,
amount number,
value number,
result varchar,
type varchar,
remaining number,
trade varchar,
export_trade varchar,
size varchar,
location varchar,
period varchar,
chennai varchar,
madurai varchar
);

copy into source_table from @bank_stage
on_error = continue;

validation_mode = return_all_errors;

select * from source_table;

select count(*) from source_table;


create or replace   table target_table (
id varchar,
amount number,
value number,
result varchar,
type varchar,
remaining number,
trade varchar,
export_trade varchar,
size varchar,
location varchar,
period varchar,
chennai varchar,
madurai varchar
);
select * from target_table;

select count(*) from target_table;
show tables;

create table mm (
id varchar
);

insert into mm values ('1d02'), ('1d03'), ('1d04'), ('1d05');
delete from mm where id in ('1d03', '1d04');
select * from mm;
drop table mm;
undrop table mm;

create or replace  pipe bank_pipe
auto_ingest = true
as 
copy into source_table from @bank_stage;

show pipes;

select get_ddl('pipe', 'bank_pipe');

select system$pipe_status('bank_pipe');

desc pipe bank_pipe;

select get_ddl ('integration', 'bank_integration');
select get_ddl ('stage', 'bank_stage');
show stages; 
desc stage bank_stage;
select get_ddl ('file_format', 'bank_format');