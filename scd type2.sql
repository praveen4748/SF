create database type2;
create schema type2;


create table source_table (
id number not null,
name varchar,
dob date,
mail varchar,
phone number,
salary number,
dept varchar,
location varchar,
primary key(id)
);

create table target_table (
id number not null,
name varchar,
dob date,
mail varchar,
phone number,
salary number,
dept varchar,
location varchar,
inser timestamp,
updat timestamp,
primary key(id)
);

create stream my_stream1 on table source_table;

create stream my_stream2 on table source_table;


create or replace procedure my_procedure()
returns varchar
language sql
execute as caller
as

declare
    time_st timestamp;
begin

time_st :=  current_timestamp();

merge into target_table t
using my_stream1 s 
on t.id = s.id

when matched 
and s.metadata$action = 'delete' 
and s.metadata$isupdate = 'true'
then 
update set t.upda = :time_st

 
when not matched then
Insert (t.id, t.name, t.dob, t.mail, t.phone, t.salary, t.dept, t.location, t.inser, t.upda)
values (s.id, s.name, s.dob, s.mail, s.phone, s.salary, s.dept, s.location, :time_st, null);




Insert into target_table  (t.id, t.name, t.dob, t.mail, t.phone, t.salary, t.dept, t.location, t.inser, t.upda)
select id, name, dob, mail, phone, salary, dept, location, :time_st, null from my_stream2 
where metadata$action = 'Insert' and metadata$isupdate = 'true';

Return 'Type 2 successfully completed'

End;


create task my_task
schedule = 'using cron * 11 * * * UTC'
when system$stream_has_data = ('my_stream1')
as
call my_procedure();


alter task my_task resume;














