create database example;
create schema example;


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

create stream my_stream on table source_table;



merge into target_table t 
using my_stream s 
on t.id = s.id

when matched and
s.metadata$action = 'Insert' and
s.metadata$isupdate = 'True' and
(t.name <> s.name or t.dob <> s.dob
or t.mail <> s.mail or t.phone <> s.phone
or t.salary <> s.salary or t.dept <> s.dept
or t.olaction <> s.location ) then
update set 
t.name = s.name, t.dob = s.dob,
t.mail = s.mail, t.phone = s.phone,
t.salary = s.salary, t.dept = s.dept,
t.loction = s.location,
t.update = current_timestamp

when not matched then
Insert (t.id, t.name, t.dob, t.mail, t.phone, t.salary, t.dept, t.location, t.inser, t.upda)
values (s.id, s.name, s.dob, s.mail, s.phone, s.salary, s.dept, s.location, current_timestamp, current_timestamp);

create task my_task
schedule = 'USING CRON * 10 * * * UTC'
When System$Stream_has_data('my_stream1')
as
merge into target_table t 
using my_stream1 s 
on t.id = s.id

when matched and
s.metadata$action = 'Insert' and
s.metadata$isupdate = 'True' and
(t.name <> s.name or t.dob <> s.dob
or t.mail <> s.mail or t.phone <> s.phone
or t.salary <> s.salary or t.dept <> s.dept
or t.olaction <> s.location ) then
update set 
t.name = s.name, t.dob = s.dob,
t.mail = s.mail, t.phone = s.phone,
t.salary = s.salary, t.dept = s.dept,
t.loction = s.location,
t.update = current_timestamp

when not matched then
Insert (t.id, t.name, t.dob, t.mail, t.phone, t.salary, t.dept, t.location, t.inser, t.upda)
values (s.id, s.name, s.dob, s.mail, s.phone, s.salary, s.dept, s.location, current_timestamp, current_timestamp);



alter task my_task resume;