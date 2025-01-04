create or replace   database   new;
create    or replace schema new;


--- CREATE ROLE  FOR ALL PRIVILEGES 

create or replace role crud_role;

grant usage on warehouse compute_wh to role crud_role;

grant usage on database new to role crud_role; 

grant usage on schema new to role crud_role;

---- in schema level   

grant all privileges  on all tables in schema new.new to role crud_role;

grant all privileges on future tables in schema new.new to role crud_role;

--- for individula tables

---  GRANT SELECT ON TABLE your_database.your_schema.your_table TO ROLE your_role;

---- GRANT INSERT ON TABLE your_database.your_schema.your_table TO ROLE your_role;

----- GRANT UPDATE ON TABLE your_database.your_schema.your_table TO ROLE your_role;

----- GRANT DELETE ON TABLE your_database.your_schema.your_table TO ROLE your_role;

--- CREATE ROLE FOR READ 

create role read_only_role;

grant usage on warehouse compute_wh to role read_only_role;

grant usage on database dnb_database to role read_only_role;

grant usage on schema dnb_database.dnb_schema to role read_only_role; 

grant select on all tables in schema dnb_database.dnb_schema to role read_only_role;

grant select on future tables in schema dnb_database.dnb_schema to role read_only_role;



--- create user

create or replace user bala
password = 'Password@123'
default_role = read_only_role
default_warehouse = 'compute_wh'
must_change_password = TRUE; 


grant role  read_only_role to user bala;

grant role crud_role  to user bala;


show warehouses;
