CREATE DATABASE apache;
CREATE SCHEMA apache_nifi;
CREATE SCHEMA file_format;
CREATE SCHEMA apache_stage;
// table to load data
CREATE or replace TABLE apache.apache_nifi.customer_data(
customer_id     int ,       
first_name  string ,  
last_name   string  ,
email   string  , 
street   string  ,
city  string,
state  string,
country string,
update_timestamp timestamp_ntz default current_timestamp()
)

CREATE or replace TABLE apache.apache_nifi.customer_data_hist(
customer_id     int ,       
first_name  string ,  
last_name   string  ,
email   string  , 
street   string  ,
city  string,
state  string,
country string,
start_time timestamp_ntz default current_timestamp(),
end_time timestamp_ntz default current_timestamp(),
is_current boolean
)

CREATE or replace TABLE apache.apache_nifi.customer_data_raw(
customer_id     int ,       
first_name  string ,  
last_name   string  ,
email   string  , 
street   string  ,
city  string,
state  string,
country string
)
create or replace stream customer_table_changes on table apache.apache_nifi.customer_data;


create or replace storage integration apache_s3_int
    type = external_stage
    storage_provider = s3
    storage_aws_role_arn = ''
    enabled = true
    storage_allowed_locations = ( 's3://a' )
     comment = 'cretaing connection to aws for apache project';

desc storage integration apache_s3_int

create file format apache.file_format.csv_format
TYPE = 'csv'

desc file format file_format.csv_format
alter file format file_format.csv_format
set SKIP_HEADER = 1

create or replace stage apache_stage.csv_folder
url = 's3://'
storage_integration = apache_s3_int
file_format =  apache.file_format.csv_format

list @apache_stage.csv_folder

create pipe apache_s3_pipe
auto_ingest = true as
copy into apache.apache_nifi.customer_data_raw
from @apache_stage.csv_folder

show pipes

apache.apache_nifi.customer_data_raw

select * from apache.apache_nifi.customer_data_raw
select * from apache.apache_nifi.customer_data
where customer_id = 0


merge into apache.apache_nifi.customer_data c 
using apache.apache_nifi.customer_data_raw cr
   on  c.customer_id = cr.customer_id
when matched and c.first_name  <> cr.first_name  or
                 c.last_name   <> cr.last_name   or
                 c.email       <> cr.email       or
                 c.street      <> cr.street      or
                 c.city        <> cr.city        or
                 c.state       <> cr.state       or
                 c.country     <> cr.country then update
    set c.customer_id = cr.customer_id
        ,c.first_name  = cr.first_name 
        ,c.last_name   = cr.last_name  
        ,c.email       = cr.email      
        ,c.street      = cr.street     
        ,c.city        = cr.city       
        ,c.state       = cr.state      
        ,c.country     = cr.country  
        ,update_timestamp = current_timestamp()
when not matched then insert
           (c.customer_id,c.first_name,c.last_name,c.email,c.street,c.city,c.state,c.country)
    values (cr.customer_id,cr.first_name,cr.last_name,cr.email,cr.street,cr.city,cr.state,cr.country);

    update  apache.apache_nifi.customer_data_raw
    set first_name = 'Ashutosh'
    where customer_id = 0

    truncate table apache.apache_nifi.customer_data_raw
    truncate table apache.apache_nifi.customer_data

CREATE OR REPLACE PROCEDURE pdr_scd_demo()
returns string not null
language javascript
as
    $$
      var cmd = `
                 merge into apache.apache_nifi.customer_data c 
                 using apache.apache_nifi.customer_data_raw cr
                    on  c.customer_id = cr.customer_id
                 when matched and c.customer_id <> cr.customer_id or
                                  c.first_name  <> cr.first_name  or
                                  c.last_name   <> cr.last_name   or
                                  c.email       <> cr.email       or
                                  c.street      <> cr.street      or
                                  c.city        <> cr.city        or
                                  c.state       <> cr.state       or
                                  c.country     <> cr.country then update
                     set c.customer_id = cr.customer_id
                         ,c.first_name  = cr.first_name 
                         ,c.last_name   = cr.last_name  
                         ,c.email       = cr.email      
                         ,c.street      = cr.street     
                         ,c.city        = cr.city       
                         ,c.state       = cr.state      
                         ,c.country     = cr.country  
                         ,update_timestamp = current_timestamp()
                 when not matched then insert
                            (c.customer_id,c.first_name,c.last_name,c.email,c.street,c.city,c.state,c.country)
                     values (cr.customer_id,cr.first_name,cr.last_name,cr.email,cr.street,cr.city,cr.state,cr.country);
      `
      var cmd1 = "truncate table apache.apache_nifi.customer_data_raw;"
      var sql = snowflake.createStatement({sqlText: cmd});
      var sql1 = snowflake.createStatement({sqlText: cmd1});
      var result = sql.execute();
      var result1 = sql1.execute();
    return cmd+'\n'+cmd1;
    $$;
call pdr_scd_demo();

select * from apache.apache_nifi.customer_data_raw
select * from apache.apache_nifi.customer_data

create or replace task run_proc warehouse = COMPUTE_WH schedule = '1 minute'
ERROR_ON_NONDETERMINISTIC_MERGE=FALSE
as
call pdr_scd_demo();
show tasks;

alter task run_proc suspend--resume 

select timestampdiff(second, current_timestamp, scheduled_time) as next_run, scheduled_time, current_timestamp, name, state 
from table(information_schema.task_history()) where state = 'SCHEDULED' order by completed_time desc;


show streams

select * from CUSTOMER_TABLE_CHANGES
WHERE METADATA$ACTION <> 'INSERT'

update apache.apache_nifi.customer_data set first_name = 'Ashutosh'
where customer_id = 0
delete from apache.apache_nifi.customer_data where customer_id = 69

insert into apache.apache_nifi.customer_data values(223136,'Jessica','Arnold','tanner39@smith.com','595 Benjamin Forge Suite 124','Michaelstad','Connecticut'
                            ,'Cape Verde',current_timestamp());
update apache.apache_nifi.customer_data set FIRST_NAME='Jessica', update_timestamp = current_timestamp()::timestamp_ntz where customer_id=72;
delete from apache.apache_nifi.customer_data where customer_id =73 ;


create or replace view v_customer_change_data as
select CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY,STATE,COUNTRY,
 start_time, end_time, is_current, 'I' as dml_type
from (
select CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY,STATE,COUNTRY,
             update_timestamp as start_time,
             lag(update_timestamp) over (partition by customer_id order by update_timestamp desc) as end_time_raw,
             case when end_time_raw is null then '9999-12-31'::timestamp_ntz else end_time_raw end as end_time,
             case when end_time_raw is null then TRUE else FALSE end as is_current
      from (select CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY,STATE,COUNTRY,UPDATE_TIMESTAMP
            from CUSTOMER_TABLE_CHANGES
            where metadata$action = 'INSERT'
            and metadata$isupdate = 'FALSE')
  )
union
-- This subquery figures out what to do when data is updated in the customer table
-- An update to the customer table results in an update AND an insert to the customer_HISTORY table
-- The subquery below generates two records, each with a different dml_type
select CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY,STATE,COUNTRY, start_time, end_time, is_current, dml_type
from (select CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY,STATE,COUNTRY,
             update_timestamp as start_time,
             lag(update_timestamp) over (partition by customer_id order by update_timestamp desc) as end_time_raw,
             case when end_time_raw is null then '9999-12-31'::timestamp_ntz else end_time_raw end as end_time,
             case when end_time_raw is null then TRUE else FALSE end as is_current, 
             dml_type
      from (-- Identify data to insert into customer_history table
            select CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY,STATE,COUNTRY, update_timestamp, 'I' as dml_type
            from CUSTOMER_TABLE_CHANGES
            where metadata$action = 'INSERT'
            and metadata$isupdate = 'TRUE'
            union
            -- Identify data in customer_HISTORY table that needs to be updated
            select CUSTOMER_ID, null, null, null, null, null,null,null, start_time, 'U' as dml_type
            from apache.apache_nifi.customer_data_hist
            where customer_id in (select distinct customer_id 
                                  from customer_table_changes
                                  where metadata$action = 'DELETE'
                                  and metadata$isupdate = 'TRUE')
     and is_current = TRUE))
union
-- This subquery figures out what to do when data is deleted from the customer table
-- A deletion from the customer table results in an update to the customer_HISTORY table
select ctc.CUSTOMER_ID, null, null, null, null, null,null,null, ch.start_time, current_timestamp()::timestamp_ntz, null, 'D'
from apache.apache_nifi.customer_data_hist ch
inner join customer_table_changes ctc
   on ch.customer_id = ctc.customer_id
where ctc.metadata$action = 'DELETE'
and   ctc.metadata$isupdate = 'FALSE'
and   ch.is_current = TRUE;

SELECT * FROM  V_CUSTOMER_CHANGE_DATA 

create or replace task tsk_scd_hist warehouse= COMPUTE_WH schedule='1 minute'
ERROR_ON_NONDETERMINISTIC_MERGE=FALSE
as
merge into apache.apache_nifi.customer_data_hist ch -- Target table to merge changes from NATION into
using v_customer_change_data ccd -- v_customer_change_data is a view that holds the logic that determines what to insert/update into the customer_history table.
   on ch.CUSTOMER_ID = ccd.CUSTOMER_ID -- CUSTOMER_ID and start_time determine whether there is a unique record in the customer_history table
   and ch.start_time = ccd.start_time
when matched and ccd.dml_type = 'U' then update -- Indicates the record has been updated and is no longer current and the end_time needs to be stamped
    set ch.end_time = ccd.end_time,
        ch.is_current = FALSE
when matched and ccd.dml_type = 'D' then update -- Deletes are essentially logical deletes. The record is stamped and no newer version is inserted
   set ch.end_time = ccd.end_time,
       ch.is_current = FALSE
when not matched and ccd.dml_type = 'I' then insert -- Inserting a new CUSTOMER_ID and updating an existing one both result in an insert
          (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY,STATE,COUNTRY, start_time, end_time, is_current)
    values (ccd.CUSTOMER_ID, ccd.FIRST_NAME, ccd.LAST_NAME, ccd.EMAIL, ccd.STREET, ccd.CITY,ccd.STATE,ccd.COUNTRY, ccd.start_time, ccd.end_time, ccd.is_current);
    
alter task tsk_scd_hist resume


--runcate table apache.apache_nifi.customer_data_raw
--truncate table apache.apache_nifi.customer_data

insert into apache.apache_nifi.customer_data values(223136,'Ashutosh','Patil','tommy12@smith.com','403 Ny street Suite 101','London','England'
                           ,'Cape Verde',current_timestamp());
update apache.apache_nifi.customer_data set FIRST_NAME='Ashu' where customer_id=7523;
delete from apache.apache_nifi.customer_data where customer_id =136 and FIRST_NAME = 'Kim';
select count(*),customer_id from apache.apache_nifi.customer_data group by customer_id having count(*)=1;
select * from apache.apache_nifi.customer_data_hist where customer_id =223136;
select * from apache.apache_nifi.customer_data_hist where IS_CURRENT=FALSE;
