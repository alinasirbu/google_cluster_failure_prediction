#This file contains all commands required to create the set of features 
#from the Google trace data, using BigQuery with the bq command line tool
#
#Copyright (C) 2015  Alina Sirbu, alina.sirbu@unibo.it
#
#This program is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
#To use it directly replace [projectid] with your Google Cloud project id and [datasetid] with a name you chose for your dataset

##########################################
###CREATE DATASET 

bq mk [projectid]:[datasetid]


##########################################
###IMPORT MACHINE EVENT TABLE FROM GOOGLE STORAGE

bq --project_id [projectid] load --source_format=CSV [projectid]:[datasetid].machine_events gs://clusterdata-2011-2/machine_events/part-00000-of-00001.csv.gz timestamp:integer,machine_id:string,event_type:integer,platform_id:string,cpu:float,memory:float

###SHOW INFORMATION ABOUT THE NEW TABLE

bq show [projectid]:[datasetid].machine_events


##########################################
###IMPORT TASK EVENT TABLE FROM GOOGLE STORAGE

bq --project_id [projectid] load --max_bad_records=6 --source_format=CSV [projectid]:[datasetid].task_events gs://clusterdata-2011-2/task_events/* timestamp:integer,missing_info:integer,job_id:string,task_index:integer,machine_id:string,event_type:integer,user_name:string,scheduling_class:integer,priority:integer,requested_cpu:float,requested_memory:float,requested_disk:float,different_machine_constraint:boolean

###SHOW INFORMATION ABOUT THE NEW TABLE

bq show [projectid]:[datasetid].task_events


##########################################
###IMPORT TASK USAGE TABLE FROM GOOGLE STORAGE

bq load --max_bad_records=6 --source_format=CSV [datasetid].task_usage gs://clusterdata-2011-2/task_usage/* start:integer,end:integer,job_id:string,task_index:integer,machine_id:string,mean_cpu_rate:float,canonical_mem:float,assigned_mem:float,cache_mem:float,total_cache_mem:float,max_mem:float,mean_disk_time:float,mean_disk_space:float,max_cpu:float,max_disk_time:float,cpi:float,mai:float,sample_portion:float,aggregation:integer,sample_cpu:float



##########################################
###CREATE TABLE RUNNING_TASKS - FOR EACH TASK WE HAVE START TIMESTAMP, END TIMESTAMP, MACHINE ID, EXIT CODE AND THE TIME IT WAS RESTARTED (SOME TASKS GET STOPPED AND RESTARTED)

###FIRST CREATE INTERMEDIATE TABLES WITH START AND END TIMES

bq --project_id [projectid]  query --allow_large_results --destination_table=[datasetid].end "select job_id, task_index, machine_id, timestamp, event_type, dense_rank() over (Partition by job_id,task_index,machine_id order by timestamp ASC) as rank from [projectid]:[datasetid].task_events where event_type in (2,3,4,5,6) and machine_id is not null"

bq --project_id [projectid]  query --allow_large_results --destination_table=[datasetid].start "select job_id, task_index, machine_id, timestamp , dense_rank() over (Partition by job_id,task_index,machine_id order by timestamp ASC) as rank from [projectid]:[datasetid].task_events where event_type=1 and machine_id is not null"

#THEN JOIN TO OBTAIN RUNNING_TASKS

bq --project_id [projectid]  query --allow_large_results --destination_table=[datasetid].running_tasks "select t1.job_id, t1.task_index, t1.machine_id, t1.timestamp, t2.timestamp, t2.event_type, t1.rank from [projectid]:[datasetid].start t1 left join each [projectid]:[datasetid].end t2 on t1.job_id=t2.job_id and t1.task_index=t2.task_index and t1.machine_id=t2.machine_id and t1.rank=t2.rank"

#SHOW INFOR ABOUT NEW TABLE AND DELETE INTERMEDIATE TABLES

bq show [datasetid].running_tasks
bq rm [datasetid].end
bq rm [datasetid].start



##########################################
###CREATE TABLE WINDOWS - CREATE A TABLE THAT HAS START AND END TIMESTAMP FOR ALL 5 MINUTE WINDOWS - A TOTAL OF 8352 WINDOWS - USED LATER IN JOIN TO CREATE TIME SERIES

###FIRST FIND TIMESTAMP LIMITS IN THE DATA : THE RESULTS IS 0 | 2506199602822 SO WE HAVE 8352 5 MINUTE WINDOWS
bq --project_id [projectid]  query "select min(timestamp),max(timestamp) from [projectid]:[datasetid].task_events where timestamp>0"

#CREATE A TABLE WITH NUMBERS 1 TO 8352
bq --project_id [projectid]  query --allow_large_results --destination_table=[datasetid].aux "select timestamp, ROW_NUMBER() OVER() from  [projectid]:[datasetid].task_events limit 8352"

#CREATE WINDOWS
 bq --project_id [projectid]  query --allow_large_results --destination_table=[datasetid].windows5 "select 600000000+300000000*(f0_ -1) as start,600000000+300000000*f0_ as end from  [projectid]:[datasetid].aux"

#CLEANUP
bq rm [datasetid].aux



##########################################
###CREATE TABLE RUNNING_TASK_COUNT - A TABLE THAT, AT THE END OF EACH TIME WINDOW, SHOWS HOW MANY RUNNING TASKS THERE ARE ON EACH MACHINE 
bq --project_id [projectid]  query --allow_large_results --destination_table=[datasetid].running_task_count "select t1.t1_machine_id as machine, count(t1.t1_timestamp) as count, t2.start as time from [projectid]:[datasetid].running_tasks t1 cross join [projectid]:[datasetid].windows5 t2 where t1.t1_timestamp<t2.start and t1.t2_timestamp>t2.start group each by machine, time"



##########################################
###CREATE TABLES failed_task_count, evicted_task_count, finished_task_count, killed_task_count, lost_task_count, started_task_count - FOR EACH TIME WINDOW AND EACH MACHINE, IT SHOWS NUMBER OF TASKS THAT HAVE STARTED OR ENDED WITH A SPECIFIC EXIT CODE

bq --project_id [projectid]  query --dry_run --allow_large_results --destination_table=[datasetid].failed_task_count "select t1.t1_machine_id as machine, count(t1.t1_timestamp) as count, t2.start as start, t2.end as end from (select * from [projectid]:[datasetid].running_tasks where t2_event_type=3) t1 cross join [projectid]:[datasetid].windows5 t2 where t1.t2_timestamp>t2.start and t1.t2_timestamp<=t2.end group each by machine, start,end"

bq --project_id [projectid]  query --allow_large_results --destination_table=[datasetid].evicted_task_count "select t1.t1_machine_id as machine, count(t1.t1_timestamp) as count, t2.start as start, t2.end as end from (select * from [projectid]:[datasetid].running_tasks where t2_event_type=2) t1 cross join [projectid]:[datasetid].windows5 t2 where t1.t2_timestamp>t2.start and t1.t2_timestamp<=t2.end group each by machine, start,end"

bq --project_id [projectid]  query --allow_large_results --destination_table=[datasetid].finished_task_count "select t1.t1_machine_id as machine, count(t1.t1_timestamp) as count, t2.start as start, t2.end as end from (select * from [projectid]:[datasetid].running_tasks where t2_event_type=4) t1 cross join [projectid]:[datasetid].windows5 t2 where t1.t2_timestamp>t2.start and t1.t2_timestamp<=t2.end group each by machine, start,end"

bq --project_id [projectid]  query --allow_large_results --destination_table=[datasetid].killed_task_count "select t1.t1_machine_id as machine, count(t1.t1_timestamp) as count, t2.start as start, t2.end as end from (select * from [projectid]:[datasetid].running_tasks where t2_event_type=5) t1 cross join [projectid]:[datasetid].windows5 t2 where t1.t2_timestamp>t2.start and t1.t2_timestamp<=t2.end group each by machine, start,end"

bq --project_id [projectid]  query --allow_large_results --destination_table=[datasetid].lost_task_count "select t1.t1_machine_id as machine, count(t1.t1_timestamp) as count, t2.start as start, t2.end as end from (select * from [projectid]:[datasetid].running_tasks where t2_event_type=6) t1 cross join [projectid]:[datasetid].windows5 t2 where t1.t2_timestamp>t2.start and t1.t2_timestamp<=t2.end group each by machine, start,end"

bq --project_id [projectid]  query --allow_large_results --destination_table=[datasetid].started_task_count "select t1.t1_machine_id as machine, count(t1.t1_timestamp) as count, t2.start as start, t2.end as end from [projectid]:[datasetid].running_tasks t1 cross join [projectid]:[datasetid].windows5 t2 where t1.t1_timestamp>t2.start and t1.t1_timestamp<=t2.end group each by machine, start,end"


##########################################
###COMPUTE CPU, CANONICAL MEMORY, CPI, MAI AND DISK TIME FOR EACH TIME WINDOW FOR EACH MACHINE

#COMPUTE CPU -  SUM FOR ALL TASKS ON A MACHINE WEIGHTED BY THE AMOUNT OF OVERLAP IN SECONDS WITH THE WINDOW

bq query --allow_large_results --destination_table=[datasetid].cpu "select t1.machine_id as machine, sum(t1.mean_cpu_rate *(t1.end-t2.start+t2.end-t1.start-abs(t2.end-t1.end)-abs(t2.start-t1.start))/2)/(t2.end-t2.start) as rate, t2.start , t2.end  from [datasetid].task_usage t1 cross join [datasetid].windows5 t2 where (t1.start>=t2.start and t1.start<=t2.end) or (t1.end>=t2.start and t1.start<=t2.end) or (t1.start<t2.start and t1.end>t2.end) group each by machine, t2.start,t2.end"

#COMPUTE MEM -  SUM FOR ALL TASKS ON A MACHINE WEIGHTED BY THE AMOUNT OF OVERLAP IN SECONDS WITH THE WINDOW

bq query --allow_large_results --destination_table=[datasetid].mem "select t1.machine_id as machine, sum(t1.canonical_mem *(t1.end-t2.start+t2.end-t1.start-abs(t2.end-t1.end)-abs(t2.start-t1.start))/2)/(t2.end-t2.start) as rate, t2.start , t2.end from [datasetid].task_usage t1 cross join [datasetid].windows5 t2 where (t1.start>=t2.start and t1.start<=t2.end) or (t1.end>=t2.start and t1.start<=t2.end) or (t1.start<t2.start and t1.end>t2.end) group each by machine, t2.start,t2.end"

#CPI
bq query --allow_large_results --destination_table=[datasetid].cpi "select t1.machine_id as machine, avg(t1.cpi) as rate, t2.start , t2.end from [datasetid].task_usage t1 cross join [datasetid].windows5 t2 where (t1.start>=t2.start and t1.start<=t2.end) or (t1.end>=t2.start and t1.start<=t2.end) or (t1.start<t2.start and t1.end>t2.end) group each by machine, t2.start,t2.end"

#MAI
bq query --allow_large_results --destination_table=[datasetid].mai "select t1.machine_id as machine, avg(t1.mai) as rate, t2.start , t2.end from [datasetid].task_usage t1 cross join [datasetid].windows5 t2 where (t1.start>=t2.start and t1.start<=t2.end) or (t1.end>=t2.start and t1.start<=t2.end) or (t1.start<t2.start and t1.end>t2.end) group each by machine, t2.start,t2.end"

#DISK TIME -  SUM FOR ALL TASKS ON A MACHINE WEIGHTED BY THE AMOUNT OF OVERLAP IN SECONDS WITH THE WINDOW

bq query --allow_large_results --destination_table=[datasetid].disk_time "select t1.machine_id as machine, sum(t1.mean_disk_time *(t1.end-t2.start+t2.end-t1.start-abs(t2.end-t1.end)-abs(t2.start-t1.start))/2)/(t2.end-t2.start) as rate, t2.start , t2.end from [datasetid].task_usage t1 cross join [datasetid].windows5 t2 where (t1.start>=t2.start and t1.start<=t2.end) or (t1.end>=t2.start and t1.start<=t2.end) or (t1.start<t2.start and t1.end>t2.end) group each by machine, t2.start,t2.end"


##########################################
###CREATE TABLE time_to_remove - FOR EACH MACHINE AND TIME WINDOW, COMPUTE THE TIME TO THE NEXT REMOVE EVENT (OR END OF TRACE - TIMESTAMP 2506200000000)

bq --project_id [projectid]  query --allow_large_results --destination_table=[datasetid].add "select machine_id, timestamp , dense_rank() over (Partition by machine_id order by timestamp ASC) as rank from [projectid]:[datasetid].machine_events where event_type=0"

bq --project_id [projectid]  query --allow_large_results --destination_table=[datasetid].remove "select machine_id, timestamp , dense_rank() over (Partition by machine_id order by timestamp ASC) as rank from [projectid]:[datasetid].machine_events where event_type=1"

bq --project_id [projectid]  query --allow_large_results --destination_table=[datasetid].addremove "select t1.machine_id as machine_id, t1.timestamp as add, t2.timestamp as remove from [projectid]:[datasetid].add t1 left join [projectid]:[datasetid].remove t2 on t1.machine_id=t2.machine_id and t1.rank=t2.rank "

bq --project_id [projectid]  query --replace --allow_large_results --destination_table=[datasetid].time_to_remove "select t1.machine_id as machine,  t2.end as time, ifnull(t1.remove-t2.end,2506200000000-t2.end) as time_left from [projectid]:[datasetid].addremove t1 cross join [projectid]:[datasetid].windows5 t2 where (t2.end>=t1.add and t2.end<=t1.remove) or (t2.end>=t1.add and t1.remove is null)"

bq rm [datasetid].add
bq rm [datasetid].remove


###COMBINE TIME SERIES TO OBTAIN TABLES datawindows1-6 - TABLES CONTAINING ALL FEATURES ABOVE FOR THE LAST 6 TIME WINDOWS 

bq query --allow_large_results --replace --destination_table=[datasetid].datawindow1 "select t1.time as time, t1.machine as machine, t1.time_left as time_left, t2.count as w1_evicted, t3.count as w1_failed, t4.count as w1_finished, t5.count as w1_killed, t6.count as w1_lost, t7.count as w1_running, t8.count as w1_started, t9.rate as w1_cpu, t10.rate as w1_mem, t11.rate as w1_disk, t12.rate as w1_cpi, t13.rate as w1_mai from [datasetid].time_to_remove t1 left join each [datasetid].evicted_task_count t2 on t1.machine=t2.machine and t1.time=t2.end left join each [datasetid].failed_task_count t3 on t1.machine=t3.machine and t1.time=t3.end left join each [datasetid].finished_task_count t4 on t1.machine=t4.machine and t1.time=t4.end left join each [datasetid].killed_task_count t5 on t1.machine=t5.machine and t1.time=t5.end left join each [datasetid].lost_task_count t6 on t1.machine=t6.machine and t1.time=t6.end left join each [datasetid].running_task_count t7 on t1.machine=t7.machine and t1.time=t7.time left join each [datasetid].started_task_count t8 on t1.machine=t8.machine and t1.time=t8.end left join each [datasetid].cpu t9 on t1.machine=t9.machine and t1.time=t9.t2_end left join each [datasetid].mem t10 on t1.machine=t10.machine and t1.time=t10.t2_end left join each [datasetid].disk_time t11 on t1.machine=t11.machine and t1.time=t11.t2_end left join each [datasetid].cpi t12 on t1.machine=t12.machine and t1.time=t12.t2_end left join each [datasetid].mai t13 on t1.machine=t13.machine and t1.time=t13.t2_end"

#SHIFT WINDOWS BY 1

bq --project_id [projectid]  query --allow_large_results --destination_table=[datasetid].time_to_removew2 "select time-300000000 as time, machine, time_left from [projectid]:[datasetid].time_to_remove"

bq --project_id [projectid]  query --replace --allow_large_results --destination_table=[datasetid].datawindow2 "select t1.time+300000000 as time, t1.machine as machine, t1.time_left as time_left, t2.count as w2_evicted, t3.count as w2_failed, t4.count as w2_finished, t5.count as w2_killed, t6.count as w2_lost, t7.count as w2_running, t8.count as w2_started , t9.rate as w2_cpu, t10.rate as w2_mem, t11.rate as w2_disk, t12.rate as w2_cpi, t13.rate as w2_mai from [projectid]:[datasetid].time_to_removew2 t1 left join each [projectid]:[datasetid].evicted_task_count t2 on t1.machine=t2.machine and t1.time=t2.end left join each [projectid]:[datasetid].failed_task_count t3 on t1.machine=t3.machine and t1.time=t3.end left join each [projectid]:[datasetid].finished_task_count t4 on t1.machine=t4.machine and t1.time=t4.end left join each [projectid]:[datasetid].killed_task_count t5 on t1.machine=t5.machine and t1.time=t5.end left join each [projectid]:[datasetid].lost_task_count t6 on t1.machine=t6.machine and t1.time=t6.end left join each [projectid]:[datasetid].running_task_count t7 on t1.machine=t7.machine and t1.time=t7.time left join each [projectid]:[datasetid].started_task_count t8 on t1.machine=t8.machine and t1.time=t8.end left join each [datasetid].cpu t9 on t1.machine=t9.machine and t1.time=t9.t2_end left join each [datasetid].mem t10 on t1.machine=t10.machine and t1.time=t10.t2_end left join each [datasetid].disk_time t11 on t1.machine=t11.machine and t1.time=t11.t2_end left join each [datasetid].cpi t12 on t1.machine=t12.machine and t1.time=t12.t2_end left join each [datasetid].mai t13 on t1.machine=t13.machine and t1.time=t13.t2_end"

bq rm [datasetid].time_to_removew2

#SHIFT WINDOWS BY 2

bq --project_id [projectid]  query --allow_large_results --destination_table=[datasetid].time_to_removew3 "select time-600000000 as time, machine, time_left from [projectid]:[datasetid].time_to_remove"

bq --project_id [projectid]  query --replace --allow_large_results --destination_table=[datasetid].datawindow3 "select t1.time+600000000 as time, t1.machine as machine, t1.time_left as time_left, t2.count as w3_evicted, t3.count as w3_failed, t4.count as w3_finished, t5.count as w3_killed, t6.count as w3_lost, t7.count as w3_running, t8.count as w3_started , t9.rate as w3_cpu, t10.rate as w3_mem, t11.rate as w3_disk, t12.rate as w3_cpi, t13.rate as w3_mai from [projectid]:[datasetid].time_to_removew3 t1 left join each [projectid]:[datasetid].evicted_task_count t2 on t1.machine=t2.machine and t1.time=t2.end left join each [projectid]:[datasetid].failed_task_count t3 on t1.machine=t3.machine and t1.time=t3.end left join each [projectid]:[datasetid].finished_task_count t4 on t1.machine=t4.machine and t1.time=t4.end left join each [projectid]:[datasetid].killed_task_count t5 on t1.machine=t5.machine and t1.time=t5.end left join each [projectid]:[datasetid].lost_task_count t6 on t1.machine=t6.machine and t1.time=t6.end left join each [projectid]:[datasetid].running_task_count t7 on t1.machine=t7.machine and t1.time=t7.time left join each [projectid]:[datasetid].started_task_count t8 on t1.machine=t8.machine and t1.time=t8.end left join each [datasetid].cpu t9 on t1.machine=t9.machine and t1.time=t9.t2_end left join each [datasetid].mem t10 on t1.machine=t10.machine and t1.time=t10.t2_end left join each [datasetid].disk_time t11 on t1.machine=t11.machine and t1.time=t11.t2_end left join each [datasetid].cpi t12 on t1.machine=t12.machine and t1.time=t12.t2_end left join each [datasetid].mai t13 on t1.machine=t13.machine and t1.time=t13.t2_end"
#197

bq rm [datasetid].time_to_removew3

#SHIFT WINDOWS BY 3

bq --project_id [projectid]  query --allow_large_results --destination_table=[datasetid].time_to_removew4 "select time-900000000 as time, machine, time_left from [projectid]:[datasetid].time_to_remove"

bq --project_id [projectid]  query --allow_large_results --destination_table=[datasetid].datawindow4 "select t1.time+900000000 as time, t1.machine as machine, t1.time_left as time_left, t2.count as w4_evicted, t3.count as w4_failed, t4.count as w4_finished, t5.count as w4_killed, t6.count as w4_lost, t7.count as w4_running, t8.count as w4_started , t9.rate as w4_cpu, t10.rate as w4_mem, t11.rate as w4_disk, t12.rate as w4_cpi, t13.rate as w4_mai from [projectid]:[datasetid].time_to_removew4 t1 left join each [projectid]:[datasetid].evicted_task_count t2 on t1.machine=t2.machine and t1.time=t2.end left join each [projectid]:[datasetid].failed_task_count t3 on t1.machine=t3.machine and t1.time=t3.end left join each [projectid]:[datasetid].finished_task_count t4 on t1.machine=t4.machine and t1.time=t4.end left join each [projectid]:[datasetid].killed_task_count t5 on t1.machine=t5.machine and t1.time=t5.end left join each [projectid]:[datasetid].lost_task_count t6 on t1.machine=t6.machine and t1.time=t6.end left join each [projectid]:[datasetid].running_task_count t7 on t1.machine=t7.machine and t1.time=t7.time left join each [projectid]:[datasetid].started_task_count t8 on t1.machine=t8.machine and t1.time=t8.end left join each [datasetid].cpu t9 on t1.machine=t9.machine and t1.time=t9.t2_end left join each [datasetid].mem t10 on t1.machine=t10.machine and t1.time=t10.t2_end left join each [datasetid].disk_time t11 on t1.machine=t11.machine and t1.time=t11.t2_end left join each [datasetid].cpi t12 on t1.machine=t12.machine and t1.time=t12.t2_end left join each [datasetid].mai t13 on t1.machine=t13.machine and t1.time=t13.t2_end"

bq rm [datasetid].time_to_removew4

#SHIFT WINDOWS BY 4

bq --project_id [projectid]  query --allow_large_results --destination_table=[datasetid].time_to_removew5 "select time-1200000000 as time, machine, time_left from [projectid]:[datasetid].time_to_remove"

bq --project_id [projectid]  query --allow_large_results --destination_table=[datasetid].datawindow5 "select t1.time+1200000000 as time, t1.machine as machine, t1.time_left as time_left, t2.count as w5_evicted, t3.count as w5_failed, t4.count as w5_finished, t5.count as w5_killed, t6.count as w5_lost, t7.count as w5_running, t8.count as w5_started , t9.rate as w5_cpu, t10.rate as w5_mem, t11.rate as w5_disk, t12.rate as w5_cpi, t13.rate as w5_mai from [projectid]:[datasetid].time_to_removew5 t1 left join each [projectid]:[datasetid].evicted_task_count t2 on t1.machine=t2.machine and t1.time=t2.end left join each [projectid]:[datasetid].failed_task_count t3 on t1.machine=t3.machine and t1.time=t3.end left join each [projectid]:[datasetid].finished_task_count t4 on t1.machine=t4.machine and t1.time=t4.end left join each [projectid]:[datasetid].killed_task_count t5 on t1.machine=t5.machine and t1.time=t5.end left join each [projectid]:[datasetid].lost_task_count t6 on t1.machine=t6.machine and t1.time=t6.end left join each [projectid]:[datasetid].running_task_count t7 on t1.machine=t7.machine and t1.time=t7.time left join each [projectid]:[datasetid].started_task_count t8 on t1.machine=t8.machine and t1.time=t8.end left join each [datasetid].cpu t9 on t1.machine=t9.machine and t1.time=t9.t2_end left join each [datasetid].mem t10 on t1.machine=t10.machine and t1.time=t10.t2_end left join each [datasetid].disk_time t11 on t1.machine=t11.machine and t1.time=t11.t2_end left join each [datasetid].cpi t12 on t1.machine=t12.machine and t1.time=t12.t2_end left join each [datasetid].mai t13 on t1.machine=t13.machine and t1.time=t13.t2_end"

bq rm [datasetid].time_to_removew5

#SHIFT WINDOWS BY 5

bq --project_id [projectid]  query --allow_large_results --destination_table=[datasetid].time_to_removew6 "select time-1500000000 as time, machine, time_left from [projectid]:[datasetid].time_to_remove"

bq --project_id [projectid]  query --allow_large_results --destination_table=[datasetid].datawindow6 "select t1.time+1500000000 as time, t1.machine as machine, t1.time_left as time_left, t2.count as w6_evicted, t3.count as w6_failed, t4.count as w6_finished, t5.count as w6_killed, t6.count as w6_lost, t7.count as w6_running, t8.count as w6_started , t9.rate as w6_cpu, t10.rate as w6_mem, t11.rate as w6_disk, t12.rate as w6_cpi, t13.rate as w6_mai from [projectid]:[datasetid].time_to_removew6 t1 left join each [projectid]:[datasetid].evicted_task_count t2 on t1.machine=t2.machine and t1.time=t2.end left join each [projectid]:[datasetid].failed_task_count t3 on t1.machine=t3.machine and t1.time=t3.end left join each [projectid]:[datasetid].finished_task_count t4 on t1.machine=t4.machine and t1.time=t4.end left join each [projectid]:[datasetid].killed_task_count t5 on t1.machine=t5.machine and t1.time=t5.end left join each [projectid]:[datasetid].lost_task_count t6 on t1.machine=t6.machine and t1.time=t6.end left join each [projectid]:[datasetid].running_task_count t7 on t1.machine=t7.machine and t1.time=t7.time left join each [projectid]:[datasetid].started_task_count t8 on t1.machine=t8.machine and t1.time=t8.end left join each [datasetid].cpu t9 on t1.machine=t9.machine and t1.time=t9.t2_end left join each [datasetid].mem t10 on t1.machine=t10.machine and t1.time=t10.t2_end left join each [datasetid].disk_time t11 on t1.machine=t11.machine and t1.time=t11.t2_end left join each [datasetid].cpi t12 on t1.machine=t12.machine and t1.time=t12.t2_end left join each [datasetid].mai t13 on t1.machine=t13.machine and t1.time=t13.t2_end"

bq rm [datasetid].time_to_removew6


##########################################
##############CREATE TABLES aggregated1h, aggregated12h, aggregated24h, aggregated48h, aggregated72h, aggregated96h - AVERAGE, STDEV AND COEFF OF VARIATION FOR ALL FEATURES ABOVE OVER MORE TIME WINDOWS (1 HOUR TO 96 HOURS)

#BASIC FEATURES
cols=('evicted' 'failed' 'finished' 'killed' 'lost' 'running' 'started' 'cpu' 'mem' 'disk' 'cpi' 'mai' )
query1=''
for c in ${cols[*]};
do
	query1=$query1', IFNULL(t2.w1_'$c',0) as '$c
done 

query2=''
for c in ${cols[*]};
do
	query2=$query2', avg('$c') as avg_'$c', STDDEV('$c') as sd_'$c
done 

#AGGREGATION LEVELS
vals=(1 12 24 48 72 96)
names=('1h' '12h' '24h' '48h' '72h' '96h')
for i in `seq 0 5`;
do
	echo $i
	####CREATE INTERMEDIATE TABLES. ATTENTION, THESE TABLES ARE VERY LARGE!!!!!!
	bq --project_id [projectid]  query --allow_large_results --destination_table=[datasetid].interm${names[$i]} "select t1.end as end, t2.machine as machine, t2.time_left as time_left $query1  from [datasetid].datawindow1 t2 cross join [datasetid].windows5 t1 where t2.time<=t1.end and t2.time>t1.end-${vals[$i]}*3600000000" 
	####AGGREGATE FROM INTERMEDIATE TABLE
	bq --project_id [projectid]  query --allow_large_results --destination_table=[datasetid].aggregated${names[$i]} "select end as time, machine, min(time_left) as time_left $query2  from [datasetid].interm${names[$i]} group each by time, machine" 
done

#####!!!!!!!!!!!!!!!!!!!!
####THE AGGREGATION QUERY ABOVE WORKS ONLY FOR 1 AND 12 HOURS - AVERAGE AND STDEV FOR ALL FEATURES IS TOO HEAVY
####SO HERE WE COMPUTE AVERAGE AND STDEV FOR EACH FEATURE SEPARATELY FOR  24h, 48h 72h 96h, THEN WE COMBINE ALL FEATURES

#BASIC FEATURES
cols=('evicted' 'failed' 'finished' 'killed' 'lost' 'running' 'started' 'cpu' 'mem' 'disk' 'cpi' 'mai' )

#FOR EACH WINDOW LENGTH( 24H TO 96H)
for i in `seq 2 5`;
do
	echo $i
	##CREATE ONE TABLE FOR EACH FEATURE (E.G. [datasetid].aggregated24h_evicted CONTAINING avg_evicted AND std_evicted)
	query=''
	for j in `seq 0 11`;
	do
		bq --project_id [projectid]  query --allow_large_results --destination_table=[datasetid].aggregated${names[$i]}_${cols[$j]} "select end as time, machine, min(time_left) as time_left, avg(${cols[$j]}) as avg_${cols[$j]}, stddev(${cols[$j]}) as std_${cols[$j]}  from [datasetid].interm${names[$i]} group each by time, machine" 
query=$query', t'$j'.avg_'${cols[$j]}' as avg_'${cols[$j]}', t'$j'.std_'${cols[$j]}' as sd_'${cols[$j]}
	done
	tables=''
	for j in `seq 1 11`;
	do
		tables=$tables' left join each [datasetid].aggregated'${names[$i]}'_'${cols[$j]}' t'$j' on t0.time=t'$j'.time and t0.machine=t'$j'.machine'
	done
	###COMBINE ALL FEATURES IN ONE TABLE (E.G. [datasetid].aggregated24h)
	bq --project_id [projectid]  query --allow_large_results --destination_table=[datasetid].aggregated${names[$i]} "select t0.time as time, t0.machine as machine, t0.time_left as time_left $query from [datasetid].aggregated${names[$i]}_${cols[0]} t0 $tables" 
done

#CLEANUP INTERMEDIATE FEATURE TABLES
for i in `seq 2 5`;
do
	for j in `seq 0 11`;
	do
		bq rm [datasetid].aggregated${names[$i]}_${cols[$j]}
	done
done



##########################################
##############CREATE TABLES corr1h, corr12h, corr24h, corr48h, corr72h, corr96h - CONTAIN CORRELATIONS BETWEEN FEATURES (WITHIN SAME MACHINE) FOR WINDOWS OF 1H, 12H, ETC
####USE INTERMEDIATE TABLES ABOVE (THE LARGE ONES)
####!!!!ATTENTION!!!!! THESE QUERIES ARE VERY LARGE AND USE A LOT OF GOOGLE CREDITS (OVER $1500 IN TOTAL) !!!

#FEATURES TO BE CORRELATED
cols=('running' 'started' 'failed' 'cpu' 'mem' 'disk' 'cpi')
#WINDOW SIZE
names=('1h' '12h' '24h' '48h' '72h' '96h')

query=''

for i in `seq 0 5`;
do
	for (( j=i+1; j<=6; j++ ));
	do
		query=$query', corr('${cols[$i]}','${cols[$j]}') as corr_'${cols[$i]}'_'${cols[$j]}
	done
done


for i in `seq 0 5`;
do
	echo $i
	bq --project_id [projectid]  query --allow_large_results --destination_table=[datasetid].corr${names[$i]} "select end as time, machine, min(time_left) as time_left $query  from [datasetid].interm${names[$i]} group each by time, machine" 
done
#ERROR AGAIN BigQuery error in query operation: Error processing job '[projectid]:bqjob_re5ad68e12f5991a_0000014b922a6806_1': Resources exceeded during query execution


#ABOVE QUERY GAVE AN ERROR AGAIN FOR 12H-96H, WE HAVE TO COMPUTE CORRELATIONS ONE BY ONE AND NOT ALL PAIRS AT A TIME. 

for c in `seq 1 5`;
do
	query=''
	tables=''
	#FIRST COMPUTE CORRLATIONS FOR EACH PAIR AND SAVE THEM IN SEPARATE TABLES
	for i in `seq 0 5`;
	do
		echo $i
		for (( j=i+1; j<=6; j++ ));
		do
			bq --project_id [projectid] query --allow_large_results --destination_table=[datasetid].corr${names[$c]}_${cols[$i]}_${cols[$j]} "select end as time, machine, min(time_left) as time_left, corr(${cols[$i]},${cols[$j]}) as corr from [datasetid].interm${names[$c]} group each by time, machine" 
			query=$query', t'$i$j'.corr as corr_'${cols[$i]}'_'${cols[$j]}
			if [ "$i$j" -ne "01" ];
			then
				tables=$tables' left join each [datasetid].corr'${names[$c]}'_'${cols[$i]}'_'${cols[$j]}' t'$i$j' on t01.time=t'$i$j'.time and t01.machine=t'$i$j'.machine'
			fi
		done
	done
	#AT THE END COMBINE ALL PAIRS IN ONE LARGE TABLE
	bq --project_id [projectid]  query --allow_large_results --destination_table=[datasetid].corr${names[$c]} "select t01.time as time, t01.machine as machine, t01.time_left as time_left $query from [datasetid].corr${names[$c]}_${cols[0]}_${cols[1]} t01 $tables" 
done


#CLEANUP - DELETE INTERMEDIATE TABLES
for c in `seq 0 5`;
do
for i in `seq 0 5`;
do
for (( j=i+1; j<=6; j++ ));
do
bq rm [datasetid].corr${names[$c]}_${cols[$i]}_${cols[$j]} 
done
done
done


##########################################
######CREATE TABLE UP_TIME

bq --project_id [projectid]  query  --replace --allow_large_results --destination_table=[datasetid].up_time "select t2.end as time, t1.machine_id as machine, t2.end-t1.add as up_time from [datasetid].addremove t1 cross join [datasetid].windows5 t2 where (t2.end between t1.add and t1.remove) or (t1.remove is null and t2.end>t1.add)"


##########################################
######CREATE TABLE [datasetid].removals24h - NUMBER OF REMOVALS IN LAST 24 HOURS

bq --project_id [projectid]  query  --allow_large_results --destination_table=[datasetid].removals24h "select t2.end as time, count(t1.machine_id) as count from [datasetid].addremove t1 cross join [datasetid].windows5 t2 where t1.remove between t2.end-24*3600000000 and t2.end group by time"


##########################################
####FIND OUT WHICH REMOVALS ARE DUE TO FAILURE (MACHINE STAYS OFF FOR MORE THAN 2 HOURS)

##CREATE AN INTERMEDIATE TABLE WHERE WE HAVE FOR EACH MACHINE THE ADD, REMOVE AND RE-ADD TIMES

#FIRST CROSS JOIN ADDREMOVE WITH ITSELF

bq --project_id [projectid]  query --replace --allow_large_results --destination_table=[datasetid].addremoveadd_interm "select t1.machine_id as machine_id, t1.add as add, t1.remove as remove, t2.add as readd from [datasetid].addremove t1 left join each [datasetid].addremove t2 on t1.machine_id =t2.machine_id"

#REPLACE READD WITH NULL IF SAME WITH ADD

bq --project_id [projectid]  query --replace --allow_large_results --destination_table=[datasetid].addremoveadd_interm1 "select machine_id, add, remove, if(add=readd,null,readd) from [datasetid].addremoveadd_interm"

#SELECT ONLY RECORDS WITH READD>REMOVE OR READD NULL (NEVER BACK ONLINE) AND FIND FIRST READD AFTER A REMOVE

bq --project_id [projectid]  query --replace --allow_large_results --destination_table=[datasetid].addremoveadd "select machine_id, add, remove, min(f0_) as readd from [datasetid].addremoveadd_interm1 where f0_>remove or f0_ is null group by machine_id,add,remove"

#SELECT EVENTS THAT ARE RE-ADDED AFTER AT LEAST 2 HOURS

bq --project_id [projectid]  query --replace --allow_large_results --destination_table=[datasetid].addremoveadd_120min "select machine_id, add, remove, readd from [datasetid].addremoveadd where readd-remove>120*60000000 or (readd is null and 2506198229242-remove>120*60000000)"


##########################################
##########COMBINE ALL DATA IN ONE DATASET
#BASE FEATURES
cols=('evicted' 'failed' 'finished' 'killed' 'lost' 'running' 'started' 'cpu' 'mem' 'disk' 'cpi' 'mai' )
#WINDOWS FOR BASE FEATURES
windows=('1' '2' '3' '4' '5' '6')
#AGGREGATION WINDOWS
agg=('1h' '12h' '24h' '48h' '72h' '96h')
#FEATURES TO BE CORRELATED
corr_c=('running' 'started' 'failed' 'cpu' 'mem' 'disk' 'cpi')


##CREATE ONE LONG QUERY THAT JOINS ALL TABLES: datawindow1-6, aggregated1-96h, corr1-96h 
tables=''
columns=''

for j in `seq 0 5`;
do
	for i in `seq 0 11`
	do
		columns=$columns', IFNULL(w'${windows[$j]}'.w'${windows[$j]}'_'${cols[$i]}',0) as w'${windows[$j]}'_'${cols[$i]}
	done
	tables=$tables' left join each [datasetid].datawindow'${windows[$j]}' w'${windows[$j]}' on t1.machine=w'${windows[$j]}'.machine and t1.time=w'${windows[$j]}'.time'
done

for j in `seq 0 5`;
do
	for i in `seq 0 11`
	do
		columns=$columns', a'${agg[$j]}'.avg_'${cols[$i]}' as avg'${agg[$j]}'_'${cols[$i]}', a'${agg[$j]}'.sd_'${cols[$i]}' as sd'${agg[$j]}'_'${cols[$i]}', a'${agg[$j]}'.sd_'${cols[$i]}'/a'${agg[$j]}'.avg_'${cols[$i]}' as cv'${agg[$j]}'_'${cols[$i]}
	done
	tables=$tables' left join each [datasetid].aggregated'${agg[$j]}' a'${agg[$j]}' on t1.machine=a'${agg[$j]}'.machine and t1.time=a'${agg[$j]}'.time'
done

for j in `seq 0 5`;
do
	for i in `seq 0 5`;
	do
		for (( k=i+1; k<=6; k++ ));
		do
			columns=$columns', c'${agg[$j]}'.corr_'${corr_c[$i]}'_'${corr_c[$k]}' as corr'${agg[$j]}'_'${corr_c[$i]}'_'${corr_c[$k]}
		done
	done
	tables=$tables' left join each [datasetid].corr'${agg[$j]}' c'${agg[$j]}' on t1.machine=c'${agg[$j]}'.machine and t1.time=c'${agg[$j]}'.time'
done


bq --project_id [projectid]  query --replace --allow_large_results --destination_table=[datasetid].all_data "select t1.time as time, t1.machine as machine, t1.time_left as time_left, t2.up_time as up_time $columns , r.count as removals24h from [datasetid].time_to_remove t1 left join each [datasetid].up_time t2 on t1.machine=t2.machine and t1.time=t2.time $tables left join each [datasetid].removals24h r on t1.time=r.time"


##########################################
##########EXTRACT ONLY DATA FOR REMOVALS DUE TO FAILURES
bq --project_id [projectid]  query  --replace --allow_large_results --destination_table=[datasetid].all_data_real "select t1.* from [datasetid].all_data t1 left join each [datasetid].addremoveadd_120min t2 on t1.machine=t2.machine_id where t1.time>t2.add and t1.time<=t2.remove"


##########################################
###########EXPORT DATA TO GOOGLE CLOUD STORAGE

#TWO CLASSES, SAFE AND FAIL BASED ON TIME_TO_REMOVE (TIME TO NEXT FAILURE UNDER 24 HOURS MEANS DATA POINT IS IN FAIL CLASS)

#CREATE TABLE WITH FAIL CLASS: SELECT FROM all_data_real AND SUBSAMPLE 20% OF THE DATA - ORDER DATA BY TIME. FIRST COLUMN IS THE CLASS, ALWAYS 1.

bq --project_id [projectid]  query --replace --allow_large_results --destination_table=[datasetid].fail24_real "select  1, * from [datasetid].all_data_real where t1_time_left<24*3600000000 and integer(t1_w1_cpi*1000)%5==0 order by t1_time"

#CREATE TABLE WITH SAFE CLASS: SELECT FROM all_data AND SUBSAMPLE 0.5% OF THE DATA. FIRST COLUMN IS THE CLASS, ALWAYS 0. ORDER BY DOES NOT WORK HERE, HAS TO BE DONE AFTERWARDS.

bq --project_id [projectid]  query --replace --allow_large_results --destination_table=[datasetid].safe24_real "select  0, * from [datasetid].all_data where time_left>=24*3600000000 and integer(w1_cpi*1000)%200==0 "


#EXTRACT TO GOOGLE CLOUD STORAGE

bq extract [datasetid].fail24_real gs://clusterdata/real/24h/fail24_real*.csv
bq extract [datasetid].safe24_real gs://clusterdata/real/24h/safe24_real*.csv

#COMPOSE TO ONE FILE PER CLASS (fail24.csv, safe24.csv)

gsutil cp gs://clusterdata/real/24h/fail24_real* gs://clusterdata/real/fail24.csv
gsutil compose gs://clusterdata/real/24h/safe24_real* gs://clusterdata/real/safe24.csv

#DOWNLOAD FILES

gsutil cp gs://clusterdata/real/fail24.csv ./fail24.csv
gsutil cp gs://clusterdata/real/safe24.csv ./safe24.csv

#GZIP TO SAVE SPACE

gzip fail24.csv
gzip safe24.csv


##########################################
###########ANALIZE RESULTS : IMPORT FAILURE 
###########PREDICTION RESULTS AND COMPUTE REDIRECTED
########### AND RECOVERED RESOURCES


bq --project_id [projectid] load --max_bad_records=6 --source_format=CSV [projectid]:[datasetid].predictions gs://clusterdata-hrd/prediction_results.csv timestamp:float,machine_id:string,ttr:float,prediction:float,real_class:float,threshold0_2:float,threshold0_1:float,threshold0_05:float,threshold0_01:float

bq --project_id [projectid] load --max_bad_records=6 --source_format=CSV [projectid]:[datasetid].test_benchmarks gs://clusterdata-hrd/benchmark_test_times.csv start:float,end:float

##jobs
bq --project_id [projectid]  query --replace --allow_large_results --destination_table=[datasetid].jobs " SELECT job_id, max(scheduling_class) as scheduling_class FROM [[datasetid].job_events]  group by job_id "


####failures during test period
bq --project_id [projectid]  query --replace --allow_large_results --destination_table=[datasetid].test_fails "select t1.* from [datasetid].addremoveadd_120min t1 cross join  [datasetid].test_benchmarks t2 where t1.remove between t2.start and t2.end "


####tasks evicted or killed for failures

bq --project_id [projectid]  query --replace --allow_large_results --destination_table=[datasetid].tasks_stopped_fail "select t1.t1_job_id as job, t1.t1_task_index as task, t1.t1_timestamp as start, t1.t2_timestamp-t1.t1_timestamp as cputime, t1.t2_event_type as event, t1.t2_timestamp-t2.t1_remove as delta_end_fail  from [datasetid].running_tasks_fixed t1 cross join [datasetid].test_fails t2  where t1.t1_machine_id=t2.t1_machine_id and t2.t1_remove>t1.t1_timestamp and t1.t2_timestamp>(t2.t1_remove-1000)  and t1.t2_event_type not in (3,4) "

bq --project_id [projectid]  query  "select count(*), sum(cputime)/3600000000 from [datasetid].tasks_stopped_fail"

##divide by scheduling class
bq --project_id [projectid]  query  "select t1.scheduling_class as class, count(*) as task_count, sum(t2.cputime)/3600000000 as cpuhours from [datasetid].jobs t1 inner join [datasetid].tasks_stopped_fail t2 on t1.job_id=t2.job group by class"


###tasks redirected after perfect prediction (baseline)

bq --project_id [projectid]  query --replace --allow_large_results --destination_table=[datasetid].tasks_redirected_perfect_predictions "select t1.t1_job_id as job, t1.t1_task_index as task, t1.t1_timestamp as start, t1.t2_timestamp-t1.t1_timestamp as cputime, t1.t2_event_type as event, t1.t2_timestamp-t2.t1_remove as delta_end_fail, t1.t1_machine_id as machine from [datasetid].running_tasks_fixed t1 cross join [datasetid].test_fails t2  where float(t1.t1_machine_id)=float(t2.t1_machine_id) and t1.t1_timestamp between (t2.t1_remove-(24*3600000000)) and t2.t1_remove "

#only start times in the test period
bq --project_id [projectid]  query --replace --allow_large_results --destination_table=[datasetid].tasks_redirected_perfect_predictions "select t1.job as job,t1.task as task,t1.start as start,t1.cputime as cputime,t1.event as event,t1.delta_end_fail as delta_end_fail from [datasetid].tasks_redirected_perfect_predictions t1 cross join [datasetid].test_benchmarks t2 where t1.start between t2.start and t2.end "


#unique
bq --project_id [projectid]  query --replace --allow_large_results --destination_table=[datasetid].tasks_redirected_perfect_predictions "select job,task,start,cputime,event,max(delta_end_fail) as delta_end_fail from [datasetid].tasks_redirected_perfect_predictions group by job,task,start,cputime,event "


bq --project_id [projectid]  query  "select t1.scheduling_class as class, count(*) as task_count, sum(t2.cputime)/3600000000 as cpuhours from [datasetid].jobs t1 inner join [datasetid].tasks_redirected_perfect_predictions t2 on t1.job_id=t2.job group by class order by class"


bq --project_id [projectid]  query  "select t1.scheduling_class as class, count(*) as task_count, sum(t2.cputime)/3600000000 as cpuhours from [datasetid].jobs t1 inner join [datasetid].tasks_redirected_perfect_predictions t2 on t1.job_id=t2.job inner join [datasetid].tasks_stopped_fail t3 on t2.job=t3.job and t2.task=t3.task and t2.start=t3.start group by class order by class"


bq --project_id [projectid]  query  "select  count(*) , sum(cputime)/3600000000 from  [datasetid].tasks_redirected_perfect_predictions "


bq --project_id [projectid]  query  "select  count(*), sum(t2.cputime)/3600000000 from  [datasetid].tasks_redirected_perfect_predictions t2 inner join [datasetid].tasks_stopped_fail t3 on t2.job=t3.job and t2.task=t3.task and t2.start=t3.start "



######Redirect after 2 consecutive alarms (non perfect prediction)

trh='0_1' #FPR 0.1, replace with 0_05 or 0_2 for FPR of 0.05 or 0.2

bq --project_id [projectid]  query --replace --allow_large_results --destination_table=[datasetid].positives$trh "select string(integer(float(machine_id))) as machine_id, timestamp ,  ttr from [datasetid].predictions where prediction>=threshold$trh"

bq --project_id [projectid]  query --replace --allow_large_results --destination_table=[datasetid].twicepositives$trh "select string(integer(float(t1.machine_id))) as machine_id, t1.timestamp as timestamp,  t1.ttr as ttr from [datasetid].positives$trh t1 cross join [datasetid].positives$trh t2 where t1.timestamp=t2.timestamp-300000000 and t1.machine_id=t2.machine_id"

minutes=30  ##quarantine window in minutes 

bq --project_id [projectid]  query --replace --allow_large_results --destination_table=[datasetid].tasks_redirected_twicepositives$trh "select t1.t1_job_id as job, t1.t1_task_index as task, t1.t1_timestamp as start,t1.t2_timestamp-t1.t1_timestamp as cputime, t1.t2_event_type as event, t1.t2_timestamp-t2.timestamp-ttr as delta_end_fail from (select * from [datasetid].running_tasks_fixed where t2_timestamp>1.1658E12 or t2_timestamp is null ) t1 inner join [datasetid].twicepositives$trh t2  on t1.t1_machine_id=t2.machine_id where t1.t1_timestamp between t2.timestamp and (t2.timestamp+${minutes}*60000000)"

#unique only
bq --project_id [projectid]  query --replace --allow_large_results --destination_table=[datasetid].tasks_redirected_twicepositives$trh "select job,task,start,cputime,event,max(delta_end_fail) as  delta_end_fail from [datasetid].tasks_redirected_twicepositives$trh group by job,task,start,cputime,event "

##only start times within test times
bq --project_id [projectid]  query --replace --allow_large_results --destination_table=[datasetid].tasks_redirected_twicepositives$trh "select t1.job as job,t1.task as task,t1.start as start,t1.cputime as cputime,t1.event as event,t1.delta_end_fail as delta_end_fail from [datasetid].tasks_redirected_twicepositives$trh t1 cross join [datasetid].test_benchmarks t2 where t1.start between t2.start and t2.end "


bq --project_id [projectid]  query  "select t1.scheduling_class as class, count(*) as task_count, sum(t2.cputime)/3600000000 as cpuhours from [datasetid].jobs t1 inner join each [datasetid].tasks_redirected_twicepositives$trh t2 on t1.job_id=t2.job group by class order by class"


bq --project_id [projectid]  query  "select t1.scheduling_class as class, count(*) as task_count, sum(t2.cputime)/3600000000 as cpuhours from [datasetid].jobs t1 inner join each [datasetid].tasks_redirected_twicepositives$trh t2 on t1.job_id=t2.job inner join [datasetid].tasks_stopped_fail t3 on t2.job=t3.job and t2.task=t3.task and t2.start=t3.start group by class order by class"



