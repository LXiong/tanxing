set mapred.job.name = 'get_brand_people_pc';

--set mapred.job.queue.name=ecomon-qe-adhoc;
--set mapred.job.tracker=szwg-idle-abaci.dmop.baidu.com:54311;
--set mapred.job.queue.name = bim;
--set mapred.job.tracker = szwg-stoff-abaci.dmop.baidu.com:54311;

set mapred.job.map.capacity = 800;
set mapred.job.reduce.capacity = 800;
set hive.exec.reducers.max = 800;
set mapred.job.priority=HIGH;

set abaci.is.dag.job=true;

tmp_db = database mydb.tmp_db;
use tmp_db; 

ADD JAR UDAFGroupConcat.jar;
CREATE TEMPORARY FUNCTION group_concat AS "com.baidu.ud.hive.udaf.UDAFGroupConcat";

ADD FILE brand_people_format.sh;
ADD FILE brand_uniq.sh;
drop table if exists get_brand_people_pc_temp_${hivevar:task_id};
create table  get_brand_people_pc_temp_${hivevar:task_id}
row format delimited fields terminated by '\t'
as
SELECT 
TRANSFORM ( cookie, channel_id, brand_id_str)
USING 'sh brand_people_format.sh'
AS ( cookie, channel_id, brand_id_str )
FROM (
    SELECT 
         s.cookie, 
         cast(10011 as bigint)  as channel_id,
         q.brand_id_str 
    FROM sem_tl_get_brand_query_${hivevar:task_id} q
    JOIN mydb.search_data_dump_from_udw_event s
    ON   q.brand_query = s.normalized_query
    WHERE 
         partition_stat_date in ( ${hivevar:date_str} ) AND
         case when "${hivevar:province}" != "0" then  province in ( ${hivevar:province} ) else 1=1 end AND 
         case when "${hivevar:city}" != "0" then city in ( ${hivevar:city} ) else 1=1 end AND
         s.isspider = 0 AND s.isbroken is NULL AND
         trim(s.cookie) != "" 
    GROUP BY s.cookie,q.brand_id_str )m
;


drop table if exists sem_tl_get_brand_people_${hivevar:task_id};
create table  sem_tl_get_brand_people_${hivevar:task_id}
row format delimited fields terminated by '\t'
as
SELECT 
TRANSFORM ( cookie, channel_id, brand_id_str_orig)
USING 'sh brand_uniq.sh'
AS ( cookie, channel_id, brand_id_str )
FROM (
    SELECT
       cookie, 
       channel_id, 
       group_concat(brand_id_str,",") as brand_id_str_orig
    FROM get_brand_people_pc_temp_${hivevar:task_id}
    GROUP BY cookie, channel_id) n;
