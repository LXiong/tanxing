set mapred.job.name = 'get_query';

--set mapred.job.queue.name=ecomon-qe-adhoc;

--set mapred.job.tracker=szwg-idle-abaci.dmop.baidu.com:a4311;
set mapred.job.queue.name = bim;
set mapred.job.tracker = szwg-stoff-abaci.dmop.baidu.com:54311;

set mapred.job.map.capacity = 800;
set mapred.job.reduce.capacity = 800;
set hive.exec.reducers.max = 800;
set mapred.job.priority=HIGH;

set abaci.is.dag.job=true;

tmp_db = database mydb.tmp_db;
use tmp_db; 

ADD JAR ${hivevar:base_dir}/UDAFGroupConcat.jar;
CREATE TEMPORARY FUNCTION group_concat AS "com.baidu.ud.hive.udaf.UDAFGroupConcat";

ADD FILE ${hivevar:base_dir}/brand_uniq.sh;

INSERT  OVERWRITE TABLE sem_tl_get_brand_people_${hivevar:task_id}
SELECT 
TRANSFORM ( cookie, channel_id, brand_id_str_orig )
USING 'sh brand_uniq.sh'
AS ( cookie, channel_id, brand_id_str )
FROM (
    SELECT 
         s.cookie, cast(11000 as bigint) as channel_id,
         group_concat(q.brand_id_str,",") as brand_id_str_orig
    FROM sem_tl_get_brand_query_${hivevar:task_id} q
    JOIN mydb.sem_al_ps_wise_search_theme s
    ON   q.brand_query = s.normalized_query
    WHERE 
         partition_stat_date in ( ${hivevar:date_str} ) AND
         case when "${hivevar:province}" != "0" then  province in ( ${hivevar:province} ) else 1=1 end AND 
         case when "${hivevar:city}" != "0" then city in ( ${hivevar:city} ) else 1=1 end AND
         s.device = "wise"
    GROUP BY s.cookie, cast(11000 as bigint) )m
;
