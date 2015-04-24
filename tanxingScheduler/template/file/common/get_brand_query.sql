SET mapred.job.name = 'get_brand_query';

--set mapred.job.queue.name=ecomon-qe-adhoc;
--set mapred.job.tracker=szwg-idle-abaci.dmop.baidu.com:54311;

set mapred.job.map.capacity = 300;
set mapred.job.reduce.capacity = 200;
set hive.exec.reducers.max = 200;
set mapred.job.priority=HIGH;
set abaci.is.dag.job=true;

tmp_db = database mydb.tmp_db;
use tmp_db; 

ADD JAR ${hivevar:base_dir}/UDAFGroupConcat.jar;
CREATE TEMPORARY FUNCTION group_concat AS "com.baidu.ud.hive.udaf.UDAFGroupConcat";

DROP TABLE sem_tl_get_brand_query_${hivevar:task_id};
CREATE TABLE sem_tl_get_brand_query_${hivevar:task_id} AS
SELECT 
     brand_query, group_concat(brand_map_id,",") as brand_id_str
FROM mydb.sem_bl_brand_query 
WHERE brand_map_id in ( ${hivevar:brand_id_str} ) 
GROUP BY brand_query





