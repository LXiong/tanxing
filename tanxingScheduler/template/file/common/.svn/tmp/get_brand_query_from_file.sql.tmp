SET mapred.job.name = 'get_brand_query';

--set mapred.job.queue.name=ecomon-qe-adhoc;
--set mapred.job.tracker=szwg-idle-abaci.dmop.baidu.com:54311;
--set mapred.job.queue.name = bim;
--set mapred.job.tracker = szwg-stoff-abaci.dmop.baidu.com:54311;

set mapred.job.map.capacity = 300;
set mapred.job.reduce.capacity = 200;
set hive.exec.reducers.max = 200;
set mapred.job.priority=HIGH;
set abaci.is.dag.job=true;

tmp_db = database mydb.tmp_db;
use tmp_db; 

ADD JAR ${hivevar:base_dir}/UDAFGroupConcat.jar;
CREATE TEMPORARY FUNCTION group_concat AS "com.baidu.ud.hive.udaf.UDAFGroupConcat";

DROP TABLE sem_tl_get_brand_query_tmp_${hivevar:task_id};
CREATE TABLE sem_tl_get_brand_query_tmp_${hivevar:task_id}(brand_query string, brand_map_id bigint)
row format delimited fields terminated by '\t';

add file ${hivevar:wordbag};
load data local inpath '${hivevar:wordbag}' overwrite into table sem_tl_get_brand_query_tmp_${hivevar:task_id};

DROP TABLE if exists sem_tl_get_brand_query_${hivevar:task_id};
CREATE TABLE sem_tl_get_brand_query_${hivevar:task_id} AS
SELECT 
     brand_query, 
     group_concat(brand_map_id,",") as brand_id_str
FROM tmp_db.sem_tl_get_brand_query_tmp_${hivevar:task_id}
group by brand_query,brand_map_id;




