set mapred.job.name = 'get_query';

--set mapred.job.queue.name=ecomon-qe-adhoc;
--set mapred.job.tracker=szwg-idle-abaci.dmop.baidu.com:54311;

set mapred.job.map.capacity = 800;
set mapred.job.reduce.capacity = 800;
set hive.exec.reducers.max = 800;
set mapred.job.priority=HIGH;

set abaci.is.dag.job=true;

tmp_db = database mydb.tmp_db;
use tmp_db; 

ADD FILE ${hivevar:base_dir}/brand_uv_stat.sh;

SELECT 
TRANSFORM ( brand_id_str )
USING 'sh brand_uv_stat.sh '
AS ( brand_id, uv )
FROM (
    SELECT brand_id_str
    FROM    sem_tl_get_brand_people_${hivevar:task_id} 
    WHERE   channel_id = ${hivevar:channel_id}
    ) m
;
