set mapred.job.name = 'get_query';

--set mapred.job.queue.name = bim;
--set mapred.job.tracker = szwg-stoff-abaci.dmop.baidu.com:54311;

set mapred.job.map.capacity = 300;
set mapred.job.reduce.capacity = 200;
set hive.exec.reducers.max = 200;
set mapred.job.priority=HIGH;

set abaci.is.dag.job=true;

tmp_db = database mydb.tmp_db;
use tmp_db; 

DROP TABLE sem_tl_get_people_${hivevar:subtask_id};
CREATE TABLE sem_tl_get_people_${hivevar:subtask_id} AS
SELECT 
     s.event_time, s.event_ip, s.event_country, s.event_city, s.event_province, s.event_userid, s.event_baiduid, s.product_name, s.session_id, s.step_id 
FROM sem_tl_get_query_${hivevar:subtask_id} q
JOIN ( select * from default.udwetl_gs_detail_ka where event_day in ( ${hivevar:date_str_default} )
    union all select * from mydb_yl.udwetl_gs_detail_ka where event_day in ( ${hivevar:date_str_yl} )
) s
--ON   q.normalized_query = s.event_query
on (q.normalized_query = s.event_query and q.normalized_query is not NULL and trim(q.normalized_query)  != "" and s.event_query is not NULL and trim(s.event_query) != "")
WHERE 
--    event_day in ( ${hivevar:date_str} ) AND
    case when "${hivevar:province}" != "0" then  event_province in ( ${hivevar:province} ) else 1=1 end AND 
    case when "${hivevar:city}" != "0" then event_city in ( ${hivevar:city} ) else 1=1 end AND
    case when "${hivevar:channel}" != "0" then product_name in ( ${hivevar:channel} ) else  1=1 end;





