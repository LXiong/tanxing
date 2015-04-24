SET mapred.job.name = 'channel_stat';

--set mapred.job.queue.name=ecomon-qe-adhoc;
--set mapred.job.tracker=szwg-idle-abaci.dmop.baidu.com:54311;

set mapred.job.map.capacity = 300;
set mapred.job.reduce.capacity = 200;
set hive.exec.reducers.max = 200;
set mapred.job.priority=HIGH;
set abaci.is.dag.job=true;

tmp_db = database mydb.tmp_db;
use tmp_db; 

ADD FILE ${hivevar:base_dir}/add_stay_time.sh;

DROP TABLE sem_tl_behavior_${hivevar:subtask_id};
CREATE TABLE sem_tl_behavior_${hivevar:subtask_id}
             ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"  AS
SELECT 
TRANSFORM( event_baiduid, session_id, event_time, step_id, product_name, event_query, event_url, action_name )
USING 'sh add_stay_time.sh'
AS ( event_baiduid, session_id, event_time, stay_time, step_id, product_name, event_query, event_url, action_name )
FROM  (
    SELECT 
        u.event_baiduid, u.session_id, unix_timestamp(u.event_time) as event_time, cast( u.step_id as bigint ) as step_id, u.product_name, u.event_query, u.event_url, u.action_name
    FROM  ( SELECT session_id, event_baiduid from sem_tl_get_people_${hivevar:subtask_id} GROUP BY session_id, event_baiduid ) p
    JOIN  ( select * from default.udwetl_gs_detail_ka where event_day in ( ${hivevar:date_str_default} )
    union all select * from mydb_yl.udwetl_gs_detail_ka where event_day in ( ${hivevar:date_str_yl} )
) u
    ON    p.session_id = u.session_id  and p.event_baiduid = u.event_baiduid
--    WHERE event_day in ( ${hivevar:date_str} )                          
    DISTRIBUTE BY event_baiduid
    sort by event_baiduid, session_id, step_id
) m














    


