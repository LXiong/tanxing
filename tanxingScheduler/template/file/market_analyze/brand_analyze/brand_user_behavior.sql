SET mapred.job.name = 'brand_user_behavior';

--SET mapred.job.queue.name=ecomon-qe-adhoc;
--SET mapred.job.tracker=szwg-idle-abaci.dmop.baidu.com:54311;

SET mapred.job.map.capacity = 300;
SET mapred.job.reduce.capacity = 200;
SET hive.exec.reducers.max = 200;
SET mapred.job.priority=HIGH;
SET abaci.is.dag.job=true;

tmp_db = DATABASE mydb.tmp_db;
USE tmp_db; 

DROP TABLE IF EXISTS sem_tl_brand_user_behavior_${hivevar:task_id};
CREATE TABLE sem_tl_brand_user_behavior_${hivevar:task_id}
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t"
AS
SELECT
    b.normalized_query, b.cookie,
    CASE WHEN CAST(substr(b.action_time,${hivevar:hour_pos},2) AS INT) < 6 THEN '深夜'
        WHEN CAST(substr(b.action_time,${hivevar:hour_pos},2) AS INT) < 9 THEN '早上'
        WHEN CAST(substr(b.action_time,${hivevar:hour_pos},2) AS INT) < 12 THEN '上午'
        WHEN CAST(substr(b.action_time,${hivevar:hour_pos},2) AS INT) < 14 THEN '中午'
        WHEN CAST(substr(b.action_time,${hivevar:hour_pos},2) AS INT) < 18 THEN '下午'
        WHEN CAST(substr(b.action_time,${hivevar:hour_pos},2) AS INT) < 21 THEN '傍晚'
        ELSE '夜晚'
    END AS time_range
FROM
    sem_tl_brand_cookie_${hivevar:task_id} a
JOIN
    ${hivevar:behavior_table} b
ON
    a.cookie = b.cookie
WHERE b.partition_stat_date IN ( ${hivevar:date_list} )
    AND ${hivevar:filter_condition_var1} = 0
    AND ${hivevar:filter_condition_var2} IS NULL
    AND ${hivevar:filter_condition_var3} = 'wise'
    ${hivevar:area_condition}
;

