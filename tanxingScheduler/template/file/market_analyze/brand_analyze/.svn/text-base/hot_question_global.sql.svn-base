SET mapred.job.name = 'brand_user_behavior';

--SET mapred.job.queue.name=ecomon-qe-adhoc;
--SET mapred.job.tracker=szwg-idle-abaci.dmop.baidu.com:54311;

SET mapred.job.map.capacity = 300;
SET mapred.job.reduce.capacity = 200;
SET hive.exec.reducers.max = 200;
SET mapred.job.priority=HIGH;
SET abaci.is.dag.job=true;

tmp_db = DATABASE mydb.tmp_db;

iknow = DATABASE "hdfs://szwg-ecomon-hdfs.dmop.baidu.com:54310/app/ns/iknow?config=iknow-meta";

SELECT
    b.title, count(*) AS pv
FROM
    tmp_db.sem_tl_brand_cookie_${hivevar:task_id} a
join
    iknow.reduced_udw_event_question b
ON
    (   b.event_day IN ( ${hivevar:date_list} )
        AND b.iknow_isreal = 0
        AND b.event_baiduid = ${hivevar:baiduid}
    )
GROUP BY b.title
ORDER BY pv DESC
limit 5
;

