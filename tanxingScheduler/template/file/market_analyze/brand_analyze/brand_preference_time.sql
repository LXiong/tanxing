SET mapred.job.name = 'brand_preference';

--SET mapred.job.queue.name=ecomon-qe-adhoc;
--SET mapred.job.tracker=szwg-idle-abaci.dmop.baidu.com:54311;

SET mapred.job.map.capacity = 300;
SET mapred.job.reduce.capacity = 200;
SET hive.exec.reducers.max = 200;
SET mapred.job.priority=HIGH;
SET abaci.is.dag.job=true;

tmp_db = DATABASE mydb.tmp_db;
USE tmp_db; 

ADD JAR ${hivevar:base_dir}/../common/SemUDF.jar;

CREATE TEMPORARY FUNCTION row_number AS "sem.RowNumber";

SELECT
    time_range, brand_map_id, pv 
FROM (
    SELECT
        time_range, brand_map_id, pv, row_number(time_range) as row_num
    FROM
    (
        SELECT time_range, brand_map_id, pv
        FROM
        (    SELECT 
                b.time_range, a.brand_map_id, count(*) AS pv
            FROM
                mydb.sem_bl_brand_query a
            JOIN
                tmp_db.sem_tl_brand_user_behavior_${hivevar:task_id} b
            ON
                a.brand_query = b.normalized_query
            WHERE a.brand_map_id IS NOT NULL
            GROUP BY
                b.time_range, a.brand_map_id
        ) c
        DISTRIBUTE BY
            time_range
        SORT BY
            time_range, pv DESC
    ) d
) e
WHERE
    row_num <= 5
ORDER BY
    time_range, pv DESC
LIMIT 1000
;

