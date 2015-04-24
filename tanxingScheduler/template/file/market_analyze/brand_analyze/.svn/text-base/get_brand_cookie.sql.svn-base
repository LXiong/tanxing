SET mapred.job.name = 'get_brand_cookie';

--SET mapred.job.queue.name=ecomon-qe-adhoc;
--SET mapred.job.tracker=szwg-idle-abaci.dmop.baidu.com:54311;

SET mapred.job.map.capacity = 300;
SET mapred.job.reduce.capacity = 200;
SET hive.exec.reducers.max = 200;
SET mapred.job.priority=HIGH;
SET abaci.is.dag.job=true;

SET brand_list_separator=,;
SET brand_id=${hivevar:brand_id};

tmp_db = DATABASE mydb.tmp_db;
USE tmp_db; 

DROP TABLE IF EXISTS sem_tl_brand_cookie_${hivevar:task_id};

CREATE TABLE sem_tl_brand_cookie_${hivevar:task_id}
AS
SELECT
    cookie
FROM
    tmp_db.sem_tl_get_brand_people_${hivevar:task_id}
WHERE
    concat("${hiveconf:brand_list_separator}",brand_id_str,"${hiveconf:brand_list_separator}") LIKE concat("%${hiveconf:brand_list_separator}${hiveconf:brand_id}${hiveconf:brand_list_separator}%")
;
