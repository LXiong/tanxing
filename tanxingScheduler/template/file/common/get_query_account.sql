set mapred.job.name = 'get_query_account';
set mapred.job.map.capacity = 300;
set mapred.job.reduce.capacity = 200;
set hive.exec.reducers.max = 200;
set mapred.job.priority=HIGH;


tmp_db = database mydb.tmp_db;
use tmp_db; 
--ADD FILE transform_account.py;

--SELECT TRANSFORM(normalized_query,user_account_id)
--USING 'transform_account.py'
SELECT normalized_query,user_account_id
FROM sem_tl_get_query_account_${hivevar:subtask_id};


