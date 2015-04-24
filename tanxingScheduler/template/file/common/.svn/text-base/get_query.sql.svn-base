SET mapred.job.name = 'get_query';

--set mapred.job.queue.name = bim;
--set mapred.job.tracker = szwg-stoff-abaci.dmop.baidu.com:54311;

set mapred.job.map.capacity = 300;
set mapred.job.reduce.capacity = 200;
set hive.exec.reducers.max = 200;
set mapred.job.priority=HIGH;
set abaci.is.dag.job=true;

tmp_db = database mydb.tmp_db;
use tmp_db; 

DROP TABLE sem_tl_get_query_${hivevar:subtask_id};
CREATE TABLE sem_tl_get_query_account_${hivevar:subtask_id} AS
SELECT 
     normalized_query,user_account_id
FROM mydb.sem_al_consume_top80_query consume_query
WHERE partition_stat_date in ( ${hivevar:date_str} ) AND 
     user_account_id in ( ${hivevar:account} )  AND
     channel_id = 10011
--     case when ${hivevar:trade_level} = 1 then trade_one_id = ${hivevar:trade}
--         when ${hivevar:trade_level} = 2 then trade_two_id = ${hivevar:trade}
--          when ${hivevar:trade_level} = 3 then trade_three_id = ${hivevar:trade}
--           when ${hivevar:trade_level} = 4 then user_id = ${hivevar:trade}
--           when ${hivevar:trade_level} = 5 then user_account_id in ( ${hivevar:account} ) end  
--      case when ${hivevar:is_pc} = 1 then channel_id = 10011
--         when ${hivevar:is_pc} = 0 then channel_id = 11000 end  
GROUP BY normalized_query,user_account_id;

DROP TABLE sem_tl_get_query_${hivevar:subtask_id};
CREATE TABLE sem_tl_get_query_${hivevar:subtask_id} AS
SELECT 
     normalized_query
FROM sem_tl_get_query_account_${hivevar:subtask_id}  
GROUP BY normalized_query;


