tmp_db = database mydb.tmp_db;
use tmp_db;

DROP TABLE sem_tl_get_brand_people_${hivevar:task_id};
CREATE TABLE sem_tl_get_brand_people_${hivevar:task_id}
(
	cookie         string
	,channel_id    bigint
	,brand_id_str	string
)
COMMENT "sem_tl_get_brand_people"
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\001'
    COLLECTION ITEMS TERMINATED BY '\002'
    MAP KEYS TERMINATED BY '\003'

