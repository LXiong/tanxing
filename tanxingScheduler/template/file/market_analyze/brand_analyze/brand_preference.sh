#!/bin/bash

task_id=$1
brand_id=$2

source ../conf/sys.conf

LDIFS="${IFS}"
IFS=,
brand_id=(${brand_id})  # string to arr
brand_id=${brand_id[0]}
IFS="${OLDIFS}"

${QE_SEM_OTHER} \
--hivevar base_dir=${base_dir} \
--hivevar task_id=$task_id \
--hivevar brand_id=$brand_id \
-f ${base_dir}/brand_preference_time.sql \
>> "${result_dir}/brand_preference_time" 2>> "${log_dir}/brand_preference_time.log"

if [ $? -ne 0 ]
then
    exit 1
fi

${QE_SEM_OTHER} \
--hivevar base_dir=${base_dir} \
--hivevar task_id=$task_id \
--hivevar brand_id=$brand_id \
-f ${base_dir}/brand_preference_category.sql \
>> "${result_dir}/brand_preference_category" 2>> "${log_dir}/brand_preference_category.log"

