#!/bin/bash

task_id=$1
brand_id=$2
channel=$3

source ../conf/sys.conf
source ../common/func.sh

LDIFS="${IFS}"
IFS=,
brand_id=(${brand_id})  # string to arr
brand_id=${brand_id[0]}
IFS="${OLDIFS}"

date_list=`cat "${result_dir}/brand_date_str.${task_id}"`

if [ ${channel} -eq 10011 ]
then
    baiduid="substr(a.cookie,9,32)"
else
    baiduid="a.cookie"
fi

${QE_SEM_OTHER} \
--hivevar task_id=$task_id \
--hivevar date_list=$date_list \
--hivevar baiduid="$baiduid" \
-f ${base_dir}/hot_question_global.sql \
>> "${result_dir}/hot_question_global" 2>> "${log_dir}/hot_question_global.log"

if [ $? -ne 0 ]
then
    exit 1
fi

${QE_SEM_OTHER} \
--hivevar date_list=$date_list \
--hivevar brand_id=$brand_id \
-f ${base_dir}/hot_question_brand.sql \
>> "${result_dir}/hot_question_brand" 2>> "${log_dir}/hot_question_brand.log"

