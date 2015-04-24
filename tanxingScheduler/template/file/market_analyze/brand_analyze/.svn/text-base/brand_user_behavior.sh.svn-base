#!/bin/bash

source ../conf/sys.conf
source ../common/func.sh

task_id=$1
province=`strFormat $2 area`
city=`strFormat $3 area`
channel=$4

if [ ${channel} = 10011 ]
then
    behavior_table=mydb.search_data_dump_from_udw_event
    hour_pos=12
    filter_condition_var1="isspider" 
    filter_condition_var2="isbroken" 
    filter_condition_var3="'wise'" 
    # "AND isspider = 0 AND isbroken != 1"
else
    behavior_table=mydb.sem_al_ps_wise_search_theme
    hour_pos=1
    filter_condition_var1="0" 
    filter_condition_var2="NULL" 
    filter_condition_var3="device" 
    # "AND device = 'wise'"
fi

date_list=`cat "${result_dir}/brand_date_str.${task_id}"`

area_condition=""
if [ ${province} != 0 ]
then
    area_condition="province IN (${province})"
fi

if [ ${city} != 0 ]
then
    if [ ${#area_condition} -gt 0 ]
    then
        area_condition="(${area_condition} OR city IN (${city}))"
    else
        area_condition="city IN (${city})"
    fi
fi
if [ ${#area_condition} -gt 0 ] 
then
    area_condition=" AND ${area_condition} "
fi

${QE_SEM_OTHER} --hivevar task_id="${task_id}" \
--hivevar hour_pos="${hour_pos}" \
--hivevar date_list="${date_list}" \
--hivevar area_condition="${area_condition}" \
--hivevar behavior_table="${behavior_table}" \
--hivevar filter_condition_var1="${filter_condition_var1}" \
--hivevar filter_condition_var2="${filter_condition_var2}" \
--hivevar filter_condition_var3="${filter_condition_var3}" \
-f ${base_dir}/brand_user_behavior.sql >> "${result_dir}/brand_user_behavior.out" 2>> "${log_dir}/brand_user_behavior.log"


