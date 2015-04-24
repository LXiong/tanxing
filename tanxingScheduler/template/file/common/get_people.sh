#!/bin/bash
source ../conf/sys.conf
source func.sh

subtask_id="$1";
province_input="$2";
city_input="$3";
channel_input="$4";
date_str_default=`cat "${result_dir}/date_str_default.${subtask_id}"`;
date_str_yl=`cat "${result_dir}/date_str_yl.${subtask_id}"`;
#echo $date_str;
#echo $province_input;
#echo $city_input

province=`strFormat $province_input "area"`
city=`strFormat $city_input "area"`
channel=`strFormat $channel_input "channel"`

echo $province $city;
#echo $subtask_id, $date_begin, $date_end, $trade, $trade_level; 

${QE_SEM_OTHER} --hivevar subtask_id=${subtask_id} --hivevar date_str_default=${date_str_default} --hivevar date_str_yl=${date_str_yl} province=${province} city=${city} channel=${channel} -f "${base_dir}/get_people.sql" >> "${result_dir}/get_people.out" 2>> "${log_dir}/get_people.log";





