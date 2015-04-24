#!/bin/bash
source ../conf/sys.conf

subtask_id="$1";
date_str_default=`cat "${result_dir}/date_str_default.${subtask_id}"`;
date_str_yl=`cat "${result_dir}/date_str_yl.${subtask_id}"`;

${QE_SEM_OTHER} --hivevar subtask_id=${subtask_id} --hivevar base_dir=${base_dir} --hivevar date_str_default=${date_str_default} --hivevar date_str_yl=${date_str_yl} -f "${base_dir}/get_behavior.sql" >> "${result_dir}/get_behavior.out" 2>> "${log_dir}/get_behavior.log";





