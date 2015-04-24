#!/bin/bash
source ../conf/sys.conf
source ../common/func.sh

task_id="$1";
channel_id="$2";
brand_id_str="$3";

${QE_SEM_OTHER} --hivevar base_dir=${base_dir} task_id=${task_id} brand_id_str=${brand_id_str} channel_id=${channel_id} -f "${base_dir}/get_brand_uv.sql" >> "${result_dir}/get_brand_uv.out_" 2>> "${log_dir}/get_brand_uv.log";

awk 'BEGIN{FS="\t";OFS="\t"}{uv_arr[$1]+=$2}END{for(brand_id_str in uv_arr){ print brand_id_str, uv_arr[brand_id_str]}}' "${result_dir}/get_brand_uv.out_" > "${result_dir}/get_brand_uv.out"

