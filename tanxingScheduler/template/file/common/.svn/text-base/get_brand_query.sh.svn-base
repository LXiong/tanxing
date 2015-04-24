#!/bin/bash
source ../conf/sys.conf
source ./func.sh

task_id=$1;
brand_id_str=$2;

${QE_SEM_OTHER} --hivevar task_id=${task_id} brand_id_str=${brand_id_str} base_dir=${base_dir} -f "${base_dir}/get_brand_query.sql" >> "${result_dir}/get_brand_query.out" 2>> "${log_dir}/get_brand_query.log";




