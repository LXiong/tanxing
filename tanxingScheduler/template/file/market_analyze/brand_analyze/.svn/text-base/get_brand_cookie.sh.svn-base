#!/bin/bash

task_id=$1
brand_id=$2

source ../conf/sys.conf

LDIFS="${IFS}"
IFS=,
brand_id=(${brand_id})  # string to arr
brand_id=${brand_id[0]}
IFS="${OLDIFS}"

${QE_SEM_OTHER} --hivevar brand_id=$brand_id --hivevar task_id=$task_id -f ${base_dir}/get_brand_cookie.sql >> "${result_dir}/get_brand_cookie.out" 2>> "${log_dir}/get_brand_cookie.log"


