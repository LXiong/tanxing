#!/bin/bash
source ../conf/sys.conf
source ./func.sh

task_id=$1;
brand_id_str=$2;

query_url=./query_url.txt
query=./query.txt
curl $3 > ${query_url}

awk -F"," -vOFS="\t" '{print $1,$3}'  ${query_url}> ${query}

iconv -f gbk -t utf-8 ${query} > ${query_url}

${QE_SEM_OTHER} --hivevar task_id=${task_id} brand_id_str=${brand_id_str} base_dir=${base_dir} wordbag=${query_url} -f "${base_dir}/get_brand_query_from_file.sql" >> "${result_dir}/get_brand_query_from_file.out" 2>> "${log_dir}/get_brand_query_from_file.log";




