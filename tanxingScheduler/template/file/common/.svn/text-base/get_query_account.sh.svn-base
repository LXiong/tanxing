#!/bin/bash
source ../conf/sys.conf

subtask_id="$1"
customer_id=${subtask_id##*_}
${QE_SEM_OTHER} --hivevar subtask_id=${subtask_id} -f "${base_dir}/get_query_account.sql" >> "${result_dir}/get_query_account.out" 2>> "${log_dir}/get_query_account.log";
queryfile=${result_dir}/get_query_account.out
query=${result_dir}/query_transformed_account.out
python transform_account.py ${queryfile} >>${query}

result_url="${result_dir}/get_query_account"
out_file=`ls ${result_dir}/query_transformed_account.out`
if [ -n "${out_file}" ]
then
    querysize=`du ${out_file} | awk '{print $1}'`
    if [ $querysize -gt 0 ]
    then
        zip -j ${result_dir}_${subtask_id}_query_account.zip ${out_file}
        size=`du ${result_dir}_${subtask_id}_query_account.zip | awk '{print $1}'`
        if [ $size -lt 100000 ] # 100000KB = 100 MB = 0.1 GB
        then
            query_account_url=`curl -F "filetype=summary" -F "account=@${result_dir}_${subtask_id}_query_account.zip" http://tanxing.baidu.com.dev:8888/data/upapi`
            echo "${query_account_url}" >> ${result_url}
    
            if [ -z "${query_account_url}" -o ${query_account_url:0:6} = "<html>" ]
            then
                echo "ERROR: Failed in call upload API" >> ${result_url}
                exit -1
            fi  
            query_account_url=${query_account_url:9}
            query_account_url=`echo ${query_account_url} | sed -e 's/"}//g'`
            query_account_url=`echo ${query_account_url} | sed -e 's/\\\//g'`
        else
            chmod +rx ${result_dir}_${subtask_id}_query_account.zip
            query_account_url=`readlink -f ${result_dir}_${subtask_id}_query_account.zip `
        fi  
        res_url_file=${result_dir}/get_query_account
        echo "${customer_id},${query_account_url}" > ${res_url_file}
    fi
fi
