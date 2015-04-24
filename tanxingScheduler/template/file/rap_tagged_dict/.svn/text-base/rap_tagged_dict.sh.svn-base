#!/bin/sh
source ../conf/sys.conf

mi_res_path=${result_dir}
log=${log_dir}/soc.log
output_file=output_file.zip
output_file_gbk=output_file_gbk.csv
subtask_id=$1

if [ $# != 4 ]
then 
    echo "ERROR: There should be 3 input parameters, sh rap_tagged_dict.sh $subtask_id $dicionary_url $rules_url $query_url" >> ${log}
    exit -1
fi

dictionary=null
if [ $2 != null ]
then
    dictionary=${mi_res_path}/dict_url.csv
    curl $2 > ${dictionary}
fi

rules=null
if [ $3 != null ]
then
    rules=${mi_res_path}/rules_url.csv
    curl $3 > ${rules}
fi

query=${mi_res_path}/query_url.csv
curl $4 > ${query}

#py script for tagging queries.
python autotag2.py ${dictionary} ${rules} ${query} ${mi_res_path}${output_file_gbk} > ${mi_res_path}/wrongRule_gbk.csv

cd ${mi_res_path}
zip -r $output_file $output_file_gbk wrongRule_gbk.csv

summary_res_url=`curl -F "filetype=dictionary" -F "dict=@${output_file}" http://tanxing.baidu.com.dev:8888/data/upapi`

summary_res_url=${summary_res_url:9}
summary_res_url=`echo ${summary_res_url} | sed -e 's/"}//g'`
summary_res_url=`echo ${summary_res_url} | sed -e 's/\\\//g'`

res_url_file=${mi_res_path}/rap_tagged_dict
echo "${summary_res_url}" >> ${res_url_file}

echo "${summary_res_url}" >> ${log}

exit 0
