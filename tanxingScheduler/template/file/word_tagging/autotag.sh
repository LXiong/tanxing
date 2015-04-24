#!/bin/sh
source ../conf/sys.conf

mi_res_path=${result_dir}
log=${log_dir}/soc.log
output_file_gbk=${mi_res_path}/output_file_gbk.csv

dictionary=${mi_res_path}/dict_url.csv
curl $1 > ${dictionary}

rules=${mi_res_path}/rules_url.csv
curl $2 > ${rules}

query=${mi_res_path}/query_url.csv
curl $3 > ${query}

if [ $# != 3 ]
then
        echo "ERROR: There should be 3 input parameters, sh autotag.sh $dictionary_url $rules_url $query_url" >> ${log}
    exit -1
fi

#py script for tagging queries.
${PY_PATH} autotag.py ${dictionary} ${rules} ${query} > ${output_file_gbk}
#convert to gbk file
#iconv -f utf-8 -t gbk ${mi_res_path}/output.csv -o ${output_file_gbk}


summary_res_url=`curl -F "filetype=dictionary" -F "dict=@${output_file_gbk}" http://tanxing.baidu.com:8080/data/upapi`

summary_res_url=${summary_res_url:9}
summary_res_url=`echo ${summary_res_url} | sed -e 's/"}//g'`
summary_res_url=`echo ${summary_res_url} | sed -e 's/\\\//g'`

res_url_file=${mi_res_path}/rap_tagged_dict
echo "${summary_res_url}" >> ${res_url_file}

echo "${summary_res_url}" >> ${log}

exit 0

