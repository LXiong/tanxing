#!/bin/bash -

#sh soc.sh taskid start_end_date acc_name_file_http_loc
#eg:
#sh soc.sh 12345 2014-09-01,2014-09-30 http://soc.com/account_name_file.txt

source ../conf/sys.conf

taskid=$1
start_end_date=$2
acc_name_file_http_loc=$3

mi_res_path=${result_dir}
log=${log_dir}/soc.log

if [ $# != 3 ]
then
	echo "ERROR: There should be 3 input parameters, sh soc.sh $taskid $start_end_date $acc_name_file_http_loc" >> ${log}
    exit -1
fi

#S1: download acc_name_file_ori
acc_name_file_ori=${mi_res_path}/soc_f1_acc_name_file.ori
sh download.sh ${acc_name_file_http_loc} ${acc_name_file_ori}

if [ $? -ne 0 ]
then
	exit -1
fi

acc_name_file_ori_utf8=${mi_res_path}/soc_f2_acc_name_file_ori_utf8.m
iconv -f 'gbk' -t 'utf-8' ${acc_name_file_ori} -o ${acc_name_file_ori_utf8}

#abs account name file
acc_name_file_utf8=${mi_res_path}/soc_f3_acc_name_file_utf8.m
awk -F"," 'NR>1{print $1}' ${acc_name_file_ori_utf8} | sort -u > ${acc_name_file_utf8}

#S2: call sms APIs to get report file
account_report_file_pc_ori=${mi_res_path}/soc_f4_account_report_file_pc_ori.m #日期	账户ID	账户	展现量	点击量	消费 ...
report_type="account_pc"
sh get_report_by_sms_api.sh ${acc_name_file_utf8} ${start_end_date} ${report_type} ${account_report_file_pc_ori} >> ${log}

if [ $? -ne 0 ]
then
	exit -1
fi

account_report_file_pc_sum=${mi_res_path}/soc_f5_account_report_file_pc_sum.m
awk -F"\t" '{print $3 "\t" $5}' ${account_report_file_pc_ori} > ${account_report_file_pc_sum}
account_report_file_pc_sum_utf8=${mi_res_path}/soc_f6_account_report_file_pc_sum_utf8.m
iconv -f 'gbk' -t 'utf-8' ${account_report_file_pc_sum} -o ${account_report_file_pc_sum_utf8}

account_report_file_wise_ori=${mi_res_path}/soc_f7_account_report_file_wise_ori.m
report_type="account_wise"
sh get_report_by_sms_api.sh ${acc_name_file_utf8} ${start_end_date} ${report_type} ${account_report_file_wise_ori} >> ${log}

if [ $? -ne 0 ]
then
	exit -1
fi

account_report_file_wise_sum=${mi_res_path}/soc_f8_account_report_file_wise_sum.m
awk -F"\t" '{print $3 "\t" $5}' ${account_report_file_wise_ori} > ${account_report_file_wise_sum}
account_report_file_wise_sum_utf8=${mi_res_path}/soc_f9_account_report_file_wise_sum_utf8.m
iconv -f 'gbk' -t 'utf-8' ${account_report_file_wise_sum} -o ${account_report_file_wise_sum_utf8}

col_index=0   #ugly code
account_report_file_pc=${mi_res_path}/soc_f10_account_report_file_pc.m
${PY_PATH} py_grep.py ${account_report_file_pc_sum_utf8} ${col_index} ${acc_name_file_utf8} ${account_report_file_pc}

account_report_file_wise=${mi_res_path}/soc_f11_account_report_file_wise.m
${PY_PATH} py_grep.py ${account_report_file_wise_sum_utf8} ${col_index} ${acc_name_file_utf8} ${account_report_file_wise}

account_report_file_tmp=${mi_res_path}/soc_f12_account_report_file.tmp
#dos2unix -q ${account_report_file_pc}
#dos2unix -q ${account_report_file_wise}
paste -d "\t" ${account_report_file_pc} ${account_report_file_wise} > ${account_report_file_tmp}

account_report_file=${mi_res_path}/soc_f13_account_report_file.csv
echo -e "account_name\tpc_total_click\twise_total_click\ttotal_click" >> ${account_report_file}
awk -F"\t" '{print $1 "\t" $2 "\t" $4 "\t" $2+$4}' ${account_report_file_tmp} >> ${account_report_file}

lines=`cat ${account_report_file} | wc -l`
if((${lines} <= 1))
then
	echo "ERROR: ${account_report_file} is empty!" >> ${log}
	exit -1
else
	echo "SUCCESS: get ${account_report_file} !" >> ${log}
fi

#S3: do cal percent work
output_res_file_utf8=${mi_res_path}/soc_f14_effect_soc_utf8.m

#has 1st column name line
#account_report_file: account_name pc_total_click wise_total_click total_click
#acc_name_file_ori_utf8: 账户,行业,品类,广告主,品牌,比例
#output_res_file_utf8: 账户	行业	品类	广告主	品牌 pc_total_click wise_total_click total_click

${PY_PATH} soc_formatted.py ${account_report_file} ${acc_name_file_ori_utf8} ${output_res_file_utf8}
lines=`cat ${output_res_file_utf8} | wc -l`
if((${lines} <= 1))
then
	echo "ERROR: ${output_res_file_utf8} is empty!" >> ${log}
	exit -1
else
	echo "SUCCESS: get ${output_res_file_utf8} !" >> ${log}
fi

#S4: upload account_report_file and output_res_file_utf8
output_res_file_gbk=${mi_res_path}/soc_f15_effect_soc_gbk.csv
iconv -f 'utf-8' -t 'gbk' ${output_res_file_utf8} -o ${output_res_file_gbk}
sed -i -e 's/\t/,/g' ${output_res_file_gbk}

summary_res_url=`curl -F "filetype=summary" -F "account=@${output_res_file_gbk}" ${CURL_API_LOC}`
echo "summary_res_url_ori=${summary_res_url}!" >> ${log}

if [ -z "${summary_res_url}" ]
then
	echo "ERROR: Failed in call upload API to upload ${output_res_file_gbk} using ${CURL_API_LOC}!" >> ${log}
	exit -1
fi

summary_res_url=${summary_res_url:9}
summary_res_url=`echo ${summary_res_url} | sed -e 's/"}//g'`
summary_res_url=`echo ${summary_res_url} | sed -e 's/\\\//g'`

if [ ${summary_res_url:0:6} = "<html>" ]
then
	echo "WARN: Using local absolute file path as download address!" >> ${log}
	chmod 777 ${output_res_file_gbk}
	summary_res_url=${output_res_file_gbk}
fi

res_url_file=${mi_res_path}/rap_summarized_report
echo "${summary_res_url}" >> ${res_url_file}

echo "summary_res_url=${summary_res_url}!" >> ${log}

exit 0
