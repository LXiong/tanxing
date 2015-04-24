#!/bin/bash -

#sh summary.sh taskid start_end_date acc_name_file_http_loc kwd_accname_tags_file_http_loc
#eg:
#sh summary.sh 1234 2014-09-01,2014-09-30 http://summary.com/account_name_file.txt http://summary.com/keyword_tags_file.txt

source ../conf/sys.conf

taskid=$1
start_end_date=$2
acc_name_file_http_loc=$3
kwd_accname_tags_file_http_loc=$4

mi_res_path=${result_dir}
log=${log_dir}/summary.log

if [ $# != 4 ]
then
	echo "ERROR: There should be 4 input parameters, sh summary.sh $taskid $start_end_date $acc_name_file_http_loc $kwd_accname_tags_file_http_loc" >> ${log}
    exit -1
fi

#S1: download acc_name_file_utf8
acc_name_file_ori=${mi_res_path}/summary_f1_acc_name_file.ori
sh download.sh ${acc_name_file_http_loc} ${acc_name_file_ori}

if [ $? -ne 0 ]
then
	exit -1
fi

acc_name_file_utf8=${mi_res_path}/summary_f2_acc_name_file_ori_utf8.m
iconv -f 'gbk' -t 'utf-8' ${acc_name_file_ori} -o ${acc_name_file_utf8}

#S2: call sms APIs to get keyword report of accounts
kwd_report_file=${mi_res_path}/summary_f4_kwd_report_file.csv
report_type="keyword"
sh get_report_by_sms_api.sh ${acc_name_file_utf8} ${start_end_date} ${report_type} ${kwd_report_file} >> ${log}

if [ $? -ne 0 ]
then
	exit -1
fi

#S3: download kwd_accname_tags_file
kwd_accname_tags_file=${mi_res_path}/summary_f5_kwd_accname_tags_file.m
sh download.sh ${kwd_accname_tags_file_http_loc} ${kwd_accname_tags_file}

if [ $? -ne 0 ]
then
	exit -1
fi

#S4: do summary work
output_res_file_utf8=${mi_res_path}/summary_f6_effect_summary_utf8.m
sh sum_clk_imp_charge_cpc_ctr.sh ${taskid}_summary ${kwd_report_file} ${kwd_accname_tags_file} ${mi_res_path} ${output_res_file_utf8} >> ${log}

if [ $? -ne 0 ]
then
	exit -1
fi

lines=`cat ${output_res_file_utf8} | wc -l`
if((${lines} <= 1))
then
	echo "ERROR: ${output_res_file_utf8} is empty!" >> ${log}
	exit -1
else
	echo "SUCCESS: get ${output_res_file_utf8}!" >> ${log}
fi

#S5: upload kwd_report_file and output_res_file_utf8 

#formal:
#kwd_report_file: sed tab to comma
sed -i -e 's/\t/,/g' ${kwd_report_file}

kwd_report_file_tgz=${mi_res_path}/summary_f4_kwd_report_file.tar.gz
tar -czvf ${kwd_report_file_tgz} ${kwd_report_file}

ori_report_url=`curl -F "filetype=summary_detail" -F "account_detail=@${kwd_report_file_tgz}" ${CURL_API_LOC}`

echo "ori_report_url_ori=${ori_report_url}!" >> ${log}

if [ -z "${ori_report_url}" ]
then
	echo "ERROR: Failed in call upload API to upload ${kwd_report_file_tgz} using ${CURL_API_LOC}!" >> ${log}
	exit -1
fi

ori_report_url=${ori_report_url:9}
ori_report_url=`echo ${ori_report_url} | sed -e 's/"}//g'`
ori_report_url=`echo ${ori_report_url} | sed -e 's/\\\//g'`

if [ ${ori_report_url:0:6} = "<html>" ]
then
	echo "WARN: Using local absolute file path as download address!" >> ${log}
	chmod 777 ${kwd_report_file_tgz}
	ori_report_url=${kwd_report_file_tgz}
fi

sleep 2

#output_res_file_utf8: utf8->gbk, tab to comma
output_res_file_gbk=${mi_res_path}/summary_f7_effect_summary_gbk.csv
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
echo "${ori_report_url}" >> ${res_url_file}

echo "ori_report_url=${ori_report_url}" >> ${log}
echo "summary_res_url=${summary_res_url}" >> ${log}

exit 0

