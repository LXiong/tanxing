#!/bin/bash -

source ../conf/sys.conf

taskid=$1
start_end_date=$2
acc_name_file_http_loc=$3

mi_res_path=${result_dir}
log=${log_dir}/summary.log

if [ $# != 3 ]
then
	echo "ERROR: There should be 3 input parameters, sh get_keyword_report.sh $taskid $start_end_date $acc_name_file_http_loc" >> ${log}
    exit -1
fi

#S1: download acc_name_file_utf8
acc_name_file_ori=${mi_res_path}/kwdreport_f1_acc_name_file.ori
sh download.sh ${acc_name_file_http_loc} ${acc_name_file_ori}

if [ $? -ne 0 ]
then
	exit -1
fi

acc_name_file_utf8=${mi_res_path}/kwdreport_f2_acc_name_file_ori_utf8.m
iconv -f 'gbk' -t 'utf-8' ${acc_name_file_ori} -o ${acc_name_file_utf8}

#S2: call sms APIs to get keyword report of accounts

report_type="keyword_pc"
kwd_report_file_pc=${mi_res_path}/kwdreport_f4_kwd_pc_report_file.csv
sh get_report_by_sms_api.sh ${acc_name_file_utf8} ${start_end_date} ${report_type} ${kwd_report_file_pc} >> ${log}

if [ $? -ne 0 ]
then
	exit -1
fi

sed -i -e 's/\t/,/g' ${kwd_report_file_pc}

report_type="keyword_wise"
kwd_report_file_w=${mi_res_path}/kwdreport_f5_kwd_wise_report_file.csv
sh get_report_by_sms_api.sh ${acc_name_file_utf8} ${start_end_date} ${report_type} ${kwd_report_file_w} >> ${log}

if [ $? -ne 0 ]
then
	exit -1
fi

sed -i -e 's/\t/,/g' ${kwd_report_file_w}

kwd_report_file=${mi_res_path}/kwdreport_kwd_report_file.tar.gz
tar -czvf ${kwd_report_file} ${kwd_report_file_pc} ${kwd_report_file_w}

ori_report_url=`curl -F "filetype=summary_detail" -F "account_detail=@${kwd_report_file}" ${CURL_API_LOC}`

echo "ori_report_url_ori=${ori_report_url}!" >> ${log}

if [ -z "${ori_report_url}" ]
then
	echo "ERROR: Failed in call upload API to upload ${kwd_report_file} using ${CURL_API_LOC}!" >> ${log}	
	exit -1
fi

ori_report_url=${ori_report_url:9}
ori_report_url=`echo ${ori_report_url} | sed -e 's/"}//g'`
ori_report_url=`echo ${ori_report_url} | sed -e 's/\\\//g'`

if [ ${ori_report_url:0:6} = "<html>" ]
then
	echo "WARN: Using local absolute file path as download address!" >> ${log}
	chmod 777 ${kwd_report_file}
	ori_report_url=${kwd_report_file}
fi

res_url_file=${mi_res_path}/rap_keyword_report
echo "${ori_report_url}" >> ${res_url_file}

echo "ori_report_url=${ori_report_url}!" >> ${log}

exit 0

