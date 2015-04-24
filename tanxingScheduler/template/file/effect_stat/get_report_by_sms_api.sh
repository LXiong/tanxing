#!/bin/bash -

#Wu Tong, BIM-IMR, 20140920
#function: get account report by calling APIs

source ../conf/sys.conf

acc_name_file_utf8=$1
start_end_date=$2 #2014-09-01,2014-09-30
report_type=$3
report_file_gbk=$4

mi_res_path=${result_dir}
log=${log_dir}/summary.log

if [ $# != 4 ]
then
	echo "ERROR: There should be 4 input parameters, sh get_report_by_sms_api.sh $acc_name_file_utf8 $start_end_date $report_type $report_file_gbk" >> ${log}
    exit -1
fi

#S2: call sms APIs to get report 
#2011-01-01T00:00:00.000
add="T00:00:00.000"
start_date=${start_end_date:0:10}${add}
end_date=${start_end_date:11}${add}

report_file_tmp=${mi_res_path}/story_f3_report_file_tmp.m
${PY_PATH} get_report_by_sms_api.py  ${acc_name_file_utf8} ${start_date} ${end_date} ${report_type} ${report_file_tmp}

cat ${report_file_tmp} | sort -ru > ${report_file_gbk}
dos2unix -q ${report_file_gbk}

lines=`cat ${report_file_gbk} | wc -l`
if((${lines} <= 1))
then
	echo "ERROR: ${report_file_gbk} is empty!" >> ${log}
	exit -1
else
	echo "SUCCESS: get ${report_file_gbk}!" >> ${log}
fi

exit 0
