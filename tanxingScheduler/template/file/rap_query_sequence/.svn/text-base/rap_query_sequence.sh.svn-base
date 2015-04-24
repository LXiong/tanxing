#!/bin/bash

source ../conf/sys.conf
CURL_API_LOC="http://tanxing.baidu.com/data/upapi"

path_run="run/"
file_log="rap_query_sequence.log"

taskid=$1 # 001
channel=$2 # 10011,11000
cookie_date_range=$3 # 20140101,20140201
cookie_active_length=$4  # 0 or 1 .. 7 ...
flag_wordbag_or_rule=$5  # 0 for wordbag and 1 for rule
wordbag_or_rule=$6 # wordbag url when flag_wordbag_or_rule=0 and rule string when flag_wordbag_or_rule=1
minimum_pv=$7 # 0 or 1 ...
cookie_num=$8 # 1000 or 10000 ....
rand_cookie_num=$9
log_date_range=${10} # 20140101,20140201

# make dir for task , where files current task needs locate
mkdir ${path_run}${taskid}
for ((n=1;n<=7;n=n+1))
do
	mkdir ${path_run}${taskid}/Step${n}
done

# start get_sql.py to get all sql file,according to parameter:flag_wordbag_or_rule
if [ ${flag_wordbag_or_rule} -eq '0' ]
then
	curl ${wordbag_or_rule} > wordbag.csv
	iconv -f gbk -t utf8 wordbag.csv > wordbag.txt
	N_step=`python get_sql.py ${path_run}${taskid}/ $taskid $channel $cookie_date_range $cookie_active_length $flag_wordbag_or_rule wordbag.txt $minimum_pv $cookie_num $rand_cookie_num $log_date_range`
else
	tmp_rule=$(echo -n $wordbag_or_rule | sed 's/\\/\\\\/g;s/\(%\)\([0-9a-fA-F][0-9a-fA-F]\)/\\x\2/g;s/+//g')"\n"
	t_rule=`echo ${tmp_rule/\\\x0D\\\x0A/;}  `
	rule=`printf $t_rule`
	N_step=`python get_sql.py ${path_run}${taskid}/ $taskid $channel $cookie_date_range $cookie_active_length $flag_wordbag_or_rule $rule $minimum_pv $cookie_num $rand_cookie_num $log_date_range`
fi

# run sql file using QE and check until succeed or failed
for ((n=1;n<=${N_step};n=n+1))
do
	cur_path=${path_run}${taskid}/Step${n}/
	
	if [ $n -eq ${N_step} ]
	then
		uv_pv_list='ps_get_log wise_get_log ps_get_log_pv wise_get_log_pv'
		for uv_pv in $uv_pv_list
		do
			f=`ls ${path_run}${taskid}/Step$((${n}-1))/*${uv_pv}.out`
			pv=`tail -1 $f`
			if [ $pv -gt 10000000 ]
			then
				echo 'pv is too large' >> $f
				rm ${cur_path}*${uv_pv}.sql
			fi
		done
	fi
	
	for sql_file in ` ls ${cur_path}*.sql`
	do
		file_name=`basename $sql_file`
		file_name=`echo ${file_name}|awk -F '.' '{print $1}'`
		nohup sh qe.sh ${QE_SEM_OTHER} ${cur_path} ${file_name} > /dev/null &
		#sh qe.sh echo ${cur_path} ${file_name}
		sleep 6
	done
	
	while [ `ls ${cur_path}*.done 2>/dev/null | wc -l ` != `ls ${cur_path}*.sql 2>/dev/null | wc -l ` ]
	do
		if [ `ls ${cur_path}/*.error 2>/dev/null | wc -l ` -gt 0 ]
		then 
			echo "QE job fails on "Step${n} >> ${file_log}
			exit -1
		fi
		sleep 30
	done
done

mv  ${path_run}${taskid}/Step$((${N_step}-1))/*.out ${path_run}${taskid}/Step${N_step}/

# tar result_file to output dir
out_file=`ls ${path_run}${taskid}/Step${N_step}/*log.out`
if [ -n "${out_file}" ]
then
	zip -j ${result_dir}rqs_${taskid}_random.zip ${out_file}
	size=`du ${result_dir}rqs_${taskid}_random.zip | awk '{print $1}'`
	if [ $size -lt 100000 ] # 100000KB = 100 MB = 0.1 GB
	then
		sequence_file_url=`curl -F "filetype=summary" -F "account=@${result_dir}rqs_${taskid}_random.zip" ${CURL_API_LOC}`
		#example of sequence_file_url:
		#{"data":"http:\/\/tanxing.baidu.com\/upload\/rap_summary\/20141210\/20141210125744418881.txt"}
		echo "sequence_file_url_ori=${sequence_file_url}" >> ${file_log}
		
		if [ -z "${sequence_file_url}" -o ${sequence_file_url:0:6} = "<html>" ]
		then
			echo "ERROR: Failed in call upload API to upload ${output_res_file_gbk} using ${CURL_API_LOC}!" >> ${file_log}
			exit -1
		fi
		sequence_file_url=${sequence_file_url:9}
		sequence_file_url=`echo ${sequence_file_url} | sed -e 's/"}//g'`
		sequence_file_url=`echo ${sequence_file_url} | sed -e 's/\\\//g'`
	else
        chmod +rx ${result_dir}rqs_${taskid}_random.zip
		sequence_file_url=`readlink -f ${result_dir}rqs_${taskid}_random.zip `
	fi
	res_url_file=${result_dir}/rap_query_sequence
	echo "random,${sequence_file_url}" >> ${res_url_file}
fi

out_file=`ls ${path_run}${taskid}/Step${N_step}/*pv.out`
if [ -n "${out_file}" ]
then
	zip -j ${result_dir}rqs_${taskid}_filtered.zip ${out_file}
	size=`du ${result_dir}rqs_${taskid}_filtered.zip | awk '{print $1}'`
	if [ $size -lt 100000 ] # 100000KB = 100 MB = 0.1 GB
	then
		sequence_file_url=`curl -F "filetype=summary" -F "account=@${result_dir}rqs_${taskid}_filtered.zip" ${CURL_API_LOC}`
		echo "sequence_file_url_ori=${sequence_file_url}" >> ${file_log}
		
		if [ -z "${sequence_file_url}" -o ${sequence_file_url:0:6} = "<html>" ]
		then
			echo "ERROR: Failed in call upload API to upload ${output_res_file_gbk} using ${CURL_API_LOC}!" >> ${file_log}
			exit -1
		fi
		sequence_file_url=${sequence_file_url:9}
		sequence_file_url=`echo ${sequence_file_url} | sed -e 's/"}//g'`
		sequence_file_url=`echo ${sequence_file_url} | sed -e 's/\\\//g'`
	else
        chmod +rx ${result_dir}rqs_${taskid}_filtered.zip
		sequence_file_url=`readlink -f ${result_dir}rqs_${taskid}_random.zip `
	fi
	res_url_file=${result_dir}/rap_query_sequence
	echo "filter,${sequence_file_url}" >> ${res_url_file}
fi

exit 0

