#!/bin/bash -

#Wu Tong, BIM-IMR, 20140920
#function: cal cpc ctr sum_clk sum_impression sum_charge  

#input:
#para1: taskid
#para2: ori_report_file: has first col_name line
#para3: group_by_col_file: has first col_name line
#para4: mi_res_path
#para5: output_res_file: has first col_name line

#output:
#summary result flie: obj_name  obj_level  clk  impression  charge  cpc  ctr

source ../conf/sys.conf

taskid=$1
ori_report_file=$2   #date account_id account_name planid plan planunitid planunit kwdidid kwdid kwd impression clk charge
group_by_col_file=$3 #kwd,account_name,tag1,tag2,tag3,...,...
mi_res_path=$4
output_res_file=$5

log=${log_dir}/summary.log

if [ $# != 5 ]
then
	echo "ERROR: There should be 5 input parameters, sh sum_clk_imp_charge_cpc_ctr.sh $taskid $ori_report_file $group_by_col_file $mi_res_path $output_res_file" >> ${log}
    exit -1
fi

#has_tag_count=`comm -12 ${ori_report_file} ${group_by_col_file} | wc -l`
#printf "has_tag_percent=%0.4f\n" `echo "scale=4; $has_tag_count / $ori_report_file_size" | bc`

dos2unix -q ${ori_report_file}
dos2unix -q ${group_by_col_file}
ori_report_file_utf8=${mi_res_path}/${taskid}_ori_report_file_utf8
iconv -f 'gbk' -t 'utf-8' ${ori_report_file} -o ${ori_report_file_utf8}
group_by_col_file_utf8=${mi_res_path}/${taskid}_group_by_col_file_utf8
iconv -f 'gbk' -t 'utf-8' ${group_by_col_file} -o ${group_by_col_file_utf8}

summary_m1=${mi_res_path}/${taskid}_summary.m1 #has 1st column name line
${PY_PATH} add_tag_to_report.py ${ori_report_file_utf8} ${group_by_col_file_utf8} ${summary_m1}

#save the 1st column to output file
echo -e "obj_name\tobj_level\ttotal_impression\ttotal_click\ttotal_charge\tcpc\tctr" >> ${output_res_file}

#get 1st line of group_by_col_file_utf8
group_by_col_file_1st=${mi_res_path}/${taskid}_group_by_col_file_1st
head -1 ${group_by_col_file_utf8} > ${group_by_col_file_1st}

#cal summary values: imp clk charge cpc ctr
#do summary from the 2nd column which is account_name, ignore the 1st column of keyword, so plus 2 after ${ori_report_file_col_size}
ori_report_file_col_size=`awk -F"\t" '{print NF;exit}' ${ori_report_file_utf8}`
group_by_col_file_col_size=`awk -F"," '{print NF;exit}' ${group_by_col_file_utf8}`
#ugly codes:
shift_col_step=2 #kwd account_name
imp_col_idx=11

i=${shift_col_step}
for((col_idx=${ori_report_file_col_size}+${shift_col_step};col_idx<=${ori_report_file_col_size}+${group_by_col_file_col_size};++col_idx));
do
	obj_level=`awk -v idx=$i -F"," '{print $idx}' ${group_by_col_file_1st}`
	awk -v col_idx=${col_idx} -v value_start_col_idx=${imp_col_idx} -v obj_level=${obj_level} -F"\t" 'NR>1{
	imp[$col_idx]+=$value_start_col_idx;
	clk[$col_idx]+=$(value_start_col_idx+1);
	charge[$col_idx]+=$(value_start_col_idx+2);
	}END{
	for (obj in clk) {		
		if (clk[obj] == 0)
			cpc = 0;
		else
			cpc = charge[obj]/clk[obj];	
		if (imp[obj] == 0)
			ctr = 0;
		else
			ctr = clk[obj]/imp[obj];
		print obj "\t" obj_level "\t" imp[obj] "\t" clk[obj] "\t" charge[obj] "\t" cpc "\t" ctr
	}	
	}' ${summary_m1} >> ${output_res_file}
	((i+=1))
done;

exit 0
