#!/bin/sh

source ../conf/sys.conf

mi_res_path=${result_dir}
log=${log_dir}/soc.log
output_file=output.zip

if [ $# != 6 ]
then
    echo "Error: There should be 6 parameters, sh run.sh taskid channel date wordbag groupy_by_colnames region" >> ${log}
    exit -1
fi
   
head=head.file
taskid=$1
channels=$2
date_info=$3
wordbag1=wordbag1.txt
wordbag=wordbag.txt
group_by_colnames=$5
region=$6

curl $4 > ${wordbag1}
iconv -f gbk -t utf-8 ${wordbag1} > ${wordbag}

#subtaskid channel partition_stat_date:20140101,20140110 wordbag group_by_colnames output_dir 
#generate para.conf
python conf.py $taskid $channels $date_info $region $wordbag $group_by_colnames $mi_res_path

for para in $(ls ${mi_res_path}/*para)
do
    #para: ${res_dir}/110_pc_1.para
    python get_sql.py $head $para
    #out_name: ${res_dir}/110_pc_0.out
    res=${para%%-para}
    nohup sh qe.sh ${QE_SEM_OTHER} ${res}  &
    sleep 30
done

#wait for first finished job, so that no error or warning reported in next while loop
while ! ls ${mi_res_path}/*.done >/dev/null 2>&1;
do
    sleep 100
done

n=`ls ${mi_res_path}/*para | wc -l `

#check if all qe jobs finished
while [ `ls ${mi_res_path}/*.done | wc -l ` -ne $n ]
do
    if ls ${mi_res_path}/*.error >/dev/null 2>&1;
    then 
        echo "QE job fails" >> ${log}
        exit -1
    fi
    sleep 60
done

num_col=`echo $group_by_colnames | awk -F ',' '{print NF}'`
aggre='$1'
for((i=1;i<$num_col;i++));
do
    aggre=${aggre}'"\t"''$'$((i+1))
done

#all x.done file exits, start to bind different parts
if ls ${mi_res_path}/*10011*.out >/dev/null 2>&1;
then
    cat `ls ${mi_res_path}/*10011*.out` | awk '{a['"$aggre"']+=$(NF-1);b['"$aggre"']+=$NF}END{for(i in a)print i "\t" a[i] "\t" b[i]}' > ${mi_res_path}/pc_output.txt
fi

if ls ${mi_res_path}/*11000*.out >/dev/null 2>&1;
then
    cat `ls ${mi_res_path}/*11000*.out`| awk '{a['"$aggre"']+=$(NF-1);b['"$aggre"']+=$NF}END{for(i in a)print i "\t" a[i] "\t" b[i]}' > ${mi_res_path}/wise_output.txt
fi

cd ${mi_res_path}
zip -r ${output_file} *_output.txt

#cd ../bin/rap_click_distribution/

if [ `ls -l ${mi_res_path}${output_file} | awk '{print int($5)}'` -gt 100000000 ];
then
    curr_dir=`pwd`
    chmod +rx ${curr_dir%/*}
    chmod +rx ${output_file}
    chmod +rx ${mi_res_path}
    summary_res_url="${curr_dir}/${output_file}"
else
    out_loc=${mi_res_path}${output_file}
    summary_res_url=`curl -F "filetype=dictionary" -F "dict=@${out_loc}" http://tanxing.baidu.com.dev:8888/data/upapi`
    summary_res_url=${summary_res_url:9}
    summary_res_url=`echo ${summary_res_url} | sed -e 's/"}//g'`
    summary_res_url=`echo ${summary_res_url} | sed -e 's/\\\//g'`
fi

res_url_file=${mi_res_path}/rap_query_trend
echo "${summary_res_url}" >> ${res_url_file}
echo "${summary_res_url}" >> ${log}

exit 0
