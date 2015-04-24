#!/bin/sh

source ../conf/sys.conf

mi_res_path=${result_dir}
log=${log_dir}/soc.log
output_file=output.zip

if [ $# != 5 ]
then
    echo "Error: There should be 5 parameters, sh run.sh taskid channel date wordbag groupy_by_colnames" >> ${log}
    exit -1
fi
   
head=head.file
taskid=$1
channels=$2
date_info=$3
wordbag1=wordbag1.txt
wordbag=wordbag.txt
group_by_colnames=$5

curl $4 > ${wordbag1}
iconv -f gbk -t utf-8 ${wordbag1} > ${wordbag}

#subtaskid channel partition_stat_date:20140101,20140110 wordbag group_by_colnames output_dir 
#generate para.conf
python conf.py $taskid $channels $date_info $wordbag $group_by_colnames $mi_res_path

for para in $(ls ${mi_res_path}/*para)
do
    #para: ${res_dir}/110_pc_1.para
    python get_sql.py $head $para
    #out_name: ${res_dir}/110_pc_0.out
    res=${para%%-*}
    nohup sh qe.sh ${QE_SEM_OTHER} ${res}  &
    sleep 30
done

n=`ls ${mi_res_path}/*para | wc -l `

#check if all qe jobs finished
while [ `ls ${mi_res_path}/*.done | wc -l ` -ne $n ]
do
    if [ `ls ${mi_res_path}/*.error | wc -l ` -gt 0 ]
    then 
        echo "QE job fails" >> ${log}
        exit -1
    fi
    sleep 60
done

num_col=`echo $group_by_colnames | awk -F ',' '{print NF}'`
k1='$1'
for((i=1;i<$num_col;i++));
do
    k1=$k1'"\t"''$'$((i+1))
done

#all x.done file exits, start to bind different parts
if [ `ls ${mi_res_path}/*pc*.out | wc -l` -gt 0 ]
then
    cat `ls ${mi_res_path}/*pc*.out` | awk '{a['"$k1"']+=$NF}END{for(i in a)print i "\t" a[i]}' > ${mi_res_path}/pc_output
    iconv -f utf-8 -t gbk ${mi_res_path}/pc_output -o ${mi_res_path}/pc_output.txt
fi

if [ `ls ${mi_res_path}/*wise*.out | wc -l` -gt 0 ]
then
    cat `ls ${mi_res_path}/*wise*.out`| awk '{a[a['"$k1"']+=$NF]+=$4}END{for(i in a)print i "\t" a[i]}' > ${mi_res_path}/wise_output
    iconv -f utf-8 -t gbk ${mi_res_path}/wise_output -o ${mi_res_path}/wise_output.txt
fi

cd ${mi_res_path}
zip -r ${output_file} *_output.txt

if [ `ls -l ${output_file} | awk '{print int($5)}'` -gt 100000000 ]
then
    echo 'exceed maximal size of dowload file' >> ${log}
    exit -1
fi

cd ../bin/rap_click_distribution/
out_loc=${mi_res_path}${output_file}
summary_res_url=`curl -F "filetype=dictionary" -F "dict=@${out_loc}" http://tanxing.baidu.com.dev:8888/data/upapi`

summary_res_url=${summary_res_url:9}
summary_res_url=`echo ${summary_res_url} | sed -e 's/"}//g'`
summary_res_url=`echo ${summary_res_url} | sed -e 's/\\\//g'`

res_url_file=${mi_res_path}/rap_clk_distribution
echo "${summary_res_url}" >> ${res_url_file}
echo "${summary_res_url}" >> ${log}

exit 0
