#!/bin/bash
source ../conf/sys.conf
source ../common/func.sh

subtask_id=$1

date_str=`cat "${result_dir}/date_str.${subtask_id}"`;

function getPartition(){
    partition_yl=`${QE_SEM_OTHER} -e 'use mydb_yl; show partitions udwetl_gs_detail_ka;' | sed -ne '/event_day=[0-9]\{8\}/p' | sed -e 's/.*event_day=\([0-9]\{8\}\).*/\1/' | sed -e ':a;N;s/\n/,/;ta'`
    partition_default=`${QE_SEM_OTHER} -e 'use default; show partitions udwetl_gs_detail_ka;' | sed -ne '/event_day=[0-9]\{8\}/p' | sed -e 's/.*event_day=\([0-9]\{8\}\).*/\1/' | sed -e ':a;N;s/\n/,/;ta'`
    echo ${partition_yl},${partition_default}
}


OLDIFS="${IFS}"
IFS=,
dateArr=(${date_str})
IFS="${OLDIFS}"
for day in ${dateArr[@]}
do
    waitLock ${global_lock_dir}/${day}
    freeLock ${global_lock_dir}/${day}
done


partitionList=`getPartition`
missing=0
for day in ${dateArr[@]}
do
    if [[ "${partitionList}" != *${day}* ]]
    then
        missing=1
    fi
done

if [ ${missing} == 1 ]
then
waitLock ${global_lock_dir}/create
partitionList=`getPartition`
missing=()
for day in ${dateArr[@]}
do
    if [[ "${partitionList}" != *${day}* ]]
    then
        missing=(${missing[@]} $day)
    fi
done
for day in ${missing[@]}
do
    # mark data generating
    waitLock ${global_lock_dir}/${day}
done
for day in ${missing[@]}
do
    # generate globalsession data
    cd gs
    bash udwetl_gs_detail_ka.sh ${day} 00 00 . '' '' '' '' '' ''
    cd -
    # mark data generated 
    freeLock ${global_lock_dir}/${day}
done
freeLock ${global_lock_dir}/create
fi

# output datestring
date_default=
date_yl=
partition_default=`${QE_SEM_OTHER} -e 'use default; show partitions udwetl_gs_detail_ka;' | sed -ne '/event_day=[0-9]\{8\}/p' | sed -e 's/.*event_day=\([0-9]\{8\}\).*/\1/' | sed -e ':a;N;s/\n/,/;ta'`
partition_yl=`${QE_SEM_OTHER} -e 'use mydb_yl; show partitions udwetl_gs_detail_ka;' | sed -ne '/event_day=[0-9]\{8\}/p' | sed -e 's/.*event_day=\([0-9]\{8\}\).*/\1/' | sed -e ':a;N;s/\n/,/;ta'`
for day in ${dateArr[@]}
do
    if [[ $partition_default == *${day}* ]]
    then
        if [ ${#date_default} -gt 0 ]
        then
            date_default=${date_default},${day}
        else
            date_default=${day}
        fi
    elif [[ $partition_yl == *${day}* ]]
    then
        if [ ${#date_yl} -gt 0 ]
        then
            date_yl=${date_yl},${day}
        else
            date_yl=${day}
        fi
    fi
done

if [ ${#date_default} -gt 0 ]
then
    echo ${date_default} > ${result_dir}/date_str_default.${subtask_id}    
else
    echo "''" > ${result_dir}/date_str_default.${subtask_id}
fi

if [ ${#date_yl} -gt 0 ]
then
    echo ${date_yl} > ${result_dir}/date_str_yl.${subtask_id}    
else
    echo "''" > ${result_dir}/date_str_yl.${subtask_id}
fi

