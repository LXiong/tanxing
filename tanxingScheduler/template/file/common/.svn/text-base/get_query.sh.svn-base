#!/bin/bash
source ../conf/sys.conf
source ./func.sh

subtask_id="$1";
date_arr="$2";
trade="$3";
trade_level="$4";
channel=`strFormat $5 channel`; # id to channel name
arr=(${date_arr//,/ });
date_begin=${arr[0]};
date_end=${arr[1]};

#echo $subtask_id, $date_begin, $date_end, $trade, $trade_level; 

date_str=`sampleDate $date_begin $date_end $channel`
echo $date_str > ${result_dir}/"date_str.${subtask_id}";

eval $( awk  -v channel="$channel" 'BEGIN{
        if( match(  channel, /ps/) ){
            print "is_pc=1";
        } else {
            print "is_pc=0";
        }
    }'
 )

echo $is_pc;

if [ ${trade_level} -eq 5 ];
then
    account=$trade;
    trade=0;
else
    account=0;    
fi


${QE_SEM_OTHER} --hivevar subtask_id=${subtask_id} date_str=${date_str} trade=${trade} account=${account} trade_level=${trade_level} is_pc=${is_pc}  -f "${base_dir}/get_query.sql" >> "${result_dir}/get_query.out" 2>> "${log_dir}/get_query.log";

sh get_query_account.sh ${subtask_id} &
