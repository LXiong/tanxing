#!/bin/bash
source ../conf/sys.conf
source func.sh

task_id="$1";
province_input="$2";
city_input="$3";
channel_input="$4";
date_arr="$5";
arr=(${date_arr//,/ });
date_begin=${arr[0]};
date_end=${arr[1]};

province=`strFormat $province_input "area"`
city=`strFormat $city_input "area"`
channel=`strFormat $channel_input "channel"`

date_str=`getDateList $date_begin $date_end`
echo $date_str > ${result_dir}/"brand_date_str.${task_id}";

eval $( awk  -v channel="$channel" 'BEGIN{
        channel_tag=0;
        if( match(  channel, /ps/) ){
            channel_tag=1;
            print "channel_tag=1";
        } 

        if( match( channel, /wise/) ) {
            if( channel_tag == 1 ){
                print "channel_tag=3"
            } else {
                print "channel_tag=2"
            }
        }
    }'
 )

#echo $province ,$city, $date_str;
#echo $task_id, $date_begin, $date_end;
#echo "channel_tag ", $channel_tag;

${QE_SEM_OTHER} --hivevar task_id=${task_id} -f "${base_dir}/create_table_brand_people.sql" >> "${result_dir}/create_table_brand_people.sql.out" 2>> "${log_dir}/create_table_brand_people.sql.log";

if [[ $(($channel_tag & 1)) != 0 ]]; then
${QE_SEM_OTHER} --hivevar base_dir=${base_dir} task_id=${task_id} --hivevar date_str=${date_str} province=${province} city=${city} channel=${channel} -f "${base_dir}/get_brand_people_pc.sql" >> "${result_dir}/get_brand_people_pc.out" 2>> "${log_dir}/get_brand_people_pc.log";
fi

if [[ $(($channel_tag & 2)) != 0 ]]; then
${QE_SEM_OTHER} --hivevar base_dir=${base_dir} task_id=${task_id} --hivevar date_str=${date_str} province=${province} city=${city} channel=${channel} -f "${base_dir}/get_brand_people_wise.sql" >> "${result_dir}/get_brand_people_wise.out" 2>> "${log_dir}/get_brand_people_wise.log";
fi



