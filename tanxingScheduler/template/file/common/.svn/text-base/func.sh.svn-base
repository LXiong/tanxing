#!/bin/bash
source ../conf/func.conf

function getDateList(){
        date_str=""
        st=`date -d "$1" +%s`
        et=`date -d "$2" +%s`
        while [ "$st" -le "$et" ]
        do
            date_str_tmp=`date -d "1970-01-01 UTC $st seconds" +%Y%m%d`
            if [ "$date_str" = "" ];then
                date_str=$date_str_tmp;
            else
                date_str=$date_str","$date_str_tmp
            fi
            st=$((st+86400))
        done

        echo $date_str
}

function sampleDate(){
       date_str=`getDateList $1 $2`;
       channel=$3;
       eval $( 
           awk -v channel="$channel" -v date_str="$date_str" 'BEGIN{
           n=split(date_str,date_arr,",");

           if( match(  channel, "ps") ){
               sample_day='"$PS_SAMPLE_DAY"';
           } else {
               sample_day='"$OTHER_SAMPLE_DAY"';
           }

           if( n <= sample_day ){
               date_sample=date_str;
           } else {
               srand(0);
               for(i=1;i<=sample_day;++i){
                   sample_num=int(rand()*(n-1))+1;
                   while( sample_num in sample_num_arr){
                       sample_num=int(rand()*(n-1))+1;
                   }
                   sample_num_arr[sample_num];
                   date_sample_arr[i]=date_arr[sample_num];
               }

               date_sample=date_sample_arr[1];
               for(i=2;i<=sample_day;++i){
                   date_sample=date_sample","date_sample_arr[i]; 
               }
           }
          
           printf("date_sample=%s", date_sample);
       }' 
       )
       echo $date_sample;       
}


function strFormat()
{
    str=$1;
    type=$2;

    if [ $str == "0" ]; then
        echo "0";
        return 0;
    fi

    awk -v str=$str -v type=$2 'BEGIN{
        if( type == "area" ){
            while(getline<"../conf/area_def"){
                str_map[$4]=$3;
            }
        } else if( type == "channel" ){
            #gsub("10011","10011_1,10011_2",str);
            while(getline<"../conf/channel_def"){
                str_map[$1]=$2;
            }
        }

        n=split(str,arr,",");
        if( n == 1 ){
            str_name=str_map[str];
            str_format="'\''"str_name"'\''";
        }else{
            str_name=str_map[arr[1]];
            str_format="'\''"str_name"'\''";
            for(i=2;i<=n;++i){
                str_name=str_map[arr[i]];
                str_format=str_format",""'\''"str_name"'\''"    
            }
            #str_format="\""str_format"\""
        }

	print str_format;
    }'
}

function waitLock(){
    loop=true
    while $loop
    do
        # read pid and modified time from lock file
        lockpid=`cat $1 2>&-`
        mod_time=`stat -c %Y $1 2>&-`
        # if mod_time can be got, lock file exists
        if [ ${#lockpid} -gt 0 -a ${#mod_time} -gt 0 ]
        then
            # get the age (in second) of the lock file
            (( age = `date +%s` - $mod_time ))
            # if the lock file is old enough, it need to be checked
            if [ $age -gt 10 ]
            then
                # check if the lock process is still running
                if [ `ps $lockpid | wc -l` -eq 1 ]
                then
                    # check if the lock file is created by a new process, if not, the lock process really dies
                    lockpid_=`cat $1 2>&-`
                    if [ $lockpid = $lockpid_ ]
                    then
                        rm -f $1;
                    fi
                fi
            fi
        fi
        lockfile -r1 $1 2>&-;
        if [ $? -eq 0 ]
        then
            loop=false
        fi
    done
    if [ $? -eq 0 ]
    then
        chmod u+w $1;
        echo $$ > $1;
        chmod u-w $1;
    fi
}

function freeLock(){
    rm -f $1;
}

#strFormat  1024,2048,3072  "area"
#strFormat 10011,10030,10040 "channel"
#sampleDate '20130414' '20130430' 'ps,zhidao'
#a=`strFormat 0 "area"`
#echo $a;

#sampleDate 20140720 20140728 wise
