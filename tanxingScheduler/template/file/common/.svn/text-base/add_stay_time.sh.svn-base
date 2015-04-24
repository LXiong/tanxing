#!/bin/bash

awk 'BEGIN{FS="\t";OFS="\t";last_cookie="";last_session="";other_info="";cookie="";session="";last_other_info=""}{
    cookie=$1;
    session=$2;
    time=$3;
    event_query=$6;
    event_url=$7;
    if(event_url ~ /^http:\/\/zhidao\.baidu\.com\/.*[?&]word=.*/){
        sub("?word=[^&]*","?word=" event_query ,$7);
        sub("&word=[^&]*","\\&word=" event_query ,$7);
    }
    other_info=$4"\t"$5"\t"$6"\t"$7"\t"$8;

    if( NR == 1 ){
    } else {
        if( cookie == last_cookie && session == last_session ){
            #print time, last_time;
            stay_time = time - last_time
        } else {
            stay_time = 0;
        }
        print last_cookie, last_session, last_time, stay_time, last_other_info;
    }

    last_cookie = cookie;
    last_session = session;
    last_time = time;
    last_other_info = other_info;
    stay_time = 0;
}END{
    print last_cookie, last_session, last_time, stay_time, last_other_info;
}'
