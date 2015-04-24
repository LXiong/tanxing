#!/bin/bash

file=$1;

awk 'BEGIN{FS="\t";OFS="\t"}{
    cookie_orig=$1;
    #cookie format
    flag = 1;
    if( match( cookie_orig, "BAIDUID=" ) ){
        start_index = index(cookie_orig, "BAIDUID=")+8;
        end_index = index(cookie_orig,":FG=");
        cookie = substr(cookie_orig, start_index, end_index - start_index );
        if (length(cookie) != 32)
            flag = 0;
    } else {
        if( length( cookie_orig) == 32 ){
            cookie = cookie_orig;
        } else {
            flag = 0;     
        }
    }

    channel_id=$2;
    brand_id_str=$3;
    if( flag )
         print cookie, channel_id,brand_id_str;
}' 
