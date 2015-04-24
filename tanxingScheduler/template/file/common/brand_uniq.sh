#!/bin/bash


awk 'BEGIN{FS="\t";OFS="\t"}{
    cookie=$1;
    channel_id=$2;
    brand_id_str=$3;
    n=split(brand_id_str, brand_arr, "," );

    for( i = 1; i <= n; ++i ){
        brand_uniq[brand_arr[i]];
    }

    first=1;
    brand_uniq_str="";
    for( brand in brand_uniq ){
        if( first == 1 ){
            brand_uniq_str = brand;
            first = 0;
        } else {
            brand_uniq_str = brand_uniq_str","brand;
        }
    }

    delete brand_uniq;

    print cookie, channel_id,brand_uniq_str;
}'
