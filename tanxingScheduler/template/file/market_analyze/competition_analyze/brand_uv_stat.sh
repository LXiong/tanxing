#!/bin/bash

awk 'BEGIN{FS="\t";OFS="\t"}{
    brand_id_str=$1;
    n=split(brand_id_str, brand_arr, ",");

    for( i = 1; i <= n; ++i ){
        brand_1=brand_arr[i];
        brand_stat_arr[brand_1]++;

        for( j = 2; j <= n; ++j ){
            if( i < j ){
                brand_2=brand_arr[j];
                #print "i j ", i,j, "brand ", brand_1, brand_2;
                if( brand_1 < brand_2 ){
                    brand_stat_arr[brand_1","brand_2]++;
                } else {
                    brand_stat_arr[brand_2","brand_1]++;
                }
            }
        }
    }
}END{
    for( one in brand_stat_arr ){
        print one, brand_stat_arr[one];
    }
}'
