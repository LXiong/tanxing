#!/bin/bash
source ../conf/sys.conf
brand_id_str=$1;
file="${result_dir}/similarity_stat_orig";

awk -v brand_id_input=$brand_id_str 'BEGIN{FS="\t";OFS="\t";
    n=split(brand_id_input, brand_id_arr, ",");
    brand_id=brand_id_arr[1];

    while( getline < "'"$file"'" ){
        similarity[$1]=$2;
    }

    for( brand_id_str in similarity ){
        if( match( ","brand_id_str",", ","brand_id"," ) ){
            split( brand_id_str, brand_id_arr_tmp, ",");
            if( brand_id == brand_id_arr_tmp[1]){
                brand_id_str_output = brand_id","brand_id_arr_tmp[2];
            } else {
                brand_id_str_output = brand_id","brand_id_arr_tmp[1];
            }

            printf("%s\t%.2f\n" , brand_id_str_output, similarity[brand_id_str] );
        }
    }
}' > "${result_dir}/overlap_ratio.brand.out";
