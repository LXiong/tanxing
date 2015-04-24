#!/bin/bash
source ../conf/sys.conf

brand_id_str=$1;
file="${result_dir}/get_brand_uv.out";

awk -v brand_id_str=$brand_id_str 'BEGIN{FS="\t";OFS="\t";
    while( getline < "'"$file"'" ){
        uv[$1]=$2;
    }

    n=split(brand_id_str,brand_arr,",");
    brand_1_tmp =brand_arr[1];

    for( j = 2; j <= n; ++j ){
        brand_2 = brand_arr[j];
        if( brand_1_tmp < brand_2 ){
            brand_1 = brand_1_tmp;
        } else {
            brand_1 = brand_2; 
            brand_2 = brand_1_tmp;
        }

        if( uv[brand_1] == 0 ){
            competitive_1 = 0;
        } else {
            competitive_1 = uv[brand_1","brand_2]/uv[brand_1];
        }

        if( uv[brand_2] == 0 ){
           competitive_2 = 0;
        } else {
           competitive_2 = uv[brand_1","brand_2]/uv[brand_2];
        }

        printf("%s\t%s\t%d\t%.2f\n", brand_1, brand_2, uv[brand_1","brand_2], competitive_1);
        printf("%s\t%s\t%d\t%.2f\n", brand_2, brand_1, uv[brand_1","brand_2], competitive_2); 
    }
}' > "${result_dir}/competitive_ratio.brand.out"
