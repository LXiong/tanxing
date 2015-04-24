#!/bin/bash
source ../conf/sys.conf
brand_id_str=$1;
file="${result_dir}/get_brand_uv.out";

awk -v brand_id_str=$brand_id_str 'BEGIN{FS="\t";OFS="\t";
    while( getline < "'"$file"'" ){
        uv[$1]=$2;
    }

    n=split(brand_id_str,brand_arr_tmp,",");
    asort(brand_arr_tmp,brand_arr);

    for( i = 1; i < n; ++i ){
        for( j = 2; j <= n; ++j ){
            if( i < j ){
                brand_1 = brand_arr[i];
                brand_2 = brand_arr[j];
                if( uv[brand_1] == 0 || uv[brand_2] == 0 || uv[brand_1","brand_2] == 0 ){
                    similarity = 0;
                    distance = 1;
                } else {
                    similarity = uv[brand_1","brand_2]/sqrt(uv[brand_1]*uv[brand_2]);
                    distance = sqrt(2-2*similarity);
                }
                print brand_1","brand_2, similarity, distance;
            }
        }
    }
}' > "${result_dir}/similarity_stat_orig"
