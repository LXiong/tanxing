#!/bin/bash
source ../conf/sys.conf

brand_id_str=$1;
file="${result_dir}/similarity_stat_orig";

#get matrix
awk  -v brand_id_str=$brand_id_str 'BEGIN{FS="\t";OFS="\t";
    while( getline < "'"$file"'" ){
        distance_arr[$1]=$3;
    }

    n=split(brand_id_str,brand_arr_tmp,",");
    asort(brand_arr_tmp, brand_arr);

    for( i = 1; i <= n; ++i ){
        for( j = 1; j <= n; ++j ){
            if( i == j ){
                distance = 0;
            } else if( i < j ){
                distance = distance_arr[brand_arr[i]","brand_arr[j]];
            } else if( i > j ){
                distance = distance_arr[brand_arr[j]","brand_arr[i]];
            }

            if( j != n ){
                printf("%f\t", distance );
            } else {
                printf("%f\n", distance );
            }
        }
    }
}' > $file"_matrix"

#R mds
Rscript mds.r $file"_matrix" | awk 'BEGIN{FS=" ";OFS="\t"}{if(NR>1){for(i=2;i<NF;++i){printf("%f\t", $i);} printf("%f\n", $NF)}}' > $file"_points"

file_points=$file"_points";
#get x_axis,y_axis compute stress
awk -v brand_id_str=$brand_id_str 'BEGIN{FS="\t";OFS="\t";
    while( getline < "'"$file"'" ){
        distance_arr[$1]=$3;
    }

    i=1;
    while( getline < "'"${file_points}"'" ){
        x[i]=$1;
        y[i]=$2;
        i++;
    }

    n=split(brand_id_str,brand_arr_tmp,",");
    asort(brand_arr_tmp, brand_arr);

    for( i = 1; i < n; ++i ){
        for( j = 2; j <= n; ++j ){
            if( i < j ){
                dis_predict=sqrt((x[i]-x[j])*(x[i]-x[j])+(y[i]-y[j])*(y[i]-y[j]));
                dis_real=distance_arr[brand_arr[i]","brand_arr[j]];
                diff += (dis_predict-dis_real)*(dis_predict-dis_real);
                denominator += dis_real*dis_real;
            }
        }
    }

    stress = sqrt( diff / denominator );

    for( i = 1; i < n; ++ i ){
        x_axis=x_axis""x[i]",";
        y_axis=y_axis""y[i]",";
        brand_id_str_output=brand_id_str_output""brand_arr[i]",";
    }
    x_axis=x_axis""x[n];
    y_axis=y_axis""y[n];
    brand_id_str_output=brand_id_str_output""brand_arr[n];

    
    printf("%s\t%s\t%s\t%.2f\n",brand_id_str_output, x_axis, y_axis, stress );

}'  > ${result_dir}/"similarity.brand.out";


