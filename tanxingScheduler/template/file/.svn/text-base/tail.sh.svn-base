#!/bin/sh

subtask_id=$1;
arr=(${subtask_id//_/ });
task_id=${arr[1]};
trade_id=${arr[2]};

echo "subtask_id", ${subtask_id};
echo "task_id", ${task_id};
echo "trade_id",${trade_id};

for file in result/*.static.out
do
    if [ "${file}" != "result/*.static.out" ]
    then
        echo "${file}" >> log/writerBack.log
	python bin/static_write_back.py ${task_id} ${trade_id} < "${file}"
    fi
done  

if [ -f "result/get_query_account" ]
then
    python bin/get_query_account_write_back.py ${task_id} ${trade_id} < result/get_query_account
fi

if [ -f "result/query_all_stat.behavior.out" ]
then
    python bin/query_all_write_back.py ${task_id} ${trade_id} < result/query_all_stat.behavior.out
fi

if [ -f "result/channel_stat.behavior.out" ]
then
    python bin/channel_stat_write_back.py ${task_id} ${trade_id} < result/channel_stat.behavior.out
fi

if [ -f "result/channel_path.behavior.out" ]
then
    python bin/channel_path_write_back.py ${task_id} ${trade_id} < result/channel_path.behavior.out
fi

if [ -f "result/sequence_by_step.behavior.out" ]
then
    python bin/behavior_by_step_write_back.py ${task_id} ${trade_id} < result/sequence_by_step.behavior.out
fi

if [ -f "result/sequence_original.behavior.out" ]
then
    python bin/behavior_origin_write_back.py ${task_id} ${trade_id} < result/sequence_original.behavior.out
fi

if [ -f "result/competitive_ratio.brand.out" ]
then
    python bin/competitive_ratio_write_back.py ${task_id} < result/competitive_ratio.brand.out
fi

if [ -f "result/overlap_ratio.brand.out" ]
then
    python bin/overlap_ratio_write_back.py ${task_id} < result/overlap_ratio.brand.out
fi

if [ -f "result/similarity.brand.out" ]
then
    python bin/similarity_write_back.py ${task_id} < result/similarity.brand.out
fi

if [ -f "result/brand_preference_time" ]
then
    python bin/brand_preference_time_write_back.py ${task_id} < result/brand_preference_time
fi

if [ -f "result/brand_preference_category" ]
then
    python bin/brand_preference_category_write_back.py ${task_id} < result/brand_preference_category
fi

if [ -f "result/hot_question_brand" ]
then
    python bin/hot_question_brand_write_back.py ${task_id} < result/hot_question_brand
fi

if [ -f "result/hot_question_global" ]
then
    python bin/hot_question_global_write_back.py ${task_id} < result/hot_question_global
fi

if [ -f "result/rap_clk_distribution" ]
then
   /home/work/.jumbo/bin/python bin/rap_click_distribution_write_back.py ${task_id} < result/rap_clk_distribution
fi

if [ -f "result/rap_query_sequence" ]
then
   /home/work/.jumbo/bin/python bin/rap_query_sequence_write_back.py ${task_id} < result/rap_query_sequence
fi

if [ -f "result/rap_tagged_dict" ]
then
    /home/work/.jumbo/bin/python bin/rap_tagged_dict_write_back.py ${task_id} < result/rap_tagged_dict
fi

if [ -f "result/rap_query_trend" ]
then
   /home/work/.jumbo/bin/python bin/rap_query_trend_write_back.py ${task_id} < result/rap_query_trend
fi
