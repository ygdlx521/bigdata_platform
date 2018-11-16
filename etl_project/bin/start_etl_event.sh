#!/bin/bash
set -x
#set -e
bin=`cd $(dirname $0);pwd`
date=$(date -d '1 hour ago' +"%Y%m%d%H")
if [ $# -eq 1 ]
then
    date=$1
fi
year=${date:0:4}
month=${date:4:2}
day=${date:6:2}
hour=${date:8:2}
output_day=${year}${month}${day}
output_hour=${hour}

input="/user/atguigu/bigdata_platform/ods/log/${year}${month}${day}"
output="/user/atguigu/bigdata_platform/dwd/dwd_user_all_event_d/year=${year}/month=${month}/day=${day}"

hdfs dfs -test -d ${output}
if [[ $? == 0 ]];then
    hdfs dfs -rm -r ${output}
#    hive -e "use player;alter table video_pc_player_total drop if exists partition (event_day='${output_day}',event_hour='${hour}')"
fi

hadoop jar /opt/module/hadoop-2.7.2/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar \
-libjars multiOutput.jar \
-D mapreduce.job.name="ETL-user_event_${day}_${hour}" \
-archives "script.tar.gz#script" \
-input "${input}" \
-output "${output}" \
-mapper "script/script/python2.7/bin/python script/script/ETL_cluster_event_mapper.py" \
-reducer "script/script/python2.7/bin/python script/script/ETL_cluster_event_reducer.py" \
-outputformat "com.custom.CustomMultiOutputFormat" \
-cmdenv LC_CTYPE=zh_CN.UTF-8 \
-cmdenv LANG=zh_CN.UTF-8 \
-cmdenv DAY=${day} \
-cmdenv HOUR=${hour}

rm script.tar.gz
rm multiOutput.jar
#-outputformat "com.custom.CustomMultiOutputFormat" \
#-D mapreduce.map.output.compress=true \
#-D mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.Lz4Codec \
#-D mapreduce.input.fileinputformat.split.minsize=134217728 \

