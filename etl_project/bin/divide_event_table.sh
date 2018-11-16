#!/bin/bash
set -x
#set -e
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

table_name=("dwd_user_view_event_d" "dwd_user_click_event_d" "dwd_user_others_event_d")
for table in ${table_name[@]}
do
    hdfs dfs -test -d /user/atguigu/bigdata_platform/dwd/${table}/year=${year}/month=${month}/day=${day}
    if [[ $? == 0 ]];then
        hdfs dfs -rm -r /user/atguigu/bigdata_platform/dwd/${table}/year=${year}/month=${month}/day=${day}
    fi
    hadoop fs -mkdir -p /user/atguigu/bigdata_platform/dwd/${table}/year=${year}/month=${month}/day=${day}
    hadoop fs -mv /user/atguigu/bigdata_platform/dwd/dwd_user_all_event_d/year=${year}/month=${month}/day=${day}/${table}  /user/atguigu/bigdata_platform/dwd/${table}/year=${year}/month=${month}/day=${day}
    hive -e "ALTER TABLE event.${table} ADD IF NOT EXISTS PARTITION(year='${year}',month='${month}',day='${day}');"
done
