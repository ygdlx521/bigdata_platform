create database event;
create database dimension;
use event;
CREATE EXTERNAL TABLE IF NOT EXISTS dwd_user_view_event_d(
    time STRING  COMMENT '时间字段格式为YYYY-mm-dd HH:MM:SS',
    unix_timestamp INT  COMMENT 'UNIX时间戳',
    ip STRING COMMENT '点分格式记录的IPV4地址，x.x.x.x',
    country  STRING COMMENT '国家',
    province  STRING COMMENT '省份',    
    city  STRING COMMENT '城市',
    userid  BIGINT COMMENT '用户的ID',
    cookie  STRING COMMENT 'cookie信息',
    url  STRING COMMENT '完整访问链接，保存原始的URL',
    url_host  STRING COMMENT 'URL域名',
    url_path  STRING COMMENT 'URL路径',
    url_params  MAP<STRING,STRING>  COMMENT 'URL参数：K-V',
    referer  STRING   COMMENT '完整来源链接',
    referer_host  STRING    COMMENT '来源URL域名',
    referer_path  STRING    COMMENT '来源URL路径',
    referer_params  MAP<STRING,STRING>   COMMENT '来源URL参数：K-V',
    useragent  STRING    COMMENT '用户代理信息',
    browser  STRING   COMMENT '来源于UserAgent的浏览器',
    browser_version  STRING   COMMENT '来源于UserAgent的浏览器版本',
    os STRING   COMMENT '来源于UserAgent的操作系统版本',
    device  STRING   COMMENT '来源于UserAgent的设备名',
    device_type  STRING   COMMENT '来源于UserAgent的设备类型',
    device_version  STRING   COMMENT '来源于UserAgent的设备版本'
)
COMMENT '页面展现事件明细表'
PARTITIONED BY ( 
  `year` string, 
  `month` string, 
  `day` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\001'
  COLLECTION ITEMS TERMINATED BY '\002'
  MAP KEYS TERMINATED BY '\003'
STORED AS TEXTFILE
LOCATION '/user/atguigu/bigdata_platform/dwd/dwd_user_view_event_d/'
;
ALTER TABLE dwd_user_view_event_d SET SERDEPROPERTIES ('serialization.encoding'='UTF-8');
;
use event;
CREATE EXTERNAL TABLE IF NOT EXISTS dwd_user_click_event_d(
    time STRING  COMMENT '时间字段格式为YYYY-mm-dd HH:MM:SS',
    unix_timestamp INT  COMMENT 'UNIX时间戳',
    ip STRING COMMENT '点分格式记录的IPV4地址，x.x.x.x',
    country  STRING COMMENT '国家',
    province  STRING COMMENT '省份',    
    city  STRING COMMENT '城市',
    userid  BIGINT COMMENT '用户的ID',
    cookie  STRING COMMENT 'cookie信息',
    url  STRING COMMENT '完整访问链接，保存原始的URL',
    url_host  STRING COMMENT 'URL域名',
    url_path  STRING COMMENT 'URL路径',
    url_params  MAP<STRING,STRING>  COMMENT 'URL参数：K-V',
    referer  STRING   COMMENT '完整来源链接',
    referer_host  STRING    COMMENT '来源URL域名',
    referer_path  STRING    COMMENT '来源URL路径',
    referer_params  MAP<STRING,STRING>   COMMENT '来源URL参数：K-V',
    useragent  STRING    COMMENT '用户代理信息',
    browser  STRING   COMMENT '来源于UserAgent的浏览器',
    browser_version  STRING   COMMENT '来源于UserAgent的浏览器版本',
    os STRING   COMMENT '来源于UserAgent的操作系统版本',
    device  STRING   COMMENT '来源于UserAgent的设备名',
    device_type  STRING   COMMENT '来源于UserAgent的设备类型',
    device_version  STRING   COMMENT '来源于UserAgent的设备版本'
)
COMMENT '页面点击事件明细表'
PARTITIONED BY ( 
  `year` string, 
  `month` string, 
  `day` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\001'
  COLLECTION ITEMS TERMINATED BY '\002'
  MAP KEYS TERMINATED BY '\003'
STORED AS TEXTFILE
LOCATION '/user/atguigu/bigdata_platform/dwd/dwd_user_click_event_d/'
;
ALTER TABLE dwd_user_view_event_d SET SERDEPROPERTIES ('serialization.encoding'='UTF-8');

use event;
CREATE EXTERNAL TABLE IF NOT EXISTS dwd_user_others_event_d(
    time STRING  COMMENT '时间字段格式为YYYY-mm-dd HH:MM:SS',
    unix_timestamp INT  COMMENT 'UNIX时间戳',
    ip STRING COMMENT '点分格式记录的IPV4地址，x.x.x.x',
    country  STRING COMMENT '国家',
    province  STRING COMMENT '省份',    
    city  STRING COMMENT '城市',
    userid  BIGINT COMMENT '用户的ID',
    cookie  STRING COMMENT 'cookie信息',
    url  STRING COMMENT '完整访问链接，保存原始的URL',
    url_host  STRING COMMENT 'URL域名',
    url_path  STRING COMMENT 'URL路径',
    url_params  MAP<STRING,STRING>  COMMENT 'URL参数：K-V',
    referer  STRING   COMMENT '完整来源链接',
    referer_host  STRING    COMMENT '来源URL域名',
    referer_path  STRING    COMMENT '来源URL路径',
    referer_params  MAP<STRING,STRING>   COMMENT '来源URL参数：K-V',
    useragent  STRING    COMMENT '用户代理信息',
    browser  STRING   COMMENT '来源于UserAgent的浏览器',
    browser_version  STRING   COMMENT '来源于UserAgent的浏览器版本',
    os STRING   COMMENT '来源于UserAgent的操作系统版本',
    device  STRING   COMMENT '来源于UserAgent的设备名',
    device_type  STRING   COMMENT '来源于UserAgent的设备类型',
    device_version  STRING   COMMENT '来源于UserAgent的设备版本'
)
COMMENT '页面其他事件明细表'
PARTITIONED BY ( 
  `year` string, 
  `month` string, 
  `day` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\001'
  COLLECTION ITEMS TERMINATED BY '\002'
  MAP KEYS TERMINATED BY '\003'
STORED AS TEXTFILE
LOCATION '/user/atguigu/bigdata_platform/dwd/dwd_user_others_event_d/'
;
ALTER TABLE dwd_user_others_event_d SET SERDEPROPERTIES ('serialization.encoding'='UTF-8');

USE dimension;
CREATE EXTERNAL TABLE IF NOT EXISTS dimension.dim_user_info(
    uid INT COMMENT '用户的ID',
    sex  STRING COMMENT '性别',
    age  INT COMMENT '年龄',
    industry STRING COMMENT '行业',
    job  STRING COMMENT '职业',
    is_car  TINYINT COMMENT '是否有车',
    is_house  TINYINT COMMENT '是否有房产',
    is_chilren  TINYINT COMMENT '是否有孩子',
    hobbies  STRING COMMENT '兴趣爱好'
)
COMMENT '用户信息表'
PARTITIONED BY (
  `year` string,
  `month` string,
  `day` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/atguigu/bigdata_platform/dim/dim_user_info/'
;
ALTER TABLE dim_user_info SET SERDEPROPERTIES ('serialization.encoding'='UTF-8');

use dimension;
CREATE EXTERNAL TABLE IF NOT EXISTS dim_video_info(
    vid INT  COMMENT '视频ID',
    name STRING  COMMENT '视频名称',
    url STRING  COMMENT '视频URL',
    release_time STRING COMMENT '发布时间',
    release_year  STRING COMMENT '年代',
    area  STRING COMMENT '地区',    
    category  STRING COMMENT '视频类型',
    language  STRING COMMENT '语言',
    score  STRING COMMENT '评分',
    length  STRING COMMENT '视频长度',
    director  STRING COMMENT '导演',
    actor  STRING COMMENT '主演',
    download  STRING  COMMENT '下载链接'
)
COMMENT '视频信息维度表'
PARTITIONED BY ( 
  `year` string, 
  `month` string, 
  `day` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/atguigu/bigdata_platform/dim/dim_video_info/'
;
ALTER TABLE dim_video_info SET SERDEPROPERTIES ('serialization.encoding'='UTF-8');

