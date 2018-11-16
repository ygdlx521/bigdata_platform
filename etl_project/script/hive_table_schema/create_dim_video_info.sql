USE user_action;

CREATE EXTERNAL TABLE IF NOT EXISTS dwd_user_view_event_d(
    year  SMALLINT  COMMENT '年',
    month SMALLINT  COMMENT '月',  
    day TINYINT  COMMENT '日',
    time STRING  COMMENT '时间字段格式为YYYY-mm-dd HH:MM:SS',
    timestamp INT  COMMENT 'UNIX时间戳',
    ip STRING COMMENT '点分格式记录的IPV4地址，x.x.x.x',
    country  STRING COMMENT '国家',
    province  STRING COMMENT '省份',    
    city  STRING COMMENT '城市',
    userid  BIGINT COMMENT '用户的ID',
    cookie  STRING COMMENT 'cookie信息',
    url  STRING COMMENT '完整访问链接，保存原始的URL',
    url_host  STRING COMMENT 'URL域名'
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
  LINE TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '......./user_action/dwd_user_view_event_d'
WITH SERDEPROPERTIES ('serialization.encoding'='UTF-8')
;
