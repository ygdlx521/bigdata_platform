USE dimension;

CREATE EXTERNAL TABLE IF NOT EXISTS dim_user_portrait_info(
    userid STRING COMMENT '用户的ID'
    sex  STRING COMMENT '性别',
    age  STRING COMMENT '年龄', 
    industry STRING COMMENT '行业',
    job  STRING COMMENT '职业',
    is_car  BIGINT COMMENT '是否有车',
    is_house  STRING COMMENT '是否有房产',
    is_chilren  STRING COMMENT '是否有孩子',
    hobbies  STRING COMMENT '兴趣爱好'
)
COMMENT '用户画像信息表'
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
