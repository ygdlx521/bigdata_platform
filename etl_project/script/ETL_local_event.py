# -*- coding: UTF-8 -*-
"""
Created on Fri Nov 16 13:48:12 2018

@author: ZiFeng

@function: ETL_local_event
"""
import os
from itertools import islice
from ETLEvent import ETLEvent

if __name__ == '__main__':
    prj_path = "/home/atguigu/bigdata_platform/etl_project/"
    result_path = prj_path + "result"
    log_path = prj_path + "log"
    if not os.path.isdir(result_path):
        print result_path + "not exist and create it"
        os.mkdir(result_path)
    if not os.path.isdir(log_path):
        print log_path + "not exist and create it"
        os.mkdir(log_path)
    inputfile = prj_path + "../data_project/nginx_log/20181019"
    etl_obj = ETLEvent("local", "user_behaviour_local",prj_path)
    regex = r'(?P<ip>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|-) - - \[(?P<dateandtime>\d{10})\]\s+((\"(GET|POST|HEAD)\s+)(?P<click_url>.+)( (http|HTTP)\/1\.1")) (?P<httpstatus>\d{3}) (?P<bytessent>(\d+|-)) (?P<sign>\S+) (?P<num>\d+) (["](?P<referer>(\-)|(.+))["]) (?P<cookie>.+) (["](?P<useragent>.+)["])'
    regex_compiled = etl_obj.compile_regex(regex)
    view_file = prj_path + 'result/user_view'
    click_file = prj_path + 'result/user_click'
    others_file = prj_path + 'result/user_others'
    view_fp = open(view_file, 'w')
    click_fp = open(click_file, 'w')
    others_fp = open(others_file, 'w')
    with open(inputfile, 'r') as infp:
        for lines in iter(lambda: tuple(islice(infp, 100000)), ()):
            for line in lines:
                print "--------------- line ----------------"
                print line
                result_dict = etl_obj.process_original_log(
                    line, regex_compiled)
                print "--------------- result_dict ----------------"
                print result_dict
                if result_dict is None:
                    continue
                table_name = etl_obj.divide_table(result_dict)
                result_dict = etl_obj.normalise_table_fields(result_dict)
                print "--------------- result_dict_merge ----------------"
                print result_dict
                hive_table_schemas = etl_obj.parse_hive_table_schema(
                    "create_event_table_page_view.sql")
                output = etl_obj.concatenate_hive_columns(
                    result_dict, hive_table_schemas)
                output = output.encode('utf8')
                print "--------------- output ----------------"
                print output
                if table_name == 'dwd_user_view_event_d':
                    etl_obj.write_hive_table_file(click_fp, table_name, output)
                elif table_name == 'dwd_user_click_event_d':
                    etl_obj.write_hive_table_file(view_fp, table_name, output)
                elif table_name == 'dwd_user_other_event_d':
                    etl_obj.write_hive_table_file(others_fp, table_name,
                                                  output)
    view_fp.close()
    click_fp.close()
    others_fp.close()
