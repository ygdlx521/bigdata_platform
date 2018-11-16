<<<<<<< HEAD
# -*- coding: utf-8 -*-

=======
# -*- coding: UTF-8 -*-
"""
Created on Fri Nov 16 13:48:12 2018

@author: ZiFeng

@function: ETL_cluster_event_mapper
"""
>>>>>>> 301e81a0502bef4d0920ba310970fb1ecfa1dddc
from ETLEvent import ETLEvent
import sys
import json

if __name__ == '__main__':
    etl_obj = ETLEvent("cluster", "user_behaviour", "script")
    regex = r'(?P<ip>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|-) - - \[(?P<dateandtime>\d{10})\]\s+((\"(GET|POST|HEAD)\s+)(?P<click_url>.+)( (http|HTTP)\/1\.1")) (?P<httpstatus>\d{3}) (?P<bytessent>(\d+|-)) (?P<sign>\S+) (?P<num>\d+) (["](?P<referer>(\-)|(.+))["]) (?P<cookie>.+) (["](?P<useragent>.+)["])'
    regex_compiled = etl_obj.compile_regex(regex)
    for line in sys.stdin:
        result_dict = etl_obj.process_original_log(line, regex_compiled)
        if result_dict is not None:
            try:
                print u'{0}'.format(
                    json.dumps(
                        result_dict, ensure_ascii=False, encoding='utf-8'))
            except:
                pass
