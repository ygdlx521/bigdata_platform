<<<<<<< HEAD
# -*- coding: utf-8 -*-

=======
# -*- coding: UTF-8 -*-
"""
Created on Fri Nov 16 13:48:12 2018

@author: ZiFeng

@function: ETL_cluster_event_reducer
"""
>>>>>>> 301e81a0502bef4d0920ba310970fb1ecfa1dddc
from ETLEvent import ETLEvent
import sys
import os
import json

if __name__ == '__main__':
    etl_obj = ETLEvent("cluster", "user_behaviour", "script")
    event_day = str(os.environ.get('DAY'))
    event_hour = str(os.environ.get('HOUR'))
    hive_table_schemas = etl_obj.parse_hive_table_schema("create_event_table_page_view.sql")
    for line in sys.stdin:
        line = line.decode('utf-8').strip()
        if len(line) == 0:
            continue
        result_dict = json.loads(line, encoding='utf-8')
        table_name = etl_obj.divide_table(result_dict)
        result_dict = etl_obj.normalise_table_fields(result_dict)
        output = etl_obj.concatenate_hive_columns(result_dict,
                                                  hive_table_schemas)
        output = output.encode('utf8')
        print u"{0}\t{1}\001".format(
            table_name + ":" + event_day + ":" + event_hour, output)
