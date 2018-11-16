# -*- coding: UTF-8 -*-
<<<<<<< HEAD

=======
"""
Created on Fri Nov 16 13:48:12 2018

@author: ZiFeng

@function: ETLEvent
"""
>>>>>>> 301e81a0502bef4d0920ba310970fb1ecfa1dddc
from ETLBase import ETLBase

class ETLEvent(ETLBase):
    def __init__(self, run_mode, task_name, prj_path):
        ETLBase.__init__(self, run_mode, task_name, prj_path)

    def divide_table(self, result_dict):
        url_path = result_dict['url_path']
        if url_path == '/view.gif':
            return 'dwd_user_view_event_d'
        elif url_path == '/click.gif':
            return 'dwd_user_click_event_d'
        else:
            return 'dwd_user_others_event_d'
