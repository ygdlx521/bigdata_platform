# -*- coding: UTF-8 -*-

import sys
import re
import os
import urlparse
import urllib
import logging
import time
from ipip.ipip import IP
reload(sys)
sys.setdefaultencoding('utf-8')


class ETLBase(object):
    
    def __init__(self, run_mode, task_name, prj_path):
        self.prj_path = prj_path
        self.log_path = self.prj_path.rstrip('/') + '/' + 'log'
        self.hive_table_schema_path = self.prj_path.rstrip('/') + '/' + 'HQL'
        self.task_name = task_name
        self.run_mode = run_mode
        IP.load(os.path.abspath(self.prj_path + "/script/ipip/17monipdb.dat"))

        #本地模式：单机运行，结果保存在本地文件，用于本地调试。
        if self.run_mode == "local":
            self.logger = logging.getLogger(self.task_name)
            formatter = logging.Formatter(
                '%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s'
            )
            file_handler = logging.FileHandler(self.log_path + "/" +
                                               self.task_name + ".log")
            stdout_handler = logging.StreamHandler(sys.stdout)
            file_handler.setFormatter(formatter)  # 可以通过setFormatter指定输出格式
            stdout_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
            #self.logger.addHandler(stdout_handler)
            self.logger.setLevel(logging.DEBUG)

        #集群模式：通过Hadoop Streaming在集群上执行
        elif self.run_mode == "cluster":
            self.logger = logging.getLogger(self.task_name)
            formatter = logging.Formatter(
                '%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s'
            )
            stdout_handler = logging.StreamHandler(sys.stderr)
            stdout_handler.setFormatter(formatter)
            self.logger.addHandler(stdout_handler)
            self.logger.setLevel(logging.DEBUG)
        else:
            print "run_mode parameter Error,only local or cluster mode can be selected!"
            sys.exit()

    def compile_regex(self, regex):
        regex_compiled = re.compile(regex, flags=re.LOCALE)
        return regex_compiled

    def parse_original_log(self, regex_compiled, line):
        m = regex_compiled.match(line)
        if not m:
            self.custom_counter("Warning", "parse_original_log", 1,
                                line.strip('\n'))
            return None
        original_log_dict = m.groupdict()
        #解析时间
        d = time.localtime(float(original_log_dict['dateandtime']))
        
        original_log_dict['year'] = time.strftime('%Y',d)
        original_log_dict['month'] = time.strftime('%m',d)
        original_log_dict['day'] = time.strftime('%d',d)
        original_log_dict['time'] = time.strftime("%Y-%m-%d %H:%M:%S",d)
        original_log_dict['unix_timestamp'] = time.strftime("%s",d)
        del original_log_dict['dateandtime']

        #根据IP获取地理位置
        if str(original_log_dict['ip']).strip() == "-":
            original_log_dict['country'] = '未知'
            original_log_dict['province'] = '未知'
            original_log_dict['city'] = '未知'
        else:
            ips = IP.find(original_log_dict['ip']).split()
            iplen = len(ips)
            if iplen >= 1:
                original_log_dict['country'] = ips[0]
            else:
                original_log_dict['country'] = '未知'
            if iplen >= 2:
                original_log_dict['province'] = ips[1]
            else:
                original_log_dict['province'] = '未知'
            if iplen >= 3:
                original_log_dict['city'] = ips[2]
            else:
                original_log_dict['city'] = '未知'

        original_log_dict['userid'] = self.parse_cookie(original_log_dict['cookie'])

        return original_log_dict

    def parse_cookie(self, cookie):
        return cookie.split("=")[1]


    def parse_url(self, url, url_path, url_params, url_host='', unquote=True):
        """
        输入：需要解析的url
        输入：从url中解析出urlpath，保存的key的名字：url_path
        输入: 其他url中的参数解析到字典key名为：url_params
        输出：字典，包含解析出来的参数与值

        """
        dict = {}
        if not url:
            self.custom_counter("Warning", "parse_url", 1, url)
            return dict
        try:
            result = urlparse.urlparse(url)
        except Exception, err:
            self.custom_counter("Exception", "parse_url", 1, str(err) + url)
            return dict
        dict[url_path] = str(result.path).strip()
        dict[url_params] = {}
        if url_host != '':
            dict[url_host] = result.netloc
        if not result.query:
            return dict
        query = str(result.query)
        param = urlparse.parse_qs(query)
        for key in param.keys():
            if isinstance(param[key], list):
                param[key] = param[key][0]
            if unquote:
                after_unquote = urllib.unquote(param[key])
                try:
                    param[key] = after_unquote.decode('utf8')
                except UnicodeDecodeError, err:
                    #self.custom_counter("Exception","parse_url",1,str(err)+url.strip('\n'))
                    try:
                        param[key] = after_unquote.decode('gbk')
                    except UnicodeDecodeError, err:
                        self.custom_counter("Exception", "parse_url", 1,
                                            str(err) + url)
                        param[key] = ""
        dict[url_params] = param
        return dict

    def parse_useragent(self, ua):
        sys.path.append(self.prj_path + "/script/useragent_parser")
        from user_agents import parse as ua_parse
        ua_dict = {}
        user_agent = ua_parse(ua)
        if user_agent.browser.family != None:
            ua_dict['browser'] = user_agent.browser.family
        if user_agent.browser.version_string != None:
            ua_dict[
                'browser_version'] = user_agent.browser.version_string
        if user_agent.os.family != None:
            ua_dict['os'] = user_agent.os.family
        if user_agent.device.family != None:
            ua_dict['device'] = user_agent.device.family
        if user_agent.device.brand != None:
            ua_dict['device_type'] = user_agent.device.brand
        if user_agent.device.model != None:
            ua_dict['device_version'] = user_agent.device.model
        return ua_dict

    def parse_hive_table_schema(self, create_table_file):
        schemas = []
        with open(self.prj_path + "/script/hive_table_schema/" + create_table_file, 'r') as fi:
            hive_schema = fi.read()
            content = hive_schema.replace('\n', ' ')
            regex = '.*CREATE.*TABLE.*IF NOT EXISTS (?P<table_name>\w+)\s*\(\s+(?P<columns>.*)\s*\)\s*COMMENT.*'
            pattern = re.compile(regex)
            #正则匹配，从hive表结构定义的hql中解析出表名,各个字段名和字段类型
            result = pattern.search(content)
            if result:
                schema_dict = result.groupdict()
                columns = schema_dict['columns']
                #注意空格！ map<string,string>
                column_list = columns.split(', ')
                for column in column_list:
                    column_pairs = column.split()
                    if len(column_pairs) >= 2:
                        column_name = column_pairs[0]
                        column_type = column_pairs[1]
                        schemas.append({
                            'name': column_name,
                            'type': column_type
                        })
                    else:
                        pass
            else:
                pass
            return schemas

    def concatenate_hive_columns(self, result_dict, hive_table_schema):
        columns_list = []
        for column in hive_table_schema:
            try:
                key = column['name']
                type = column['type']
                column_value = '{0}'.format('')
                if key == 'source_host':
                    continue
                if key in result_dict:
                    column_value = result_dict[key]
                    columns_list.append(column_value)
                else:
                    if type == 'SMALLINT' or type == 'INT' or type == 'BIGINT':
                        column_value = '0'
                    columns_list.append(column_value)
                    continue
            except KeyError, err:
                self.custom_counter("Exception", "concatenate_hive_columns", 1,
                                    str(err) + str(hive_table_schema))
                return None
        try:
            output_str = u'\001'.join(columns_list)
            return output_str.replace('\n', '%0A').replace('\r', '%0D')
        except Exception, err:
            print columns_list
            self.custom_counter("Exception", "concatenate_hive_columns", 1,
                                str(err) + str(columns_list))
            return None

    def concatenate_params_dict(self, params_dict):
        params_list = []
        for k, v in params_dict.iteritems():
            if type(v) is list:
                v = v[-1]
            try:
                params_list.append(k + u"\003" + v)
            except UnicodeDecodeError:
                pass
        result = u'\002'.join(params_list)
        return result

    def normalise_table_fields(self, result_dict):
        for k, v in result_dict.iteritems():
            if isinstance(v, dict):
                result_dict[k] = self.concatenate_params_dict(v)
        return result_dict


    def divide_table():
        pass

    def process_original_log(self, line, regex_compiled):
        line = line.decode('utf-8').strip()
        parse_log_dict = self.parse_original_log(regex_compiled, line)
        if not parse_log_dict or 'click_url' not in parse_log_dict:
            return
        else:
            url = 'http://www.atguigu.com%s' % parse_log_dict['click_url']
            urlparam = self.parse_url(url, 'url_path',
                                      'url_params', 'url_host')
            urlparam['url'] = url
            del parse_log_dict['click_url']
            result = dict(parse_log_dict.items() + urlparam.items())
            
            referer_param = self.parse_url(
                result['referer'], 'referer_path',
                'referer_params', 'referer_host')
            result_dict = dict(result.items() + referer_param.items())
            
            ua_dict = self.parse_useragent(result_dict['useragent'])
            result_dict = dict(result_dict.items() + ua_dict.items())
            return result_dict

    def write_hive_table_file(self, table_file_handle, table_name, output):
        table_file_handle.write(output + "\n")

    def custom_counter(self, group, counter, amount, line):
        cnt = 'reporter:counter:{g},{c},{a}'.format(
            g=group, c=counter, a=amount)
        sys.stderr.write(cnt + '\n')
        self.logger.warn(group + ": " + counter + " " + line + '\n')
