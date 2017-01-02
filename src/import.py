#!/usr/bin/env python
# coding=utf-8
import json
import os
import platform
import pprint
import time
from collections import OrderedDict
from copy import deepcopy

import argparse
import requests
from influxdb import InfluxDBClient
from prettytable import PrettyTable

from constants import CONFIG_NAME, get_import_log_handler, ORG_KEY_LIST, fill_order_org_empty_dict, \
    fill_order_org_list_dict, \
    init_interval_empty_dict


class DataHandler(object):
    def __init__(self, name=None, border=False, platform='linux'):
        self.trade_period = 'day'
        self.log_logger = get_import_log_handler()
        self.border = border
        self.datadict = OrderedDict()
        self.static_first_record_timestamp = 0
        self.first_record_timestamp = 0
        self.last_record_timestamp = 0
        self.db_port = '8086' if platform.lower() == 'linux' else '4086'
        self.endpoint_url = 'http://localhost:8086/query' if platform.lower() == 'linux' else 'http://localhost:4086/query'
        self.folder_path = os.path.abspath(os.path.dirname(__file__)) + '/'
        self.config_name = name
        self.cfg_file = self.read_config_file()
        self.config_data = self.cfg_file[name]
        self.date_list = list()
        self.time_list = list()
        self.each_interval_data = deepcopy(fill_order_org_empty_dict(OrderedDict()))
        self.interval_sum_data_dict = deepcopy(fill_order_org_list_dict(OrderedDict()))
        self.interval_datadict = OrderedDict()

    def read_config_file(self):
        with open(self.folder_path + CONFIG_NAME) as cfg_file:
            file_data = json.load(cfg_file, encoding='utf-8')
        cfg_print_tbl = PrettyTable(['变量名', '数值'])
        for (k, v) in file_data[self.config_name].iteritems():
            cfg_print_tbl.add_row([k, v])
        self.log_logger.debug(u'配置文件内容为：\n%s', cfg_print_tbl)
        print(u'配置文件内容为：\n%s' % cfg_print_tbl)
        return file_data

    def read_file(self):
        start_time = time.time()
        index = 0
        with open(self.folder_path + self.cfg_file[self.config_name]['filename'], 'r') as data_file:
            for line in data_file.readlines():
                try:
                    data_elements = line.strip(' ').split('\t')
                    record_time = (str(data_elements[1]) + ',' + str(data_elements[0]))
                    pattern = '%Y%m%d,%H:%M'
                    time_format = time.strptime(record_time, pattern)
                    epech_time = time.mktime(time_format)
                    self.datadict[index] = {
                        'SHIJ': int(epech_time),  # 时间
                        'JIAG': int(data_elements[2]) if data_elements[2] else 0,  # 价格
                        'CJL': int(data_elements[3]) if data_elements[3] else 0,  # 成交量
                        'CJE': int(data_elements[4]) if data_elements[4] else 0,  # 成交额
                        'CANGL': int(data_elements[5]) if data_elements[5] else 0,  # 仓量
                        'KAIC': float(data_elements[9]) if data_elements[9] else 0,  # 开仓
                        'PINGC': int(data_elements[10] if data_elements[10] else 0),  # 平仓
                        # 'FANGX': data_elements[11].strip() if data_elements[11] else 0,  # 方向
                        'FANGX': 'fangxiang',  # 方向
                        'WEIZ': 0,  # 交易位置
                        'KDKD': 0,  # 开多开多
                        'KDKK': 0,  # 开多开空
                        'KDPD': 0,  # 开多平多
                        'PKPK': 0,  # 平空平空
                        'PKKK': 0,  # 平空开空
                        'PKPD': 0,  # 平空平多
                        'PDPD': 0,  # 平多平多
                        'PDKD': 0,  # 平多开多
                        'PDPK': 0,  # 平多平空
                        'KKKK': 0,  # 开空开空
                        'KKKD': 0,  # 开空开多
                        'KKPK': 0,  # 开空平空
                        'SHSP': 0,  # 上换手平
                        'SHSK': 0,  # 上换手开
                        'XHSP': 0,  # 下换手平
                        'XHSK': 0,  # 下换手开
                        'KPAN': 0,  # 开盘价
                        'SPAN': 0,  # 收盘价
                        'ZUIG': 0,  # 最高价
                        'ZUID': 0  # 最低价
                    }
                except Exception as e:
                    self.log_logger.info(u'问题行原始数据为: %r', line)
                    self.log_logger.exception(e)
                index += 1
            print(u'读取文件花费时间为：%s' % (time.time() - start_time))
            self.log_logger.info(u'读取文件花费时间为：%s', time.time() - start_time)

    def generate_dynamic_data(self):  # 根据价格计算方向
        start_time = time.time()

        reversed_keys = deepcopy(self.datadict.keys())
        reversed_keys.reverse()
        earliest_record_index_number = len(reversed_keys) - 1  # 最早的那条记录的索引值
        # 设置最早和最晚的时间戳
        self.static_first_record_timestamp = deepcopy(self.datadict[earliest_record_index_number]['SHIJ'])
        self.first_record_timestamp = deepcopy(self.datadict[earliest_record_index_number]['SHIJ'])
        self.last_record_timestamp = deepcopy(self.datadict[0]['SHIJ'])
        self.trade_period = 'night' if time.strftime('%Y-%m-%d,%H:%M:%S', time.localtime(self.first_record_timestamp)).split(',')[1] == '19:00:00' else 'day'

        # 确定第一个交易位置值
        for each in reversed_keys:
            if each < earliest_record_index_number:
                if self.datadict[each]['JIAG'] > self.datadict[earliest_record_index_number]['JIAG']:
                    # 如果下一条记录的价格比最开始的那条记录的价格大的话. 最开始的那条记录赋值为-1
                    self.datadict[earliest_record_index_number]['WEIZ'] = -1
                    self.generate_each_dynamic_data(self.datadict[earliest_record_index_number]['WEIZ'], earliest_record_index_number)
                    break
                elif self.datadict[each]['JIAG'] < self.datadict[earliest_record_index_number]['JIAG']:
                    # 如果下一条记录的价格比最开始的那条记录的价格小的话. 最开始的那条记录赋值为1
                    self.datadict[earliest_record_index_number]['WEIZ'] = 1
                    self.generate_each_dynamic_data(self.datadict[earliest_record_index_number]['WEIZ'], earliest_record_index_number)
                    break
                else:
                    # 如果下一条记录的价格跟最开始的那条记录的价格一样的话. 忽略，进行下一条记录的比较
                    pass
        # print(u'确定第一个交易位置值花费时间为：%s' % (time.time() - start_time))
        self.log_logger.debug(u'确定第一个交易位置值花费时间为：%s, 第一个交易位置值为: %s', time.time() - start_time, self.datadict[earliest_record_index_number]['WEIZ'])

        for each in reversed_keys:
            current_idx = each + 1 - len(reversed_keys)
            if current_idx % 10000 == 0 and current_idx / 10000 > 0:
                print(u'成功处理 一万条 数据。总花费时间为: %s' % (time.time() - start_time))
                self.log_logger.info(u'成功处理 一万条 数据。总花费时间为: %s', (time.time() - start_time))
            if self.datadict[each]['WEIZ'] == 0 and self.datadict.get(each + 1):
                current_price = self.datadict[each]['JIAG']
                previous_price = self.datadict[each + 1]['JIAG']
                if int(current_price) - int(previous_price) > 0:
                    WEIZ = 1
                elif int(current_price) - int(previous_price) < 0:
                    WEIZ = -1
                else:
                    WEIZ = self.datadict[each + 1]['WEIZ']
                self.datadict[each]['WEIZ'] = WEIZ
                self.generate_each_dynamic_data(WEIZ, each)
                self.insert_into_interval_dict(each)

        # self.log_logger.info(pprint.pformat(dict(self.interval_datadict)))
        self.log_logger.debug(u'计算其他动态值所花费时间为：%s', time.time() - start_time)
        # print(u'计算其他动态值所花费时间为：%s' % (time.time() - start_time))

    def generate_each_dynamic_data(self, weizhi, each):
        if weizhi == 1:
            if self.datadict[each]['KAIC'] < self.datadict[each]['PINGC']:
                # 开仓小于平仓
                self.datadict[each]['PKPK'] = self.datadict[each]['CJL'] / 2
                self.datadict[each]['PKKK'] = self.datadict[each]['KAIC']
                self.datadict[each]['PKPD'] = self.datadict[each]['CJL'] / 2 - self.datadict[each]['KAIC']
            elif self.datadict[each]['KAIC'] > self.datadict[each]['PINGC']:
                # 开仓大于平仓
                self.datadict[each]['KDKD'] = self.datadict[each]['CJL'] / 2
                self.datadict[each]['KDKK'] = self.datadict[each]['CJL'] / 2 - self.datadict[each]['PINGC']
                self.datadict[each]['KDPD'] = self.datadict[each]['PINGC']
            else:
                # 开仓等于平仓
                self.datadict[each]['SHSP'] = self.datadict[each]['KAIC']
                self.datadict[each]['SHSK'] = self.datadict[each]['PINGC']
        elif weizhi == -1:
            if self.datadict[each]['KAIC'] < self.datadict[each]['PINGC']:
                # 开仓小于平仓
                self.datadict[each]['PDPD'] = self.datadict[each]['CJL'] / 2
                self.datadict[each]['PDKD'] = self.datadict[each]['KAIC']
                self.datadict[each]['PDPK'] = self.datadict[each]['CJL'] / 2 - self.datadict[each]['KAIC']
            elif self.datadict[each]['KAIC'] > self.datadict[each]['PINGC']:
                # 开仓大于平仓
                self.datadict[each]['KKKK'] = self.datadict[each]['CJL'] / 2
                self.datadict[each]['KKKD'] = self.datadict[each]['CJL'] / 2 - self.datadict[each]['PINGC']
                self.datadict[each]['KKPK'] = self.datadict[each]['PINGC']
            else:
                # 开仓等于平仓
                self.datadict[each]['XHSP'] = self.datadict[each]['PINGC']
                self.datadict[each]['XHSK'] = self.datadict[each]['KAIC']
        else:
            self.log_logger.error(u'索引值: %s有问题， 交易位置非1 或 -1', each)
            self.log_logger.error(u'问题行详细信息为: %s', pprint.pformat(dict(self.datadict[each])))
            self.log_logger.error(u'问题前一行详细信息为: %s', pprint.pformat(dict(self.datadict[each + 1])))

    def insert_into_interval_dict(self, each):
        tk = self.datadict[each]['SHIJ']
        each_interval_dict = init_interval_empty_dict(OrderedDict())

        if self.interval_datadict.get(tk):
            for k in ORG_KEY_LIST:
                if k == 'ZUIG':
                    self.interval_datadict[tk][k] = self.datadict[each]['JIAG'] if self.interval_datadict[tk][k] < self.datadict[each]['JIAG'] else self.interval_datadict[tk][k]
                elif k == 'ZUID':
                    self.interval_datadict[tk][k] = self.datadict[each]['JIAG'] if self.interval_datadict[tk][k] > self.datadict[each]['JIAG'] else self.interval_datadict[tk][k]
                elif k == 'KPAN':
                    self.interval_datadict[tk][k] = self.datadict[0]['JIAG']
                elif k == 'SPAN':
                    self.interval_datadict[tk][k] = self.datadict[len(self.datadict.keys()) - 1]['JIAG']
                else:
                    self.interval_datadict[tk][k] = self.datadict[each][k] + self.interval_datadict[tk].get(k, 0)
        else:
            for k in ORG_KEY_LIST:
                if k == 'ZUIG':
                    each_interval_dict[k] = self.datadict[each]['JIAG'] if each_interval_dict[k] < self.datadict[each]['JIAG'] else each_interval_dict[k]
                elif k == 'ZUID':
                    each_interval_dict[k] = self.datadict[each]['JIAG'] if each_interval_dict[k] > self.datadict[each]['JIAG'] else each_interval_dict[k]
                elif k == 'KPAN':
                    each_interval_dict[k] = self.datadict[0]['JIAG']
                elif k == 'SPAN':
                    each_interval_dict[k] = self.datadict[len(self.datadict.keys()) - 1]['JIAG']
                else:
                    each_interval_dict[k] = self.datadict[each][k] + each_interval_dict.get(k, 0)
            self.interval_datadict[tk] = each_interval_dict

    @staticmethod
    def get_point_str_data(name, time, data):
        new_data_dict = dict()
        for (k, v) in data.iteritems():
            new_data_dict[k] = float(v)
        return {
            "measurement": name,
            "time": time,
            "fields": new_data_dict}

    def load_dynamic_data_into_influxdb(self):
        self.create_database()
        self.write_data_into_db()
        self.query_data_from_db()

    def write_data_into_db(self):
        start_time = time.time()
        json_body = list()
        for (k, v) in self.interval_datadict.iteritems():
            point_string_data = self.get_point_str_data(self.config_name, int(k), dict(v))
            json_body.append(point_string_data)
        client = InfluxDBClient('localhost', self.db_port, 'root', 'root', self.config_name)
        return_value = client.write_points(json_body, time_precision='s')
        self.log_logger.debug(u'写入InfluxDB数据库结果为%s， 花费时间为:%s', u'成功' if return_value else u'失败', time.time() - start_time)

    def query_data_from_db(self):
        start_time = time.time()
        query_cmd = 'select * from %s' % self.config_name
        query_params = {
            'q': query_cmd,
            'db': self.config_name,
            'epoch': 's'
        }
        self.log_logger.debug(u'InfluxDB查询信息: URL=>%s 查询参数=>%s', self.endpoint_url, query_params)
        r = requests.get(self.endpoint_url, params=query_params, timeout=5)
        self.log_logger.debug(u'InfluxDB查询结果为%s, 所用时间为:%s，', u'成功' if r.status_code == 200 else u'失败', time.time() - start_time)
        # self.log_logger.debug(u"InfluxDB查询返回数据为:\n%s", pprint.pformat(r.json()['results'][0]['series'][0]['values']))

    def create_database(self):
        create_db_cmd = 'CREATE DATABASE %s' % self.config_name
        create_db_params = {
            'q': create_db_cmd
        }
        r = requests.get(self.endpoint_url, params=create_db_params, timeout=5)
        self.log_logger.debug(u'创建InfluxDB数据库:%s, 结果为%s', self.config_name, u'成功' if r.status_code == 200 else u'失败')


def main(name=None, border=False, platform=None):
    data_handler = DataHandler(name=name, border=border, platform=platform)
    data_handler.read_file()
    data_handler.generate_dynamic_data()
    data_handler.load_dynamic_data_into_influxdb()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--interval', default=30, help=u'间隔时间，时间单位为分钟。')
    parser.add_argument('-n', '--name', help=u'配置信息名称', required=True)
    parser.add_argument('--border', default=False, action='store_true', help=u'是否显示表格边框，默认为不显示。')
    args = parser.parse_args()

    main(name=args.name, border=True, platform=platform.system())
