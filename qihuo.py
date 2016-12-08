#!/usr/bin/env python
# coding=utf-8
import argparse
import json
import logging.handlers
import math
import pprint
import re
import sys
import time
from collections import OrderedDict
from copy import deepcopy

from prettytable import PrettyTable

LOG_FILE = 'qihuo.log'
FILE_NAME = 'baicha1.txt'
CONFIG_NAME = 'config.json'
TEXT_EXCEL_FILE_NAME = 'temp.file.txt'

FENZHONG_1 = 60
FENZHONG_5 = 5 * FENZHONG_1
FENZHONG_15 = 15 * FENZHONG_1
FENZHONG_30 = 30 * FENZHONG_1

MAX = sys.maxint
MIN = 0

KEY_DICT = {
    'CANGL': u'仓量', 'CJE': u'成交额', 'CJL': u'成交量', 'FANGX': u'方向', 'JIAG': u'价格', 'KAIC': u'开仓',
    'KDKD': u'开多开多', 'KDKK': u'开多开空', 'KDPD': u'开多平多', 'KKKD': u'开空开多', 'KKKK': u'开空开空',
    'KKPK': u'开空平空', 'PDKD': u'平多开多', 'PDPD': u'平多平多', 'PDPK': u'平多平空', 'PINGC': u'平仓',
    'PKKK': u'平空开空', 'PKPD': u'平空平多', 'PKPK': u'平空平空', 'SHIJ': u'时间', 'SHSP': u'上换手平',
    'SHSK': u'上换手开', 'WEIZ': u'交易位置', 'XHSP': u'下换手平', 'XHSK': u'下换手开', 'ZUIG': u'最高价',
    'ZUID': u'最低价', 'KPAN': u'开盘价', 'SPAN': u'收盘价'
}
EMPTY__DATA_DICT = {
    'KDKD': 0, 'KDKK': 0, 'KDPD': 0, 'PKPK': 0, 'PKKK': 0, 'PKPD': 0, 'PDPD': 0, 'PDKD': 0, 'PDPK': 0,
    'KKKK': 0, 'KKKD': 0, 'KKPK': 0, 'SHSP': 0, 'SHSK': 0, 'XHSP': 0, 'XHSK': 0, 'KPAN': 0, 'SPAN': 0,
    'ZUIG': list(), 'ZUID': list()
}
NON_ZERO_LIST = ['KPAN', 'SPAN', 'ZUIG', 'ZUID']

handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=1024 * 1024)
fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(message)s'
formatter = logging.Formatter(fmt)
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


def fill_order_dict(data_dict):
    data_dict['KDKD'] = list()
    data_dict['KDKK'] = list()
    data_dict['KDPD'] = list()
    data_dict['PDPD'] = list()
    data_dict['PDPK'] = list()
    data_dict['PDKD'] = list()
    data_dict['KKKK'] = list()
    data_dict['KKKD'] = list()
    data_dict['KKPK'] = list()
    data_dict['PKPK'] = list()
    data_dict['PKKK'] = list()
    data_dict['PKPD'] = list()
    data_dict['SHSK'] = list()
    data_dict['SHSP'] = list()
    data_dict['XHSK'] = list()
    data_dict['XHSP'] = list()
    data_dict['ZUIG'] = list()
    data_dict['ZUID'] = list()
    data_dict['KPAN'] = list()
    data_dict['SPAN'] = list()
    data_dict['ZUIG'] = list()
    data_dict['ZUID'] = list()
    return data_dict


def reset_dict(dict_data):
    for (k, v) in dict_data.iteritems():
        if k == 'ZUIG' or k == 'ZUID':
            dict_data[k] = list()
        else:
            dict_data[k] = 0


class DataHandler(object):
    def __init__(self, name=None, border=False):
        self.logger = logger
        self.border = border
        self.datadict = OrderedDict()
        self.first_record_timestamp = 0
        self.last_record_timestamp = 0
        self.entry_name = name
        self.cfg_file = self.read_config_file()
        self.entry_data = self.cfg_file[name]
        self.timestamp_list = list()
        self.all_data_dict = deepcopy(EMPTY__DATA_DICT)
        self.non_filter_data = deepcopy(EMPTY__DATA_DICT)
        self.big_data_dict = deepcopy(EMPTY__DATA_DICT)
        self.middle_data_dict = deepcopy(EMPTY__DATA_DICT)
        self.small_data_dict = deepcopy(EMPTY__DATA_DICT)
        self.other_data_dict = deepcopy(EMPTY__DATA_DICT)

        self.non_filter_printout_dict = deepcopy(fill_order_dict(OrderedDict()))
        self.big_printout_dict = deepcopy(fill_order_dict(OrderedDict()))
        self.middle_printout_dict = deepcopy(fill_order_dict(OrderedDict()))
        self.small_printout_dict = deepcopy(fill_order_dict(OrderedDict()))
        self.other_printout_dict = deepcopy(fill_order_dict(OrderedDict()))

    def read_config_file(self):
        with open(CONFIG_NAME) as cfg_file:
            file_data = json.load(cfg_file, encoding='utf-8')
        cfg_print_tbl = PrettyTable(['变量名', '数值'])
        for (k, v) in file_data[self.entry_name].iteritems():
            cfg_print_tbl.add_row([k, v])
        self.logger.debug(u'配置文件内容为：\n%s', cfg_print_tbl)
        print(u'配置文件内容为：\n%s' % cfg_print_tbl)
        return file_data

    def read_file(self, redis_client):
        start_time = time.time()
        index = 0
        with open('%s' % self.cfg_file[self.entry_name]['filename'], 'r') as data_file:
            while True:
                line = data_file.readline()
                if line:
                    data_elements = re.split(' |\t', line)
                    record_time = (str(data_elements[1]) + ',' + str(data_elements[0]))
                    pattern = '%Y%m%d,%H:%M'
                    time_format = time.strptime(record_time, pattern)
                    epech_time = time.mktime(time_format)
                    self.datadict[index] = {
                        'SHIJ': epech_time,  # 时间
                        'JIAG': int(data_elements[2]) if data_elements[2] else 0,  # 价格
                        'CJL': int(data_elements[3]) if data_elements[3] else 0,  # 成交量
                        'CJE': int(data_elements[4]) if data_elements[4] else 0,  # 成交额
                        'CANGL': int(data_elements[5]) if data_elements[5] else 0,  # 仓量
                        'KAIC': int(data_elements[9]) if data_elements[9] else 0,  # 开仓
                        'PINGC': int(data_elements[10] if data_elements[10] else 0),  # 平仓
                        'FANGX': data_elements[11].strip() if data_elements[11] else 0,  # 方向
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
                        'ZUID': 0.  # 最低价
                    }
                    # redis_client.write_data(str(index), json.dumps(data_dict, ensure_ascii=False))
                else:
                    break
                index += 1
            self.logger.info(u'读取文件花费时间为：%s', time.time() - start_time)

    def generate_dynamic_data(self):  # 根据价格计算方向
        start_time = time.time()

        reversed_keys = deepcopy(self.datadict.keys())
        reversed_keys.reverse()
        earliest_record_index_number = len(reversed_keys) - 1  # 最早的那条记录的索引值
        # 设置最早和最晚的时间戳
        self.first_record_timestamp = self.datadict[earliest_record_index_number]['SHIJ']
        self.last_record_timestamp = self.datadict[0]['SHIJ']
        # 确定第一个交易位置值
        for each in reversed_keys:
            if each < earliest_record_index_number:
                if self.datadict[each]['JIAG'] > self.datadict[earliest_record_index_number]['JIAG']:
                    # 如果下一条记录的价格比最开始的那条记录的价格大的话. 最开始的那条记录赋值为-1
                    self.datadict[earliest_record_index_number]['WEIZ'] = -1
                    break
                elif self.datadict[each]['JIAG'] < self.datadict[earliest_record_index_number]['JIAG']:
                    # 如果下一条记录的价格比最开始的那条记录的价格小的话. 最开始的那条记录赋值为1
                    self.datadict[earliest_record_index_number]['WEIZ'] = 1
                    break
                else:
                    # 如果下一条记录的价格跟最开始的那条记录的价格一样的话. 忽略，进行下一条记录的比较
                    pass
        # print(u'确定第一个交易位置值花费时间为：%s' % (time.time() - start_time))
        self.logger.debug(u'确定第一个交易位置值花费时间为：%s', time.time() - start_time)

        for each in reversed_keys:
            if self.datadict[each]['WEIZ'] == 0:
                current_price = self.datadict[each]['JIAG']
                previous_price = self.datadict[each + 1]['JIAG']
                if int(current_price) - int(previous_price) > 0:
                    WEIZ = 1
                elif int(current_price) - int(previous_price) < 0:
                    WEIZ = -1
                else:
                    WEIZ = self.datadict[each + 1]['WEIZ']
                self.datadict[each]['WEIZ'] = WEIZ

                if WEIZ == 1:
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
                elif WEIZ == -1:
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
                    self.logger.error(u'有问题， 交易位置非1 或 -1')

        self.logger.debug(u'计算其他动态值所花费时间为：%s', time.time() - start_time)
        # print(u'计算其他动态值所花费时间为：%s' % (time.time() - start_time))

    def print_to_file(self):
        self.logger.info(u'所有原始数据为:\n%s', pprint.pformat(dict(self.datadict)))

    def print_as_text(self):
        reversed_keys = deepcopy(self.datadict.keys())
        reversed_keys.reverse()
        with open(TEXT_EXCEL_FILE_NAME, 'w') as text_file:
            for (k, v) in KEY_DICT.iteritems():
                text_file.write(k)
                text_file.write('\t')
            text_file.write('\n')
            for each in reversed_keys:
                for (k, v) in KEY_DICT.iteritems():
                    text_file.write(str(self.datadict[each][k]))
                    text_file.write('\t')
                text_file.write('\n')
        print(u'打印到文本文件成功')

    def read_all_sum(self):
        for each in self.datadict.values():
            self.pack_data_into_dict(each, self.all_data_dict, filter_range=(MIN, MAX))
        self.all_data_dict['ZUIG'] = max(self.all_data_dict['ZUIG'])
        self.all_data_dict['ZUID'] = min(self.all_data_dict['ZUID'])
        self.all_data_dict['KPAN'] = self.datadict[len(self.datadict.keys()) - 1]['JIAG']
        self.all_data_dict['SPAN'] = self.datadict[0]['JIAG']

        all_sum_table = PrettyTable([u'名称', u'全部数据'], padding_width=1, border=self.border)
        all_sum_table.align = 'l'

        for (k, v) in self.all_data_dict.iteritems():
            all_sum_table.add_row([KEY_DICT[k], v])
        self.logger.info(u'\"%s\"的汇总数据为:(数据源:%s)\n %s',
                         self.cfg_file[self.entry_name]['chinese'],
                         self.cfg_file[self.entry_name]['filename'],
                         all_sum_table)
        print(u'\"%s\"的汇总数据为:(数据源:%s)\n %s' %
              (self.cfg_file[self.entry_name]['chinese'],
               self.cfg_file[self.entry_name]['filename'],
               all_sum_table))

    def read_interval_sum(self, interval):
        number_of_interval = int(math.ceil((self.last_record_timestamp - self.first_record_timestamp) / 60 / interval))
        self.logger.debug(u'对\"%s\"的所有数据(数据源:%s)进行按照%s分钟的间隔，共被分割为%s段的数据',
                          self.cfg_file[self.entry_name]['chinese'],
                          self.cfg_file[self.entry_name]['filename'],
                          interval, number_of_interval)
        print(u'对\"%s\"的所有数据(数据源:%s)进行按照%s分钟的间隔，共被分割为%s段的数据'
              % (self.cfg_file[self.entry_name]['chinese'],
                 self.cfg_file[self.entry_name]['filename'],
                 interval, number_of_interval))

        self.non_filter_data = EMPTY__DATA_DICT
        for each_loop in range(1, number_of_interval):
            reset_dict(self.non_filter_data)
            reset_dict(self.big_data_dict)
            reset_dict(self.middle_data_dict)
            reset_dict(self.small_data_dict)
            reset_dict(self.other_data_dict)
            for each in self.datadict.values():
                if 0 <= each['SHIJ'] - self.first_record_timestamp < FENZHONG_1 * int(interval):
                    # 无过滤条件
                    self.pack_data_into_dict(each, self.non_filter_data,
                                             filter_range=(MIN, MAX))
                    # 大单
                    self.pack_data_into_dict(each, self.big_data_dict,
                                             filter_range=(self.cfg_file[self.entry_name]['big'], MAX))
                    # 中单
                    self.pack_data_into_dict(each, self.middle_data_dict,
                                             filter_range=(self.cfg_file[self.entry_name]['middle'],
                                                           self.cfg_file[self.entry_name]['big']))
                    # 小单
                    self.pack_data_into_dict(each, self.small_data_dict,
                                             filter_range=(self.cfg_file[self.entry_name]['small'],
                                                           self.cfg_file[self.entry_name]['middle']))
                    # 其他
                    self.pack_data_into_dict(each, self.other_data_dict,
                                             filter_range=(MIN, self.cfg_file[self.entry_name]['small']))


            self.non_filter_data['ZUIG'] = max(self.non_filter_data['ZUIG']) if self.non_filter_data['ZUIG'] else 0
            self.non_filter_data['ZUID'] = min(self.non_filter_data['ZUID']) if self.non_filter_data['ZUID'] else 0
            self.non_filter_data['KPAN'] = self.datadict[len(self.datadict.keys()) - 1]['JIAG']
            self.non_filter_data['SPAN'] = self.datadict[0]['JIAG']

            self.big_data_dict['ZUIG'] = max(self.big_data_dict['ZUIG']) if self.big_data_dict['ZUIG'] else 0
            self.big_data_dict['ZUID'] = min(self.big_data_dict['ZUID']) if self.big_data_dict['ZUID'] else 0
            self.big_data_dict['KPAN'] = self.datadict[len(self.datadict.keys()) - 1]['JIAG']
            self.big_data_dict['SPAN'] = self.datadict[0]['JIAG']

            self.middle_data_dict['ZUIG'] = max(self.middle_data_dict['ZUIG']) if self.middle_data_dict['ZUIG'] else 0
            self.middle_data_dict['ZUID'] = min(self.middle_data_dict['ZUID']) if self.middle_data_dict['ZUID'] else 0
            self.middle_data_dict['KPAN'] = self.datadict[len(self.datadict.keys()) - 1]['JIAG']
            self.middle_data_dict['SPAN'] = self.datadict[0]['JIAG']

            self.small_data_dict['ZUIG'] = max(self.small_data_dict['ZUIG']) if self.small_data_dict['ZUIG'] else 0
            self.small_data_dict['ZUID'] = min(self.small_data_dict['ZUID']) if self.small_data_dict['ZUID'] else 0
            self.small_data_dict['KPAN'] = self.datadict[len(self.datadict.keys()) - 1]['JIAG']
            self.small_data_dict['SPAN'] = self.datadict[0]['JIAG']

            self.other_data_dict['ZUIG'] = max(self.other_data_dict['ZUIG']) if self.other_data_dict['ZUIG'] else 0
            self.other_data_dict['ZUID'] = min(self.other_data_dict['ZUID']) if self.other_data_dict['ZUID'] else 0
            self.other_data_dict['KPAN'] = self.datadict[len(self.datadict.keys()) - 1]['JIAG']
            self.other_data_dict['SPAN'] = self.datadict[0]['JIAG']

            self.cleanup_non_filter_printout_table_dict(interval, self.non_filter_printout_dict, self.non_filter_data)
            self.cleanup_filter_printout_table_dict(self.big_printout_dict, self.big_data_dict)
            self.cleanup_filter_printout_table_dict(self.middle_printout_dict, self.middle_data_dict)
            self.cleanup_filter_printout_table_dict(self.small_printout_dict, self.small_data_dict)
            self.cleanup_filter_printout_table_dict(self.other_printout_dict, self.other_data_dict)

        self.print_generated_table(interval, self.non_filter_printout_dict, u'无过滤')
        self.print_generated_table(interval, self.big_printout_dict, u'大单')
        self.print_generated_table(interval, self.middle_printout_dict, u'中单')
        self.print_generated_table(interval, self.small_printout_dict, u'小单')
        self.print_generated_table(interval, self.other_printout_dict, u'其他')

    def print_generated_table(self, interval, printout_dict, string_name):
        printout_table = PrettyTable(border=self.border)
        printout_table.add_column(u'名称(' + string_name + ')', self.timestamp_list)
        for (k, v) in printout_dict.iteritems():
            printout_table.add_column(KEY_DICT[k], v)
        self.logger.info(u'%s分钟间隔合计数据为:\n %s', interval, printout_table)
        print(u'%s分钟间隔合计数据为:\n %s' % (interval, printout_table))

    def cleanup_non_filter_printout_table_dict(self, interval, non_filter_printout_dict, data_dict):
        updated_record_timestamp = self.first_record_timestamp + FENZHONG_1 * int(interval)
        validate_value_dict = deepcopy(data_dict)
        # 删除永远不为零的4个元素
        for key in NON_ZERO_LIST:
            validate_value_dict.pop(key)
        # if any(validate_value_dict.values()):
        start_time = time.strftime('%H:%M', time.localtime(self.first_record_timestamp))
        end_time = time.strftime('%H:%M', time.localtime(updated_record_timestamp))
        self.timestamp_list.append(u'%s-%s' % (start_time, end_time))
        for key in EMPTY__DATA_DICT.keys():
            non_filter_printout_dict[key].append(data_dict[key])
        else:
            pass
        self.first_record_timestamp = updated_record_timestamp

    def cleanup_filter_printout_table_dict(self, filter_printout_dict, data_dict):
        validate_value_dict = deepcopy(data_dict)
        # 删除永远不为零的4个元素
        for key in NON_ZERO_LIST:
            validate_value_dict.pop(key)
        # if any(validate_value_dict.values()):
        for key in EMPTY__DATA_DICT.keys():
            filter_printout_dict[key].append(data_dict[key])
        else:
            pass

    def pack_data_into_dict(self, each, data_dict, filter_range=None):
        min_range, max_range = filter_range if filter_range else (MIN, MAX)
        for key in EMPTY__DATA_DICT.keys():
            if key == 'ZUIG' or key == 'ZUID':
                data_dict[key].append(each['JIAG'])
            elif key == 'KPAN' or key == 'SPAN':
                pass
            else:
                if min_range < each[key] <= max_range:
                    data_dict[key] += each[key]
        return data_dict


def main(interval=None, name=None, border=False):
    redis_client = None  # RedisConnection()
    data_handler = DataHandler(name=name, border=border)
    data_handler.read_file(redis_client)
    data_handler.generate_dynamic_data()
    # data_handler.print_to_file()
    # data_handler.print_as_text()
    data_handler.read_all_sum()
    data_handler.read_interval_sum(interval)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--interval', default=30, help=u'间隔时间，时间单位为分钟。')
    parser.add_argument('-n', '--name', help=u'配置信息名称', required=True)
    parser.add_argument('--border', default=False, action='store_true', help=u'是否显示表格边框，默认为不显示。')
    args = parser.parse_args()

    main(interval=int(args.interval), name=args.name, border=args.border)
