#!/usr/bin/env python
# coding=utf-8
import json
import math
import platform
import pprint
import time
from collections import OrderedDict
from copy import deepcopy

import argparse
import requests
from prettytable import PrettyTable

from constants import CONFIG_NAME, MIN, MAX, ALL_KEY_CHN_TITLE_DICT, FENZHONG_1, \
    get_export_data_handler, ORG_INTERVAL_DATA_EXCEL, HDL_INTERVAL_BIG_EXCEL, fill_order_org_empty_dict, fill_order_org_list_dict, \
    fill_order_hdl_empty_dict, fill_order_hdl_list_dict, reset_dict, is_trade_time, is_trade_end_time, ORG_KEY_LIST
from excel_writer import IntervalSumExceTableWriter, IntervalHandledSumExceTableWriter


class DataHandler(object):
    def __init__(self, logger=None, name=None, border=False, platform='linux', start=None, end=None):
        self.platform = platform.lower()
        self.start_time = time.mktime(time.strptime(start, '%Y-%m-%dT%H:%MZ')) if start else None
        self.end_time = time.mktime(time.strptime(end, '%Y-%m-%dT%H:%MZ')) if end else None
        self.trade_period = 'day'
        self.export_logger = logger if logger else get_export_data_handler()
        self.border = border
        self.datadict = OrderedDict()
        self.static_first_record_timestamp = 0
        self.first_record_timestamp = 0
        self.last_record_timestamp = 0
        self.endpoint_url = 'http://localhost:8086/query' if platform.lower() == 'linux' else 'http://localhost:4086/query'
        self.config_name = name
        self.cfg_file = self.read_config_file()
        self.config_data = self.cfg_file[name]
        self.date_list = list()
        self.time_list = list()

        self.non_filter_org_dict = deepcopy(fill_order_org_empty_dict(OrderedDict()))
        self.big_org_dict = deepcopy(fill_order_org_empty_dict(OrderedDict()))
        self.middle_org_dict = deepcopy(fill_order_org_empty_dict(OrderedDict()))
        self.small_org_dict = deepcopy(fill_order_org_empty_dict(OrderedDict()))

        self.non_filter_org_print_dict = deepcopy(fill_order_org_list_dict(OrderedDict()))
        self.big_org_printout_dict = deepcopy(fill_order_org_list_dict(OrderedDict()))
        self.middle_org_printout_dict = deepcopy(fill_order_org_list_dict(OrderedDict()))
        self.small_org_printout_dict = deepcopy(fill_order_org_list_dict(OrderedDict()))

        self.non_filter_hdl_data = deepcopy(fill_order_hdl_empty_dict(OrderedDict()))
        self.big_data_hdl_dict = deepcopy(fill_order_hdl_empty_dict(OrderedDict()))
        self.middle_data_hdl_dict = deepcopy(fill_order_hdl_empty_dict(OrderedDict()))
        self.small_data_hdl_dict = deepcopy(fill_order_hdl_empty_dict(OrderedDict()))

        self.non_filter_hdl_printout_dict = deepcopy(fill_order_hdl_list_dict(OrderedDict()))
        self.big_hdl_printout_dict = deepcopy(fill_order_hdl_list_dict(OrderedDict()))
        self.middle_hdl_printout_dict = deepcopy(fill_order_hdl_list_dict(OrderedDict()))
        self.small_hdl_printout_dict = deepcopy(fill_order_hdl_list_dict(OrderedDict()))

    def read_config_file(self):
        with open(CONFIG_NAME) as cfg_file:
            file_data = json.load(cfg_file, encoding='utf-8')
        cfg_print_tbl = PrettyTable(['变量名', '数值'])
        for (k, v) in file_data[self.config_name].iteritems():
            cfg_print_tbl.add_row([k, v])
        self.export_logger.debug(u'配置文件内容为：\n%s', cfg_print_tbl)
        print(u'配置文件内容为：\n%s' % cfg_print_tbl)
        return file_data

    def print_interval_sum_tbls(self, interval):
        non_filter_duo_sum = 0
        non_filter_kong_sum = 0
        big_duo_sum = 0
        big_kong_sum = 0
        middle_duo_sum = 0
        middle_kong_sum = 0
        small_duo_sum = 0
        small_kong_sum = 0

        last_record_timestamp = self.last_record_timestamp if not self.end_time else min(self.end_time, self.last_record_timestamp)
        first_record_timestamp = self.first_record_timestamp if not self.start_time else max(self.start_time, self.first_record_timestamp)
        number_of_interval = int(math.ceil((last_record_timestamp - first_record_timestamp) / 60 / interval))
        self.export_logger.debug(u'对\"%s\"的所有数据(数据源:%s)进行按照%s分钟的间隔，共被分割为%s段的数据',
                                 self.cfg_file[self.config_name]['chinese'],
                                 self.cfg_file[self.config_name]['filename'],
                                 interval, number_of_interval)
        print(u'对\"%s\"的所有数据(数据源:%s)进行按照%s分钟的间隔，共被分割为%s段的数据'
              % (self.cfg_file[self.config_name]['chinese'],
                 self.cfg_file[self.config_name]['filename'],
                 interval, number_of_interval))

        start_time_1 = time.time()
        for each_loop in range(0, number_of_interval):
            reset_dict(self.non_filter_org_dict)
            reset_dict(self.big_org_dict)
            reset_dict(self.middle_org_dict)
            reset_dict(self.small_org_dict)
            reset_dict(self.non_filter_hdl_data)
            reset_dict(self.big_data_hdl_dict)
            reset_dict(self.middle_data_hdl_dict)
            reset_dict(self.small_data_hdl_dict)

            start_time_diff = FENZHONG_1 * int(interval) * each_loop
            end_time_diff = FENZHONG_1 * int(interval) * (each_loop + 1)
            loop_dict_values = deepcopy(self.datadict.values())
            for each_value in loop_dict_values:
                loop_time_diff = each_value['time'] - self.static_first_record_timestamp
                # self.export_logger.error('%s, %s, %s, %s, %s', start_time_diff, loop_time_diff, end_time_diff, each_value['time'], self.static_first_record_timestamp)
                if is_trade_end_time(each_value['time']):  # 将每段交易结束时间的数据合并到上一段的数据里面
                    in_this_interval = start_time_diff <= loop_time_diff <= end_time_diff
                else:
                    in_this_interval = start_time_diff <= loop_time_diff < end_time_diff
                is_start_time = each_value['time'] >= self.start_time if self.start_time else True
                is_end_time = each_value['time'] <= self.end_time if self.end_time else True

                if is_start_time and is_end_time and in_this_interval and is_trade_time(self.trade_period, epoch_time=each_value['time']):
                    # 无过滤条件
                    self.pack_data_into_dict(each_value, self.non_filter_org_dict, filter_range=(MIN, MAX))
                    # 大单
                    self.pack_data_into_dict(each_value, self.big_org_dict, filter_range=(self.cfg_file[self.config_name]['big'], MAX))
                    # 小单
                    self.pack_data_into_dict(each_value, self.middle_org_dict, filter_range=(self.cfg_file[self.config_name]['small'], self.cfg_file[self.config_name]['big']))
                    # 其他
                    self.pack_data_into_dict(each_value, self.small_org_dict, filter_range=(MIN, self.cfg_file[self.config_name]['small']))
                elif loop_time_diff > end_time_diff:
                    # 如果当前时间超出最大时间范围则直接跳出。因为每条的时间戳都线性递增的。所以没有必要继续后面的
                    break

            # ---------------------------------------------- 原始数据处理开始 ----------------------------------------------
            self.non_filter_org_dict['ZUIG'] = max(self.non_filter_org_dict['ZUIG']) if self.non_filter_org_dict['ZUIG'] else 0
            self.non_filter_org_dict['ZUID'] = min(self.non_filter_org_dict['ZUID']) if self.non_filter_org_dict['ZUID'] else 0
            self.non_filter_org_dict['KPAN'] = self.non_filter_org_dict['KPAN'][0] if self.non_filter_org_dict['KPAN'] else 0
            self.non_filter_org_dict['SPAN'] = self.non_filter_org_dict['SPAN'][len(self.non_filter_org_dict['SPAN']) - 1] if self.non_filter_org_dict['SPAN'] else 0

            self.big_org_dict['ZUIG'] = max(self.big_org_dict['ZUIG']) if self.big_org_dict['ZUIG'] else 0
            self.big_org_dict['ZUID'] = min(self.big_org_dict['ZUID']) if self.big_org_dict['ZUID'] else 0
            self.big_org_dict['KPAN'] = self.big_org_dict['KPAN'][0] if self.big_org_dict['KPAN'] else 0
            self.big_org_dict['SPAN'] = self.big_org_dict['SPAN'][len(self.big_org_dict['SPAN']) - 1] if self.big_org_dict['SPAN'] else 0

            self.middle_org_dict['ZUIG'] = max(self.middle_org_dict['ZUIG']) if self.middle_org_dict['ZUIG'] else 0
            self.middle_org_dict['ZUID'] = min(self.middle_org_dict['ZUID']) if self.middle_org_dict['ZUID'] else 0
            self.middle_org_dict['KPAN'] = self.middle_org_dict['KPAN'][0] if self.middle_org_dict['KPAN'] else 0
            self.middle_org_dict['SPAN'] = self.middle_org_dict['SPAN'][len(self.middle_org_dict['SPAN']) - 1] if self.middle_org_dict['SPAN'] else 0

            self.small_org_dict['ZUIG'] = max(self.small_org_dict['ZUIG']) if self.small_org_dict['ZUIG'] else 0
            self.small_org_dict['ZUID'] = min(self.small_org_dict['ZUID']) if self.small_org_dict['ZUID'] else 0
            self.small_org_dict['KPAN'] = self.small_org_dict['KPAN'][0] if self.small_org_dict['KPAN'] else 0
            self.small_org_dict['SPAN'] = self.small_org_dict['SPAN'][len(self.small_org_dict['SPAN']) - 1] if self.small_org_dict['SPAN'] else 0

            self.org_non_filter_printout_table_dict(interval, self.non_filter_org_print_dict, self.non_filter_org_dict)
            self.org_filter_printout_table_dict(self.big_org_printout_dict, self.big_org_dict)
            self.org_filter_printout_table_dict(self.middle_org_printout_dict, self.middle_org_dict)
            self.org_filter_printout_table_dict(self.small_org_printout_dict, self.small_org_dict)
            # ---------------------------------------------- 原始数据处理完毕 ----------------------------------------------

            # ---------------------------------------------- '处理后'数据处理开始 ----------------------------------------------
            non_filter_duo = self.non_filter_org_dict['KDKD'] + self.non_filter_org_dict['SHSK'] + self.non_filter_org_dict['PKPK']
            non_filter_kong = self.non_filter_org_dict['KKKK'] + self.non_filter_org_dict['XHSK'] + self.non_filter_org_dict['PDPD']
            non_filter_duo_kong_bi = float(non_filter_duo) / non_filter_kong if non_filter_kong != 0 else 0
            non_filter_duo_sum += non_filter_duo
            non_filter_kong_sum += non_filter_kong
            epoch_to_string_time = time.strftime('%Y-%m-%d,%H:%M:%S', time.localtime(self.first_record_timestamp))
            is_start_time = self.first_record_timestamp >= self.start_time if self.start_time else True
            is_end_time = self.first_record_timestamp <= self.end_time if self.end_time else True

            if is_start_time and is_end_time and is_trade_time(self.trade_period, string_time=epoch_to_string_time):
                self.non_filter_hdl_printout_dict['DKB'].append(round(non_filter_duo_kong_bi, 3))

            big_duo = self.big_org_dict['KDKD'] + self.big_org_dict['SHSK'] + self.big_org_dict['PKPK']
            big_kong = self.big_org_dict['KKKK'] + self.big_org_dict['XHSK'] + self.big_org_dict['PDPD']
            big_duo_kong_bi = float(big_duo) / big_kong if big_kong != 0 else 0
            big_duo_sum += big_duo
            big_kong_sum += big_kong
            if is_start_time and is_end_time and is_trade_time(self.trade_period, string_time=epoch_to_string_time):
                self.big_hdl_printout_dict['DKB'].append(round(big_duo_kong_bi, 3))

            middle_duo = self.middle_org_dict['KDKD'] + self.middle_org_dict['SHSK'] + self.middle_org_dict['PKPK']
            middle_kong = self.middle_org_dict['KKKK'] + self.middle_org_dict['XHSK'] + self.middle_org_dict['PDPD']
            middle_duo_kong_bi = float(middle_duo) / middle_kong if middle_kong != 0 else 0
            middle_duo_sum += middle_duo
            middle_kong_sum += middle_kong
            if is_start_time and is_end_time and is_trade_time(self.trade_period, string_time=epoch_to_string_time):
                self.middle_hdl_printout_dict['DKB'].append(round(middle_duo_kong_bi, 3))

            small_duo = self.small_org_dict['KDKD'] + self.small_org_dict['SHSK'] + self.small_org_dict['PKPK']
            small_kong = self.small_org_dict['KKKK'] + self.small_org_dict['XHSK'] + self.small_org_dict['PDPD']
            small_duo_kong_bi = float(small_duo) / small_kong if small_kong != 0 else 0
            small_duo_sum += small_duo
            small_kong_sum += small_kong
            if is_start_time and is_end_time and is_trade_time(self.trade_period, string_time=epoch_to_string_time):
                self.small_hdl_printout_dict['DKB'].append(round(small_duo_kong_bi, 3))

            # 在循环内最后一步更新first_record_timestamp 时间戳
            self.first_record_timestamp += FENZHONG_1 * int(interval)
        print(u'所有数据循环读取完毕，耗时: %s' % (time.time() - start_time_1))
        start_time_4 = time.time()
        # 打印原始数据表格
        self.print_generated_table(interval, self.non_filter_org_print_dict, u'无过滤', self.export_logger)
        self.print_generated_table(interval, self.big_org_printout_dict, u'大单', self.export_logger)
        self.print_generated_table(interval, self.middle_org_printout_dict, u'小单', self.export_logger)
        self.print_generated_table(interval, self.small_org_printout_dict, u'其他', self.export_logger)
        print(u'打印原始数据表格完毕。 耗时:%s' % (time.time() - start_time_4))
        start_time_5 = time.time()
        # 打印处理过的数据表格
        self.print_generated_table(interval, self.non_filter_hdl_printout_dict, u'无过滤', self.export_logger)
        self.print_generated_table(interval, self.big_hdl_printout_dict, u'大单', self.export_logger)
        self.print_generated_table(interval, self.middle_hdl_printout_dict, u'小单', self.export_logger)
        self.print_generated_table(interval, self.small_hdl_printout_dict, u'其他', self.export_logger)
        print(u'打印原始数据表格完毕。 耗时:%s' % (time.time() - start_time_5))
        start_time_6 = time.time()
        # 打印原始数据到Excel文件
        IntervalSumExceTableWriter(ORG_INTERVAL_DATA_EXCEL,
                                   self.non_filter_org_print_dict,
                                   self.big_org_printout_dict,
                                   self.middle_org_printout_dict,
                                   self.small_org_printout_dict,
                                   self.date_list, self.time_list)
        print(u'打印原始数据到Excel文件。 耗时:%s' % (time.time() - start_time_6))
        start_time_7 = time.time()
        # 打印处理过的数据到Excel文件
        IntervalHandledSumExceTableWriter(HDL_INTERVAL_BIG_EXCEL,
                                          self.non_filter_hdl_printout_dict,
                                          self.big_hdl_printout_dict,
                                          self.middle_hdl_printout_dict,
                                          self.small_hdl_printout_dict,
                                          (float(non_filter_duo_sum) / non_filter_kong_sum) if non_filter_kong_sum else 0,
                                          (float(big_duo_sum) / big_kong_sum) if big_kong_sum else 0,
                                          (float(middle_duo_sum) / middle_kong_sum) if middle_kong_sum else 0,
                                          (float(small_duo_sum) / small_kong_sum) if small_kong_sum else 0,
                                          self.date_list, self.time_list)
        print(u'打印处理过的数据到Excel文件。 耗时:%s' % (time.time() - start_time_7))

    def print_generated_table(self, interval, printout_dict, string_name, logger):
        printout_table = PrettyTable(border=self.border)
        printout_table.add_column(u'日期(' + string_name + ')', self.date_list)
        printout_table.add_column(u'时间', self.time_list)
        for (k, v) in printout_dict.iteritems():
            printout_table.add_column(ALL_KEY_CHN_TITLE_DICT[k], v)
        logger.info(u'%s分钟间隔\'%s\'合计数据为:\n %s', interval, string_name, printout_table)
        print(u'%s分钟间隔\'%s\'合计数据为:\n %s' % (interval, string_name, printout_table))

    def org_non_filter_printout_table_dict(self, interval, non_filter_printout_dict, data_dict):
        updated_record_timestamp = self.first_record_timestamp + FENZHONG_1 * int(interval)
        date_string = time.strftime('%Y-%m-%d', time.localtime(self.first_record_timestamp))
        end_time = time.strftime('%H:%M', time.localtime(updated_record_timestamp))
        epoch_to_string_time = time.strftime('%Y-%m-%d,%H:%M:%S', time.localtime(self.first_record_timestamp))

        is_start_time = self.first_record_timestamp >= self.start_time if self.start_time else True
        is_end_time = self.first_record_timestamp <= self.end_time if self.end_time else True

        if is_start_time and is_end_time and is_trade_time(self.trade_period, string_time=epoch_to_string_time):
            self.date_list.append(u'%s' % date_string)
            self.time_list.append(u'%s' % end_time)
            for key in ORG_KEY_LIST:
                non_filter_printout_dict[key].append(data_dict[key])

    def org_filter_printout_table_dict(self, filter_printout_dict, data_dict):
        epoch_to_string_time = time.strftime('%Y-%m-%d,%H:%M:%S', time.localtime(self.first_record_timestamp))
        is_start_time = self.first_record_timestamp >= self.start_time if self.start_time else True
        is_end_time = self.first_record_timestamp <= self.end_time if self.end_time else True
        if is_start_time and is_end_time and is_trade_time(self.trade_period, string_time=epoch_to_string_time):
            for key in ORG_KEY_LIST:
                filter_printout_dict[key].append(data_dict[key])

    @staticmethod
    def pack_data_into_dict(each, data_dict, filter_range=None):
        min_range, max_range = filter_range if filter_range else (MIN, MAX)
        for key in ORG_KEY_LIST:
            if key == 'ZUIG' or key == 'ZUID' or key == 'KPAN' or key == 'SPAN':
                data_dict[key].append(each[key])
            else:
                if min_range <= each[key] < max_range:
                    data_dict[key] += each[key]
        return data_dict

    def load_dynamic_data_from_influxdb(self):
        start_time = time.time()
        query_cmd = 'select * from %s' % self.config_name
        query_params = {
            'q': query_cmd,
            'db': self.config_name,
            'epoch': 's'
        }
        self.export_logger.debug(u'InfluxDB查询信息: URL=>%s 查询参数=>%s', self.endpoint_url, query_params)
        r = requests.get(self.endpoint_url, params=query_params, timeout=5)
        self.export_logger.debug(u'InfluxDB查询结果为%s, 所用时间为:%s，', u'成功' if r.status_code == 200 else u'失败', time.time() - start_time)
        # self.export_logger.debug(u"InfluxDB查询返回数据为:\n%s", pprint.pformat(r.json()['results'][0]['series'][0]))
        if r.status_code == 200:
            key_list = r.json()['results'][0]['series'][0]['columns']
            value_lists = r.json()['results'][0]['series'][0]['values']
            for value_list in value_lists:
                # value_list[0] is the timestamp of each value list. It is an unique value so can be used as a dict key
                self.datadict[value_list[0]] = dict(zip(key_list, value_list))

            self.first_record_timestamp = deepcopy(value_lists[0][0])
            self.last_record_timestamp = deepcopy(value_lists[-1][0])
            self.static_first_record_timestamp = deepcopy(value_lists[0][0])
            self.export_logger.debug(u"静态起始时间为: %s. 动态起始时间戳为:%s. 结束时间戳为: %s",
                                     self.static_first_record_timestamp, self.first_record_timestamp, self.last_record_timestamp)
            # self.export_logger.debug(u"生成的字典数据为:\n%s", pprint.pformat(dict(self.datadict)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--interval', default=30, type=int, help=u'间隔时间，时间单位为分钟。')
    parser.add_argument('-n', '--name', help=u'配置信息名称', required=True)
    parser.add_argument('--border', default=False, action='store_true', help=u'是否显示表格边框，默认为不显示。')
    parser.add_argument('--start', help=u'起始时间。')
    parser.add_argument('--end', help=u'结束时间。')
    args = parser.parse_args()

    logger = get_export_data_handler()

    try:
        data_handler = DataHandler(logger=logger, name=args.name, border=True, platform=platform.system(), start=args.start, end=args.end)
        data_handler.load_dynamic_data_from_influxdb()
        data_handler.print_interval_sum_tbls(args.interval)
    except Exception as e:
        print(u'运行错误.错误信息为: %s' % str(e))
        logger.exception(u'运行错误.错误信息为: %s', str(e))
