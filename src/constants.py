#!/usr/bin/env python
# coding=utf-8
import logging
import logging.handlers

import sys

import time

IMPORT_LOG_FILE = u'导入日志.log'
EXPORT_LOG_FILE = u'导出日志.log'

OLD_LOG_FILE = u'旧版日志.log'

ORG_ALL_DETAIL_EXCEL = u'全部详细数据.xlsx'
ORG_ALL_SUM_EXCEL = u'全部汇总数据.xlsx'
ORG_INTERVAL_DATA_EXCEL = u'分段原始数据.xlsx'
HDL_INTERVAL_BIG_EXCEL = u'分段处理数据.xlsx'

FILE_NAME = 'baicha1.txt'
CONFIG_NAME = 'config.json'
TEXT_EXCEL_FILE_NAME = 'temp.file.txt'
INFLUX_DB_NAME = 'qihuo'

positive = '#66cc33'
negative = '#cc3333'
light_blue = '#3c78d8'
light_green = '#6aa84f'

FENZHONG_1 = 60
FENZHONG_5 = 5 * FENZHONG_1
FENZHONG_15 = 15 * FENZHONG_1
FENZHONG_30 = 30 * FENZHONG_1

BASIC_INTERVAL = 1

MAX = sys.maxint
MIN = 0

ORG_KEY_CHN_TITLE_DICT = {
    'CANGL': u'仓量', 'CJE': u'成交额', 'CJL': u'成交量', 'FANGX': u'方向', 'JIAG': u'价格', 'KAIC': u'开仓',
    'KDKD': u'开多开多', 'KDKK': u'开多开空', 'KDPD': u'开多平多', 'KKKD': u'开空开多', 'KKKK': u'开空开空',
    'KKPK': u'开空平空', 'PDKD': u'平多开多', 'PDPD': u'平多平多', 'PDPK': u'平多平空', 'PINGC': u'平仓',
    'PKKK': u'平空开空', 'PKPD': u'平空平多', 'PKPK': u'平空平空', 'SHIJ': u'时间', 'SHSP': u'上换手平',
    'SHSK': u'上换手开', 'WEIZ': u'交易位置', 'XHSP': u'下换手平', 'XHSK': u'下换手开', 'ZUIG': u'最高价',
    'ZUID': u'最低价', 'KPAN': u'开盘价', 'SPAN': u'收盘价'
}

HDL_KEY_CHN_TITLE_DICT = {
    'DKB': u'多空比'
}

ALL_KEY_CHN_TITLE_DICT = dict(ORG_KEY_CHN_TITLE_DICT.items() + HDL_KEY_CHN_TITLE_DICT.items())

ORG_EMPTY_DATA_DICT = {
    'KDKD': 0, 'KDKK': 0, 'KDPD': 0, 'PKPK': 0, 'PKKK': 0, 'PKPD': 0, 'PDPD': 0, 'PDKD': 0,
    'PDPK': 0, 'KKKK': 0, 'KKKD': 0, 'KKPK': 0, 'SHSP': 0, 'SHSK': 0, 'XHSP': 0, 'XHSK': 0,
    'KPAN': list(), 'SPAN': list(), 'ZUIG': list(), 'ZUID': list()
}

NON_ZERO_LIST = ['KPAN', 'SPAN', 'ZUIG', 'ZUID']

EMPTY_CLEAN_PRINTOUT_DICT = {
    'DCB': 0, 'KCB': 0
}

EMPTY_CLEAN_PRINTOUT_INTERVAL_DICT = {
    'DKB': 0
}


def get_import_log_handler():
    log_handler = logging.handlers.RotatingFileHandler(IMPORT_LOG_FILE, maxBytes=1024 * 1024)
    fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(message)s'
    formatter = logging.Formatter(fmt)
    log_handler.setFormatter(formatter)
    logger = logging.getLogger('log')
    logger.addHandler(log_handler)
    logger.setLevel(logging.DEBUG)
    return logger


def get_export_data_handler():
    log_handler = logging.handlers.RotatingFileHandler(EXPORT_LOG_FILE, maxBytes=1024 * 1024)
    fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(message)s'
    formatter = logging.Formatter(fmt)
    log_handler.setFormatter(formatter)
    logger = logging.getLogger('org')
    logger.addHandler(log_handler)
    logger.setLevel(logging.DEBUG)
    return logger


def get_old_logger_handler():
    log_handler = logging.handlers.RotatingFileHandler(OLD_LOG_FILE, maxBytes=1024 * 1024)
    fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(message)s'
    formatter = logging.Formatter(fmt)
    log_handler.setFormatter(formatter)
    logger = logging.getLogger('org')
    logger.addHandler(log_handler)
    logger.setLevel(logging.DEBUG)
    return logger


def init_interval_empty_dict(data_dict):
    data_dict['KDKD'] = 0
    data_dict['KDKK'] = 0
    data_dict['KDPD'] = 0
    data_dict['PDPD'] = 0
    data_dict['PDPK'] = 0
    data_dict['PDKD'] = 0
    data_dict['KKKK'] = 0
    data_dict['KKKD'] = 0
    data_dict['KKPK'] = 0
    data_dict['PKPK'] = 0
    data_dict['PKKK'] = 0
    data_dict['PKPD'] = 0
    data_dict['SHSK'] = 0
    data_dict['SHSP'] = 0
    data_dict['XHSK'] = 0
    data_dict['XHSP'] = 0
    data_dict['ZUIG'] = 0
    data_dict['ZUID'] = 0
    data_dict['KPAN'] = 0
    data_dict['SPAN'] = 0
    data_dict['ZUIG'] = 0
    data_dict['ZUID'] = 0
    return data_dict


def fill_order_org_empty_dict(data_dict):
    data_dict['KDKD'] = 0
    data_dict['KDKK'] = 0
    data_dict['KDPD'] = 0
    data_dict['PDPD'] = 0
    data_dict['PDPK'] = 0
    data_dict['PDKD'] = 0
    data_dict['KKKK'] = 0
    data_dict['KKKD'] = 0
    data_dict['KKPK'] = 0
    data_dict['PKPK'] = 0
    data_dict['PKKK'] = 0
    data_dict['PKPD'] = 0
    data_dict['SHSK'] = 0
    data_dict['SHSP'] = 0
    data_dict['XHSK'] = 0
    data_dict['XHSP'] = 0
    data_dict['ZUIG'] = 0
    data_dict['ZUID'] = 0
    data_dict['KPAN'] = list()
    data_dict['SPAN'] = list()
    data_dict['ZUIG'] = list()
    data_dict['ZUID'] = list()
    return data_dict


def fill_order_org_list_dict(data_dict):
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


def fill_order_hdl_empty_dict(data_dict):
    data_dict['DKB'] = 0
    return data_dict


def fill_order_hdl_list_dict(data_dict):
    data_dict['DKB'] = list()
    return data_dict


def reset_dict(dict_data):
    for (k, v) in dict_data.iteritems():
        if k == 'ZUIG' or k == 'ZUID' or k == 'KPAN' or k == 'SPAN':
            dict_data[k] = list()
        else:
            dict_data[k] = 0


def is_trade_time(trade_period, epoch_time=None, string_time=None):
    if epoch_time:
        date_time_string = time.strftime('%Y-%m-%d,%H:%M:%S', time.localtime(epoch_time))
    else:
        date_time_string = string_time
        epoch_time = time.mktime(time.strptime(date_time_string, '%Y-%m-%d,%H:%M:%S'))
    date_string = date_time_string.split(',')[0]

    morning_start_time = date_string + ',09:00:00'
    morning_end_time = date_string + ',11:30:00'
    afternoon_start_time = date_string + ',13:30:00'
    afternoon_end_time = date_string + ',15:30:00'
    night_start_time = date_string + ',19:00:00'
    night_end_time = date_string + ',21:00:00'

    in_morning = time.mktime(time.strptime(morning_start_time, '%Y-%m-%d,%H:%M:%S')) <= epoch_time <= time.mktime(time.strptime(morning_end_time, '%Y-%m-%d,%H:%M:%S'))
    in_afternoon = time.mktime(time.strptime(afternoon_start_time, '%Y-%m-%d,%H:%M:%S')) <= epoch_time <= time.mktime(time.strptime(afternoon_end_time, '%Y-%m-%d,%H:%M:%S'))
    in_night = time.mktime(time.strptime(night_start_time, '%Y-%m-%d,%H:%M:%S')) <= epoch_time <= time.mktime(time.strptime(night_end_time, '%Y-%m-%d,%H:%M:%S'))

    if trade_period == 'night':
        return in_night
    else:
        return in_morning or in_afternoon


def is_trade_end_time(epoch_time):
    date_time_string = time.strftime('%Y-%m-%d,%H:%M:%S', time.localtime(epoch_time))
    if date_time_string.split(',')[1] == '11:30:00' or date_time_string.split(',')[1] == '15:30:00' or date_time_string.split(',')[1] == '21:00:00':
        return True
    return False
