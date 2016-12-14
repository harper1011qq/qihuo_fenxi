#!/usr/bin/env python
# coding=utf-8
import logging
import logging.handlers

import sys

LOG_FILE = u'日志文件.log'
ORG_LOG_FILE = u'原始.log'
HDL_LOG_FILE = u'处理.log'

ORG_ALL_DETAIL_EXCEL = u'全部详细数据.xlsx'
ORG_ALL_SUM_EXCEL = u'全部汇总数据.xlsx'
ORG_INTERVAL_DATA_EXCEL = u'分段原始数据.xlsx'
HDL_INTERVAL_BIG_EXCEL = u'分段处理数据.xlsx'

FILE_NAME = 'baicha1.txt'
CONFIG_NAME = 'config.json'
TEXT_EXCEL_FILE_NAME = 'temp.file.txt'
INFLUX_DB_NAME = 'qihuo'

FENZHONG_1 = 60
FENZHONG_5 = 5 * FENZHONG_1
FENZHONG_15 = 15 * FENZHONG_1
FENZHONG_30 = 30 * FENZHONG_1

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


def get_log_handler():
    log_handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=1024 * 1024)
    fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(message)s'
    formatter = logging.Formatter(fmt)
    log_handler.setFormatter(formatter)
    logger = logging.getLogger('log')
    logger.addHandler(log_handler)
    logger.setLevel(logging.DEBUG)
    return logger


def get_org_data_handler():
    log_handler = logging.handlers.RotatingFileHandler(ORG_LOG_FILE, maxBytes=1024 * 1024)
    fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(message)s'
    formatter = logging.Formatter(fmt)
    log_handler.setFormatter(formatter)
    logger = logging.getLogger('org')
    logger.addHandler(log_handler)
    logger.setLevel(logging.DEBUG)
    return logger


def get_hdl_data_handler():
    log_handler = logging.handlers.RotatingFileHandler(HDL_LOG_FILE, maxBytes=1024 * 1024)
    fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(message)s'
    formatter = logging.Formatter(fmt)
    log_handler.setFormatter(formatter)
    logger = logging.getLogger('hdl')
    logger.addHandler(log_handler)
    logger.setLevel(logging.DEBUG)
    return logger
