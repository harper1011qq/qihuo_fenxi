#!/usr/bin/env python
# coding=utf-8

import logging.handlers
import pprint
import re
import time
from collections import OrderedDict
from copy import deepcopy

KEY_READ_ERROR = 'Not exist'
LOG_FILE = 'qihuo.log'
FILE_NAME = 'baicha1.txt'

FENZHONG_1 = 60
FENZHONG_5 = 5 * FENZHONG_1
FENZHONG_15 = 15 * FENZHONG_1
FENZHONG_30 = 30 * FENZHONG_1

KEY_DICT = {'CANGL': u'仓量', 'CJE': u'成交额', 'CJL': u'成交量', 'FANGX': u'方向', 'JIAG': u'价格',
            'KAIC': u'开仓', 'KDKD': u'开多开多', 'KDKK': u'开多开空', 'KDPD': u'开多平多',
            'KKKD': u'开空开多', 'KKKK': u'开空开空', 'KKPK': u'开空平空', 'PDKD': u'平多开多',
            'PDPD': u'平多平多', 'PDPK': u'平多平空', 'PINGC': u'平仓', 'PKKK': u'平空开空',
            'PKPD': u'平空平多', 'PKPK': u'平空平空', 'SHIJ': u'时间', 'SHSD': u'上换手多',
            'SHSK': u'上换手空', 'WEIZ': u'交易位置', 'XHSD': u'下换手多', 'XHSK': u'下换手空'}

handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=1024 * 1024)
fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(message)s'
formatter = logging.Formatter(fmt)
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


class DataHandler(object):
    def __init__(self):
        self.logger = logger
        self.data_dict = OrderedDict()
        self.first_record_time_stamp = 0

    # 根据价格计算方向
    def generate_dynamic_data(self):
        start_time = time.time()
        reversed_keys = deepcopy(self.data_dict.keys())
        reversed_keys.reverse()
        earliest_record_index_number = len(reversed_keys) - 1  # 最早的那条记录的索引值
        # 设置最早的时间戳
        self.first_record_time_stamp = self.data_dict[earliest_record_index_number]['SHIJ']
        # 确定第一个交易位置值
        for each in reversed_keys:
            if each < earliest_record_index_number:
                if self.data_dict[each]['JIAG'] > self.data_dict[earliest_record_index_number]['JIAG']:
                    # 如果下一条记录的价格比最开始的那条记录的价格大的话. 最开始的那条记录赋值为-1
                    self.data_dict[earliest_record_index_number]['WEIZ'] = -1
                    break
                elif self.data_dict[each]['JIAG'] < self.data_dict[earliest_record_index_number]['JIAG']:
                    # 如果下一条记录的价格比最开始的那条记录的价格小的话. 最开始的那条记录赋值为1
                    self.data_dict[earliest_record_index_number]['WEIZ'] = 1
                    break
                else:
                    # 如果下一条记录的价格跟最开始的那条记录的价格一样的话. 忽略，进行下一条记录的比较
                    pass
        print(u'确定第一个交易位置值花费时间为：%s' % (time.time() - start_time))

        for each in reversed_keys:
            if self.data_dict[each]['WEIZ'] == 0:
                current_price = self.data_dict[each]['JIAG']
                previous_price = self.data_dict[each + 1]['JIAG']
                if int(current_price) - int(previous_price) > 0:
                    WEIZ = 1
                elif int(current_price) - int(previous_price) < 0:
                    WEIZ = -1
                else:
                    WEIZ = self.data_dict[each + 1]['WEIZ']
                self.data_dict[each]['WEIZ'] = WEIZ

                if WEIZ == 1:
                    if self.data_dict[each]['KAIC'] < self.data_dict[each]['PINGC']:
                        # 开仓小于平仓
                        self.data_dict[each]['PKPK'] = self.data_dict[each]['CJL'] / 2
                        self.data_dict[each]['PKKK'] = self.data_dict[each]['KAIC']
                        self.data_dict[each]['PKPD'] = self.data_dict[each]['CJL'] / 2 - self.data_dict[each]['KAIC']
                    elif self.data_dict[each]['KAIC'] > self.data_dict[each]['PINGC']:
                        # 开仓大于平仓
                        self.data_dict[each]['KDKD'] = self.data_dict[each]['CJL'] / 2
                        self.data_dict[each]['KDKK'] = self.data_dict[each]['CJL'] / 2 - self.data_dict[each]['PINGC']
                        self.data_dict[each]['KDPD'] = self.data_dict[each]['PINGC']
                    else:
                        # 开仓等于平仓
                        self.data_dict[each]['SHSD'] = self.data_dict[each]['KAIC']
                        self.data_dict[each]['SHSK'] = self.data_dict[each]['PINGC']
                elif WEIZ == -1:
                    if self.data_dict[each]['KAIC'] < self.data_dict[each]['PINGC']:
                        # 开仓小于平仓
                        self.data_dict[each]['PDPD'] = self.data_dict[each]['CJL'] / 2
                        self.data_dict[each]['PDKD'] = self.data_dict[each]['KAIC']
                        self.data_dict[each]['PDPK'] = self.data_dict[each]['CJL'] / 2 - self.data_dict[each]['KAIC']
                    elif self.data_dict[each]['KAIC'] > self.data_dict[each]['PINGC']:
                        # 开仓大于平仓
                        self.data_dict[each]['KKKK'] = self.data_dict[each]['CJL'] / 2
                        self.data_dict[each]['KKKD'] = self.data_dict[each]['CJL'] / 2 - self.data_dict[each]['PINGC']
                        self.data_dict[each]['KKPK'] = self.data_dict[each]['PINGC']
                    else:
                        # 开仓等于平仓
                        self.data_dict[each]['XHSD'] = self.data_dict[each]['PINGC']
                        self.data_dict[each]['XHSK'] = self.data_dict[each]['KAIC']
                else:
                    self.logger.error(u'有问题， 交易位置非1 或 -1')

        self.logger.debug('Time Consumption for generate_price_trade_position(): %s', time.time() - start_time)
        print(u'计算其他动态值所花费时间为：%s' % (time.time() - start_time))

    def read_file(self, redis_client):
        start_time = time.time()
        index = 0
        with open('%s' % FILE_NAME, 'r') as data_file:
            while True:
                line = data_file.readline()
                if line:
                    data_elements = re.split(' |\t', line)
                    record_time = (str(data_elements[1]) + ',' + str(data_elements[0]))
                    pattern = '%Y%m%d,%H:%M'
                    time_format = time.strptime(record_time, pattern)
                    epech_time = time.mktime(time_format)
                    self.data_dict[index] = {
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
                        'SHSD': 0,  # 上换手多
                        'SHSK': 0,  # 上换手空
                        'XHSD': 0,  # 下换手多
                        'XHSK': 0,  # 下换手空
                    }
                    # redis_client.write_data(str(index), json.dumps(data_dict, ensure_ascii=False))
                else:
                    print(u'读取文件完毕')
                    break
                index += 1

            print(u'读取文件花费时间为：%s' % (time.time() - start_time))
            self.logger.info(u'读取文件花费时间为：%s', time.time() - start_time)

    def print_to_file(self):
        self.logger.info('All data is:\n%s', pprint.pformat(dict(self.data_dict)))

    def print_as_text(self):
        reversed_keys = deepcopy(self.data_dict.keys())
        reversed_keys.reverse()
        with open('temp.file.txt', 'w') as text_file:
            for (k, v) in KEY_DICT.iteritems():
                text_file.write(k)
                text_file.write('\t')
            text_file.write('\n')
            for each in reversed_keys:
                for (k, v) in KEY_DICT.iteritems():
                    text_file.write(str(self.data_dict[each][k]))
                    text_file.write('\t')
                text_file.write('\n')
        print(u'打印到文本文件成功')

    def read_all_sum(self):
        all_sum_dict = {
            'ALL_KDKD': 0,
            'ALL_KDKK': 0,
            'ALL_KDPD': 0,
            'ALL_PKPK': 0,
            'ALL_PKKK': 0,
            'ALL_PKPD': 0,
            'ALL_PDPD': 0,
            'ALL_PDKD': 0,
            'ALL_PDPK': 0,
            'ALL_KKKK': 0,
            'ALL_KKKD': 0,
            'ALL_KKPK': 0,
            'ALL_SHSD': 0,
            'ALL_SHSK': 0,
            'ALL_XHSD': 0,
            'ALL_XHSK': 0
        }
        for each in self.data_dict.values():
            all_sum_dict['ALL_KDKD'] += each['KDKD']
            all_sum_dict['ALL_KDKK'] += each['KDKK']
            all_sum_dict['ALL_KDPD'] += each['KDPD']
            all_sum_dict['ALL_PKPK'] += each['PKPK']
            all_sum_dict['ALL_PKKK'] += each['PKKK']
            all_sum_dict['ALL_PKPD'] += each['PKPD']
            all_sum_dict['ALL_PDPD'] += each['PDPD']
            all_sum_dict['ALL_PDKD'] += each['PDKD']
            all_sum_dict['ALL_PDPK'] += each['PDPK']
            all_sum_dict['ALL_KKKK'] += each['KKKK']
            all_sum_dict['ALL_KKKD'] += each['KKKD']
            all_sum_dict['ALL_KKPK'] += each['KKPK']
            all_sum_dict['ALL_SHSD'] += each['SHSD']
            all_sum_dict['ALL_SHSK'] += each['SHSK']
            all_sum_dict['ALL_XHSD'] += each['XHSD']
            all_sum_dict['ALL_XHSK'] += each['XHSK']

        print('All SUM data are:\n %s' % pprint.pformat(all_sum_dict))
        self.logger.info('All SUM data are:\n %s', pprint.pformat(all_sum_dict))

    def read_interval_sum(self, interval):
        interval_sum_dict = {
            'INTERVAL_KDKD': 0,
            'INTERVAL_KDKK': 0,
            'INTERVAL_KDPD': 0,
            'INTERVAL_PKPK': 0,
            'INTERVAL_PKKK': 0,
            'INTERVAL_PKPD': 0,
            'INTERVAL_PDPD': 0,
            'INTERVAL_PDKD': 0,
            'INTERVAL_PDPK': 0,
            'INTERVAL_KKKK': 0,
            'INTERVAL_KKKD': 0,
            'INTERVAL_KKPK': 0,
            'INTERVAL_SHSD': 0,
            'INTERVAL_SHSK': 0,
            'INTERVAL_XHSD': 0,
            'INTERVAL_XHSK': 0
        }
        for each in self.data_dict.values():
            if each['SHIJ'] - self.first_record_time_stamp < FENZHONG_1 * int(interval):
                interval_sum_dict['INTERVAL_KDKD'] += each['KDKD']
                interval_sum_dict['INTERVAL_KDKK'] += each['KDKK']
                interval_sum_dict['INTERVAL_KDPD'] += each['KDPD']
                interval_sum_dict['INTERVAL_PKPK'] += each['PKPK']
                interval_sum_dict['INTERVAL_PKKK'] += each['PKKK']
                interval_sum_dict['INTERVAL_PKPD'] += each['PKPD']
                interval_sum_dict['INTERVAL_PDPD'] += each['PDPD']
                interval_sum_dict['INTERVAL_PDKD'] += each['PDKD']
                interval_sum_dict['INTERVAL_PDPK'] += each['PDPK']
                interval_sum_dict['INTERVAL_KKKK'] += each['KKKK']
                interval_sum_dict['INTERVAL_KKKD'] += each['KKKD']
                interval_sum_dict['INTERVAL_KKPK'] += each['KKPK']
                interval_sum_dict['INTERVAL_SHSD'] += each['SHSD']
                interval_sum_dict['INTERVAL_SHSK'] += each['SHSK']
                interval_sum_dict['INTERVAL_XHSD'] += each['XHSD']
                interval_sum_dict['INTERVAL_XHSK'] += each['XHSK']
        print(u'%s 分钟合计数据为:\n %s' % (interval, pprint.pformat(interval_sum_dict)))
        self.logger.info(u'%s 分钟合计数据为::\n %s', interval, pprint.pformat(interval_sum_dict))


def main():
    redis_client = None  # RedisConnection()
    data_handler = DataHandler()
    data_handler.read_file(redis_client)
    data_handler.generate_dynamic_data()
    # data_handler.print_to_file()
    data_handler.print_as_text()
    data_handler.read_all_sum()
    data_handler.read_interval_sum(30)
    data_handler.read_interval_sum(15)
    data_handler.read_interval_sum(5)
    data_handler.read_interval_sum(1)


if __name__ == '__main__':
    main()
