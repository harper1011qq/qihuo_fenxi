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
KEY_LIST = ['CANGL', 'CJE', 'CJL', 'FANGX', 'JIAG', 'KAIC', 'KDKD', 'KDKK', 'KDPD', 'KKKD', 'KKKK', 'KKPK', 'PDKD',
            'PDPD', 'PDPK', 'PINGC', 'PKKK', 'PKPD', 'PKPK', 'SHIJ', 'SHSD', 'SHSK', 'WEIZ', 'XHSD', 'XHSK']

handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=1024 * 1024)  # 实例化handler
fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
formatter = logging.Formatter(fmt)  # 实例化formatter
handler.setFormatter(formatter)  # 为handler添加formatter
logger = logging.getLogger()  # 获取名为tst的logger
logger.addHandler(handler)  # 为logger添加handler
logger.setLevel(logging.DEBUG)


class DataHandler(object):
    def __init__(self):
        self.logger = logger
        self.data_dict = OrderedDict()
    
    # 根据价格计算方向
    def generate_price_trade_position(self):
        start_time = time.time()
        reversed_keys = deepcopy(self.data_dict.keys())
        reversed_keys.reverse()
        # 确定第一个交易位置值
        earliest_record_index_number = len(reversed_keys) - 1  # 最早的那条记录的索引值
        for each in reversed_keys:
            if each < earliest_record_index_number:
                if self.data_dict[each]['JIAG'] > self.data_dict[0]['JIAG']:
                    # 如果下一条记录的价格比最开始的那条记录的价格大的话. 最开始的那条记录赋值为-1
                    self.data_dict[earliest_record_index_number]['WEIZ'] = -1
                    break
                elif self.data_dict[each]['JIAG'] < self.data_dict[0]['JIAG']:
                    # 如果下一条记录的价格比最开始的那条记录的价格小的话. 最开始的那条记录赋值为1
                    self.data_dict[earliest_record_index_number]['WEIZ'] = 1
                    break
                else:
                    # 如果下一条记录的价格跟最开始的那条记录的价格一样的话. 忽略，进行下一条记录的比较
                    pass
        print('1st time Consumption: %s', time.time() - start_time)
        
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
                        self.data_dict[each]['PKPK'] = self.data_dict[each]['KAIC']
                        self.data_dict[each]['PKKK'] = self.data_dict[each]['KAIC']
                        self.data_dict[each]['PKPD'] = self.data_dict[each]['PINGC']
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
        print('2nd time Consumption: %s', time.time() - start_time)
    
    def read_file(self, redis_client):
        start_time = time.time()
        index = 0
        with open('%s' % FILE_NAME, 'r') as data_file:
            while True:
                line = data_file.readline()
                if line:
                    data_elements = re.split(' |\t', line)
                    self.data_dict[index] = {
                        'SHIJ': (str(data_elements[1]) + ',' + str(data_elements[0])),  # 时间
                        'JIAG': int(data_elements[2]) if data_elements[2] else 0,  # 价格
                        'CJL': int(data_elements[3]) if data_elements[3] else 0,  # 成交量
                        'CJE': int(data_elements[4]) if data_elements[4] else 0,  # 成交额
                        'CANGL': int(data_elements[5]) if data_elements[5] else 0,  # 仓量
                        'KAIC': int(data_elements[9]) if data_elements[9] else 0,  # 开仓
                        'PINGC': int(data_elements[10] if data_elements[10] else 0),  # 平仓
                        'FANGX': data_elements[11] if data_elements[11] else 0,  # 方向
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
                    print('End of File')
                    break
                index += 1
            
            print('Time Consumption is %s', time.time() - start_time)
            self.logger.info('Time Consumption is %s', time.time() - start_time)
    
    def print_to_file(self):
        self.logger.info('All data is:\n%s', pprint.pformat(dict(self.data_dict)))
    
    def print_as_text(self):
        reversed_keys = deepcopy(self.data_dict.keys())
        reversed_keys.reverse()
        with open('temp.file.txt', 'w') as text_file:
            text_file.write('\n')
            for each in reversed_keys:
                text_file.write(self.data_dict[each])
                text_file.write('\t'.join([str(x) for x in self.data_dict[each].values()]))
                text_file.write('\n')
        print('Done')


def main():
    redis_client = None  # RedisConnection()
    data_handler = DataHandler()
    data_handler.read_file(redis_client)
    data_handler.generate_price_trade_position()
    # data_handler.print_to_file()
    data_handler.print_as_text()


if __name__ == '__main__':
    main()
