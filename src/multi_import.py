#!/usr/bin/env python
# coding=utf-8
import fnmatch
import json
import os
import subprocess

import argparse
import time
from prettytable import PrettyTable

from constants import CONFIG_NAME, get_import_log_handler, SPLIT_LINE_NUMBER, IMPORT_LOG_FILE


class FileHandler(object):
    def __init__(self, name=None, core=None):
        self.log_logger = get_import_log_handler()
        self.folder_path = os.path.abspath(os.path.dirname(__file__))
        self.config_name = name
        self.cfg_file = self.read_config_file()
        self.config_data = self.cfg_file[name]
        self.start_time = time.time()

    def read_config_file(self):
        with open(os.path.join(self.folder_path, CONFIG_NAME)) as cfg_file:
            file_data = json.load(cfg_file, encoding='utf-8')
        cfg_print_tbl = PrettyTable(['变量名', '数值'])
        for (k, v) in file_data[self.config_name].iteritems():
            cfg_print_tbl.add_row([k, v])
        print(u'配置文件内容为：\n%s' % cfg_print_tbl)
        self.log_logger.debug(u'配置文件内容为：\n%s', cfg_print_tbl)
        return file_data

    def split_file(self):
        # target_file = os.path.join(self.folder_path, self.cfg_file[self.config_name]['filename'])
        target_file = self.cfg_file[self.config_name]['filename']
        split_command = 'split -l %s --additional-suffix _%s_split %s' % (SPLIT_LINE_NUMBER, self.config_name, target_file)
        print(u'分割文件所用命令为: %s' % split_command)
        self.log_logger.debug(u'分割文件所用命令为: %s', split_command)
        split_result = subprocess.check_output(split_command, shell=True, stderr=subprocess.STDOUT)
        print(u'分割文件所结果为: %s' % split_result)
        self.log_logger.debug(u'分割文件所结果为: %s', split_result)

        split_file_string = '*_%s_split' % self.config_name
        splited_files = [f for f in os.listdir(self.folder_path) if fnmatch.fnmatch(f, split_file_string)]
        # import_file = os.path.join(self.folder_path, 'import.py')
        import_file = 'import.py'
        process_command_list = list()
        for each_file in splited_files:
            process_cmd = 'python %s -f %s -n %s' % (import_file, each_file, self.config_name)
            print(u'处理分割文件: %s, 命令为: %s' % (each_file, process_cmd))
            self.log_logger.info(u'处理分割文件: %s, 命令为: %s', each_file, process_cmd)
            process_command_list.append(process_cmd)
            # os.remove(each_file_abs_path)
        final_process_command = ' | '.join(process_command_list)
        # print(u'汇总处理命令为: %s' % final_process_command)
        self.log_logger.info(u'汇总处理命令为: %s', final_process_command)

        print(u'启用%s个进程处理数据中..... 大约需要一小时左右..... 详细信息请打开日志文件: %s' % (len(splited_files), IMPORT_LOG_FILE))
        self.log_logger.info(u'启用%s个进程处理数据中..... 大约需要一小时左右..... 详细信息请打开日志文件: %s', len(splited_files), IMPORT_LOG_FILE)
        process_result = subprocess.check_output(final_process_command, shell=True, stderr=subprocess.STDOUT)
        print(u'处理分割文件所结果为: %s' % (process_result))
        self.log_logger.debug(u'处理分割文件所结果为: %s ', process_result)

        for each_file in splited_files:
            each_file_abs_path = os.path.join(self.folder_path, each_file)
            os.remove(each_file_abs_path)
            print(u'删除临时分割文件: %s' % each_file_abs_path)
            self.log_logger.info(u'删除临时分割文件: %s', each_file_abs_path)
            
        print(u'脚本执行完毕.总耗时: %s' % time.time() - self.start_time)
        self.log_logger.info(u'脚本执行完毕.总耗时: %s', time.time() - self.start_time)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--name', help=u'配置信息名称', required=True)
    parser.add_argument('-c', '--core', help=u'CPU内核数')
    args = parser.parse_args()

    file_handler = FileHandler(name=args.name, core=args.core)
    file_handler.split_file()
