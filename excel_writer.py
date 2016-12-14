#!/usr/bin/env python
# coding=utf-8
import sys

import xlsxwriter as xlsxwriter

from qihuo_fenxi.constants import ORG_KEY_CHN_TITLE_DICT, ORG_INTERVAL_DATA_EXCEL, ORG_ALL_SUM_EXCEL, ORG_ALL_DETAIL_EXCEL


class ExcelTableWriter(object):
    def __init__(self, excel_file_name, sheet_name, title_names, data_values):
        self.excel_file_handler = xlsxwriter.Workbook(excel_file_name)
        self.table_handler = self.excel_file_handler.add_worksheet(sheet_name)
        self.title_bold = self.excel_file_handler.add_format({
            'bold': True,
            'border': 2,
            'bg_color': 'green'
        })
        self.border = self.excel_file_handler.add_format({'border': 1})
        self.title_names = title_names
        self.data_values = data_values


class AllDetailExcelTableWriter(ExcelTableWriter):
    def __init__(self, title_names, data_values):
        ExcelTableWriter.__init__(self, ORG_ALL_DETAIL_EXCEL, ORG_ALL_DETAIL_EXCEL.split('.')[0], title_names, data_values)
        self.create_all_detail_excel_file()

    def create_all_detail_excel_file(self):
        for i, j in enumerate(self.title_names):
            self.table_handler.set_column(i, i, 8)
            self.table_handler.write_string(0, i, ORG_KEY_CHN_TITLE_DICT[j], self.title_bold)
        for (row_idx, value) in self.data_values.iteritems():
            column_idx = 0
            for (k, v) in value.iteritems():
                self.table_handler.write_string(row_idx + 1, column_idx, str(v))
                column_idx += 1
        self.excel_file_handler.close()


class AllSumExceTableWriter(ExcelTableWriter):
    def __init__(self, title_names, data_values):
        ExcelTableWriter.__init__(self, ORG_ALL_SUM_EXCEL, ORG_ALL_SUM_EXCEL.split('.')[0], title_names, data_values)
        self.create_all_sum_excel_file()

    def create_all_sum_excel_file(self):
        for i, j in enumerate(self.title_names):
            self.table_handler.set_column(i, i, 8)
            self.table_handler.write_string(0, i, ORG_KEY_CHN_TITLE_DICT[j], self.title_bold)
        column_idx = 0
        for (key, value) in self.data_values.iteritems():
            row_idx = 0
            self.table_handler.write_string(row_idx + 1, column_idx, str(value))
            column_idx += 1
        self.excel_file_handler.close()


class IntervalSumExceTableWriter(ExcelTableWriter):
    def __init__(self, table_name, title_names, data_values, date_str, time_str_list):
        ExcelTableWriter.__init__(self, ORG_INTERVAL_DATA_EXCEL, table_name, title_names, data_values)
        self.create_interval_sum_excel_file(date_str, time_str_list)

    def create_interval_sum_excel_file(self, date_str, time_str_list):
        self.table_handler.write_string(0, 0, u'日期', self.title_bold)
        self.table_handler.write_string(0, 1, u'时间', self.title_bold)
        for i, j in enumerate(self.title_names):
            self.table_handler.set_column(i, i, 8)
            self.table_handler.write_string(0, i + 2, ORG_KEY_CHN_TITLE_DICT[j], self.title_bold)
        column_idx = 0
        for (key, value) in self.data_values.iteritems():
            row_idx = 0
            for each in value:
                self.table_handler.write_string(row_idx + 1, 0, date_str)  # 日期
                self.table_handler.write_string(row_idx + 1, 1, time_str_list[row_idx])  # 时间
                self.table_handler.write_string(row_idx + 1, column_idx, str(each))
                row_idx += 1
            column_idx += 1
        self.excel_file_handler.close()
