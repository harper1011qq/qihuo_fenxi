#!/usr/bin/env python
# coding=utf-8
import xlsxwriter as xlsxwriter


class ExcelTableWriter(object):
    def __init__(self, excel_file_name, sheet_name, title_names, data_values):
        self.excel_file_handler = xlsxwriter.Workbook(excel_file_name)
        self.table_handler = self.excel_file_handler.add_worksheet(sheet_name)
        self.title_bold = self.excel_file_handler.add_format({
        #    'bold': True,
        #    'border': 2,
        #    'bg_color': 'blue'
        })
        self.border = self.excel_file_handler.add_format({'border': 1})
        self.title_names = title_names
        self.data_values = data_values

    def create_excel_file(self):
        for i, j in enumerate(self.title_names):
            self.table_handler.set_column(i, i, len(j) + 1)
            self.table_handler.write_string(0, i, j, self.title_bold)
        for k, v in self.data_values.items():
            for i in range(len(v)):
                j = v.get(self.title_names[i])
                print('%s, %s, %s, %s' % (k, i, j, self.border))
                self.table_handler.write_string(k, i, j, self.border)
        self.table_handler.set_column(1, 1, 16)
        self.table_handler.set_column(0, 0, 16)
        self.excel_file_handler.close()
