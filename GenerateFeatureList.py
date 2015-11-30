#!/usr/bin/python

import sys
import re
import traceback
import yaml
from dango.data import *
import xlrd 

XLS_ENCODING = 'utf-8'

# row with bad format slot will be SKIPPED
# row with empty feature name will using F_<depends>.replace(',','-') as default

class GenerateFeatureList(object):
    def __init__(self, feature_list_xls_file, feature_list_sheet_name):
        
        xls_book = xlrd.open_workbook(feature_list_xls_file, encoding_override=XLS_ENCODING)
        if xls_book is None:
            raise Exception('load xls file %s failed' % feature_list_xls_file)
        self.feature_list_sheet = xls_book.sheet_by_name(feature_list_sheet_name)
        if self.feature_list_sheet is None:
            raise Exception('Sheet %s does not in the Workbook' % feature_list_sheet_name)
        self.input_table_name = ''

    def dump_feature_list(self, output_feature_list_file):
        self.input_table_name = str(self.feature_list_sheet.cell(0, 1).value).strip()
        if self.input_table_name == '':
            raise Exception('Empty input dango table name')
        if not Data.exists(self.input_table_name):
            raise Exception('Input dango table %s does not exists in dango DB' 
                    % self.input_table_name)
        input_table = Data.query(self.input_table_name)
        self.input_table_meta = input_table.meta
        self.input_table_schema = input_table.schema
        self.input_table_uri = input_table.uri

        extra_schema = []
        slot_list = []
        with open(output_feature_list_file, 'w') as fp:
            for i in range(3, self.feature_list_sheet.nrows):
                cols = self.feature_list_sheet.row(i)
                if len(cols) < 3:
                    sys.stderr.write('Skip leak of column, [%s]\n' % str(cols))
                    continue
                feature = str(cols[0].value).strip()
                slot = str(cols[1].value).strip()
                method = str(cols[2].value).strip()
                depends = str(cols[3].value).strip() if len(cols) >=4 else None
                args = str(cols[4].value).strip() if len(cols) >=5 else None
                # skip empty or invalid slot
                if slot is None or not re.match(r'^[0-9]*(.[0-9]+)?$', slot):
                    sys.stderr.write('Skip invalid slot, [%s]\n' % str(cols))
                    continue
                # check slot range and uniqueness
                #slot = str(int(float(slot)))
                if int(slot) < 0 or int(slot) > 1023:
                    raise Exception('Invalid slot %s, exceeded range [0, 1023]' % slot)
                if int(slot) > 0:
                    if slot in slot_list:
                        raise Exception('Duplicated slot %s' % slot)
                slot_list.append(slot)
                # auto gen feature if not set
                if feature is None or feature == '':
                    if depends is None:
                        raise Exception('Cannot determine feature name, slot=%s'%slot)
                    feature = 'F_%s' % (depends.replace(',', '-'))
                # check feature name uniqueness
                if feature in self.input_table_schema or feature in extra_schema:
                    raise Exception('Duplicated feature %s, slot=%s' % (feature, slot))
                # check depends exists
                if depends is not None and depends != '':
                    for dep in depends.split(','):
                        if dep not in self.input_table_schema and dep not in extra_schema:
                            raise Exception('depends %s not exists, slot=%s' % (dep, slot))
                extra_schema.append(feature)
                # output
                fp.write('feature=%s; slot=%s; method=%s%s%s\n' % (
                        feature,
                        slot,
                        method,
                        ('; depends=%s'%depends) if depends is not None and depends != '' else '',
                        ('; args=%s'%args) if args is not None and args != '' else ''))

    def input_table_info(self):
        sys.stdout.write('Input table name: %s\n' % str(self.input_table_name))
        sys.stdout.write('Input table schema: %s\n' % str(self.input_table_schema))
        sys.stdout.write('Input table URI: %s\n' % str(self.input_table_uri))
        sys.stdout.write('Input table meta: %s\n' % str(self.input_table_meta))
        

if __name__ == '__main__':
    try:
        if len(sys.argv) != 4:
            sys.stderr.write('invalid input args\n')
            sys.stderr.write('usage: %s <input_fealist_xls_file> <input_fealist_sheet_name> <output_fealist_file>\n' % sys.argv[0])
            exit(1)
        
        input_feature_list_xls_file = sys.argv[1]
        input_feature_list_sheet_name = sys.argv[2]
        output_feature_list_file = sys.argv[3]

        sys.stderr.write('===== LOADING XLS FILE =====\n')
        gf = GenerateFeatureList(input_feature_list_xls_file, input_feature_list_sheet_name)
        sys.stderr.write('===== DUMPING FEATURE LIST =====\n')
        gf.dump_feature_list(output_feature_list_file)
        sys.stderr.write('===== INPUT TABLE INFO =====\n')
        gf.input_table_info()
        sys.stderr.write('===== SUCCESS =====\n')
    except Exception as e:
        sys.stderr.write('===== FAILED =====\nmsg: %s\n' % str(e))
        traceback.print_exc(file=sys.stderr)
        exit(-1)

    exit(0)


