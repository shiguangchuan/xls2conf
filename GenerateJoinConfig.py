#!/usr/bin/python

import sys
import re
import traceback
import yaml
from dango.data import *
import xlrd 

XLS_ENCODING = 'utf-8'

# BaseTableKey, JoinTableKey, ',' seperate multiple keys for one table
# JoinTableName ',' seperate multiple tables, and ';' seperate keys of different table in JoinTableKey

class GenerateJoinConfig(object):
    def __init__(self, join_xls_file, join_sheet_name):
        xls_book = xlrd.open_workbook(join_xls_file, encoding_override=XLS_ENCODING)
        if xls_book is None:
            raise Exception('load xls file %s failed' % feature_list_xls_file)
        self.join_sheet = xls_book.sheet_by_name(join_sheet_name)
        if self.join_sheet is None:
            raise Exception('Sheet %s does not in the Workbook' % join_sheet_name)
        self.table_dict = {}

    def _add_table(self, table_name, table_type):
        if table_name in self.table_dict:
            raise Exception('Duplicated table name %s' % table_name)
        tag = str(len(self.table_dict))
        if not Data.exists(table_name):
            raise Exception('Input dango table %s does not exists in dango DB' % table_name)
        d = Data.query(table_name)
        self.table_dict[table_name] = dict(
                    tag = tag,
                    schema = d.schema,
                    meta = d.meta,
                    uri = d.uri,
                    type = table_type
                )

    def dump_join_conf(self, output_join_file):
        join_yaml = {}
        field_list = []
        base_table_name = str(self.join_sheet.cell(0, 1).value).strip()
        base_table_key  = str(self.join_sheet.cell(1, 1).value).strip()
        join_table_name = str(self.join_sheet.cell(0, 3).value).strip()
        join_table_key  = str(self.join_sheet.cell(1, 3).value).strip()
        reduce_num      = str(self.join_sheet.cell(0, 5).value).strip()
        reduce_mem      = str(self.join_sheet.cell(1, 5).value).strip()
        if base_table_name == '' or base_table_key == '':
            raise Exception('Empty base table or key')
        if join_table_name == '' or join_table_key == '':
            raise Exception('Empty join table or key')
        reduce_num = int(float(reduce_num))
        reduce_mem = int(float(reduce_mem))
        if reduce_num <= 0 or reduce_mem <= 0:
            raise Exception('Invalid reduce_num or reduce_mem')
        if base_table_name in self.table_dict:
            raise Exception('Duplicated table name %s' % base_table_name)
        self._add_table(base_table_name, 'base')
        join_yaml['base_table'] = dict(
                    keys = base_table_key.split(','),
                    tag = self.table_dict[base_table_name]['tag']
                )
        join_tables = join_table_name.split(',')
        join_keys = join_table_key.split(';')
        if len(join_tables) != len(join_keys):
            raise Exception('Join table number and join key number mismatch')
        for i in range(len(join_tables)):
            self._add_table(join_tables[i], 'join')
            join_yaml['join_table_%d'%i] = dict(
                        keys = join_keys[i].split(','),
                        tag  = self.table_dict[join_tables[i]]['tag']
                    )
        join_yaml['out_table'] = {}
        join_yaml['out_table']['meta'] = []

        for i in range(4, self.join_sheet.nrows):
            cols = self.join_sheet.row(i)
            if len(cols) < 3:
                sys.stderr.write('Skip leak of column, [%s]\n' % str(cols))
                continue
            field_name = str(cols[0].value).strip()
            depends    = str(cols[1].value).strip()
            method     = str(cols[2].value).strip()
            cursors    = str(cols[3].value).strip() if len(cols) >= 4 else None
            tracker    = str(cols[4].value).strip() if len(cols) >= 5 else None
            args       = str(cols[5].value).strip() if len(cols) >= 6 else None
            if field_name == '':
                field_name = '_'.join([dep.replace(':','.') for dep in depends.split(',')])
            if field_name in field_list:
                raise Exception('Duplicated output field %s' % field_name)
            field_list.append(field_name)
            item = {}
            item[field_name] = {}
            item[field_name]['method'] = method
            item[field_name]['depends'] = []
            for dep in depends.split(','):
                token = dep.split(':', 1)
                if token[0] not in self.table_dict:
                    raise Exception('Depends table not exists, %s' % dep)
                if token[1] not in self.table_dict[token[0]]['schema']:
                    raise Exception('Depends col %s not exists in table %s' % (token[1], token[0]))
                item[field_name]['depends'].append('%s:%s' % (self.table_dict[token[0]]['tag'], token[1]))
            item[field_name]['depends'] = ','.join(item[field_name]['depends'])
            if cursors is not None and cursors != '':
                item[field_name]['cursors'] = []
                for cur in cursors.split(','):
                    token = cur.split(':', 1)
                    if token[0] not in self.table_dict:
                        raise Exception('Cursor table not exists, %s' % cur)
                    if token[1] not in self.table_dict[token[0]]['schema']:
                        raise Exception('Cursor col %s not exists in table %s' % (token[1], token[0]))
                    item[field_name]['cursors'].append('%s:%s' % (self.table_dict[token[0]]['tag'], token[1]))
                item[field_name]['cursors'] = ','.join(item[field_name]['cursors'])
            if tracker is not None and tracker != '':
                item[field_name]['tracker'] = tracker
            if args is not None and args != '':
                item[field_name]['args'] = args
            join_yaml['out_table']['meta'].append(item)

        join_yaml['env'] = {}
        join_yaml['env']['mapred.reduce.tasks'] = reduce_num
        join_yaml['env']['mapreduce.reduce.memory.mb'] = reduce_mem
        
        with open(output_join_file, 'w') as fp:
            fp.write(yaml.dump(join_yaml, indent=4, default_flow_style=False))

    def input_table_info(self):
        for table in self.table_dict:
            sys.stdout.write('Table: %s [%s][%s]\n' % (table, self.table_dict[table]['tag'], self.table_dict[table]['type']))
            sys.stdout.write('\tschema: %s\n' % self.table_dict[table]['schema'])
            sys.stdout.write('\turi: %s\n' % self.table_dict[table]['uri'])
            sys.stdout.write('\tmeta: %s\n' % self.table_dict[table]['meta'])
            sys.stdout.write('\n')
        

if __name__ == '__main__':
    try:
        if len(sys.argv) != 4:
            sys.stderr.write('invalid input args\n')
            sys.stderr.write('usage: %s <input_join_xls_file> <input_join_sheet_name> <output_join_file>\n' % sys.argv[0])
            exit(1)
        
        input_join_xls_file = sys.argv[1]
        input_join_sheet_name = sys.argv[2]
        output_join_file = sys.argv[3]

        sys.stderr.write('===== LOADING XLS FILE =====\n')
        gj = GenerateJoinConfig(input_join_xls_file, input_join_sheet_name)
        sys.stderr.write('===== DUMPING JOIN CONFIG =====\n')
        gj.dump_join_conf(output_join_file)
        sys.stderr.write('===== INPUT TABLE INFO =====\n')
        gj.input_table_info()
        sys.stderr.write('===== SUCCESS =====\n')
    except Exception as e:
        sys.stderr.write('===== FAILED =====\nmsg: %s\n' % str(e))
        traceback.print_exc(file=sys.stderr)
        exit(-1)

    exit(0)


