import os
import sqlite3
import json

import global_var

'''
def GetAllTableNames(path):
    table_name_set = set()
    for root,dirs,files in os.walk(path):
        for cur_file in files:
            if not cur_file.__contains__('.sqlite'):
            #if not cur_file.__contains__('peeringdb_2017_05_15.sqlite'):
                continue
            #print(cur_file)
            db = sqlite3.connect(path + cur_file)
            cursor = db.cursor()
            cursor.execute("select name from sqlite_master where type='table' order by name")
            res = cursor.fetchall()
            for elem in res:
                table_name = elem[0]
                if table_name.__contains__('peeringdb_'):
                    table_name_set.add(table_name)
    
    for elem in table_name_set:
        print(elem)
'''

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def TransSqlite2Json(sqlite_file):
    print(sqlite_file)
    # connect to the SQlite databases
    connection = sqlite3.connect(sqlite_file)
    connection.row_factory = dict_factory    
    cursor = connection.cursor()

    # select all the tables from the database
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    # for each of the bables , select all the records from the table
    for table_name in tables:
            # table_name = table_name[0]
            #if not table_name['name'].__contains__('peeringdb_'):
            if table_name['name'] != 'peeringdb_ix':
                continue
            print(table_name['name'])
            conn = sqlite3.connect(sqlite_file)
            conn.row_factory = dict_factory            
            cur1 = conn.cursor()
            cmd = "SELECT * FROM "+table_name['name']
            print(cmd)
            cur1.execute(cmd)
            # fetch all or one we'll go for all.            
            results = cur1.fetchall()
            #print(results)
            # generate and save JSON files with the table name for each of the database tables
            with open(sqlite_file[0:sqlite_file.index('.')]+'.json', 'w') as the_file:
                the_file.write(format(results).replace(" u'", "'").replace("'", "\""))
    connection.close()

def BatchTransSqlite2Json(path):
    for root,dirs,files in os.walk(path):
        for cur_file in files:
            if not cur_file.__contains__('.sqlite'):
            #if not cur_file.__contains__('peeringdb_2017_05_15.sqlite'):
                continue
            TransSqlite2Json(path + cur_file)

if __name__ == '__main__':
    #BatchTransSqlite2Json(global_var.par_path + global_var.peeringdb_dir)
    TransSqlite2Json(global_var.par_path + global_var.peeringdb_dir + 'peeringdb_2016_05_27.sqlite')
