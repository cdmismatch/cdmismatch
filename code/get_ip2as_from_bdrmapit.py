
import sqlite3
import os
import multiprocessing as mp
import sys

import global_var

bdrmapit_db = None
bdrmapit_db_cursor = None

def ConnectToBdrMapItDb(dbname):
    global bdrmapit_db
    global bdrmapit_db_cursor
    bdrmapit_db = sqlite3.connect(dbname)
    # 使用 cursor() 方法创建一个游标对象 cursor
    if not bdrmapit_db:
        print("ConnectToDb failed!")
    bdrmapit_db_cursor = bdrmapit_db.cursor()

bdr_cache = dict()
def InitBdrCache():
    global bdr_cache
    bdr_cache.clear()
    
def ConstrBdrCache():
    global bdr_cache
    bdr_cache.clear()
    #print('ConstrBdrCache')
    select_sql = "SELECT addr,asn FROM annotation"
    bdrmapit_db_cursor.execute(select_sql)
    result = bdrmapit_db_cursor.fetchall()
    for elem in result:
        #print(elem[0] + ' ' + str(elem[1]))
        bdr_cache[elem[0]] = str(elem[1])
        
def GetBdrCache():
    return bdr_cache

def ConstrLocalBdrCache(cache):
    #print('ConstrBdrCache')
    select_sql = "SELECT addr,asn FROM annotation"
    bdrmapit_db_cursor.execute(select_sql)
    result = bdrmapit_db_cursor.fetchall()
    for elem in result:
        #print(elem[0] + ' ' + str(elem[1]))
        cache[elem[0]] = str(elem[1])

def UpdateASInBdrDb(updates_info):
    for (_ip, asn) in updates_info.items():
        update_sql = "UPDATE annotation SET asn=%d WHERE addr='%s'" %(int(asn), _ip)
        bdrmapit_db_cursor.execute(update_sql)
    bdrmapit_db.commit()

def GetIp2ASFromBdrMapItDb(ip):
    global bdr_cache
    global bdrmapit_db_cursor
    
    if ip in bdr_cache.keys():
        #print('hit')
        #print(bdr_cache[ip])
        return bdr_cache[ip] #hit
    return ''

    #print('add')
    select_sql = "SELECT asn FROM annotation WHERE addr=?"
    bdrmapit_db_cursor.execute(select_sql, (ip,))
    result = bdrmapit_db_cursor.fetchall()
    if result:
        asn = str(result[0][0])
        bdr_cache[ip] = asn
        #print(asn)
        return asn
    bdr_cache[ip] = ''
    #print('')
    return ''

def CloseBdrMapItDb():
    global bdrmapit_db
    global bdrmapit_db_cursor
    bdrmapit_db_cursor.close()
    bdrmapit_db.close()   






bdrmapit_db_dict = dict()
bdrmapit_db_cursor_dict = dict()
def ConnectToBdrMapItDb_2(dbname, pathname):
    global bdrmapit_db_dict
    global bdrmapit_db_cursor_dict
    bdrmapit_db_dict[dbname] = sqlite3.connect(pathname)
    # 使用 cursor() 方法创建一个游标对象 cursor
    if not bdrmapit_db_dict[dbname]:
        print("ConnectToDb %s failed!" %dbname)
    bdrmapit_db_cursor_dict[dbname] = bdrmapit_db_dict[dbname].cursor()

bdr_cache_dict = dict()
def InitBdrCache_2(dbname):
    global bdr_cache_dict
    bdr_cache_dict[dbname].clear()
    
def ConstrBdrCache_2(dbname):
    global bdr_cache_dict
    bdr_cache_dict[dbname] = dict()
    #print('ConstrBdrCache')
    select_sql = "SELECT addr,asn FROM annotation"
    bdrmapit_db_cursor_dict[dbname].execute(select_sql)
    result = bdrmapit_db_cursor_dict[dbname].fetchall()
    for elem in result:
        #print(elem[0] + ' ' + str(elem[1]))
        bdr_cache_dict[dbname][elem[0]] = str(elem[1])
        
def GetIp2ASFromBdrMapItDb_2(dbname, ip):
    global bdr_cache_dict
    
    if dbname not in bdr_cache_dict.keys():
        return ''
    if ip in bdr_cache_dict[dbname].keys():
        #print('hit')
        #print(bdr_cache[ip])
        return bdr_cache_dict[dbname][ip] #hit
    return ''

def CloseBdrMapItDb_2(dbname):
    global bdrmapit_db_dict
    global bdrmapit_db_cursor_dict
    bdrmapit_db_cursor_dict[dbname].close()
    bdrmapit_db_dict[dbname].close()  

def QueryDb(db_filename, command, queue):
    res = None
    tmp_db = sqlite3.connect(db_filename)
    if not tmp_db:
        print("ConnectToDb %s failed!" %db_filename)
        return ''
    tmp_db_cursor = tmp_db.cursor()
    tmp_db_cursor.execute(command)
    result = tmp_db_cursor.fetchall()
    if result:
        res = result[0][0]
    tmp_db_cursor.close()
    tmp_db.close()
    if res:
        queue.put(res)

def GetAsOfIpByVotes(ip, files):
    asn_dict = dict()
    sub_proc_list = []
    queue = mp.Queue()
    # QueryDb('bdrmapit_201801.db', select_sql)
    # return
    for filename in files:
        sub_proc_list.append(mp.Process(target=QueryDb, args=(filename, select_sql, queue)))
    for elem in sub_proc_list:
        elem.start()
    for elem in sub_proc_list:
        elem.join()
    res_dict = dict()
    while not queue.empty():
        tmp = queue.get()
        if tmp not in res_dict.keys():
            res_dict[tmp] = 0
        res_dict[tmp] += 1
    sort_list = sorted(res_dict.items(), key=lambda d:d[1], reverse = True)
    # for elem in sort_list:
    #     print('%s (%d): ' %(elem[0], elem[1]))
    return sort_list[0][0]
            
def main_func():    
    ConnectToBdrMapItDb(sys.argv[1])
    ConstrBdrCache()
    cache = GetBdrCache()
    print(cache[sys.argv[2]])
    CloseBdrMapItDb()
    InitBdrCache()
    return
    select_sql = "SELECT asn FROM annotation WHERE addr=\'%s\'" %sys.argv[1]
    os.chdir(global_var.all_trace_par_path + global_var.all_trace_download_dir + 'back/')
    db_dict = dict()
    cursor_dict = dict()
    for year in range(2018,2021):
        for month in range(1, 13):
            if (year == 2020 and month > 4) or (year == 2019 and month == 2): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
                continue
            date = str(year) + str(month).zfill(2)
            db_dict[date] = sqlite3.connect('bdrmapit_%s.db' %date)
            if db_dict[date]:
                cursor_dict[date] = db_dict[date].cursor()
    print('begin')
    res_dict = dict()
    for (key, val) in cursor_dict.items():
        val.execute(select_sql)
        result = val.fetchall()
        if result:
            res = result[0][0]
            if res not in res_dict.keys():
                res_dict[res] = 0
            res_dict[res] += 1
    sort_list = sorted(res_dict.items(), key=lambda d:d[1], reverse = True)
    print(sort_list[0][0])
    print('end')
    for key in cursor_dict.keys():
        cursor_dict[key].close()
        db_dict[key].close()

    #print(GetAsOfIpByVotes('63.223.15.174', files))
    while True:
        pass

    os.chdir('/mountdisk1/ana_c_d_incongruity/out_bdrmapit/')
    for root,dirs,files in os.walk('.'):
        for filename in files:
            if not filename.__contains__('zrh2-ch'):
                continue
            print(filename)
            ConnectToBdrMapItDb(filename)
            select_sql = "SELECT asn FROM annotation LIMIT 10"
            bdrmapit_db_cursor.execute(select_sql)
            result = bdrmapit_db_cursor.fetchall()
            print(result)
    
    CloseBdrMapItDb()    
        
if __name__ == '__main__':
    main_func()

