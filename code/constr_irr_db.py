

import sqlite3
import os
import socket
import struct
import json

import global_var
from download_irrdata import GetBelongedOrg, GetOrgAsnFromIRROnLine, GetBelongedOrgFromAfrinic, GetBelongedOrgFromArin, \
                            GetBelongedOrgFromRipe, GetIpRangeOfIp, PreGetNonOverlayIpRanges

full_ip_space = '0.0.0.0 - 255.255.255.255'
irr_db = None
irr_db_cursor = None
def ConnectToIrrDb():
    global irr_db
    global irr_db_cursor
    irr_db = sqlite3.connect(global_var.par_path + global_var.irr_dir + global_var.irr_dbname)
    # 使用 cursor() 方法创建一个游标对象 cursor
    if not irr_db:
        print("ConnectToDb failed!")
    irr_db_cursor = irr_db.cursor()
    
def CloseIrrDb():
    global irr_db
    global irr_db_cursor
    if irr_db:
        irr_db_cursor.close()
        irr_db.close() 

def InsertApnicInfo():
    os.chdir(global_var.par_path + global_var.irr_dir)
    ConnectToIrrDb()
    ip_range_list = []
    
    #创建表
    irr_db_cursor.execute('CREATE TABLE IF NOT EXISTS irr_orgs(ip_range TEXT, org TEXT, asn TEXT, src TEXT)')
    irr_db_cursor.execute('CREATE INDEX IF NOT EXISTS index_ip_range ON irr_orgs (ip_range)')
    # for i in range(1, 256):
    #     irr_db_cursor.execute('DROP TABLE irr_%d' %i)
    #     irr_db_cursor.execute('DROP TABLE irr_index_%d' %i)
    
    rf = open(global_var.irr_filename_default, 'r')
    curline = rf.readline()
    while curline:
        ip_range = curline[1:curline.index(':')].strip('"')
        if ip_range == full_ip_space or ip_range == 'null':
            curline = rf.readline()
            continue
        #print(ip_range)
        info = json.loads(curline.strip('\n'))
        data = info[ip_range]
        (org_set, asn_set, src) = GetBelongedOrg(data)
        orgs = ';'.join(list(org_set))
        orgs = orgs.replace('\'', '_')
        insert_sql = "INSERT INTO irr_orgs VALUES('%s','%s','%s','%s')" %(ip_range, orgs, ';'.join(list(asn_set)), src)
        #print(insert_sql)
        irr_db_cursor.execute(insert_sql) 
        ip_range_list.append(ip_range)       
        curline = rf.readline()
    rf.close()
    irr_db.commit()
    CloseIrrDb()
    with open(global_var.ip_ranges_filename, 'w') as wf:
        wf.write(';'.join(ip_range_list))

def InsertOtherSrcInfo():
    os.chdir(global_var.par_path + global_var.irr_dir)
    ConnectToIrrDb()
    
    for src in global_var.irrs:
        if src == 'apnic' or src == 'lacnic':
            continue
        filename = 'irrdata_' + src
        print(src)
        rf = open(filename, 'r')
        curline = rf.readline()
        while curline:
            ip_range = curline[1:curline.index(':')].strip('"')
            if ip_range == full_ip_space or ip_range == 'null':
                curline = rf.readline()
                continue
            #print(ip_range)
            info = json.loads(curline.strip('\n'))
            data = info[ip_range]
            if src == 'afrinic':
                (org_set, asn_set, tmp_src) = GetBelongedOrgFromAfrinic(data)
            elif src == 'ripe':
                (org_set, asn_set, tmp_src) = GetBelongedOrgFromRipe(data)
            elif src == 'arin':
                (org_set, asn_set, tmp_src) = GetBelongedOrgFromArin(data)
            orgs = ';'.join(list(org_set))
            orgs = orgs.replace('\'', '_')
            insert_sql = "INSERT INTO irr_orgs VALUES('%s','%s','%s','%s')" %(ip_range, orgs, ';'.join(list(asn_set)), src)
            #print(insert_sql)
            irr_db_cursor.execute(insert_sql)   
            curline = rf.readline()
        rf.close()
    irr_db.commit()
    CloseIrrDb()

irr_cache = dict()
def IniIrrCache():
    global irr_cache
    irr_cache.clear()

def ConstrIrrCache():
    global irr_cache
    global irr_db_cursor
    irr_cache.clear()
    #print('ConstrBdrCache')
    select_sql = "SELECT ip_range,org,asn FROM irr_orgs"
    irr_db_cursor.execute(select_sql)
    result = irr_db_cursor.fetchall()
    for elem in result:
        #print(elem[0] + ' ' + str(elem[1]))
        org_set = set(elem[1].replace('_', '\'').split(';'))
        asn_set = set(elem[2].split(';'))
        if elem[0] in irr_cache.keys():
            [org_set1, asn_set1] = irr_cache[elem[0]]
            org_set |= org_set1
            asn_set |= asn_set1
        irr_cache[elem[0]] = [org_set, asn_set]
        
def TmpAnaIpRanges():
    filename = global_var.par_path + global_var.irr_dir + global_var.ip_ranges_filename
    with open(filename, 'r') as rf:
        data = rf.read()
    ip_ranges = data.split(';')    
    new_ip_ranges_list = []
    while ip_ranges:
        new_ip_ranges = []
        new_remain_ip_ranges = []
        pre_ip_int = 0
        for ip_range in ip_ranges:
            elems = ip_range.split(' ')
            first_ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton(elems[0]))[0])
            last_ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton(elems[0]))[0])
            if first_ip_int > pre_ip_int: #no overlap
                new_ip_ranges.append(ip_range)
            else: #overlap
                new_remain_ip_ranges.append(ip_range)
            pre_ip_int = last_ip_int
        new_ip_ranges_list.append(new_ip_ranges)
        ip_ranges = new_remain_ip_ranges.copy()
    print(len(new_ip_ranges_list))
    for elem in new_ip_ranges_list:
        print(len(elem))
    # for elem in ip_ranges:
    #     if elem[-3:] != '255':
    #         print(elem)

            
def SplitIpRangesIntoNonOverlay():
    filename = global_var.par_path + global_var.irr_dir + global_var.ip_ranges_filename
    with open(filename, 'r') as rf:
        data = rf.read()
    ip_ranges = data.split(';')    
    new_ip_ranges_with_int_index_list = []
    while ip_ranges:
        new_ip_ranges_with_int_index = []
        new_remain_ip_ranges = []
        pre_ip_int = 0
        for ip_range in ip_ranges:
            elems = ip_range.split(' ')
            first_ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton(elems[0]))[0])
            last_ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton(elems[-1]))[0])
            if first_ip_int > pre_ip_int: #no overlap
                new_ip_ranges_with_int_index.append([first_ip_int, last_ip_int, ip_range])
            else: #overlap
                new_remain_ip_ranges.append(ip_range)
            pre_ip_int = last_ip_int
        new_ip_ranges_with_int_index_list.append(new_ip_ranges_with_int_index)
        ip_ranges = new_remain_ip_ranges.copy()
    print(len(new_ip_ranges_with_int_index_list))
    wf = open(filename + '_dealed', 'w')
    for new_ip_ranges_with_int_index in new_ip_ranges_with_int_index_list:
        for elem in new_ip_ranges_with_int_index:
            wf.write("%d,%d,%s" %(elem[0], elem[1], elem[2]))
            wf.write(';')
        wf.write('\n')        
    # for elem in ip_ranges:
    #     if elem[-3:] != '255':
    #         print(elem)

#delete 2021.7.15
def ConstrIpIndexTable_Deleted():
    global irr_db
    global irr_db_cursor
    ConnectToIrrDb()
    os.chdir(global_var.par_path + global_var.irr_dir)
    with open(global_var.ip_ranges_filename, 'r') as rf:
        data = rf.read()
    ip_range_list = data.split(';')
    
    #创建表
    for i in range(1, 256):
        irr_db_cursor.execute('CREATE TABLE IF NOT EXISTS irr_ip_index_%d(ip TEXT, ip_range TEXT)' %i)
        irr_db_cursor.execute('CREATE INDEX IF NOT EXISTS index_ip ON irr_ip_index_%d (ip)' %i)
        
    begin = False
    for ip_range in ip_range_list:
        print(ip_range)
        if ip_range.split(' ')[0] == '141.70.0.0':
            begin = True
        if not begin:
            continue
        elems = ip_range.split(' ')
        cur_ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton(elems[0]))[0])
        last_ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton(elems[-1]))[0])
        first_ip_seg = int(ip_range[:ip_range.index('.')])
        while cur_ip_int < last_ip_int:
            cur_ip = str(socket.inet_ntoa(struct.pack('I',socket.htonl(cur_ip_int))))
            insert_sql = "INSERT INTO irr_ip_index_%d VALUES('%s','%s')" %(first_ip_seg, cur_ip, ip_range)
            irr_db_cursor.execute(insert_sql) 
            cur_ip_int += 1
    irr_db.commit()
    CloseIrrDb()

def GetIpRangeOfIp_Deleted(ip):
    global irr_db
    global irr_db_cursor

    connect_inside = False
    if not irr_db:
        ConnectToIrrDb()
        connect_inside = True

    print('1.1')
    first_ip_seg = int(ip[:ip.index('.')])
    select_sql = "SELECT ip_range FROM irr_ip_index_%d WHERE ip='%s'" %(first_ip_seg, ip)
    irr_db_cursor.execute(select_sql)
    result = irr_db_cursor.fetchall()
    print('1.2')
    ip_range_set = set()
    for elem in result:
        ip_range_set.add(elem[0])
    print('1.3')
    if connect_inside:
        CloseIrrDb()
    return ip_range_set

def GetIrrOrgFromDb(ip):
    global irr_cache
    ip_range_set = GetIpRangeOfIp(ip)
    org_set = set()
    asn_set = set()
    for ip_range in ip_range_set:
        #print(ip_range)
        org_set |= irr_cache[ip_range][0] #org_set
        asn_set |= irr_cache[ip_range][1] #asn_set
    return (org_set, asn_set)

if __name__ == '__main__':
    #InsertApnicInfo()
    # PreGetIrrIpRanges()
    # ConnectToIrrDb()
    # ConstrIrrCache()
    # print(GetIrrOrgFromDb('212.74.66.0'))
    # CloseIrrDb()

    #InsertOtherSrcInfo()
    #ConstrIpIndexTable()
    
    #SplitIpRangesIntoNonOverlay()
    PreGetNonOverlayIpRanges()
    print(GetIpRangeOfIp('200.0.1.0'))
    #print(GetIpRangeOfIp_Deleted('200.0.1.0'))

