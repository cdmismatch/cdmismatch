
import sys
import socket
import struct
import ujson
import re
import os
import pymysql
import time
import requests
import glob
from math import radians, cos, sin, asin, sqrt
import sqlite3
import ipaddress
import datetime
#import commands
import json

import global_var
from gen_ip2as_command import GetCloseDateFile, PreGetSrcFilesInDirs
from traceutils.ixps import AbstractPeeringDB, create_peeringdb
from get_ip2as_from_bdrmapit import ConnectToBdrMapItDb, GetIp2ASFromBdrMapItDb,CloseBdrMapItDb, InitBdrCache, \
                                    ConstrBdrCache
from download_irrdata import GetOrgAsnFromIRROnLine, PreGetNonOverlayIpRanges, GetCountryFromIrrFiles, \
                            GetCountryOrgAsnFromIrrOnline
from constr_irr_db import GetIrrOrgFromDb


def GetClosestDate(given_date, date_list):
    min_val = 0xFFFFFFF
    closest_date = ''
    given_datetime = datetime.datetime(int(given_date[:4]), int(given_date[4:6]), int(given_date[6:]))
    for date in date_list:
        #print(date)
        diff = abs((datetime.datetime(int(date[:4]), int(date[4:6]), int(date[6:])) - given_datetime).days)
        if diff < min_val:
            min_val = diff
            closest_date = date
    return closest_date

#pfx2as_file_name = '../../data/routeviews-rv2-20190301-1200.pfx2as_coalesced'
#pfx2as_file_name = '../../data/routeviews-rv2-20190401-1200.pfx2as_coalesced'
#as_rel_file_name = '../../data/20190301.as-rel2.txt'
as_rel_file_name = '../../data/20190401.as-rel2.txt'
as_rel_file_name_2 = '../../data/20201201.as-rel2' #用一个最新的版本
#as2org_file_name = '../../data/as2org'

def IsReservedASN(asn_str):
    asn = int(asn_str)
    if (asn > 64495 and asn < 131072) or \
        (asn > 143674 and asn < 196607) or \
        (asn > 213404 and asn < 262143) or \
        (asn > 272797 and asn < 327679) or \
        (asn > 329728 and asn < 393215) or \
        asn > 401309:
        return True
    return False

def DealAsSet(ases): #in this function also delete invalid asn
    if False:
        #rf = open('test', 'r', encoding='utf-8')
        #wf_test = open('test2', 'w')
        #cur_line = rf.readline()
        #while cur_line:
        #    cur_line = cur_line.strip('\n')
        moas_elems = ases.split('_')
        normal_as_list = []
        for tmp in moas_elems:
            if not tmp.__contains__(','): #a single as
                if not IsReservedASN(tmp):
                    normal_as_list.append(tmp)
        set_as_list = []
        for tmp in moas_elems:
            if tmp.__contains__(','):#has as set
                tmp_as_list = tmp.split(',')
                candidate_as_list = []
                for tmp_as in tmp_as_list:
                    if IsReservedASN(tmp_as):
                        pass
                    elif tmp_as in normal_as_list:
                        candidate_as_list = []
                        break
                    else:
                        candidate_as_list.append(tmp_as)
                if candidate_as_list:
                    if len(candidate_as_list) == 1:
                        normal_as_list.append(candidate_as_list[0])
                    else:
                        set_as_list.append(candidate_as_list)
        res_str = ""
        for elem in normal_as_list:
            res_str += elem + '_'
        for set_as in set_as_list:
            for tmp in set_as:
                res_str += tmp + ','
            res_str = res_str.strip(',')
            res_str += '_'
        res_str = res_str.strip('_')
        return res_str
            #wf_test.write("%s\n" %wf_str)
            #cur_line = rf.readline()
        #rf.close()
        #wf_test.close()
    #20210727更改，如果是as set直接舍弃，等着用bdrmapit 去map
    if ases.__contains__(','):
        return None
    elems = ases.split('_')
    res_list = []
    for elem in elems:
        if not IsReservedASN(elem):
            res_list.append(elem)
    if not res_list:
        return None
    return '_'.join(res_list)

#from this file get pfx2as mapping, record moas and asset, later we also should get subprefix info
ip2as_dict = dict() #key: prefix/slash; val: list of ases(in case of moas)
#和v1的区别：把moas也考虑进来，as_set不考虑
def GetPfx2ASByRv(year, month):    #从routeviews中获取数据
    global ip2as_dict
    #with open(glob.glob(global_var.par_path + global_var.rib_dir + '*_coalesced')[0], 'r', encoding='utf-8') as rf:
    if (year > 2019) or (year == 2019 and month > 8): #2019年8月后没有数据了
        year = 2019
        month = 8
    with open(global_var.par_path + global_var.rib_dir + 'coalesced/routeviews-rv2-%s15.pfx2as_coalesced' %(str(year) + str(month).zfill(2)), 'r', encoding='utf-8') as rf:
        cur_line = rf.readline()
        while cur_line:
            elems = cur_line.strip("\n").split("\t")
            prefix = elems[0] + '/' + elems[1]
            ases = elems[2]
            #先处理AS set
            modi_ases = DealAsSet(ases)
            if modi_ases: #invalid AS, pass
                ip2as_dict[prefix] = modi_ases.split('_')
            cur_line = rf.readline()

def ClearIp2AsDict():
    global ip2as_dict
    ip2as_dict.clear()

ip2as_v6_dict = dict()
def GetPfx2ASByRvV6(year, month):    #从routeviews v6中获取数据
    global ip2as_v6_dict
    with open(global_var.par_path + global_var.rib_dir + 'coalesced/routeviews-rv6-%s15.pfx2as_coalesced' %(str(year) + str(month).zfill(2)), 'r', encoding='utf-8') as rf:
        cur_line = rf.readline()
        while cur_line:
            elems = cur_line.strip("\n").split("\t")
            prefix = elems[0] + '/' + elems[1]
            ases = elems[2]
            #先处理AS set
            modi_ases = DealAsSet(ases)
            if modi_ases: #invalid AS, pass
                ip2as_v6_dict[prefix] = modi_ases.split('_')
            cur_line = rf.readline()

def ClearIp2AsDictV6():
    global ip2as_v6_dict
    ip2as_v6_dict.clear()
    
#from this file get pfx2as mapping, record moas and asset, later we also should get subprefix info
#和v2的区别：从bgp表中获取数据, 经过实验，bgp表数据和routeviews-rv2-20190301-1200.pfx2as_coalesced里的数据差的还挺多的
#因为trace是从特定的VP发出的，使用该VP的bgp进行map和比较也更合理
#2021.3.13 替换掉GetPfx2ASByRv()
def GetPfx2ASByBgp(filename):    #从bgp表中获取数据
    global ip2as_dict
    rf = open(filename, 'r', encoding='utf-8')
    #wf = open('..\\srcdata\\res_moas', 'w')
    #wf_as_set = open('..\\srcdata\\res_pfx2as_set', 'w')
    cur_line = rf.readline()
    while cur_line:
        elems = cur_line.split('|')
        if len(elems) < 3:
            cur_line = rf.readline()
            continue
        if elems[0] == '-' or elems[1].__contains__(':'): #撤销的update和ipv6，过滤
            cur_line = rf.readline()
            continue
        prefix_list = elems[1].split(' ')
        ori_as = elems[2].split(' ')[-1]
        for prefix in prefix_list:
            if prefix not in ip2as_dict.keys():
                ip2as_dict[prefix] = []
            if ori_as not in ip2as_dict[prefix]:
                ip2as_dict[prefix].append(ori_as)
        cur_line = rf.readline()
    rf.close()

def GetLongestMatchPrefixByRv(ip):
    #print(ip)
    ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton(ip))[0])
    for mask_len in range(24, 7, -1):
        mask = 0xFFFFFFFF - (1 << (32 - mask_len)) + 1
        cur_prefix_int = ip_int & mask
        cur_prefix = str(socket.inet_ntoa(struct.pack('I',socket.htonl(cur_prefix_int))))
        cur_prefix = cur_prefix + '/' + str(mask_len)
        #print(cur_prefix)
        if cur_prefix in ip2as_dict.keys():
            return cur_prefix
    return ''

def GetLongestMatchPrefixByRvV6(ip):
    global ip2as_v6_dict
    #print(ip)
    #ipaddress.ip_interface('2001:468:ff:110::1/126').network
    for mask_len in range(127, 1, -1):
        cur_prefix = ipaddress.ip_interface(ip + '/' + str(mask_len)).network
        if cur_prefix in ip2as_v6_dict.keys():
            return cur_prefix
    return ''

def GetAsListOfPrefixByRv(prefix): #返回列表
    if prefix and prefix in ip2as_dict.keys():
        return ip2as_dict[prefix]
    return []
    
def GetAsListOfPrefixByRvV6(prefix): #返回列表
    if prefix and prefix in ip2as_v6_dict.keys():
        return ip2as_v6_dict[prefix]
    return []

def GetAsStrOfPrefixByRv(prefix): #返回字符串，AS之间以'_'隔开
    as_str = ""
    as_list = GetAsListOfPrefixByRv(prefix)
    if as_list:
        for elem in as_list:
            as_str += elem + '_'
        as_str = as_str.strip('_')
    return as_str

def GetAsStrOfPrefixByRvV6(prefix): #返回字符串，AS之间以'_'隔开
    return '_'.join(GetAsListOfPrefixByRvV6(prefix))
    
def GetAsStrOfIpByRv(ip):
    prefix = GetLongestMatchPrefixByRv(ip)
    return GetAsStrOfPrefixByRv(prefix)
    
def GetAsStrOfIpByRvV6(ip):
    prefix = GetLongestMatchPrefixByRvV6(ip)
    return GetAsStrOfPrefixByRvV6(prefix)

def GetAsListOfIpByRv(ip):
    #print('ip: %s' %ip)
    prefix = GetLongestMatchPrefixByRv(ip)
    return GetAsListOfPrefixByRv(prefix)

bgp_path_of_as_dict = dict()
def GetPathAsDict(filename): #从bgp表中获取dst_as到path的字典
    global bgp_path_of_as_dict
    rf = open(filename, 'r', encoding='utf-8')
    cur_line = rf.readline()
    while cur_line:
        elems = cur_line.split('|')
        if elems[0] == '-' or elems[1].__contains__(':'): #撤销的update和ipv6，过滤
            cur_line = rf.readline()
            continue
        if len(elems) < 3:
            cur_line = rf.readline()
            continue
        as_path = CompressAsPath(elems[2])
        ori_as = as_path.split(' ')[-1]
        if ori_as not in bgp_path_of_as_dict.keys():
            bgp_path_of_as_dict[ori_as] = []
        if as_path not in bgp_path_of_as_dict[ori_as]:
            bgp_path_of_as_dict[ori_as].append(as_path)
        cur_line = rf.readline()
    rf.close()

def GetBgpPathByAs(asn):
    global bgp_path_of_as_dict
    if asn in bgp_path_of_as_dict.keys():
        return bgp_path_of_as_dict[asn]
    return []

def ClearPathAsDict():
    global bgp_path_of_as_dict
    bgp_path_of_as_dict.clear()

rfs_router_ip = []
rfs_as_router = []
rfs_router_geo = []

ip_router_dict = dict() #used as a cache
'''
def GetRouterOfIpByMi(ip):  #从midar-iff中获取数据
    if ip in ip_router_dict.keys(): #cache hit
        return ip_router_dict[ip]
    #read the source file
    rf = open('..\\midar-iff.nodes', 'r', encoding='utf-8')
    curline = rf.readline()
    while curline:
        if curline.startswith('#'):
            curline = rf.readline()
            continue
        elems = curline.split(':')
        snd_elems = elems[1].split(' ')
        if ip in snd_elems: #find
            router = elems[0][5:]
            ip_router_dict[ip] = router #add to cache
            return router
        curline = rf.readline()
    rf.close()
    return ""
'''

def BinarySearch(lista, key): #搜索ip是，key是没有第一个八字节的ip
    min = 0
    max = len(lista) - 1
    while min <= max:
        # 得到中位数
        mid = int((min + max) / 2)
        cmp_key = (lista[mid].split(' '))[0]
        # key在数组左边
        if cmp_key > key:
            max = mid - 1
        # key在数组右边
        elif cmp_key < key:
            min = mid + 1
        # key在数组中间
        elif cmp_key == key:
            return lista[mid]
    return ""

def BinarySearch_2(lista, key): #搜索router，这时key是整形
    min = 0
    max = len(lista) - 1
    int_key = int(key)
    while min <= max:
        # 得到中位数
        mid = int((min + max) / 2)
        cmp_key = int((lista[mid].split(' '))[0])
        # key在数组左边
        if cmp_key > int_key:
            max = mid - 1
        # key在数组右边
        elif cmp_key < int_key:
            min = mid + 1
        # key在数组中间
        elif cmp_key == int_key:
            return lista[mid]
    return ""

def PreOpenRouterIpFiles():
    global rfs_router_ip
    rfs_router_ip = []
    for i in range(0, 256):
        rfs_router_ip.append(open('../../data/mi/sorted_ip2node_' + str(i), 'r'))

def CloseRouterIpFiles():
    global rfs_router_ip
    for rf in rfs_router_ip:
        rf.close()
    rfs_router_ip = []

pre_router_ip_dict_dict = dict()
def PreConstrRouterIpDict(pre_seg_list):
    global pre_router_ip_dict_dict
    for pre_seg in pre_seg_list:
        filename = '../../data/mi/sorted_ip2node_' + pre_seg
        rf = open(filename, 'r')
        content = rf.read().strip('\n').strip(',')
        if not content:
            continue
        ip_list = content.split(',')
        pre_router_ip_dict_dict[pre_seg] = dict()
        for tmp in ip_list:
            map_info = tmp.split(' ')
            #if len(map_info) < 2:
                #print(content)
                #print(ip_list)
                #print(pre_seg)
                #print(tmp)
            pre_router_ip_dict_dict[pre_seg][map_info[0]] = map_info[1]
        rf.close()

def ClearRouterIpDict():
    global pre_router_ip_dict_dict
    pre_router_ip_dict_dict.clear()

cache_route_ip_dict = dict()
'''
def GetRouterOfIpByMi(ip, use_tmp_cache = False):  #从midar-iff中获取数据
    if use_tmp_cache:
        if ip in cache_route_ip_dict.keys():
            return cache_route_ip_dict[ip]
    #print(ip)
    pre = ip[0:ip.find('.')]
    rem = ip[ip.find('.') + 1:]
    if pre in pre_router_ip_dict_dict.keys():
        if rem in pre_router_ip_dict_dict[pre].keys():
            return pre_router_ip_dict_dict[pre][rem]
        else:
            return ""
    rf = None
    if not rfs_router_ip:
        rf = open('..\\srcdata\\mi\\sorted_ip2node_' + pre, 'r')
    else:
        #print(pre)
        rf = rfs_router_ip[int(pre)]
        rf.seek(0)
    content = rf.read().strip('\n').strip(',')
    if not rfs_router_ip:
        rf.close()
    if not content:
        return ""
    ip_list = content.split(',')
    find = BinarySearch(ip_list, ip[len(pre)+1:])
    if find:
        val = find.split(' ')[1]        
        if use_tmp_cache:
            cache_route_ip_dict[ip] = val
        return val
    return ""
'''
def GetRouterOfIpByMi(ip, use_tmp_cache = False):
    global db_cursor
    global cur_midar_table_date
    if cur_midar_table_date == '': #未定义
        print('cur_midar_table_date not set. return')
        return None
    if not ip.__contains__('.'):
        print('NOTE! ip error! ' + ip)
        return None
    first_seg = ip[0:ip.index('.')]
    index = int(int(first_seg) / 16)
    prefix = ip[0:ip.rindex('.')]
    ip_suffix = ip[ip.rindex('.') + 1:]
    select_sql = "SELECT nodeid FROM midarip2node_%s_%s WHERE prefix24='%s' AND ip='%s'" %(cur_midar_table_date, str(index), prefix, ip_suffix)
    db_cursor.execute(select_sql)
    res = db_cursor.fetchall() #当没找到的时候，返回0
    if res: #找到        
        return res[0][0]
    return None

'''
router_geo_dict = dict() #used as a cache
def GetGeoOfRouterByMi(router):
    if router in router_geo_dict.keys(): #cache hit
        return router_geo_dict[router]
    rf = open('mi\\node2geo_', 'r', encoding='utf-8')
    curline = rf.readline()
    while curline:
        if curline.startswith('#'):
            curline = rf.readline()
            continue
        elems = curline.split(':')
        temp_router = elems[0][len(str('node.geo ')):]
        if temp_router == router:
            geo = re.sub('[a-z].*', '', elems[1]) #elems[1]: "node.geo N1:					43.6319	-79.3716			maxmind"
            geo = re.sub('\t', ' ', geo)
            geo = geo.strip('\n')
            geo = geo.strip(' ')
            router_geo_dict[router] = geo
            return geo
        curline = rf.readline()
    rf.close()
    return ""  
'''

def PreOpenRouterGeoFiles():
    global rfs_router_geo
    rfs_router_geo = []
    for i in range(0, 100):
        rfs_router_geo.append(open('../../data/mi/node2geo_' + str(i), 'r'))

def CloseGeoRouterFiles():
    global rfs_router_geo
    for rf in rfs_router_geo:
        rf.close()
    rfs_router_geo = []

max_router_geo_dict_size = 10000
router_geo_dict = dict() #val: [as, freq]
'''
def GetGeoOfRouterByMi(router):  #从midar-iff中获取数据
    if not router:
        return ""
    if router in router_geo_dict.keys():
        router_geo_dict[router][1] += 1
        return router_geo_dict[router][0]
    rf = None
    if not rfs_router_geo:
        #print('NOTICE')
        rf = open('..\\srcdata\\mi\\node2geo_' + str(int(router) % 100), 'r')
    else:
        rf = rfs_router_geo[int(router) % 100]
        rf.seek(0)
    content = rf.read().strip('\n').strip(',')
    if not rfs_router_geo:
        rf.close()
    if not content:
        return ""
    router_list = content.split(',')
    while True:
        if re.search('[a-zA-Z]', router_list[-1]):
            router_list.pop()
        else:
            break
    find = BinarySearch_2(router_list, router)
    if find:
        geo = find[find.index(' ') + 1:]
        if len(router_geo_dict) < max_router_geo_dict_size:
            router_geo_dict[router] = [geo, 1]
        else:
            pass #这里想不出好方法了，暂时放弃
        return geo
    return "" 
'''
def GetGeoOfRouterByMi(router):  #从midar-iff中获取数据
    global db_cursor
    global cur_midar_table_date
    if cur_midar_table_date == '': #未定义
        print('cur_midar_table_date not set. return')
        return None
    index = int(router) % 10
    select_sql = "SELECT geo FROM midarnode2geo_%s_%s WHERE nodeid='%s'" %(cur_midar_table_date, str(index), router)
    #print(select_sql)
    db_cursor.execute(select_sql)
    res = db_cursor.fetchall() #当没找到的时候，返回0
    if res: #找到        
        return res[0][0]
    return None

geo_cache = dict()
def InitGeoCache():
    global geo_cache
    geo_cache.clear()

def GetGeoOfIpByMi(ip):
    global geo_cache
    if ip in geo_cache.keys():
        return geo_cache[ip] #hit
        
    router = GetRouterOfIpByMi(ip)
    #print("router: %s" %router)
    if not router:
        geo_cache[ip] = None
        return None
    #print(router)
    geo = GetGeoOfRouterByMi(router)
    #print("asn: %s" %asn)
    geo_cache[ip] = geo
    return geo


#公式计算两点间距离(km)
#lng1,lat1,lng2,lat2 = (120.12802999999997,30.28708,115.86572000000001,28.7427)
#def GeoDistance(lat1,lng1,lat2,lng2):
none_distance = 1000000
def GeoDistance(geo1, geo2):
    #if geo1.__contains__('EU DE 05 F'):
        #print('')
    #print(geo1)
    #print(geo2)
    if not geo1 or not geo2:
        return none_distance
    if len(geo1.split(' ')) < 2 or len(geo1.split(' ')) < 2:
        print("geo1: %s" %geo1)
        print("geo2: %s" %geo2)
        return none_distance
    (lat1,lng1) = geo1.split(' ')
    (lat2,lng2) = geo2.split(' ')
    lng1, lat1, lng2, lat2 = map(radians, [float(lng1), float(lat1), float(lng2), float(lat2)]) # 经纬度转换成弧度
    dlon=lng2-lng1
    dlat=lat2-lat1
    a=sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    distance=2*asin(sqrt(a))*6371*1000 # 地球平均半径，6371km
    distance=round(distance/1000,3)
    return distance

'''
router_as_dict = dict()
def GetAsOfRouterByMi(router):
    if router in router_as_dict.keys(): #cache hit
        return router_as_dict[router]
    rf = open('..\\midar-iff.nodes.as', 'r', encoding='utf-8')
    curline = rf.readline()
    while curline:
        elems = curline.split(' ')
        if elems[1] == router:
            router_as_dict[router] = elems[2]
            return elems[2]
        curline = rf.readline()
    rf.close()
    return ""   
'''

def PreOpenAsRouterFiles():
    global rfs_as_router
    rfs_as_router = []
    for i in range(0, 100):
        rfs_as_router.append(open('../../data/mi/sorted_node2as_' + str(i), 'r'))

def CloseAsRouterFiles():
    global rfs_as_router
    for rf in rfs_as_router:
        rf.close()
    rfs_as_router = []

max_router_as_dict_size = 10000
router_as_dict = dict() #val: [as, freq]
'''
def GetAsOfRouterByMi(router):  #从midar-iff中获取数据
    if not router:
        return ""
    if router in router_as_dict.keys():
        #print('h')
        router_as_dict[router][1] += 1
        return router_as_dict[router][0]
    rf = None
    if not rfs_as_router:
        rf = open('..\\srcdata\\mi\\sorted_node2as_' + str(int(router) % 100), 'r')
    else:
        rf = rfs_as_router[int(router) % 100]
        rf.seek(0)
    content = rf.read().strip('\n').strip(',')
    if not rfs_as_router:
        rf.close()
                    GetDstIpIntSet, ClearDstIpIntSet, GetPathAsDict, GetBgpPathByAs, ClearPathAsDict, ConnectToDb, \
    if not content:
        return ""
    router_list = content.split(',')
    find = BinarySearch(router_list, router)
    if find:
        asn = find.split(' ')[1]
        if len(router_as_dict) < max_router_as_dict_size:
            router_as_dict[router] = [asn, 1]
        else:
            pass #这里想不出好方法了，暂时放弃
        return asn
    return ""
'''
def GetAsOfRouterByMi(router):  #从midar-iff中获取数据
    global db_cursor
    global cur_midar_table_date
    if cur_midar_table_date == '': #未定义
        print('cur_midar_table_date not set. return')
        return None
    index = int(router) % 10
    select_sql = "SELECT asn FROM midarnode2as_%s_%s WHERE nodeid='%s'" %(cur_midar_table_date, str(index), router)
    #print(select_sql)
    db_cursor.execute(select_sql)
    res = db_cursor.fetchall() #当没找到的时候，返回0
    if res: #找到        
        return res[0][0]
    return None

midar_cache = dict()
def InitMidarCache():
    global midar_cache
    midar_cache.clear()

cur_midar_table_date = ''
def SetCurMidarTableDate(year, month):
    global cur_midar_table_date
    global db_cursor
    db_cursor.execute("SHOW TABLES")
    res = db_cursor.fetchall() #当没找到的时候，返回0
    nearest_month = 0
    if res: #找到     
        pre = 'midarip2node_' + str(year)
        pre_len = len(pre)
        min_dist = 12
        for elem in res:   
            if elem[0].__contains__(pre) and elem[0].__contains__('_10'):
                cur_month = int(elem[0][pre_len:pre_len + 2])
                if abs(cur_month - month) < min_dist:
                    min_dist = abs(cur_month - month)
                    nearest_month = cur_month
    nearest_month_str = str(nearest_month)
    if nearest_month < 10:
        nearest_month_str = '0' + nearest_month_str
    cur_midar_table_date = str(year) + nearest_month_str
    #print(cur_midar_table_date)   

mi_dates = set()
def SetCurMidarTableDate_2(_date):
    global mi_dates
    global cur_midar_table_date
    if not mi_dates:
        for filename in os.listdir('/mountdisk1/ana_c_d_incongruity/midar_data/'):
            if filename.startswith('20'):
                #2018-03
                mi_dates.add(filename[:4] + filename[5:7] + '01')
    closest_date = GetClosestDate(_date, mi_dates)
    cur_midar_table_date = closest_date[:6]
    print('select mi date: %s' %cur_midar_table_date)

def GetAsOfIpByMi(ip, use_tmp_cache = False):
    global midar_cache
    if ip in midar_cache.keys():
        return midar_cache[ip] #hit
    
    #if ip == "124.215.192.22" or ip == "206.51.71.170":
        #print('')    
    router = GetRouterOfIpByMi(ip, use_tmp_cache)
    #print("router: %s" %router)
    if not router:
        midar_cache[ip] = None
        return None
    #print(router)
    asn = GetAsOfRouterByMi(router)
    #print("asn: %s" %asn)
    midar_cache[ip] = asn
    return asn

'''
customer_dict_then = dict()
provider_dict_then = dict()
peer_dict_then = dict()
customer_dict_newest = dict()
provider_dict_newest = dict()
peer_dict_newest = dict()
def GetOneAsRel(filename, provider_dict, customer_dict, peer_dict):
    rf_rel = open(filename, 'r')
    cur_line = rf_rel.readline()
    while cur_line:
        if cur_line[0] == '#':
            pass
        else:
            cur_line = cur_line.strip("\n")
            elems = cur_line.split('|')
            if elems[2] == "-1":
                if not elems[0] in customer_dict.keys():
                    customer_dict[elems[0]] = []
                customer_dict[elems[0]].append(elems[1])
                if not elems[1] in provider_dict.keys():
                    provider_dict[elems[1]] = []
                provider_dict[elems[1]].append(elems[0])
            elif elems[2] == "0":
                if not elems[0] in peer_dict.keys():
                    peer_dict[elems[0]] = []
                peer_dict[elems[0]].append(elems[1])
                if not elems[1] in peer_dict.keys():
                    peer_dict[elems[1]] = []
                peer_dict[elems[1]].append(elems[0])
        cur_line = rf_rel.readline()
    rf_rel.close()  
'''
rel_dict = dict()
def GetOneAsRel(filename):
    global rel_dict
    rf_rel = open(filename, 'r')
    cur_line = rf_rel.readline()
    while cur_line:
        if cur_line[0] == '#':
            pass
        else:
            cur_line = cur_line.strip("\n")
            elems = cur_line.split('|')
            rel_dict[elems[0] + ' ' + elems[1]] = int(elems[2])
        cur_line = rf_rel.readline()
    rf_rel.close() 

as_neighs_dict = dict()
def GetAsNeighs(year, month):
    global as_neighs_dict
    filename = GetCloseDateFile(year, month, global_var.rel_flag)
    filepath = global_var.par_path +  global_var.rel_cc_dir + filename
    rf_rel = open(filepath, 'r')
    cur_line = rf_rel.readline()
    while cur_line:
        if cur_line[0] == '#':
            pass
        else:
            (fst_as, snd_as, rel) = cur_line.strip("\n").split('|')
            if fst_as not in as_neighs_dict.keys():
                as_neighs_dict[fst_as] = set()
            as_neighs_dict[fst_as].add(snd_as)
            if snd_as not in as_neighs_dict.keys():
                as_neighs_dict[snd_as] = set()
            as_neighs_dict[snd_as].add(fst_as)
        cur_line = rf_rel.readline()
    rf_rel.close() 

def GetAsNeighsFromDict(asn):
    global as_neighs_dict
    if asn not in as_neighs_dict.keys():
        print('%s not in as_rel_num_dict' %asn)
        return set()
    return as_neighs_dict[asn]

def ClearAsNeighs():
    global as_neighs_dict
    as_neighs_dict.clear()

def GetAsRel(year, month): #2021.5.29修改
    #GetOneAsRel(as_rel_file_name, provider_dict_then, customer_dict_then, peer_dict_then)
    #GetOneAsRel(as_rel_file_name_2, provider_dict_newest, customer_dict_newest, peer_dict_newest)
    rel_filename = global_var.par_path + global_var.rel_cc_dir + GetCloseDateFile(year, month, global_var.rel_flag)
    GetOneAsRel(rel_filename)

rel_dict_2 = dict()
def GetAsRel_2(filename): #建立新的数据库，以每个AS为key，不以link为key
    global rel_dict_2
    rf_rel = open(filename, 'r')
    cur_line = rf_rel.readline()
    while cur_line:
        if cur_line[0] == '#':
            pass
        else:
            (as1, as2, rel, src) = cur_line.strip('\n').split('|')
            rel_int = int(rel)
            if as1 not in rel_dict_2.keys():
                rel_dict_2[as1] = []
            rel_dict_2[as1].append([as2, rel_int])
            if as2 not in rel_dict_2.keys():
                rel_dict_2[as2] = []
            rel_dict_2[as2].append([as1, -1 * rel_int])
        cur_line = rf_rel.readline()
    rf_rel.close() 

def GetNeighOfAs(asn):
    global rel_dict_2
    neighs = set()
    if asn in rel_dict_2.keys():
        for elem in rel_dict_2[asn]:
            neighs.add(elem[0])
    return neighs

def ClearAsRel_2():
    global rel_dict_2
    rel_dict_2.clear()

lg_url_dict = dict()
def GetLgUrlDict():
    global lg_url_dict
    filename = 'lg_conn_res_succeed_with_traceroute_valid'
    if not os.path.exists(filename):
        tmp_dict = dict()
        with open('lg', 'r') as rf:
            for curline in rf.read().strip('\n').split('\n'):
                [asn, url] = curline.split('\t')
                if url not in tmp_dict.keys():
                    tmp_dict[url] = set()
                tmp_dict[url].add(asn)
        with open('lg_conn_res_succeed_with_traceroute_checked', 'r'):
            for curline in rf.read().strip('\n').split('\n'):
                if curline.__contains__('#'): #未尝试成功
                    continue
                for asn in tmp_dict[curline]:
                    if asn not in lg_url_dict.keys():
                        lg_url_dict[asn] = set()
                    lg_url_dict[asn].add(curline)
        with open(filename, 'w') as wf:
            for (asn, urls) in lg_url_dict.items():
                wf.write('%s]%s\n' %(asn, ','.join(list(urls))))
    else:
        with open(filename, 'r') as rf:
            for curline in rf.read().strip('\n').split('\n'):
                [asn, urls] = curline.split(']')
                lg_url_dict[asn] = set(urls.split(','))

def GetUrlsOfLg(asn):
    global lg_url_dict
    if asn in lg_url_dict.keys():
        return lg_url_dict[asn]
    return None

def ClearLgUrlDict():
    global lg_url_dict
    lg_url_dict.clear()

tr_lg_func_dict = dict()
def GetFuncLgDict():
    GetLgUrlDict()
    global tr_lg_func_dict
    with open('traceroute_lg.py', 'r') as rf:
        for curline in rf.read().split('\n'):
            if curline.startswith('def tr_'): #tr_函数入口
                func_name = curline.split('(')[0][4:]
                url = curline.split('#')[1]
                tr_lg_func_dict[url] = func_name

def GetFuncOfLg(asn):
    global tr_lg_func_dict
    funcs = []
    urls = GetUrlsOfLg(asn)
    for url in urls:
        if url in tr_lg_func_dict.keys():
            funcs.append(tr_lg_func_dict[url])
    return funcs

def ClearFuncLgDict():
    global tr_lg_func_dict
    ClearLgUrlDict()
    tr_lg_func_dict.clear()

'''
def ClearOneAsRel(provider_dict, customer_dict, peer_dict):
    provider_dict.clear()
    customer_dict.clear()
    peer_dict.clear()
'''

def ClearAsRel():
    #ClearOneAsRel(provider_dict_then, customer_dict_then, peer_dict_then)
    #ClearOneAsRel(provider_dict_newest, customer_dict_newest, peer_dict_newest)
    global rel_dict
    rel_dict.clear()

as_pfx_dict = dict()
def GetAsPfxDict():
    global as_pfx_dict
    with open(global_var.par_path + global_var.rib_dir + 'coalesced/routeviews-rv2-20210815-1200.pfx2as', 'r') as rf:
        for curline in rf.read().split('\n'):
            [pfx, pfx_len, asn] = curline.split('\t')
            if asn not in as_pfx_dict.keys():
                as_pfx_dict[asn] = set
            as_pfx_dict[asn].add(pfx + '/' + pfx_len)
    with open(global_var.par_path + global_var.rib_dir + 'coalesced/routeviews-rv6-20210815-1600.pfx2as', 'r') as rf:
        for curline in rf.read().split('\n'):
            [pfx, pfx_len, asn] = curline.split('\t')
            if asn not in as_pfx_dict.keys():
                as_pfx_dict[asn] = set
            as_pfx_dict[asn].add(pfx + '/' + pfx_len)

def GetRepIpsOfAs(asn):
    global as_pfx_dict
    ips = set()
    if asn in as_pfx_dict.keys():
        for pfx in as_pfx_dict[asn]:
            ip = pfx.split('/')[0]
            delim = '.'
            if ip.__contains__(':'): #ipv6 address
                delim = ':'                
            last_ip_seg = ip.split(delim)[-1]
            new_last_ip_seg = '1' #default
            if last_ip_seg:
                new_last_ip_seg = str(int(last_ip_seg) + 1)
            ips.add(ip[:ip.rindex(delim) + delim + new_last_ip_seg])
    return ips

def ClearAsPfxDict():
    global as_pfx_dict
    as_pfx_dict.clear()

def GetAsConnDegree(asn, use_newest_dict):
    provider_num = 0
    customer_num = 0
    peer_num = 0
    if asn in provider_dict_then.keys():
        provider_num += len(provider_dict_then[asn])
    if asn in customer_dict_then.keys():
        customer_num += len(customer_dict_then[asn])
    if asn in peer_dict_then.keys():
        peer_num += len(peer_dict_then[asn])
    if use_newest_dict:
        if asn in provider_dict_newest.keys():
            provider_num += len(provider_dict_newest[asn])
        if asn in customer_dict_newest.keys():
            customer_num += len(customer_dict_newest[asn])
        if asn in peer_dict_newest.keys():
            peer_num += len(peer_dict_newest[asn])
    return (provider_num, customer_num, peer_num)

def IsPeer(as1, as2):
    #if IsPc(as1, as2) != 2: #是PC关系 #考虑到效率，这一步不做，外部在调用该函数之前，应先调用IsPc
        #return False
    if False:
        if (as1 in peer_dict_then.keys() and as2 in peer_dict_then[as1]) or \
            (as1 in peer_dict_newest.keys() and as2 in peer_dict_newest[as1]) or \
            (as2 in peer_dict_then.keys() and as1 in peer_dict_then[as2]) or \
            (as2 in peer_dict_newest.keys() and as1 in peer_dict_newest[as2]):
            return True
        return False
    global rel_dict
    key = as1 + ' ' + as2
    if key in rel_dict.keys() and rel_dict[key] == 0:
        return True
    key = as2 + ' ' + as1
    if key in rel_dict.keys() and rel_dict[key] == 0:
        return True
    return False
    
def IsPeer_2(as1, as2): #考虑moas的情况
    for cur_as1 in as1.split('_'):
        for cur_as2 in as2.split('_'):
            if IsPeer(cur_as1, cur_as2):
                return True
    return False

def IsPc(as1, as2): #as1是as2的customer，或as2是as1的customer
    if False:
        if (as1 in customer_dict_then.keys() and as2 in customer_dict_then[as1]) or \
            (as1 in customer_dict_newest.keys() and as2 in customer_dict_newest[as1]):
            return -1
        if (as2 in customer_dict_then.keys() and as1 in customer_dict_then[as2]) or \
            (as2 in customer_dict_newest.keys() and as1 in customer_dict_newest[as2]):
            return 1
        return 2    
    global rel_dict
    key = as1 + ' ' + as2
    if key in rel_dict.keys() and rel_dict[key] == -1:
        return True
    key = as2 + ' ' + as1
    if key in rel_dict.keys() and rel_dict[key] == -1:
        return True
    return False

def IsPc_2(as1, as2): #考虑moas的情况
    for cur_as1 in as1.split('_'):
        for cur_as2 in as2.split('_'):
            res = IsPc(cur_as1, cur_as2)
            if res != 2:
                return res
    return 2

sib_dict = dict() #这个sib_dict其实是as2org_dict
def GetSibRel(year, month):
    global sib_dict
    if False:   #2021.5.6 做时间系列化测试时注释掉，重写函数
        global sib_dict
        rf = open(as2org_filename, 'r', encoding='utf-8')
        curline = rf.readline()
        while curline:
            curline = curline.strip('\n')
            elems = curline.split(':')
            sib_dict[elems[0]] = elems[1]
            curline = rf.readline()
        rf.close()
    as2org_filename = global_var.par_path + global_var.as2org_dir + GetCloseDateFile(year, month, global_var.as2org_flag)
    #print(as2org_filename)
    rf = open(as2org_filename, 'r', encoding='utf-8')
    tmp_dict = dict()
    curline = rf.readline()
    while curline:
        if curline.startswith('#'):
            curline = rf.readline()
            continue
        elems = curline.split('|')
        if len(elems) == 5: #format: org_id|changed|name|country|source            
            tmp_dict[elems[0]] = elems[2]
        elif len(elems) == 6:   #format: aut|changed|aut_name|org_id|opaque_id|source
            sib_dict[elems[0]] = elems[3]
        else:
            print("Format error in %s. Exit" %as2org_filename)
        curline = rf.readline()
    rf.close()
    #print('read file end')
    for key in sib_dict.keys():
        if sib_dict[key] in tmp_dict.keys():
            sib_dict[key] = tmp_dict[sib_dict[key]]
    tmp_dict.clear()

def GetSibRel_2(year, month):
    global sib_dict
    dates = []
    for filename in os.listdir('/mountdisk1/ana_c_d_incongruity/as_org_data/'):
        dates.append(filename[:filename.index('.')])
    closest_date = GetClosestDate(str(year) + str(month).zfill(2) + '01', dates)
    rf = open('/mountdisk1/ana_c_d_incongruity/as_org_data/' + closest_date + '.as-org2info.txt', 'r', encoding='utf-8')
    tmp_dict = dict()
    curline = rf.readline()
    while curline:
        if curline.startswith('#'):
            curline = rf.readline()
            continue
        elems = curline.split('|')
        if len(elems) == 5: #format: org_id|changed|name|country|source            
            tmp_dict[elems[0]] = elems[2]
        elif len(elems) == 6:   #format: aut|changed|aut_name|org_id|opaque_id|source
            sib_dict[elems[0]] = elems[3]
        else:
            print("Format error. Exit")
        curline = rf.readline()
    rf.close()
    #print('read file end')
    for key in sib_dict.keys():
        if sib_dict[key] in tmp_dict.keys():
            sib_dict[key] = tmp_dict[sib_dict[key]]
    tmp_dict.clear()

def ClearSibRel():
    global sib_dict
    sib_dict.clear()

def IsSib(as1, as2):
    if as1 in sib_dict.keys() and as2 in sib_dict.keys() and \
        sib_dict[as1] == sib_dict[as2]:
        return True
    return False

def IsSib_2(as1, as2): #考虑moas的情况
    for cur_as1 in as1.split('_'):
        for cur_as2 in as2.split('_'):
            if IsSib(cur_as1, cur_as2):
                return True
    return False

def GetOrgOfAS(asn):
    global sib_dict
    if asn in sib_dict.keys():
        return sib_dict[asn]
    return ''

sib_multi_files_dict = dict()
def GetSibRelByMultiDataFiles_Unit(as2org_filename):
    global sib_multi_files_dict
    rf = open(as2org_filename, 'r', encoding='utf-8')
    tmp_dict1 = dict()
    tmp_dict2 = dict()
    curline = rf.readline()
    while curline:
        if curline.startswith('#'):
            curline = rf.readline()
            continue
        elems = curline.split('|')
        if len(elems) == 5: #format: org_id|changed|name|country|source            
            tmp_dict1[elems[0]] = elems[2]
        elif len(elems) == 6:   #format: aut|changed|aut_name|org_id|opaque_id|source
            tmp_dict2[elems[0]] = elems[3]
        else:
            print("Format error in %s. Exit" %as2org_filename)
        curline = rf.readline()
    rf.close()
    #print('read file end')
    for key in tmp_dict2.keys():
        if tmp_dict2[key] in tmp_dict1.keys():
            if key not in sib_multi_files_dict.keys():
                sib_multi_files_dict[key] = set()
            sib_multi_files_dict[key].add(tmp_dict1[tmp_dict2[key]])
    tmp_dict1.clear()
    tmp_dict2.clear()

def GetSibRelByMultiDataFiles(year, month):
    global sib_multi_files_dict
    as2org_filename_set = set()
    for cur_month in range(month, 13):
        as2org_filename = global_var.par_path + global_var.as2org_dir + GetCloseDateFile(year, cur_month, global_var.as2org_flag)
        if as2org_filename not in as2org_filename_set:
            as2org_filename_set.add(as2org_filename)
            GetSibRelByMultiDataFiles_Unit(as2org_filename)
    for cur_year in range(year + 1, 2022):
        for cur_month in range(1, 13):
            as2org_filename = global_var.par_path + global_var.as2org_dir + GetCloseDateFile(cur_year, cur_month, global_var.as2org_flag)
            if as2org_filename not in as2org_filename_set:
                as2org_filename_set.add(as2org_filename)
                GetSibRelByMultiDataFiles_Unit(as2org_filename)

def ClearSibRelByMultiDataFiles():
    global sib_multi_files_dict
    sib_multi_files_dict.clear()

def IsSibByMultiDataFiles(as1, as2, record = None):
    global sib_multi_files_dict
    if as1 in sib_multi_files_dict.keys() and as2 in sib_multi_files_dict.keys():
        return TwoOrgSetJoin(sib_multi_files_dict[as1], sib_multi_files_dict[as2], record)
    return False

def IsSibByMultiDataFiles_2(as1, as2, record = None): #考虑moas的情况
    for cur_as1 in as1.split('_'):
        for cur_as2 in as2.split('_'):
            if IsSibByMultiDataFiles(cur_as1, cur_as2, record):
                return True
    return False

def GetOrgByMultiDataFiles(asn):
    global sib_multi_files_dict
    if asn in sib_multi_files_dict.keys():
        return sib_multi_files_dict[asn]
    return set()

def GetOrgByMultiDataFiles_2(asn): #考虑moas的情况
    res = set()
    for cur_as in asn.split('_'):
        res |= GetOrgByMultiDataFiles(cur_as)
    return res

def Get2AsRel(as1, as2): #不考虑moas的情况，上层考虑
    if as1 == as2:
        return 100
    if IsSib(as1, as2):
        return 2
    global rel_dict
    key = as1 + ' ' + as2
    if key in rel_dict.keys():
        rel = rel_dict[key]
        if rel == 0:
            rel = -2    #peer定值为-2，这样好比
        return rel
    key = as2 + ' ' + as1
    if key in rel_dict.keys():
        rel = rel_dict[key]
        if rel == 0:
            rel = -2    #peer定值为-2，这样好比
        elif rel == -1: #as2是as1的provider
            rel = 1
        return rel
    return -100

def Get2AsRel_2(as1, as2): #考虑moas的情况
    max_rel = -100
    for elem1 in as1.split('_'):
        for elem2 in as2.split('_'):
            rel = Get2AsRel(elem1, elem2)
            if rel > max_rel:
                max_rel = rel
    return max_rel

def GetPrefixOfIp(ip, mask_len):
    ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton(ip))[0])
    mask = 0xFFFFFFFF - (1 << (32 - mask_len)) + 1
    cur_prefix_int = ip_int & mask
    cur_prefix = str(socket.inet_ntoa(struct.pack('I',socket.htonl(cur_prefix_int))))
    cur_prefix = cur_prefix + '/' + str(mask_len)
    return cur_prefix

def WriteIxpPrefixToFile():
    rf = open('../../data/ixs_201904.jsonl', 'r', encoding='utf-8')
    wf = open('../../data/res_ixs_prefix', 'w')
    curline = rf.readline()
    while curline:
        if not curline.startswith('#'):
            obj = ujson.loads(curline)
            if obj.__contains__('prefixes'):
                pfx_obj = obj['prefixes']
                if pfx_obj.__contains__('ipv4'):
                    raw = pfx_obj['ipv4']
                    prefix_str = re.sub('[\[\]\']', '', str(raw))
                    prefix_list = prefix_str.split(', ')
                    for prefix in prefix_list:
                        if prefix:
                            wf.write("%s\n" %prefix)
        curline = rf.readline()
    rf.close()
    wf.close()

def SuppleIxpPrefixByIxAsn():
    rf = open('../../data/ix-asns_201904.jsonl', 'r', encoding='utf-8')
    rf_ori = open('../../data/res_ixs_prefix', 'r')
    wf = open('../../data/res_ixs_prefix_2', 'w')

    prefix_dict = dict()
    curline = rf_ori.readline()
    while curline:
        wf.write(curline)
        curline = curline.strip('\n')
        elems = curline.split('/')
        if elems[1] not in prefix_dict.keys():  #以mask_len作为key，在做ip匹配的时候可以加快速度
            prefix_dict[elems[1]] = set()
        prefix_dict[elems[1]].add(curline)
        curline = rf_ori.readline()
    rf_ori.close()

    match_num = 0
    not_match_num = 0
    curline = rf.readline()
    while curline:
        if not curline.startswith('#'):
            obj = ujson.loads(curline)
            if obj.__contains__('ipv4'):
                raw = obj['ipv4']
                ip_str = re.sub('[\[\]\']', '', str(raw))
                ip_list = ip_str.split(', ')
                for ip in ip_list:
                    if ip:
                        #print(ip)
                        find_prefix = False
                        for mask_len in prefix_dict.keys():
                            prefix = GetPrefixOfIp(ip, int(mask_len))
                            if prefix in prefix_dict[mask_len]:
                                find_prefix = True
                                break
                        if not find_prefix:
                            new_prefix = ip + '/32'                          
                            wf.write("%s\n" %new_prefix)
                            not_match_num += 1
                        else:
                            match_num += 1
        curline = rf.readline()
    rf.close()
    wf.close()
    print("not_match_num: %d" %not_match_num)
    print("match_num: %d" %match_num)

ixp_prefix_dict = dict()
ixp_ip_dict = dict()
def GetIxpPfxDict(year, month):
    global ixp_prefix_dict
    global ixp_ip_dict
    if False:   #2021.5.6 做时间系列化测试时注释掉，重写函数
        rf = open('../../data/res_ixs_prefix_2', 'r')
        curline = rf.readline()
        while curline:
            curline = curline.strip('\n')
            elems = curline.split('/')
            if elems[1] not in ixp_prefix_dict.keys():  #以mask_len作为key，在做ip匹配的时候可以加快速度
                ixp_prefix_dict[elems[1]] = set()
            ixp_prefix_dict[elems[1]].add(curline)
            curline = rf.readline()
        rf.close()
    peeringdb_filename = global_var.par_path + global_var.peeringdb_dir + GetCloseDateFile(year, month, global_var.peeringdb_flag)
    #print(peeringdb_filename)
    ixp = create_peeringdb(peeringdb_filename)
    #ixp_prefixes = [(prefix, asn) for prefix, asn in ixp.prefixes.items()]
    for prefix in ixp.prefixes.keys():
        prefix.strip(' ')
        prefix = prefix.strip(' ').split(' ')[0] #有些格式不对：有的前面有空格，有的在ip地址后面有加了一些注释信息
        if prefix.__contains__(':'):
            continue #filter IPv6 address
        mask_len_str = prefix[prefix.index('/') + 1:]
        if mask_len_str not in ixp_prefix_dict.keys():  #以mask_len作为key，在做ip匹配的时候可以加快速度
            ixp_prefix_dict[mask_len_str] = set()
        ixp_prefix_dict[mask_len_str].add(prefix)
    ixp_ip_dict.clear()

g_ixp = {}
def GetIxpPfxDict_2(year, month):
    global ixp_prefix_dict
    global g_ixp
    for filename in os.listdir('/mountdisk1/ana_c_d_incongruity/peeringdb_data/'):
        if filename.__contains__(str(year) + '_' + str(month).zfill(2)):
            g_ixp = create_peeringdb('/mountdisk1/ana_c_d_incongruity/peeringdb_data/' + filename)
            break
    for prefix in g_ixp.prefixes.keys():
        prefix.strip(' ')
        prefix = prefix.strip(' ').split(' ')[0] #有些格式不对：有的前面有空格，有的在ip地址后面有加了一些注释信息
        if prefix.__contains__(':'):
            continue #filter IPv6 address
        mask_len_str = prefix[prefix.index('/') + 1:]
        if mask_len_str not in ixp_prefix_dict.keys():  #以mask_len作为key，在做ip匹配的时候可以加快速度
            ixp_prefix_dict[mask_len_str] = set()
        ixp_prefix_dict[mask_len_str].add(prefix)

def GetPeerASNByIp(ip):
    global ixp_prefix_dict
    global g_ixp
    prefix = None
    res = set()
    for mask_len in ixp_prefix_dict.keys():
        #print("mask_len: %s" %mask_len)
        tmp = GetPrefixOfIp(ip, int(mask_len))
        if tmp in ixp_prefix_dict[mask_len]:
            prefix = tmp
            break
    if prefix:
        ixid = g_ixp.prefixes[prefix]
        for (ipaddr4, asn) in g_ixp.ixid_addrasns[ixid]:
            res.add(asn)
    return res

def ClearIxpPfxDict():
    global ixp_prefix_dict
    global ixp_ip_dict
    ixp_prefix_dict.clear()
    ixp_ip_dict.clear()

def IsIxpIp(ip):
    global ixp_prefix_dict
    global ixp_ip_dict
    if ip in ixp_ip_dict.keys():
        return ixp_ip_dict[ip]
    find_prefix = False
    for mask_len in ixp_prefix_dict.keys():
        #print("mask_len: %s" %mask_len)
        prefix = GetPrefixOfIp(ip, int(mask_len))
        if prefix in ixp_prefix_dict[mask_len]:
            find_prefix = True
            break
    ixp_ip_dict[ip] = find_prefix
    return find_prefix

def IsIXPName(name):
    elems = name.split(' ')
    if 'IX' in elems or 'IXP' in elems or '-IX' in elems or '-IXP' in elems or 'IX-' in elems or 'IXP-' in elems:
        return True
    if name.__contains__("Internet Exchange"):
        return True
    if name.__contains__("Inter-Exchange"):
        return True
    return False

ixp_as_set = set()
def GetIxpAsSet(date=None):
    global ixp_as_set
    if False:   #2021.5.6 做时间系列化测试时注释掉，重写函数
        rf = open("../../data/as_OrgIXP", "r", encoding='utf-8')
        curline = rf.readline()
        while curline:
            elems = curline.split(':')
            ixp_as_set.add(elems[0])
            curline = rf.readline()
        rf.close()

    if ixp_as_set:
        return
    
    global sib_dict
    if not sib_dict:
        if not date:
            return
        GetSibRel_2(int(date[:4]), int(date[4:6]))
    for (asn, org) in sib_dict.items():
        if IsIXPName(org):
            ixp_as_set.add(asn)  

    if date:
        if date[:6] == '201801' or date[:6] == '201802':
            date = '201803' + date[6:]
        #print(date)
        PreGetSrcFilesInDirs()
        peeringdb_filename = global_var.par_path + global_var.peeringdb_dir + GetCloseDateFile(int(date[:4]), int(date[4:6]), global_var.peeringdb_flag)
        with open(peeringdb_filename, 'r') as rf:
            data = json.load(rf)
            for net in data['net']['data']:
                if IsIXPName(net['name']):
                    ixp_as_set.add(str(net['asn']))
    
    as_rel_file_name = global_var.par_path + global_var.rel_cc_dir + GetCloseDateFile(int(date[:4]), int(date[4:6]), global_var.rel_flag)
    #print(as_rel_file_name)
    with open(as_rel_file_name, 'r') as rf:
        for line in rf:
            if line.__contains__('IXP ASes'):
                ixp_ases = set(line.split(':')[1].strip(' ').strip('\n').split(' '))
                #print(ixp_ases)
                ixp_as_set = ixp_as_set | ixp_ases

def ClearIxpAsSet():
    global ixp_as_set
    ixp_as_set.clear()

def IsIxpAs(asn):
    if asn in ixp_as_set:
        return True
    return False

def Test():
    rf = open('..\\midar-iff.nodes.as', 'r', encoding='utf-8')
    num = 0
    while True:
        lines = rf.readlines(100000)
        if not lines:
            break
        for line in lines:
            num += 1
    print(num)
    rf.close()

def TmpChgRecordOrder():
    rf = open('ori_res_2_multi_as_path_of_prefix_nrt-jp.20190301', 'r')
    wf = open('res_2_multi_as_path_of_prefix_nrt-jp.20190301', 'w')
    line_ip = rf.readline()
    num = 0
    pre_key = ""
    while line_ip:
        line_as = rf.readline()
        cur_key = line_as.split(']')[0]
        if pre_key != cur_key:
            num += 1
            pre_key = cur_key
        wf.write(line_as)
        wf.write(line_ip)
        line_ip = rf.readline()
    rf.close()
    wf.close()
    print(num)
    

#def CalIpRouterFreq():
def CalIpFreq(filename, top_num, has_bgp_path):
    rf = open(filename, 'r')
    ip_freq_dict = dict()
    #as_freq_dict = dict()
    curline = rf.readline()
    while curline:
        '''
        elem = curline.strip('\n').split('] ')
        if elem and len(elem) > 1:
            as_list = elem[1].split(' ')
            for asn in as_list:
                if asn not in as_freq_dict.keys():
                    as_freq_dict[asn] = 0
                as_freq_dict[asn] += 1
        '''
        if has_bgp_path:
            curline = rf.readline()
        curline = rf.readline()
        elem = curline.strip('\n').split('] ')
        if elem and len(elem) > 1:
            ip_list = elem[1].split(' ')
            for ip in ip_list:
                if ip.startswith('<'):
                    continue
                fst_seg = ip.split('.')[0]
                if fst_seg == '*':
                    continue
                if fst_seg not in ip_freq_dict.keys():
                    ip_freq_dict[fst_seg] = 0
                ip_freq_dict[fst_seg] += 1
        curline = rf.readline()
    ip_sort_list = sorted(ip_freq_dict.items(), key=lambda d:d[1], reverse=True)
    #as_sort_list = sorted(as_freq_dict.items(), key=lambda d:d[1], reverse=True)
    #for i in range (0, 10):
        #print(ip_sort_list[i])
    return ip_sort_list[0:top_num] 

as_rank_dict = dict()
def GetAsRankDict(year, month):
    global as_rank_dict
    filename = GetCloseDateFile(year, month, global_var.asrank_flag)
    filepath = global_var.par_path +  global_var.rel_cc_dir + filename
    with open(filepath) as rf:
        data = rf.read()
    elems = data.strip('\n').strip(' ').split(' ')
    i = 1
    for elem in elems:
        as_rank_dict[elem] = i
        i += 1
    print(len(as_rank_dict))

def AsnInBgpPathList(asn, bgp_path_list):
    for temp in asn.split('_'):
        if temp in bgp_path_list:
            return True
    return False

def SelAsnInBgpPathList(asn, bgp_path_list):
    res_list = []
    for temp in asn.split('_'):
        if temp in bgp_path_list and temp not in res_list:
            res_list.append(temp)
    return '_'.join(res_list)

def AsnInTracePathList(asn, trace_list):
    for temp in asn.split('_'):
        for trace_as in trace_list:
            if temp in trace_as.split('_'):
                return True
    return False

def CountAsnInTracePathList(asn, trace_list):
    max_count = 0
    sel_asn = ''
    for temp in asn.split('_'):
        count = 0
        for trace_as in trace_list:
            if temp in trace_as.split('_'):
                count += 1
        if count > max_count:
            max_count = count
            sel_asn = temp
    return (sel_asn, max_count)

def FstPathContainedInSnd(path1, path2):
    #re.sub('_', ' ', path2)
    path2 = path2.replace('_', ' ')
    path2_list = path2.split(' ')
    for elem in path1.split(' '):
        find = False
        for tmp in elem.split('_'):
            if tmp in path2_list:
                find = True
                break
        if not find:
            return False
    return True

def AsIsEqual(trace_as, bgp_as): #考虑moas的情况
    return bgp_as in trace_as.split('_')

def DropStarsInTraceList(trace_list): #把'*', '?', '<'等hop去掉
    res_list = []
    for elem in trace_list:
        if elem.__contains__('*') or elem.__contains__('?') or elem.__contains__('<'):
            continue
        res_list.append(elem)
    return res_list

'''
def TraceAsIsInBgpPath(trace_as, bgp_path): #考虑moas的情况
    for hop in trace_as.split('_'):
        if hop in bgp_path:
            return True
    return False
'''
def FindTraceAsInBgpPath(trace_as, bgp_path_list): #考虑moas的情况
    for hop in trace_as.split('_'):
        if hop in bgp_path_list:
            return bgp_path_list.index(hop)
    return -1

def FindTraceAsSetInBgpPath(trace_as, bgp_path_list): #考虑moas的情况
    res = set()
    for hop in trace_as.split('_'):
        if hop in bgp_path_list:
            res.add(hop)
    return res

def FindBgpAsInTracePath(bgp_as, trace_path_list): #考虑moas的情况
    for i in range(0, len(trace_path_list)):
        if AsIsEqual(trace_path_list[i], bgp_as):
            return i
    return -1
    
def GetDiffList(lista, listb): #lista是trace_list，listb是bgp_list，考虑trace中有moas的情况
    ia = 0
    ib = 0
    diflist = []
    while ia < len(lista) and ib < len(listb):
        if AsIsEqual(lista[ia], listb[ib]):
            while ia < len(lista) and AsIsEqual(lista[ia], listb[ib]):
                ia += 1
            ib += 1
        else:
            nextb = FindTraceAsInBgpPath(lista[ia], listb[ib + 1:])
            if nextb != -1:
                nextb += ib + 1
                diflist.append([None, range(ib, nextb)])
                ib = nextb
            else:
                nexta = FindBgpAsInTracePath(listb[ib], lista[ia + 1:])
                if nexta != -1:
                    nexta += ia + 1
                    diflist.append([range(ia, nexta),None])
                    ia = nexta
                else:
                    nextb = ib + 1
                    while nextb < len(listb):
                        nexta = FindBgpAsInTracePath(listb[nextb], lista[ia + 1:])
                        if nexta != -1:
                            break
                        nextb += 1
                    if nextb < len(listb):
                        nexta += ia + 1
                    else:
                        nexta = len(lista)
                    diflist.append([range(ia, nexta),range(ib, nextb)])
                    ia = nexta
                    ib = nextb
    if ia < len(lista):
        diflist.append([range(ia, len(lista)),None])
    if ib < len(listb):
        diflist.append([None, range(ib, len(listb))])
    '''
    for difsec in diflist:
        print('{', end="")
        (difa, difb) = difsec
        if difa:
            for tmp in difa:
                print("%s " %lista[tmp], end="")
        print(",", end="")
        if difb:
            for tmp in difb:
                print("%s " %listb[tmp], end="")
        print("}")
    '''
    return diflist

'''
def GetDiffList(lista, listb): 
    ia = 0
    ib = 0
    diflist = []
    while ia < len(lista) and ib < len(listb):
        if AsIsEqual(lista[ia], listb[ib]):
            ia += 1
            ib += 1
        else:
            if lista[ia] in listb[ib + 1:]:
                nextb = listb[ib + 1:].index(lista[ia]) + ib + 1
                diflist.append([None, range(ib, nextb)])
                ib = nextb
            elif listb[ib] in lista[ia + 1:]:
                nexta = lista[ia + 1:].index(listb[ib]) + ia + 1
                diflist.append([range(ia, nexta),None])
                ia = nexta
            else:
                nextb = ib + 1
                while nextb < len(listb) and listb[nextb] not in lista[ia + 1:]:
                    nextb += 1
                if nextb < len(listb):
                    nexta = lista[ia + 1:].index(listb[nextb]) + ia + 1
                else:
                    nexta = len(lista)
                diflist.append([range(ia, nexta),range(ib, nextb)])
                ia = nexta
                ib = nextb
    if ia < len(lista):
        diflist.append([range(ia, len(lista)),None])
    if ib < len(listb):
        diflist.append([None, range(ib, len(listb))])
    #print(diflist)
    #for difsec in diflist:
        #print('{', end="")
        #(difa, difb) = difsec
        #if difa:
            #for tmp in difa:
                #print("%s " %lista[tmp], end="")
        #print(",", end="")
        #if difb:
            #for tmp in difb:
                #print("%s " %listb[tmp], end="")
        #print("}")
    return diflist
'''

def CompressAsPath(ori_as_path):
    ori_as_path = ori_as_path.strip(' ')
    elems = ori_as_path.split(' ')
    as_trace_compress = ""
    pre_elem = ''
    temp_stars = ""
    for elem in elems:
        if elem == pre_elem:
            temp_stars = ""
        elif elem == '?' or elem == '*' or elem.startswith('<'):
            temp_stars = temp_stars + elem + ' '
        else:
            if temp_stars != "":
                as_trace_compress = as_trace_compress + temp_stars
                temp_stars = ""
            as_trace_compress = as_trace_compress + elem + ' '
            pre_elem = elem
    as_trace_compress = as_trace_compress + temp_stars
    as_trace_compress = as_trace_compress.strip(' ')
    return as_trace_compress

#2021.2.19修改，对于moas的hop，如果包含前一跳或者后一跳的as，则舍弃该hop
def CompressAsPathToMin(path):
    return path #2021.5.19不应该像下面一样压缩，可能有错
    debug_modi = False
    elems = path.split(' ')
    res_elems = []
    for elem in elems:
        #if elem == '*' or elem == '?':
        if elem == '*' or elem == '?' or elem.startswith('<'): #把ixp情况考虑进去
            continue
        if not res_elems.__contains__(elem):
            same = False
            if elem.__contains__('_'):
                if res_elems:
                    for prev_elem in res_elems[-1].split('_'):
                        if prev_elem in elem.split('_'):
                            same = True
                            debug_modi = True
                            break
                if not same:
                    res_elems.append(elem)
            else:
                if res_elems and elem in res_elems[-1].split('_'):
                    res_elems.pop()
                    debug_modi = True
                res_elems.append(elem)
    #if debug_modi:
        #print(" ".join(res_elems))
    return " ".join(res_elems)

bgp_dict_1 = dict()
def GetBgp_1(asn): #从原始格式中提取数据
    global bgp_dict_1
    rf = open('bgp_' + asn, 'r')
    curline = rf.readline()
    while curline:
        as_path = curline.split('|')[2]
        if as_path:
            dst_as = as_path.split(' ')[-1]
            if dst_as not in bgp_dict_1.keys():
                bgp_dict_1[dst_as] = []
            compress_as_path = CompressAsPath(as_path)
            if compress_as_path not in bgp_dict_1[dst_as]:
                bgp_dict_1[dst_as].append(compress_as_path)
        curline = rf.readline()
    rf.close()

def ClearBGP_1():
    global bgp_dict_1
    bgp_dict_1.clear()

neighbor_asn_set = set()
def GetVpNeighborFromBgp(asn): #从原始格式中提取数据
    global neighbor_asn_set
    rf = open('bgp_' + asn, 'r')
    curline = rf.readline()
    while curline:
        as_path = curline.split('|')[2]
        if as_path:
            for elem in as_path.split(' '):
                if elem != asn:
                    neighbor_asn_set.add(elem)
                    break
        curline = rf.readline()
    rf.close()
    
bgp_by_prefix_dict = dict()
def GetBgpByPrefix(filename): #从原始格式中提取数据
    global bgp_by_prefix_dict
    rf = open(filename, 'r')
    curline = rf.readline()
    while curline:
        elems = curline.split('|')
        if len(elems) < 3:
            curline = rf.readline()
            continue
        if elems[0] == '-' or elems[1].__contains__(':'): #撤销的update和ipv6，过滤
            curline = rf.readline()
            continue
        prefix_list = elems[1].split(' ')
        as_path = CompressAsPath(elems[2])
        for prefix in prefix_list:
            if prefix not in bgp_by_prefix_dict:
                bgp_by_prefix_dict[prefix] = []
            if as_path not in bgp_by_prefix_dict[prefix]:
                bgp_by_prefix_dict[prefix].append(as_path)
        curline = rf.readline()
    rf.close()

def Prefix1InvolvesPrefix2(prefix1, prefix2):
    ip1 = prefix1.split('/')[0]
    mask_len1 = int(prefix1.split('/')[1])
    ip2 = prefix2.split('/')[0]
    mask_len2 = int(prefix2.split('/')[1])
    if mask_len1 > mask_len2:
        return False
    ip_int1 = socket.ntohl(struct.unpack("I",socket.inet_aton(ip1))[0])
    ip_int1 = ip_int1 & (0xFFFFFFFF - (1 << (32 - mask_len1)) + 1)
    ip_int2 = socket.ntohl(struct.unpack("I",socket.inet_aton(ip2))[0])
    ip_int2 = ip_int2 & (0xFFFFFFFF - (1 << (32 - mask_len1)) + 1)
    return ip_int1 == ip_int2

dst_ip_int_set = set()
def GetDstIpIntSet(trace_filename):
    global dst_ip_int_set
    rf = open(trace_filename, 'r')
    curline = rf.readline()
    while curline:
        if curline.startswith('T'):
            dst_ip = curline.split('\t')[2]
            dst_ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton(dst_ip))[0]) & 0xFFFFFF00
            dst_ip_int_set.add(dst_ip_int)
        curline = rf.readline()
    rf.close()

def ClearDstIpIntSet():
    global dst_ip_int_set
    dst_ip_int_set.clear()

def GetBgpPathFromBgpPrefixDict_2(prefix): #前提：prefix确实在bgp_by_prefix_dict中
    global bgp_by_prefix_dict
    if prefix in bgp_by_prefix_dict.keys():
        return bgp_by_prefix_dict[prefix]
    return None

def GetCommonAsInMoasList(as_list): #在一组trace_as(每一跳可能map到多个AS)中寻找是否有共同map到的AS
    if len(as_list) == 1:
        return set(as_list)
    res = set(as_list[0].split('_')) & set(as_list[1].split('_'))
    if len(as_list) == 2:
        return res
    for i in range(2, len(as_list) - 1):
        res = res & set(as_list[i].split('_'))
        if not res:
            return res
    return res

def GetBgpPathFromBgpPrefixDict(prefix):
    global bgp_by_prefix_dict
    global dst_ip_int_set
    ip = prefix.split('/')[0]
    mask_len = int(prefix.split('/')[1])
    ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton(ip))[0])
    #candidate_ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton(candidate_ip))[0])
    #if candidate_ip_int & 0xFFFFFFFF - (1 << (32 - mask_len)) + 1 == ip_int: #prefix包含candidate_ip, 使用candidate_ip的/24 prefix
        #mask_len = 24
        #ip_int = candidate_ip_int & 0xFFFFFF00
    for cur_mask_len in range(mask_len, 7, -1):
        mask = 0xFFFFFFFF - (1 << (32 - cur_mask_len)) + 1
        cur_prefix_int = ip_int & mask
        cur_prefix = str(socket.inet_ntoa(struct.pack('I',socket.htonl(cur_prefix_int))))
        cur_prefix = cur_prefix + '/' + str(cur_mask_len)
        #print(cur_prefix)
        if cur_prefix in bgp_by_prefix_dict.keys():
            return (cur_prefix, bgp_by_prefix_dict[cur_prefix])
    #prefix not found, back to the tracefile and find the original dst ip. This is an ugly padding
    find = False
    for inc in range(0x0, 1 << (32 - mask_len), 0x100):
        cur_prefix_int = ip_int + inc
        if cur_prefix_int in dst_ip_int_set:
            mask_len = 24
            ip_int = cur_prefix_int
            find = True
            break
    if find:
        for cur_mask_len in range(mask_len, 7, -1):
            mask = 0xFFFFFFFF - (1 << (32 - cur_mask_len)) + 1
            cur_prefix_int = ip_int & mask
            cur_prefix = str(socket.inet_ntoa(struct.pack('I',socket.htonl(cur_prefix_int))))
            cur_prefix = cur_prefix + '/' + str(cur_mask_len)
            #print(cur_prefix)
            if cur_prefix in bgp_by_prefix_dict.keys():
                return (cur_prefix, bgp_by_prefix_dict[cur_prefix])
        print(str(socket.inet_ntoa(struct.pack('I',socket.htonl(ip_int)))))
    else:
        print("NOTICE: GetBgpPathFromBgpPrefixDict prefix %s cannot find dst ip in trace file" %prefix)
    return (None, None)

def ClearBGPByPrefix():
    global bgp_by_prefix_dict
    bgp_by_prefix_dict.clear()

def AsnIsVpNeighbor(asn):
    if asn in neighbor_asn_set:
        return True
    return False

def ClearVpNeighbor():
    global neighbor_asn_set
    neighbor_asn_set.clear()

def DebugGetBgpRoute(dst_as):
    global bgp_dict_1
    if dst_as in bgp_dict_1.keys():
        #for path in bgp_dict_1[dst_as]:
            #print(path)
        return bgp_dict_1[dst_as]
    return []

def DebugGetBgpLink(link):
    global bgp_dict_1
    print(link)
    for (key, paths) in bgp_dict_1.items():
        for path in paths:
            if path.__contains__(link):
                print(path)

'''
def GetSubnetGeo_Slash24(ip):
    fst_dot_index = ip.find('.')
    pre = ip[0:fst_dot_index]
    mid = ip[fst_dot_index + 1:ip.rfind('.')]
    rf = open('..\\srcdata\\mi\\sorted_ip2node_' + pre, 'r')
    content = rf.read().strip('\n').strip(',')
    rf.close()
    if not content:
        print("NOTICE: '..\\srcdata\\mi\\sorted_ip2node_%s' has no content" %pre)
        return ""
    key = str(',') + mid
    start_index = content.find(key)
    if start_index == -1:   #没找到后怎么处理还没想好
        return ""
    temp_list = content[start_index + 1:].split(',')
    geo_list = []
    for info in temp_list:
        elems = info.split(' ')
        if len(elems) < 2:
            continue
        tmp_mid_ip = elems[0]
        if mid == tmp_mid_ip[0:tmp_mid_ip.rfind('.')]:
            tmp_geo = GetGeoOfRouterByMi(elems[1])
            if tmp_geo not in geo_list:
                geo_list.append(tmp_geo)
        else:
            break
    return geo_list
'''
def GetSubnetGeo_Slash24(ip):
    global db_cursor
    first_seg = ip[0:ip.index('.')]
    index = int(int(first_seg) / 16)
    prefix = ip[0:ip.rindex('.')]
    select_sql = "SELECT nodeid FROM midarip2node_%s WHERE prefix24='%s'" %(str(index), prefix)
    db_cursor.execute(select_sql)
    res = db_cursor.fetchall() #当没找到的时候，返回0
    router_set = set()
    if res: #找到     
        for elem in res:   
            router_set.add(elem[0])    
    geo_set = set()
    for router in router_set:
        index_2 = int(router) % 10
        select_sql_2 = "SELECT geo FROM midarnode2geo_%s WHERE nodeid='%s'" %(str(index_2), router)
        db_cursor.execute(select_sql_2)
        res = db_cursor.fetchall() #当没找到的时候，返回0
        if res: #找到        
            geo_set.add(res[0][0])
    return list(geo_set)

min_dist_limit = 100
def IfPossibleIxp_ForASList(ip, as_list):  #如果ip的/24子网内有邻居ip和ab_hop离得特别近，且该邻居ip属于as_list中的一个，则认为可能是IXP
    obj_geo = GetGeoOfIpByMi(ip)
    if not obj_geo:
        return False #如果ip本身找不到router，不进行比较

    global db_cursor
    global cur_midar_table_date
    first_seg = ip[0:ip.index('.')]
    index = int(int(first_seg) / 16)
    prefix = ip[0:ip.rindex('.')]
    select_sql = "SELECT nodeid FROM midarip2node_%s_%s WHERE prefix24='%s'" %(cur_midar_table_date, str(index), prefix)
    db_cursor.execute(select_sql) #找prefix下的所有router
    res = db_cursor.fetchall() #当没找到的时候，返回0
    router_set = set()
    if res: #找到     
        for elem in res:
            router_set.add(elem[0])
        for router in router_set:
            index_2 = int(router) % 10
            select_sql_2 = "SELECT asn FROM midarnode2as_%s_%s WHERE nodeid='%s'" %(cur_midar_table_date, str(index_2), router)
            db_cursor.execute(select_sql_2)
            res = db_cursor.fetchall() #当没找到的时候，返回0
            if res: #找到        
                cur_as = res[0][0]
                if cur_as in as_list:    #在as_list中，进一步检查距离
                    select_sql_3 = "SELECT geo FROM midarnode2geo_%s_%s WHERE nodeid='%s'" %(cur_midar_table_date, str(index_2), router)
                    db_cursor.execute(select_sql_3)
                    res = db_cursor.fetchall() #当没找到的时候，返回0
                    if res: #找到        
                        cur_geo = res[0][0]
                        if GeoDistance(cur_geo, obj_geo) < min_dist_limit: #找到了
                            return True
    return False
    
def GetDistOfIps(ip1, ip2): #如果找不到确切位置，就返回子网内最接近的位置
    geo1 = GetGeoOfIpByMi(ip1)
    geo2 = GetGeoOfIpByMi(ip2)
    if geo1 and geo2:
        if geo1.__contains__('EU DE 05 F'):
            print("ip1: %s" %ip1)
        return (True, GeoDistance(geo1, geo2))
    return (False, none_distance)   #2021.4.24 不再在/24子网内寻找有位置相近的IP，可能是错的

    geo1_list = []
    geo2_list = []
    if geo1:
        geo1_list.append(geo1)
    else:
        geo1_list = GetSubnetGeo_Slash24(ip1)
    if geo2:
        geo2_list.append(geo2)
    else:
        geo2_list = GetSubnetGeo_Slash24(ip2)
    min_dist = none_distance
    for tmp_geo1 in geo1_list:
        for tmp_geo2 in geo2_list:
            tmp_dist = GeoDistance(tmp_geo1, tmp_geo2)
            if min_dist > tmp_dist:
                min_dist = tmp_dist
    return (False, min_dist)     

def GetAsnIndexInList(asn, asn_list):
    for i in range(0, len(asn_list)):
        if asn in asn_list[i].split('_'):
            return i
    return -1  

as_country_dict = dict()
def GetAsCountryDict():
    global as_country_dict
    rf = open("../../data/ASNS", "r")
    cur_str = rf.readline()
    while cur_str:
        obj = ujson.loads(cur_str)
        cur_asn = obj["asn"]
        as_country_dict[cur_asn] = obj["country"]["name"]
        cur_str = rf.readline() 
    rf.close()

def ClearAsCountryDict():
    global as_country_dict
    as_country_dict.clear()

def GetAsCountry(asn):
    global as_country_dict
    if asn in as_country_dict.keys():
        return as_country_dict[asn]
    return ''

def GetAsRank(asn_list):
    rf = open("../../data/ASNS", "r")
    num = len(asn_list)
    rank_list = [None for i in range(0, num)]
    i = 0
    cur_str = rf.readline()
    while cur_str:
        obj = ujson.loads(cur_str)
        cur_asn = obj["asn"]
        index = GetAsnIndexInList(cur_asn, asn_list)
        if index != -1:
            if rank_list[index] == None:
                rank_list[index] = str(obj["rank"]).strip(' ')
            else:
                rank_list[index] = '_' + str(obj["rank"]).strip(' ')
            i += 1
            if i == num:
                break
        cur_str = rf.readline() 
    return rank_list
'''
as_rank_dict = dict()
def GetAsRankDict():
    global as_rank_dict
    rf = open("../../data/ASNS", "r")
    cur_str = rf.readline()
    while cur_str:
        obj = ujson.loads(cur_str)
        cur_asn = obj["asn"]
        as_rank_dict[cur_asn] = str(obj["rank"]).strip(' ')
        cur_str = rf.readline() 
    rf.close()
'''

def ClearAsRankDict():
    global as_rank_dict
    as_rank_dict.clear()

def GetAsRankFromDict(asn):
    if asn in as_rank_dict.keys():
        return as_rank_dict[asn]
    return None

def GetAsRankFromDict_2(hop): #考虑moas
    min_asrank = 0x1FFFFFFF
    for asn in hop.split('_'):
        if asn in as_rank_dict.keys():
            tmp = as_rank_dict[asn]
            if min_asrank < tmp:
                min_asrank = tmp
    #if min_asrank == 0x1FFFFFFF:
        #print("%s rank not found" %hop)
    return min_asrank

def GetAsRankStrFromDict(asn_str):
    rank_list = []
    for asn in asn_str.split('_'):
        if asn in as_rank_dict.keys():
            rank_list.append(as_rank_dict[asn])
    if rank_list:
        return '_'.join(rank_list)
    else:
        return ''

def FindSuperPrefixInDict(prefix, data_dict):    
    ip = prefix.split('/')[0]
    mask_len = int(prefix.split('/')[1])
    ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton(ip))[0])
    for cur_mask_len in range(mask_len, 7, -1):
        mask = 0xFFFFFFFF - (1 << (32 - cur_mask_len)) + 1
        cur_prefix_int = ip_int & mask
        cur_prefix = str(socket.inet_ntoa(struct.pack('I',socket.htonl(cur_prefix_int))))
        cur_prefix = cur_prefix + '/' + str(cur_mask_len)
        #print(cur_prefix)
        if cur_prefix in data_dict.keys():
            return cur_prefix
    return None

def CmpIp2ASDictAndBgp(bgp_filename):
    global ip2as_dict
    GetPfx2ASByRv()
    rf = open(bgp_filename, 'r')
    wf = open('dif_from_ip2as_' + bgp_filename, 'w')
    curline = rf.readline()
    while curline:
        elems = curline.split('|')
        if elems[0] == '-' or elems[1].__contains__(':'):
            curline = rf.readline()
            continue
        prefix_list = elems[1].split(' ')
        ori_as = elems[2].split(' ')[-1]
        for prefix in prefix_list:
            match_prefix = FindSuperPrefixInDict(prefix, ip2as_dict)
            if not match_prefix:
                wf.write("+|%s %s\n" %(prefix, ori_as))
            else:
                as_list = ip2as_dict[match_prefix]
                if ori_as not in as_list:
                    wf.write("-|%s %s(%s)\n" %(prefix, ori_as, '_'.join(as_list)))
        curline = rf.readline()
    rf.close()
    wf.close()
    ClearIp2AsDict()
    
def GetPathInBgp(asn, path, contact_flag):
    filename = 'bgp_' + asn
    if not os.path.exists(filename):
        print('bgp file %s not exists' %filename)
        return
    rf = open(filename, 'r')
    w_filename = filename + '_' + path.replace(' ', '_')
    if contact_flag:
        w_filename += '_1'
    else:
        w_filename += '_0'
    #wf = open(w_filename, 'w')
    curline = rf.readline()

    while curline:
        elems = curline.split('|')
        prefix = elems[1]
        if prefix.__contains__(':'):
            curline = rf.readline()
            continue
        cur_path = elems[2]
        if contact_flag:
            if cur_path.__contains__(path):
                wf.write("%s|%s\n" %(prefix, cur_path))
        else:
            find = True
            cur_path_list = cur_path.split(' ')
            for elem in path.split(' '):
                if elem not in cur_path_list:
                    find = False
                    break
            if find:
                #wf.write("%s|%s\n" %(prefix, cur_path))
                print("%s|%s" %(prefix, cur_path))
        curline = rf.readline()
    rf.close()
    #wf.close()

path_db = None
db_cursor = None
def ConnectToDb():
    global path_db
    global db_cursor
    path_db = pymysql.connect(host="127.0.0.1",user="root",password="123",database="bgp_map") 
    # 使用 cursor() 方法创建一个游标对象 cursor
    if not path_db:
        print("ConnectToDb failed!")
    db_cursor = path_db.cursor()

def FindPathInDb(path_list):
    global db_cursor
    srcdst = path_list[0] + ' ' + path_list[-1]
    select_sql = "SELECT path FROM integpath WHERE srcdst=%s"
    db_cursor.execute(select_sql, srcdst)
    res = db_cursor.fetchall() #当没找到的时候，返回0
    if res: #找到
        for elem in res:
            if not set(path_list).difference(set(elem[0].split(' '))):
                return True
    return False

def CloseDb():
    global path_db
    global db_cursor
    db_cursor.close()
    path_db.close()             

def InsertDstPathIntoDb_Test(filename):
    print(filename)
    #print(dirname + '\\' + dir + '\\' + filename)
    #rf = open(filename, 'r')
    #rf = open(dirname + '\\' + dir + '\\' + filename, 'r', encoding='utf-8')
    with open(filename) as rf:
        for curline in rf:
            elems = curline.split('|')
            if len(elems) < 3:
                curline = rf.readline()
                continue
            if elems[0] == '-' or elems[1].__contains__(':') or elems[2].__contains__('{'): #撤销的update, ipv6以及AS SET，过滤
                curline = rf.readline()
                continue
            cur_path = CompressAsPath(elems[2])
            path_list = cur_path.split(' ')
            dst = path_list[-1]
            for i in range(0, len(path_list) - 1):
                path_seg = ' '.join(path_list[i:])
                srcdst = path_list[i] + ' ' + dst
                if len(path_seg) > 200 or len(srcdst) > 30:
                    print(curline)
                    return
                #select_sql = "SELECT path FROM integpath_test WHERE srcdst=%s AND path=%s"
                #db_cursor.execute(select_sql, (srcdst, path_seg))
                #res = db_cursor.fetchall() #当没找到的时候，返回0
                #if res: #找到，不做后续操作，终止循环
                    #break
                #未找到，插入表项
                try:
                    insert_sql = "INSERT INTO integpath_test(srcdst,path) VALUES(%s,%s)"
                    db_cursor.execute(insert_sql, (srcdst, path_seg))
                except Exception:
                    break
    #rf.close()
    #db.commit()
    print("end a file")
    
def CombineFiles(src_file_list, obj_file):
    wf = open(obj_file,'w')
    for filename in src_file_list:
        for data in open(filename,'r'):
            wf.write(data)
    wf.close()

def CalEndHop(trace_file_name):
    rf = open(trace_file_name, 'r', encoding='utf-8')
    mid_hop_set = set()
    last_hop_set = set()
    count = 0
    hop_count = 0
    hit_count = 0
    global db_cursor
    curline = rf.readline()
    while curline:
        count += 1
        if count % 1000 == 0:
            print(count)
        if not curline.startswith('T'):
            curline = rf.readline()
            continue
        curline = curline.strip('\n')
        elems = curline.split('\t')
        for i in range(13, len(elems)):
            curhops = elems[i]
            if curhops.__contains__('q'):
                continue
            else:
                #curhops: "210.171.224.41,1.210,1;210.171.224.41,1.216,1"
                hop_count += 1
                hopelems = curhops.split(';')
                ip = hopelems[0].split(',')[0] #只取第一个ip
                #temp[0]: "210.171.224.41"
                first_seg = ip[0:ip.index('.')]
                index = int(int(first_seg) / 16)
                prefix = ip[0:ip.rindex('.')]
                ip_suffix = ip[ip.rindex('.') + 1:]
                select_sql = "SELECT nodeid FROM midarip2node_%s WHERE prefix24='%s' AND ip='%s'" %(str(index), prefix, ip_suffix)
                db_cursor.execute(select_sql)
                res = db_cursor.fetchall() #当没找到的时候，返回0
                cur_hop = ip
                if res: #找到        
                    cur_hop = 'r_' + res[0][0]
                    hit_count += 1
                if i == len(elems) - 1: #last hop
                    if cur_hop not in mid_hop_set: #firstly appeared last hop
                        last_hop_set.add(cur_hop)
                    else: #已经在中间出现过，不管
                        pass
                else:
                    if cur_hop in last_hop_set: #在last_hop_set中，删去
                        last_hop_set.remove(cur_hop)
                    mid_hop_set.add(cur_hop)
        curline = rf.readline()        
    rf.close()
    print("mid_hop_set count: %d" %len(mid_hop_set))
    print("last_hop_set count: %d" %len(last_hop_set))
    print("hop_count: %d" %hop_count)
    print("hit_count: %d" %hit_count)

def GetSpecIpInTrace(trace_file, ip_list, w_file):
    rf = open(trace_file, 'r')
    wf = None
    if w_file:
        wf = open(w_file, 'w')
    curline = rf.readline()
    while curline:
        if not curline.startswith('T'):
            curline = rf.readline()
            continue
        hit = True
        for ip in ip_list:
            if not curline.__contains__(ip):
                hit = False
                break
        if hit:
            if wf:
                wf.write(curline)
            else:
                print(curline)
        curline = rf.readline()
    rf.close()
    
    
def GetSpecDstIpInTrace(trace_file, prefix, wf):
    rf = open(trace_file, 'r')
    curline = rf.readline()
    ip_list = []
    for i in range(0, 256):
        ip_list.append(prefix + str(i) + '.1')
    while curline:
        if not curline.startswith('T'):
            curline = rf.readline()
            continue
        dst_ip = curline.split('\t')[2]
        if dst_ip in ip_list:
            print(dst_ip)
            if wf:
                wf.write(curline)
            else:
                print(curline)
        curline = rf.readline()
    rf.close()

def PrintRouterInterfaces(router):
    rf = open('..\\midar-iff.nodes', 'r')
    curline = rf.readline()

    while curline:
        if curline.startswith('#'):
            curline = rf.readline()
            continue
        router_id = curline[6:curline.index(':')]
        if router == router_id:
            print(curline)
            break
        curline = rf.readline()
    rf.close()

def TranslateAsRel(rel):
    if rel == 100:
        return 'same'
    elif rel == 2:
        return 'sibling'
    elif rel == 1:
        return 'provider'
    elif rel == -1:
        return 'customer'
    elif rel == -2:
        return 'peer'
    else:
        return 'unknown'

def GetAsRelAndTranslate(as1, as2):
    rel = Get2AsRel(as1, as2)
    return TranslateAsRel(rel)

def AnaAsRel(filename):
    rf = open(filename, 'r')
    trace_line = rf.readline()

    while trace_line:
        bgp_line = rf.readline()
        ip_line = rf.readline()
        trace_elems = trace_line.strip('\n').split(' ')
        ab_as = trace_elems[-1]
        bgp_elems = bgp_line.strip('\n').strip('\t').split(' ')
        last_norm_as_index_in_bgp = -1
        for i in range(len(trace_elems) - 1, -1, -1):
            if trace_elems[i] in bgp_elems:
                last_norm_as_index_in_bgp = bgp_elems.index(trace_elems[i])
                break
        print(trace_line, end="")
        print(bgp_line, end="")
        for cur_ab_as in ab_as.split('_'):
            for cur_as in bgp_elems[last_norm_as_index_in_bgp:]:
                print("%s(%s) %s(%s): %s" %(cur_ab_as, GetAsCountry(cur_ab_as), cur_as, GetAsCountry(cur_as), GetAsRelAndTranslate(cur_ab_as, cur_as)))
        for i in range(last_norm_as_index_in_bgp, len(bgp_elems) - 1):
            print("%s(%s) %s(%s): %s" %(bgp_elems[i], GetAsCountry(bgp_elems[i]), bgp_elems[i + 1], GetAsCountry(bgp_elems[i + 1]), GetAsRelAndTranslate(bgp_elems[i], bgp_elems[i + 1])))
        print('\n')
        trace_line = rf.readline()
    rf.close()

def GetP2PSubneNeighbors(ip):   #/30, /31 subnet
    last_seg_num = int(ip[ip.rindex('.') + 1:])
    the_other_list = []
    if last_seg_num & 3 == 0:
        the_other_list.append(last_seg_num + 1)
    elif last_seg_num & 3 == 1 or last_seg_num & 3 == 2:
        the_other_list.append(last_seg_num - 1)
        the_other_list.append(last_seg_num + 1)
    else:
        the_other_list.append(last_seg_num - 1)
    neighbor_list = []
    pre_seg = ip[0:ip.rindex('.') + 1]
    for the_other in the_other_list:
        neighbor_list.append(pre_seg + str(the_other))
    return neighbor_list

def GetCCofAs(year, month, asn):
    PreGetSrcFilesInDirs()
    filename = GetCloseDateFile(year, month, global_var.cone_flag)
    print(filename)
    rf = open(global_var.par_path + global_var.rel_cc_dir + filename, 'r')
    curline = rf.readline()
    while curline:        
        if curline.startswith(asn + ' '):
            print(curline)
            break
        curline = rf.readline()

as_cc_num_dict = dict()
def GetCCNums(year, month):
    global as_cc_num_dict
    PreGetSrcFilesInDirs()
    filename = GetCloseDateFile(year, month, global_var.cone_flag)
    print(filename)
    with open(global_var.par_path + global_var.rel_cc_dir + filename, 'r') as rf:
        curline = rf.readline()
        while curline:
            if not curline.startswith('#'):
                elems = curline.strip('\n').strip(' ').split(' ')
                if len(elems) > 0:
                    as_cc_num_dict[elems[0]] = len(elems) - 1
            curline = rf.readline()

def GetCCNumsFromDict(asn):
    global as_cc_num_dict
    if asn in as_cc_num_dict.keys():
        return as_cc_num_dict[asn]
    return 0
    

def RelHasValley(rel_str):
    if rel_str.__contains__('-1 1') or rel_str.__contains__('-1 0') or rel_str.__contains__('0 1'):
        return True
    return False

def PathHasValley(year, month, path):
    ClearAsRel()
    GetAsRel(year, month)
    rel_str = ''
    prev_elem = ''
    for elem in path.split(' '):
        if prev_elem != '':
            rel = GetAsRelAndTranslate(prev_elem, elem)
            if rel == 'same' or rel == 'sibling': #不管
                pass
            elif rel == 'provider':
                rel_str += ' 1'
            elif rel == 'peer':
                rel_str += ' 0'
            elif rel == 'customer':
                rel_str += ' -1'
            else: #unknown
                return (False, False) #第一个返回值表示path中含有未知link
        prev_elem = elem
    return (True, RelHasValley(rel_str.strip(' ')))

def PathHasValley_2(path): #as rel dict has alloc before, return valley seg 
    valley_set = set()
    rel_list = []
    path_list = path.split(' ')
    # if path.__contains__('7575 11537 20965'):
    #     print('')
    prev_elem = ''
    for elem in path_list:
        if prev_elem != '':
            rel = GetAsRelAndTranslate(prev_elem, elem)
            if rel == 'same' or rel == 'sibling':
                rel_list.append('*')
            elif rel == 'provider':
                rel_list.append('1')
            elif rel == 'peer':
                rel_list.append('?') #2021.9.7由于不同日期之间AS rel相差太大，这里不再考虑peer导致的泄露，只查p-c-p型路由泄露
            elif rel == 'customer':
                rel_list.append('-1')
            else: #unknown                
                rel_list.append('?') #这里，没有商业关系的先没管，实际应该考虑这种情况
        prev_elem = elem
    pre_rel = '?'
    for i in range(0, len(rel_list)):
        rel = rel_list[i]
        if pre_rel != '?' and rel != '?' and pre_rel != '*' and rel != '*':
            if int(rel) > int(pre_rel): #valley
                valley_set.add(path_list[i-1] + ' ' + path_list[i] + ' ' + path_list[i+1])
        pre_rel = rel
    return valley_set

invalid_words = {'Backbone', 'backbone', 'Network', 'network', 'Networks', 'networks', \
                'Addressing', 'addressing', 'Core', 'core', 'IPv4', 'IPv6', 'IP', \
                'Infrastructure', 'infrastructure', 'Links', 'links', 'Servers', 'servers'}
def TwoOrgSetJoin(org_set1, org_set2, record = None):
    common_set = org_set1 & org_set2
    if common_set:
        for elem in common_set:
            tmp_set = set(elem.split(' '))
            if tmp_set.difference(invalid_words):
                if record:
                    record.write(elem + '\n')
                return True
    return False

irr_data_cache = dict()
def GetOrgByIRR(ip):
    global irr_data_cache
    if ip in irr_data_cache.keys():
        [org_set, asn_set] = irr_data_cache[ip]
    else:
        #print('mid_1')
        #(org_set, asn_set) = GetOrgAsnFromIRROnLine(ip) #在线查
        (org_set, asn_set) = GetIrrOrgFromDb(ip) #查数据库
        #print('mid_2')
        irr_data_cache[ip] = [org_set, asn_set]
    return (org_set, asn_set)

def IsSibByIRR(asn, ip, record = None):
    '''
    cur_asn = GetAsStrOfIpByRv(ip)
    if cur_asn:
        if IsSib_2(asn, cur_asn):
            return True
        return False
    '''
    #print('start')
    (org_set, asn_set) = GetOrgByIRR(ip)
    if asn_set and asn in asn_set:
        return True
    #print('mid')
    asn_org_set = GetOrgByMultiDataFiles_2(asn)
    #print('end')
    return TwoOrgSetJoin(asn_org_set, org_set, record)

def IsSibByIRR_2(ip1, ip2, record = None):
    #rib表里没有ip的源AS，查IRR
    (org_set1, asn_set1) = GetOrgByIRR(ip1)
    (org_set2, asn_set2) = GetOrgByIRR(ip2)
    if asn_set1 & asn_set2:
        return True
    return TwoOrgSetJoin(org_set1, org_set2, record)

def Tmp():
    rf = open(global_var.par_path + 'test3', 'r')
    wf = open(global_var.par_path + 'test3_back', 'w')
    curline = rf.readline()
    while curline:
        if not curline.__contains__('"name": "Multicast"'):
            wf.write(curline)
        curline = rf.readline()
    rf.close()
    wf.close()

def Tmp1():
    rf = open(global_var.par_path + 'backup/test2', 'r')
    curline = rf.readline()
    while curline:
        if not curline.__contains__('"name": "Multicast"'):
            print(curline[1:curline.index(':')])
        curline = rf.readline()
    rf.close()

def ModiAsOrgInfoIn201801(): #20180101.as-org2info.txt这个文件格式和之后日期的格式不一样，修改
    wf = open('/mountdisk1/ana_c_d_incongruity/as_org_data/20180101.as-org2info.txt', 'w')
    with open('/mountdisk1/ana_c_d_incongruity/as_org_data/20180101.as-org2info.txt_ori', 'r') as rf:
        curline = rf.readline()
        start_modi = False
        while curline:
            if curline.__contains__('format:aut|changed|aut_name|org_id|source'):
                start_modi = True
            if not start_modi:
                wf.write(curline)
            else:
                last_index = curline.rindex('|')
                wf.write(curline[:last_index] + '|' + curline[last_index:])
            curline = rf.readline()
    wf.close()

asn_info_dict = dict()
def PreLoadAsnInfoFromASNS():
    global asn_info_dict
    with open(global_var.par_path + 'ASNS', 'r') as rf:
        curline = rf.readline()
        while curline:
            data = ujson.loads(curline.strip('\n'))
            asn = data['asn']
            asn_info_dict[asn] = data
            # print(asn_info_dict[asn]['organization']['orgName'])
            # print(asn_info_dict[asn]['country']['name'])            
            # print(asn_info_dict[asn]['rank'])
            curline = rf.readline()

def GetAsnInfoFromASNS(asn):
    global asn_info_dict
    tmp = None
    if asn == '26009':
        tmp = asn_info_dict[asn]
    org = ''
    country = ''
    rank = ''
    if asn in asn_info_dict.keys():
        if 'organization' in asn_info_dict[asn].keys() and asn_info_dict[asn]['organization'] and \
            'orgName' in asn_info_dict[asn]['organization'].keys():
            org = asn_info_dict[asn]['organization']['orgName']
        if 'country' in asn_info_dict[asn].keys() and asn_info_dict[asn]['country'] and \
            'name' in asn_info_dict[asn]['country'].keys():
            country = asn_info_dict[asn]['country']['name']
        if 'rank' in asn_info_dict[asn].keys():
            rank = asn_info_dict[asn]['rank']
    return (org, country, rank)

def ClearAsnInfoFromASNS():
    global asn_info_dict
    asn_info_dict.clear()


asn_ix_dict = dict()
asn_fac_dict = dict()
asn_ix_newest_dict = dict()
asn_fac_newest_dict = dict()
def ConstrPeerDbInfoDict(year, month): #'/mountdisk1/ana_c_d_incongruity/peeringdb_data/peeringdb_2020_11_15.json'
    global asn_ix_newest_dict
    global asn_fac_newest_dict
    if len(asn_ix_newest_dict) == 0: #把最新的peeringdb load进来
        with open(global_var.par_path + global_var.peeringdb_dir + 'peeringdb_2021_08_15.json', 'r') as rf:
            j = ujson.load(rf)
        for elem in j['netixlan']['data']:
            asn = elem['asn']
            if asn not in asn_ix_newest_dict.keys():
                asn_ix_newest_dict[asn] = set()
            asn_ix_newest_dict[asn].add(elem['ix_id'])
        for elem in j['netfac']['data']:
            asn = elem['local_asn']
            if asn not in asn_fac_newest_dict.keys():
                asn_fac_newest_dict[asn] = set()
            asn_fac_newest_dict[asn].add(elem['fac_id'])

    if year == 2018 and month < 3:
        month = 3 #3月份以前的是sqlite格式，里面没有ix_id，不知道怎么比
    global asn_ix_dict
    global asn_fac_dict        
    with open(global_var.par_path + global_var.peeringdb_dir + 'peeringdb_' + str(year) + '_' + str(month).zfill(2) + '_15.json', 'r') as rf:
        j = ujson.load(rf)
        # con = sqlite3.connect(filename)
        # con.row_factory = sqlite3.Row
        # cur = con.cursor()
        # j['netfac'] = {'data': cur.execute('select * from peeringdb_network_facility').fetchall()}
        # j['netixlan'] = {'data': cur.execute('select * from peeringdb_network_ixlan').fetchall()}
        # cur.close()
        # con.close()
        
    for elem in j['netixlan']['data']:
        asn = elem['asn']
        if asn not in asn_ix_dict.keys():
            asn_ix_dict[asn] = set()
        asn_ix_dict[asn].add(elem['ix_id'])
    for elem in j['netfac']['data']:
        asn = elem['local_asn']
        if asn not in asn_fac_dict.keys():
            asn_fac_dict[asn] = set()
        asn_fac_dict[asn].add(elem['fac_id'])

def IsAsSet(asn):
    return asn.__contains__('{')

def IsTwoAsPeerInIXP(as1_str, as2_str):
    global asn_ix_newest_dict
    global asn_fac_newest_dict
    global asn_ix_dict
    global asn_fac_dict
    as1 = int(as1_str)
    as2 = int(as2_str)
    if as1 in asn_ix_dict.keys() and as2 in asn_ix_dict.keys() and \
        (asn_ix_dict[as1] & asn_ix_dict[as2]):
        return True
    if as1 in asn_ix_newest_dict.keys() and as2 in asn_ix_newest_dict.keys() and \
        (asn_ix_newest_dict[as1] & asn_ix_newest_dict[as2]):
        return True
    #下面这一步要不要存疑
    if as1 in asn_fac_dict.keys() and as2 in asn_fac_dict.keys() and \
        (asn_fac_dict[as1] & asn_fac_dict[as2]):
        return True
        #print('%d and %d have common fac' %(as1, as2))
    if as1 in asn_fac_newest_dict.keys() and as2 in asn_fac_newest_dict.keys() and \
        (asn_fac_newest_dict[as1] & asn_fac_newest_dict[as2]):
        return True
        #print('%d and %d have common fac 2' %(as1, as2))
    return False    

def ClearPeerDbInfoDict():
    global asn_ix_dict
    global asn_fac_dict
    asn_ix_dict.clear()
    asn_fac_dict.clear()

neigh_dict_from_ripe = dict()
new_neigh_dict_from_ripe = dict()
def GetNeighFromRipe(asn):
    global neigh_dict_from_ripe
    global new_neigh_dict_from_ripe
    filename = global_var.par_path + global_var.irr_dir + 'neigh_from_ripe'
    if len(neigh_dict_from_ripe) == 0 and os.path.exists(filename) and os.path.getsize(filename):
        with open(filename, 'r') as rf:
            for data in rf.read().strip('\n').split('\n'):
                elems = data.split(':')
                if elems[1] == '':
                    neigh_dict_from_ripe[elems[0]] = set()
                else:    
                    neigh_dict_from_ripe[elems[0]] = set(elems[1].split(','))                
    if asn in neigh_dict_from_ripe.keys():
        return neigh_dict_from_ripe[asn]
    url = 'https://apps.db.ripe.net/db-web-ui/api/whois/ripe/aut-num/AS%s?abuse-contact=true&managed-attributes=true&resource-holder=true&unfiltered=true' %asn
    req = requests.Session()
    #headers = {'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'}
    headers = {'accept': 'application/json, text/plain, */*'}
    resource = req.get(url, headers=headers)
    neigh_set = set()
    if resource:                
        if resource.status_code == 200:
            info = ujson.loads(resource.text)
            if 'objects' in info.keys() and 'object' in info['objects'].keys():
                for elem in info['objects']['object']:
                    if 'attributes' in elem.keys() and 'attribute' in elem['attributes'].keys():
                        for sub_elem in elem['attributes']['attribute']:
                            if 'name' in sub_elem.keys() and (sub_elem['name'] == 'import' or sub_elem['name'] == 'export'):
                                find_res = re.findall(r'AS(\d+)', sub_elem['value'])
                                for elem in find_res:
                                    if elem != asn:
                                        neigh_set.add(elem)
    neigh_dict_from_ripe[asn] = neigh_set
    new_neigh_dict_from_ripe[asn] = neigh_set
    if len(new_neigh_dict_from_ripe) == 50:
        print('write 50 res')
        with open(filename, 'a') as wf:
            for (cur_asn, cur_neigh_set) in new_neigh_dict_from_ripe.items():
                if len(cur_neigh_set) == 0:
                    wf.write('%s:\n' %cur_asn)
                else:
                    wf.write('%s:%s\n' %(cur_asn, ','.join(list(cur_neigh_set))))
            new_neigh_dict_from_ripe.clear()
    return neigh_set


def TmpTestPeerDb():
    output = os.popen('ls ' + global_var.par_path + global_var.peeringdb_dir + 'peeringdb_*.json')
    for filename in output.read().strip('\n').split('\n'):
        with open(filename, 'r') as rf:
            j = ujson.load(rf)        
            for elem in j['netixlan']['data']:
                if elem['ix_id'] == 78:
                    if elem['ix_id'] != elem['ixlan_id']:
                        print(filename + ':' + str(elem['ix_id']) + ',' + str(elem['ixlan_id']))
                    else:
                        print(filename + ':' + str(elem['ix_id']) + ',' + str(elem['ixlan_id']))

if __name__ == '__main__':
    GetSibRel_2(2018, 8)
    GetIxpAsSet()
    # for asn in ixp_as_set:
    #     print(asn + ':' + sib_dict[asn])
        
    ixp_ases_2 = set()
    with open('/mountdisk1/ana_c_d_incongruity/peeringdb_data/peeringdb_2018_08_15.json', 'r') as rf:
        data = ujson.load(rf)
        for net in data['net']['data']:
            if IsIXPName(net['name']):
                ixp_ases_2.add(str(net['asn']))
    #print(ixp_ases_2.difference(ixp_as_set))
    for asn in ixp_ases_2.difference(ixp_as_set):
        if asn in sib_dict:
            print(asn + ':' + sib_dict[asn])
        else:
            print(asn + ':')
    while True:
        pass
    #print(IP('10.0.0.0/8').net())
    #print(ipaddress.ip_interface('2001:468:ff:110::1/126').network)
    # ConstrPeerDbInfoDict(2018,1)
    # ret = IsTwoAsPeerInIXP('8220', '9231')
    #GetNeighFromRipe('208951')
    #PreGetNonOverlayIpRanges()
    #print(GetCountryOrgAsnFromIrrOnline('203.181.248.51'))
    #ModiAsOrgInfoIn201801()
    #GetIrrDict()
    '''
    with open(global_var.par_path + global_var.irr_dir + global_var.irr_filename_pre + 'afrinic', 'r') as rf:
        curline = rf.readline()
        while curline:
            key = curline[1:curline.index(':')].strip('"')
            if key == full_ip_space:
                curline = rf.readline()
                continue
            info = ujson.loads(curline.strip('\n'))
            data = info[key]
            (org_set, asn_set, src) = GetBelongedOrgFromAfrinic(data)
            print(key)
            print(org_set)
            print(asn_set)
            curline = rf.readline()
    '''
    
    #Tmp()
    #print(irr_dict)
    #print(irr_index_dict)
    #PreGetSrcFilesInDirs()
    #GetSibRel(2018, 10)
    #print(sib_dict['20940'])
    #GetIxpPfxDict(2018, 10)

    #ReadPfx2AS()
    #SuppleIxpPrefixByIxAsn()
    #Test()
    #print(GetRouterOfIpByMi('224.2.131.38'))
    #print(GetAsOfRouterByMi('28'))
    #lista = ['2', '3', '4', '7_15', '8', '9', '10_11', '16', '17']
    #listb = ['2', '3', '7', '11', '8', '12', '13', '16']
    #GetDiffList(lista, listb)
    #CalIpFreq(10)
    #PreOpenRouterGeoFiles()
    #print(GetGeoOfRouterByMi('287'))
    #CloseGeoRouterFiles()
    #GetBgp_1('7660')
    #DebugGetBgpRoute('13999')

    ConnectToDb()
    #CalEndHop("hkg-cn.20190315\\hkg-cn.20190315")
    #GetDistOfIps('206.126.236.19', '213.254.227.26')
    #start_time = time.time()
    #InsertDstPathIntoDb_Test("..\\..\\DataFromRouteViews\\2019.03\\2019.0315\\ribs\\test.txt")
    #path_db.commit()
    #end_time = time.time()
    #insert_sql = "INSERT INTO integpath_test(srcdst,path) VALUES(%s,%s)"
    #db_cursor.execute(insert_sql, ('a b', 'a c d b'))
    #db_cursor.execute(insert_sql, ('a b', 'a c d b'))
    #print("time:%d" %(end_time-start_time)) 
    #FindPathInDb('10026 27932 1'.split(' '))
    #res = GetAsOfRouterByMi('10005068')
    #print(res)
    #res = GetGeoOfRouterByMi('10005068')
    #res = GetSubnetGeo_Slash24('')
    #res = GetRouterOfIpByMi('224.223.243.170')
    #res = GetSubnetGeo_Slash24('224.223.243.170')
    #CloseDb()
    #return

    #GetPfx2ASByBgp('bgp_7660') #2021.3.2使用VP本身的bgp表进行匹配，而不是用routeviews-rv2-20190301-1200.pfx2as_coalesced
    #GetAsListOfIpByRv('201.143.16.1')
    #for i in range(0, 256):
        #ip = '201.143.' + str(i) + '.1'
        #print(ip, end=' ')
    #GetSpecDstIpInTrace('nrt-jp_back.20190315', '201.143.16.1', None)
    #GetPfx2ASByBgp('bgp_7660')
    #prefix = GetLongestMatchPrefixByRv('170.84.82.90')
    
    if sys.argv[1] == 'AnaAsRel':
        GetAsCountryDict()
        GetAsRel()
        AnaAsRel('test1')
        ClearAsCountryDict()
        ClearAsRel()
    if sys.argv[1] == 'GetSpecIpInTrace':
        GetSpecIpInTrace(sys.argv[2], sys.argv[3:], None)
    elif sys.argv[1] == 'GetSpecDstPrefixInTrace':
        wf = open('trace_select_by_prefix', 'w')
        GetSpecDstIpInTrace(sys.argv[2], sys.argv[3], wf)
        wf.close()
    elif sys.argv[1] == 'ip2asbyallbgp':
        for year in range(2018,2021):
            for month in range(1,13):
                if (year == 2020 and month > 4): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
                    continue
                month_str = str(month).zfill(2)
                date = str(year) + month_str + '15'
                for vp in global_var.vps:
                    bgp_filename = global_var.par_path + global_var.rib_dir + 'bgpdata/bgp_' + global_var.trace_as_dict[vp] + '_' + date
                    GetPfx2ASByBgp(bgp_filename)
                    asn = GetAsListOfIpByRv(sys.argv[2])
                    if asn:
                        print("%s %s %s" %(vp, date, asn))
                    ClearIp2AsDict()
    elif sys.argv[1] == 'ip2as':
        vp = sys.argv[2]
        date = sys.argv[3]
        bgp_filename = global_var.par_path + global_var.rib_dir + 'bgpdata/bgp_' + global_var.trace_as_dict[vp] + '_' + date + '15'
        print(bgp_filename)
        GetPfx2ASByBgp(bgp_filename) #2021.3.2使用VP本身的bgp表进行匹配，而不是用routeviews-rv2-20190301-1200.pfx2as_coalescedGetSibRel()
        ConnectToDb()
        SetCurMidarTableDate(int(date[0:4]), int(date[4:]))
        ConnectToBdrMapItDb(global_var.par_path + global_var.out_bdrmapit_dir + 'bdrmapit_' + vp + '_' + date + '15.db')
        ConstrBdrCache()
        for elem in sys.argv[4:]:
            print(elem)
            print(GetAsListOfIpByRv(elem))
            print(GetAsOfIpByMi(elem))
            print(GetIp2ASFromBdrMapItDb(elem))
    elif sys.argv[1] == 'ip2router':        
        SetCurMidarTableDate(int(sys.argv[2]), int(sys.argv[3]))
        print(GetRouterOfIpByMi(sys.argv[4], False))
    elif sys.argv[1] == 'router2geo':        
        SetCurMidarTableDate(int(sys.argv[2]), int(sys.argv[3]))
        print(GetGeoOfRouterByMi(sys.argv[4]))
    elif sys.argv[1] == 'routerInterfaces':
        PrintRouterInterfaces(sys.argv[2])
    elif sys.argv[1] == 'cc':
        GetCCofAs(int(sys.argv[2]), int(sys.argv[3]), sys.argv[4])
    elif sys.argv[1] == 'ip2geo':
        SetCurMidarTableDate(int(sys.argv[2]), int(sys.argv[3]))
        list_ip = sys.argv[4:]
        for ip in list_ip:
            print("%s,%s" %(ip, GetGeoOfIpByMi(ip)))
    elif sys.argv[1] == 'router2asAndgeo':
        list_router = sys.argv[2:]
        for router in list_router:
            print("%s,%s,%s" %(router, GetAsOfRouterByMi(router), GetGeoOfRouterByMi(router)))
    elif sys.argv[1] == 'ipdist':
        SetCurMidarTableDate(int(sys.argv[2]), int(sys.argv[3]))
        geo1 = GetGeoOfIpByMi(sys.argv[4])
        geo2 = GetGeoOfIpByMi(sys.argv[5])
        print(geo1)
        print(geo2)
        print(GeoDistance(geo1, geo2))
    elif sys.argv[1] == 'ipdist2':
        print(GetDistOfIps(sys.argv[2], sys.argv[3]))
    elif sys.argv[1] == 'geodist':
        print(GeoDistance(sys.argv[2] + ' ' + sys.argv[3], sys.argv[4] + ' ' + sys.argv[5]))
    elif sys.argv[1] == 'ip2subnet2geo':
        ip_list = sys.argv[2:]
        for ip in ip_list:
            print(ip)
            geo = GetGeoOfIpByMi(ip)
            if geo:
                print("accurate: %s" %geo)
            else:
                print("similar:")
                print(GetSubnetGeo_Slash24(ip))
    elif sys.argv[1] == 'ip2subnet':
        GetPfx2ASByRv()
        prefix = GetLongestMatchPrefixByRv(sys.argv[2])
        print(prefix)
        ClearIp2AsDict()
    elif sys.argv[1] == 'asrel':
        PreGetSrcFilesInDirs()
        GetAsRel(int(sys.argv[2]), int(sys.argv[3]))
        print(GetAsRelAndTranslate(sys.argv[4], sys.argv[5]))
    elif sys.argv[1] == 'asrels':
        PreGetSrcFilesInDirs()
        GetAsRel(int(sys.argv[2]), int(sys.argv[3]))
        list_as = sys.argv[4:]
        for i in range(0, len(list_as) - 1):
            rel = Get2AsRel(list_as[i], list_as[i + 1])
            if rel == 100:
                print('same ', end = '')
            elif rel == 2:
                print('sibling ', end = '')
            elif rel == 1:
                print('provider ', end = '')
            elif rel == -1:
                print('customer ', end = '')
            elif rel == -2:
                print('peer ', end = '')
            else:
                print('unknown ', end = '')
    elif sys.argv[1] == 'CmpIp2ASDictAndBgp':
        CmpIp2ASDictAndBgp(sys.argv[2])
    elif sys.argv[1] == 'GetPathInBgp':
        GetPathInBgp(sys.argv[2], ' '.join(sys.argv[2:]), False)
    elif sys.argv[1] == 'getorgfromirronLine':
        (org_set, asn_set) = GetOrgAsnFromIRROnLine(sys.argv[2])
        print(org_set)
        print(asn_set)
    elif sys.argv[1] == 'issibbyirr':
        PreGetSrcFilesInDirs()
        GetSibRelByMultiDataFiles(2018, 1)
        print('start')
        print(IsSibByIRR(sys.argv[2], sys.argv[3], sys.stdout)) #asn, ip
    elif sys.argv[1] == 'issibbyirr2':
        print(IsSibByIRR_2(sys.argv[2], sys.argv[3], sys.stdout)) #asn, ip
    elif sys.argv[1] == 'as2org_2':
        PreGetSrcFilesInDirs()
        GetSibRelByMultiDataFiles(2018, 1)
        print(GetOrgByMultiDataFiles_2(sys.argv[2]))
    elif sys.argv[1] == 'as2org':
        PreGetSrcFilesInDirs()
        GetSibRel(int(sys.argv[2]), int(sys.argv[3]))
        print(sib_dict[sys.argv[4]])
    elif sys.argv[1] == 'pathhasvalley':
        PreGetSrcFilesInDirs()
        (ret1, ret2) = PathHasValley(int(sys.argv[2]), int(sys.argv[3]), ' '.join(sys.argv[4:]))
        print(ret1)
        print(ret2)
    else:
        print('failed')
    '''
    ips = ['98.124.179.90', '216.198.168.73', '*', '69.8.0.197']
    for ip in ips:
        print("%s,%s" %(ip, GetGeoOfIpByMi(ip)))
    '''
    #CloseDb()
