
import pymysql
import re
import os

from utils_v2 import CompressAsPath_1

def InsertPathIntoDb(dirname):
    db = pymysql.connect(host="127.0.0.1",user="root",password="",database="dbbgppath") 
    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    for root,dirs,files in os.walk(dirname):
        for filename in files:
            print(filename)
            rf = open(dirname + filename, 'r')
            curline = rf.readline()
            while curline:
                elems = curline.split('|')
                if len(elems) < 3:
                    curline = rf.readline()
                    continue
                if elems[0] == '-' or elems[1].__contains__(':') or elems[2].__contains__('{'): #撤销的update, ipv6以及AS SET，过滤
                    curline = rf.readline()
                    continue
                cur_path = CompressAsPath_1(elems[2])
                path_list = cur_path.split(' ')
                for i in range(0, len(path_list) - 1):
                    for j in range(len(path_list) - 1, i, -1):
                        path_seg = ' '.join(path_list[i:j + 1])
                        select_sql = "SELECT * FROM paths WHERE path=%s"
                        cursor.execute(select_sql, path_seg)
                        res = cursor.fetchall() #当没找到的时候，返回0
                        if res: #找到，不做任何操作
                            j = i + 1   #子串也不做检查，直接退出此循环
                            continue
                        #未找到，插入表项
                        srcdstkey = path_list[i] + ' ' + path_list[j]
                        if len(path_seg) > 200 or len(srcdstkey) > 30:
                            print(curline)
                            return
                        insert_sql_1 = "INSERT INTO paths(path) VALUES(%s)"
                        cursor.execute(insert_sql_1, path_seg)
                        insert_sql_2 = "INSERT INTO pathseg(srcdst,path) VALUES(%s,%s)"
                        value = (srcdstkey, path_seg)
                        #insert_sql_2 = "INSERT INTO pathseg(srcdst) VALUES(%s)"
                        #value = (path_list[i] + ' ' + path_list[j])
                        cursor.execute(insert_sql_2, value)
                curline = rf.readline()
            rf.close()
            db.commit()
            print("end a file")
    cursor.close()
    db.close()


def InsertDstPathIntoDb(dirname):
    db = pymysql.connect(host="127.0.0.1",user="root",password="",database="dbbgppath") 
    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    for root,dirs,files in os.walk(dirname):        
        #for dir in dirs:
            #for sub_root, sub_dirs, sub_files in os.walk(dirname + '\\' + dir):
                #for filename in sub_files:
        for filename in files:
            print(filename)
            #print(dirname + '\\' + dir + '\\' + filename)
            rf = open(dirname + filename, 'r')
            #rf = open(dirname + '\\' + dir + '\\' + filename, 'r', encoding='utf-8')
            curline = rf.readline()
            while curline:
                elems = curline.split('|')
                if len(elems) < 3:
                    curline = rf.readline()
                    continue
                if elems[0] == '-' or elems[1].__contains__(':') or elems[2].__contains__('{'): #撤销的update, ipv6以及AS SET，过滤
                    curline = rf.readline()
                    continue
                cur_path = CompressAsPath_1(elems[2])
                path_list = cur_path.split(' ')
                dst = path_list[-1]
                for i in range(0, len(path_list) - 1):
                    path_seg = ' '.join(path_list[i:])
                    srcdst = path_list[i] + ' ' + dst
                    if len(path_seg) > 200 or len(srcdst) > 30:
                        print(curline)
                        return
                    #select_sql = "SELECT path FROM integpath WHERE srcdst=%s AND path=%s"
                    #cursor.execute(select_sql, (srcdst, path_seg))
                    #res = cursor.fetchall() #当没找到的时候，返回0
                    #if res: #找到，不做后续操作，终止循环
                        #break
                    #未找到，插入表项
                    #insert_sql = "INSERT INTO integpath(srcdst,path) VALUES(%s,%s)"
                    #cursor.execute(insert_sql, (srcdst, path_seg))
                    try:
                        insert_sql = "INSERT INTO integpath_test(srcdst,path) VALUES(%s,%s)"
                        cursor.execute(insert_sql, (srcdst, path_seg))
                    except Exception:
                        break
                curline = rf.readline()
            rf.close()
            db.commit()
            print("end a file")
            os.remove(dirname + filename)
    cursor.close()
    db.close()

def InsertNode2ASIntoDb(filename):
    db = pymysql.connect(host="127.0.0.1",user="root",password="",database="dbbgppath") 
    cursor = db.cursor()
    rf = open(filename, 'r')
    curline = rf.readline()

    while curline:
        elems = curline.split(' ')
        if len(elems) > 3:
            nodeid = elems[1][1:]
            asn = elems[2]
            insert_sql = "INSERT INTO midarnode2as(nodeid,asn) VALUES(%s,%s)"
            cursor.execute(insert_sql, (nodeid, asn))
        curline = rf.readline()
    rf.close()
    db.commit()
    cursor.close()
    db.close()

def InsertNode2ASIntoDb_2():
    db = pymysql.connect(host="127.0.0.1",user="root",password="123",database="bgp_map") 
    cursor = db.cursor()

    for i in range(0, 100):
        print(i)
        rf = open("../../data/mi/sorted_node2as_" + str(i), 'r')
        content = rf.read().strip('\n').strip(',')
        rf.close()
        if not content:
            continue
        index = i % 10
        elems = content.split(',')
        for elem in elems:
            data_info = elem.split(' ')
            nodeid = data_info[0]
            asn = data_info[1]
            insert_sql = "INSERT INTO midarnode2as_%s(nodeid,asn) VALUES('%s','%s')" %(str(index), nodeid, asn)
            cursor.execute(insert_sql)
        db.commit()
    cursor.close()
    db.close()

def InsertIp2NodeIntoDb(filename):
    db = pymysql.connect(host="127.0.0.1",user="root",password="",database="dbbgppath") 
    cursor = db.cursor()
    rf = open(filename, 'r')
    curline = rf.readline()
    linenum = 0

    while curline:
        if linenum % 1000 == 0:
            print(linenum)
        if curline.__contains__('#'):
            curline = rf.readline()
            linenum += 1
            continue
        elems = curline.split(':')
        if len(elems) < 2:
            curline = rf.readline()
            linenum += 1
            continue
        ips = elems[1].strip('\n').strip('\t').strip(' ')
        nodeid = elems[0][6:]
        for cur_ip in ips.split(' '):
            prefix = cur_ip[0:cur_ip.rindex('.')]
            ip = cur_ip[cur_ip.rindex('.') + 1:]
            insert_sql = "INSERT INTO midarip2node(prefix24,ip,nodeid) VALUES(%s,%s,%s)"
            cursor.execute(insert_sql, (prefix, ip, nodeid))
        curline = rf.readline()
        linenum += 1
    rf.close()
    db.commit()
    cursor.close()
    db.close()

def InsertIp2NodeIntoDb_2():
    db = pymysql.connect(host="127.0.0.1",user="root",password="123",database="bgp_map") 
    cursor = db.cursor()

    for i in range(0, 16):
        print(i)
        for j in range(0, 16):
            index = i * 16 + j
            print("index:%d" %index)
            rf = open("../../data/mi/sorted_ip2node_" + str(index), 'r')
            content = rf.read().strip('\n').strip(',')
            rf.close()
            if not content:
                continue
            ip_list = content.split(',')
            for elem in ip_list:
                map_info = elem.split(' ')
                part_ip = map_info[0]
                nodeid = map_info[1]
                prefix = str(index) + '.' + part_ip[0:part_ip.rindex('.')]
                ip_suffix = part_ip[part_ip.rindex('.') + 1:]
                insert_sql = "INSERT INTO midarip2node_%s(prefix24,ip,nodeid) VALUES('%s','%s','%s')" %(str(i), prefix, ip_suffix, nodeid)
                cursor.execute(insert_sql)
        db.commit()
    #db.commit()
    cursor.close()
    db.close()

def InsertNode2GeoIntoDb(filename):
    db = pymysql.connect(host="127.0.0.1",user="root",password="",database="dbbgppath") 
    cursor = db.cursor()
    rf = open(filename, 'r')
    curline = rf.readline()

    while curline:
        if curline.__contains__('#'):
            curline = rf.readline()
            continue
        elems = curline.split(':')
        nodeid = elems[0][len(str('node.geo N')):]
        geo = re.sub('[a-z].*', '', elems[1]) #elems[1]: "					43.6319	-79.3716			maxmind"
        geo = re.sub('\t', ' ', geo)
        geo = geo.strip('\n').strip(' ')
        insert_sql = "INSERT INTO midarnode2geo(nodeid,geo) VALUES(%s,%s,%s)"
        cursor.execute(insert_sql, (nodeid, geo))
        curline = rf.readline()
    rf.close()
    db.commit()
    cursor.close()
    db.close()
    
def InsertNode2GeoIntoDb_2():
    db = pymysql.connect(host="127.0.0.1",user="root",password="123",database="bgp_map") 
    cursor = db.cursor()

    for i in range(0, 100):
        print(i)
        rf = open("../../data/mi/node2geo_" + str(i), 'r')
        content = rf.read().strip('\n').strip(',')
        rf.close()
        if not content:
            continue
        index = i % 10
        elems = content.split(',')
        for elem in elems:
            first_deli = elem.index(' ')
            nodeid = elem[0:first_deli]
            geo = elem[first_deli + 1:]
            insert_sql = "INSERT INTO midarnode2geo_%s(nodeid,geo) VALUES('%s','%s')" %(str(index), nodeid, geo)
            cursor.execute(insert_sql)
        db.commit()
    cursor.close()
    db.close()

def ModiNode2GeoIntoDb_2():
    db = pymysql.connect(host="127.0.0.1",user="root",password="123",database="bgp_map") 
    cursor = db.cursor()

    rf = open('../../data/mi/supple_geo', 'r')
    content = rf.read().strip('\n').strip(',')
    rf.close()
    elems = content.split(',')
    for elem in elems:
        info_list = elem.split(':')
        nodeid = info_list[0]
        geo = info_list[1]
        index = int(nodeid) % 10
        delete_sql = "DELETE FROM midarnode2geo_%s WHERE nodeid='%s'" %(str(index), nodeid)
        cursor.execute(delete_sql)
        insert_sql = "INSERT INTO midarnode2geo_%s(nodeid,geo) VALUES('%s','%s')" %(str(index), nodeid, geo)
        cursor.execute(insert_sql)
    db.commit()
    cursor.close()
    db.close()

if __name__ == '__main__':
    #InsertPathIntoDb("..\\..\\DataFromRouteViews\\20190301\\rib\\")
    #InsertPathIntoDb("..\\..\\DataFromRouteViews\\20190301\\updates\\")
    #InsertDstPathIntoDb("..\\..\\DataFromRouteViews\\2019.03\\2019.0315\\ribs\\")
    #InsertDstPathIntoDb("..\\..\\DataFromRouteViews\\20190315\\updates\\")
    #InsertDstPathIntoDb("..\\..\\DataFromRRC\\20190301\\")
    #InsertDstPathIntoDb("..\\..\\DataFromIsolario\\")
    #InsertIp2NodeIntoDb('midar-iff.nodes')
    
    #InsertIp2NodeIntoDb_2()
    #InsertNode2ASIntoDb_2()
    #InsertNode2GeoIntoDb_2()
    ModiNode2GeoIntoDb_2()
