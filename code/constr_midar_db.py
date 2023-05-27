
import pymysql
import re
import os

import global_var

def InsertNode2ASIntoDb_2(dirname):
    db = pymysql.connect(host="127.0.0.1",user="root",password="123",database="bgp_map") 
    #db = pymysql.connect(host="127.0.0.1",user="root",password="",database="dbbgppath") 
    cursor = db.cursor()

    os.chdir(dirname)
    for root,dirs,files in os.walk('.'):
        for filename in files:
            #if not filename.__contains__('nodes.as.bz2'):
            if filename != '2022-02_midar-iff.nodes.as':
                continue
            #"2016-03_midar-iff.nodes.as.bz2"
            #os.system('bzip2 -d ' + filename)
            #"2016-03_midar-iff.nodes.as"
            print(filename)
            dbtable_name_pre = 'midarnode2as_' + filename[0:4] + filename[5:8] #因为table 名字中不允许使用'-'字符，这里把'-'字符去掉
            for i in range(0, 10): 
                create_sql = "CREATE TABLE %s(nodeid VARCHAR(10) NOT NULL, asn VARCHAR(10), UNIQUE INDEX (nodeid))" %(dbtable_name_pre + str(i))
                cursor.execute(create_sql)
            db.commit()
            line_num = 0
            #rf = open(filename[0:filename.index('.bz2')], 'r')
            #with open(filename[0:filename.index('.bz2')], 'r') as f:
            with open(filename, 'r') as f:
                for line in f:
                    if line.startswith('#'):
                        continue
                    line = line.replace("  ", " ") #有的文件格式不准确！调整格式
                    elems = line.strip('\n').split(' ')
                    nodeid = int(elems[1][1:].strip(':'))
                    asn = elems[2]
                    index = int(nodeid) % 10
                    insert_sql = "INSERT INTO %s(nodeid,asn) VALUES('%s','%s')" %(dbtable_name_pre + str(index), nodeid, asn)
                    try:
                        cursor.execute(insert_sql)
                    except Exception as e:
                        print("INSERT INTO %s(nodeid,asn) VALUES('%s','%s') failed" %(dbtable_name_pre + str(index), nodeid, asn))
                    line_num += 1
                    if line_num % 10000 == 0:
                        db.commit()
                        print(line_num / 10000)
            db.commit()
    cursor.close()
    db.close()
    #os.chdir('../')

def InsertIp2NodeIntoDb_2(dirname):
    db = pymysql.connect(host="127.0.0.1",user="root",password="123",database="bgp_map") 
    #db = pymysql.connect(host="127.0.0.1",user="root",password="",database="dbbgppath") 
    cursor = db.cursor()

    os.chdir(dirname)
    for root,dirs,files in os.walk('.'):
        for filename in files:
            #if (not filename.__contains__('nodes.bz2')):
                #continue
            if filename != '2022-02_midar-iff.nodes':
                continue
            print(filename)
            #"2016-03_midar-iff.nodes.bz2 "
            #os.system('bzip2 -d ' + filename)
            #"2016-03_midar-iff.nodes"
            dbtable_name_pre = 'midarip2node_' + filename[0:4] + filename[5:8] #因为table 名字中不允许使用'-'字符，这里把'-'字符去掉
            for i in range(0, 16): 
                create_sql = "CREATE TABLE %s(prefix24 VARCHAR(16) NOT NULL, ip VARCHAR(4) NOT NULL, nodeid VARCHAR(10), UNIQUE INDEX (prefix24, ip), INDEX (prefix24))" %(dbtable_name_pre + str(i))
                cursor.execute(create_sql)
            db.commit()
            line_num = 0
            #with open(filename[0:filename.index('.bz2')], 'r') as f:
            with open(filename, 'r') as f:
                for line in f:
                    if line.startswith('#'):
                        continue
                    elems = line.split(':')
                    nodeid = elems[0][6:] #len(node N)
                    ips = elems[1].strip('\n').strip('\t').strip(' ')
                    for ip in ips.split(' '):
                        first_seg = ip[0:ip.index('.')]
                        index = int(int(first_seg) / 16)
                        rpos = ip.rindex('.')
                        prefix = ip[0:rpos]
                        ip_suffix = ip[rpos + 1:]
                        insert_sql = "INSERT INTO %s(prefix24,ip,nodeid) VALUES('%s','%s','%s')" %(dbtable_name_pre + str(index), prefix, ip_suffix, nodeid)
                        #print(insert_sql)
                        cursor.execute(insert_sql)
                    line_num += 1
                    if line_num % 10000 == 0:
                        db.commit()
                        print(int(line_num / 10000))
            print('done 1')
            db.commit()
            print('done 2')
            #os.system('mv ' + filename[0:filename.index('.bz2')] + ' decompressed/')
            #print('done 3')
    cursor.close()
    print('done 4')
    db.close()
    print('done 5')
    os.chdir('../')
    print('done 6')

def InsertNode2GeoIntoDb_2(dirname):
    db = pymysql.connect(host="127.0.0.1",user="root",password="123",database="bgp_map") 
    cursor = db.cursor()

    print(dirname)
    os.chdir(dirname)
    for root,dirs,files in os.walk('.'):
        #for filename in files:
        for filename in ['2019-01_midar-iff.nodes.geo']:
            #if not filename.__contains__('nodes.geo.bz2'):
                #continue
            #print(filename)
            # if int(filename[:filename.index('-')]) < 2018:    #2018年以前的先不算
            #     continue
            #os.system('bzip2 -d ' + filename)
            print(filename)
            dbtable_name_pre = 'midarnode2geo_' + filename[0:4] + filename[5:8] #因为table 名字中不允许使用'-'字符，这里把'-'字符去掉
            for i in range(0, 10): 
                create_sql = "CREATE TABLE IF NOT EXISTS %s(nodeid VARCHAR(10) NOT NULL, geo VARCHAR(24), UNIQUE INDEX (nodeid))" %(dbtable_name_pre + str(i))
                cursor.execute(create_sql)
            db.commit()
            line_num = 0
            with open(filename, 'r', encoding='unicode_escape') as rf:
                curline = rf.readline()
                while curline:
                    if curline.startswith('#'):
                        curline = rf.readline()
                        continue
                    nodeid = curline[len('node.geo N'):curline.index(':')]
                    #res = re.findall(r'-?\d+\.\d*e?-?\d*?', line)
                    res = re.findall(r'(-?\d+\.\d+)', curline)
                    if not res:
                        print("NOTE! format error 1: %s" %curline)
                        curline = rf.readline()
                        continue
                    geo = res[0] + ' ' + res[1]
                    #print(nodeid + ':' + geo)
                    index = int(nodeid) % 10
                    insert_sql = "INSERT INTO %s(nodeid,geo) VALUES('%s','%s')" %(dbtable_name_pre + str(index), nodeid, geo)
                    cursor.execute(insert_sql)
                    line_num += 1
                    if line_num % 10000 == 0:
                        db.commit()
                        print(line_num / 10000)
                    curline = rf.readline()
            db.commit()
    cursor.close()
    db.close()
    os.chdir('../')

def ReadSomeContent(filename):
    rf = open(filename, 'r')
    wf = open('2016-03_midar-iff.nodes.geo', 'w')
    #lines = rf.readlines(100)
    #print(len(lines))
    #for line in lines:
        #wf.write(line)
    for i in range(0, 100000):
        line = rf.readline()
        wf.write(line)
    rf.close()
    wf.close()

if __name__ == '__main__':
    #ReadSomeContent()
    #InsertIp2NodeIntoDb_2(global_var.par_path + global_var.midar_dir)
    InsertNode2ASIntoDb_2(global_var.par_path + global_var.midar_dir)
    #InsertNode2GeoIntoDb_2(global_var.par_path + global_var.midar_dir)
    #ModiNode2GeoIntoDb_2()
