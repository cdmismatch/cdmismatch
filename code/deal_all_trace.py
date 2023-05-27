
import os
import multiprocessing as mp
import threading
import sqlite3

import global_var
from utils_v2 import ClearIxpAsSet, ClearIxpPfxDict, ClearSibRel, CompressAsPath, CompressAsPathToMin, IsIxpIp, IsIxpAs, GetAsStrOfIpByRv, GetSibRel, GetIxpPfxDict, GetIxpAsSet, \
                    GetPfx2ASByRv, ClearIp2AsDict
from gen_ip2as_command import PreGetSrcFilesInDirs
from get_ip2as_from_bdrmapit import InitBdrCache_2, ConnectToBdrMapItDb_2, ConstrBdrCache_2, CloseBdrMapItDb_2, \
                                    GetIp2ASFromBdrMapItDb_2

def ResolveTraceWartsPerMonth(year, month):
    os.chdir(str(year) + '/' + str(month).zfill(2) + '/')
    for root,dirs,files in os.walk('.'):
        for filename in files: #例：cdg-fr.20180125
            print(filename)
            if filename.endswith('.gz'):
                os.system('gunzip ' + filename)
                os.system('sc_analysis_dump ' + filename[0:-3] + ' > ' + filename[0:-9])               

def GetLinksFromTrace(trace_list, all_trace_links_set, all_possi_trace_links_set):
    pre_hop = ''
    ixp_flag = False
    for i in range(0, len(trace_list)):
        cur_hop = trace_list[i]
        if cur_hop.__contains__('<'):
            ixp_flag = True
            continue
        if (not pre_hop) or (pre_hop == '*') or (pre_hop == '?') or \
            (cur_hop == '*') or (cur_hop == '?'): # or (set(pre_hop.split('_')) & set(cur_hop.split('_'))):
            pre_hop = cur_hop
            ixp_flag = False
            continue
        for pre_elem in pre_hop.split('_'):
            for cur_elem in cur_hop.split('_'):
                if pre_elem == cur_elem:
                    continue
                link = pre_elem + ' ' + cur_elem
                if link == '3491 58593':
                    print(1)
                if link.__contains__('<'):
                    print('')
                if ixp_flag:
                    all_possi_trace_links_set.add(link)
                else:
                    all_trace_links_set.add(link)
        pre_hop = cur_hop
        ixp_flag = False
    return

#bdrmapit_date_cursor_dict = dict()
def CheckBdrmapitPerDb(ip, as_str, date, bdrmapit_vote_dict):
    # global bdrmapit_date_cursor_dict
    # if date not in bdrmapit_date_cursor_dict.keys():
    #     return None
    # select_sql = "SELECT asn FROM annotation WHERE addr=\'%s\'" %ip
    # bdrmapit_date_cursor_dict[date].execute(select_sql)
    # result = bdrmapit_date_cursor_dict[date].fetchall() 
    # if result:
    #     res = str(result[0][0])   
    res = GetIp2ASFromBdrMapItDb_2(date, ip)
    if res:
        if res in as_str.split('_'):
            return res
        if res not in bdrmapit_vote_dict.keys():
            bdrmapit_vote_dict[res] = 0
        bdrmapit_vote_dict[res] += 1
    return None

def NextDate(date):
    year = int(date[:4])
    month = int(date[4:])
    if year == 2020 and month == 4: #last one
        return None
    if month < 12:
        return str(year) + str(month + 1).zfill(2)
    return str(year + 1) + '01'

def PrevDate(date):
    year = int(date[:4])
    month = int(date[4:])
    if year == 2018 and month == 1:
        return None
    if month > 1:
        return str(year) + str(month - 1).zfill(2)
    return str(year - 1) + '12'

def FurtherCheckBdrmapit(ip, as_str, as_bdrmapit, date):    
    bdrmapit_vote_dict = dict()
    bdrmapit_vote_dict[as_bdrmapit] = 1
    for cur_date in [date, NextDate(date), PrevDate(date)]:
        if not cur_date:
            continue
        res = CheckBdrmapitPerDb(ip, as_str, date, bdrmapit_vote_dict)
        if res:
            return res
    sorted_list = sorted(bdrmapit_vote_dict.items(), key=lambda d:d[1], reverse = True)
    if sorted_list[0][1] == 1: #no bdrmapit result conform, use bgp-map
        return as_str
    else:
        return sorted_list[0][0]

bgp_ip_as_cache = dict()
def GetAsOfIpByBGPAndBdrmapit(ip, dbname):
    global bgp_ip_as_cache
    as_str = ''
    if ip in bgp_ip_as_cache.keys():
        as_str = bgp_ip_as_cache[ip]
    else:
        as_str = GetAsStrOfIpByRv(ip) #moas之间以'_'隔开
        bgp_ip_as_cache[ip] = as_str
    as_bdrmapit = GetIp2ASFromBdrMapItDb_2(dbname, ip)
    if as_bdrmapit != '':
        if as_str:
            if as_bdrmapit not in as_str.split('_'):
                as_str = FurtherCheckBdrmapit(ip, as_str, as_bdrmapit, dbname.split('.')[1][:6])
        else:
            as_str = as_bdrmapit
    return as_str

def ChgTrace2ASPath_2(trace_file_name, w_filename, all_trace_links_set, all_possi_trace_links_set, all_trace_paths_set): #不将trace分类，考虑IXP，只用ribs map
    ConnectToBdrMapItDb_2(trace_file_name, '../back/bdrmapit_' + trace_file_name + '.db')
    ConstrBdrCache_2(trace_file_name)
    wf = None
    if w_filename:
        #print(w_filename)
        wf = open(w_filename, 'w')
    with open(trace_file_name, 'r') as rf:
        data_list = rf.read().strip('\n').split('\n')
    line_num = 0
    for curline in data_list:
        if not curline.startswith('T'):
            continue
        elems = curline.strip('\n').split('\t')
        as_path = ''
        for i in range(13, len(elems)):
            curhops = elems[i]
            if curhops.__contains__('q'):
                as_path = as_path + ' *'
            else:
                #curhops: "210.171.224.41,1.210,1;210.171.224.41,1.216,1"
                hopelems = curhops.split(';')
                hop_as_list = []
                for elem in hopelems:
                    #elem: "210.171.224.41,1.210,1"
                    temp = elem.split(',')
                    #先检查ip是否是ixp的ip
                    #temp[0]: "210.171.224.41"
                    is_ixp_ip = False
                    #print("temp[0]: %s" %temp[0])
                    if IsIxpIp(temp[0]):
                        is_ixp_ip = True #从打印来看挺多的
                    #2021.4.26修改
                    as_str = GetAsOfIpByBGPAndBdrmapit(temp[0], trace_file_name)
                    if as_str:
                        for asn in as_str.split('_'):
                            if IsIxpAs(asn):
                                is_ixp_ip = True
                                break
                        #if not is_ixp_ip and as_str not in hop_as_list:
                        if as_str not in hop_as_list:
                            hop_as_list.append(as_str)
                    else:
                        if '?' not in hop_as_list:
                            hop_as_list.append('?')
                    if is_ixp_ip:
                        hop_as_list = ['<>'] #2021.1.30 做标记，暂时先这样做
                        break #有一个ixp ip就记当前位置为ixp ip
                    
                if len(hop_as_list) == 0: #2021.1.27这里原来没有这一步，有bug，应该考虑过滤掉IXP后没有ip和AS的情况
                    print('NOTE: hop not exist')
                elif len(hop_as_list) == 1:
                    # if hop_as_list[0] == '3491_58593':
                    #     print(curhops)
                    #     print(curline)
                    #     print(trace_file_name)
                    as_path = as_path + ' ' + hop_as_list[0]
                else:
                    as_path = as_path + ' {' + ' '.join(hop_as_list) + '}'
        if wf:
            wf.write(curline + '\n')
            wf.write(as_path + '\n')
        else:
            GetLinksFromTrace(CompressAsPathToMin(CompressAsPath(as_path)).split(' '), all_trace_links_set, all_possi_trace_links_set)
            all_trace_paths_set.add(CompressAsPathToMin(CompressAsPath(as_path)))
            line_num += 1   
            if line_num % 100000 == 0:
                print(line_num)
    #wf.close()
    InitBdrCache_2(trace_file_name)
    CloseBdrMapItDb_2(trace_file_name)
    if wf:
        wf.close()

def ChgTrace2ASPath_2WithPadding(trace_file_name, w_filename, get_link_flag, all_trace_path_set = None, all_trace_links_set = None, all_possi_trace_links_set = None): #不将trace分类，考虑IXP，只用ribs map
    # ConnectToBdrMapItDb_2(trace_file_name, '../back/bdrmapit_' + trace_file_name + '.db') #20210807临时补丁
    # ConstrBdrCache_2(trace_file_name) #20210807临时补丁
    #wf = open(w_filename, 'w')
    with open('back_as_' + trace_file_name, 'r') as rf_back: #20210807临时补丁
        bdrmapit_result_list = rf_back.read().strip('\n').split('\n') #20210807临时补丁
    with open(trace_file_name, 'r') as rf:
        data_list = rf.read().strip('\n').split('\n')
    line_num = 0
    for curline in data_list:
        if not curline.startswith('T'):
            continue
        bdrmapit_line_list = bdrmapit_result_list[line_num].strip(' ').split(' ') #20210807临时补丁
        elems = curline.strip('\n').split('\t')
        as_path = ''
        for i in range(13, len(elems)):
            curhops = elems[i]
            as_bdrmapit = bdrmapit_line_list[i - 13] #20210807临时补丁
            if curhops.__contains__('q'):
                as_path = as_path + ' *'
            else:
                #curhops: "210.171.224.41,1.210,1;210.171.224.41,1.216,1"
                hopelems = curhops.split(';')
                hop_as_list = []
                for elem in hopelems:
                    #elem: "210.171.224.41,1.210,1"
                    temp = elem.split(',')
                    #先检查ip是否是ixp的ip
                    #temp[0]: "210.171.224.41"
                    is_ixp_ip = False
                    #print("temp[0]: %s" %temp[0])
                    if IsIxpIp(temp[0]):
                        is_ixp_ip = True #从打印来看挺多的
                    #2021.4.26修改
                    as_str = ''
                    #print(cur_map_method)
                    as_str = GetAsStrOfIpByRv(temp[0]) #moas之间以'_'隔开
                    #as_bdrmapit = GetIp2ASFromBdrMapItDb_2(trace_file_name, temp[0]) #20210807临时补丁
                    if as_bdrmapit != '':
                        if as_str:
                            if as_bdrmapit not in as_str.split('_'):
                                as_str = as_str + '_' + as_bdrmapit
                        else:
                            as_str = as_bdrmapit
                    if as_str:
                        for asn in as_str.split('_'):
                            if IsIxpAs(asn):
                                is_ixp_ip = True
                                break
                        #if not is_ixp_ip and as_str not in hop_as_list:
                        if as_str not in hop_as_list:
                            hop_as_list.append(as_str)
                    else:
                        if '?' not in hop_as_list:
                            hop_as_list.append('?')
                    if is_ixp_ip:
                        hop_as_list = ['<>'] #2021.1.30 做标记，暂时先这样做
                        break #有一个ixp ip就记当前位置为ixp ip
                    
                if len(hop_as_list) == 0: #2021.1.27这里原来没有这一步，有bug，应该考虑过滤掉IXP后没有ip和AS的情况
                    print('NOTE: hop not exist')
                elif len(hop_as_list) == 1:
                    as_path = as_path + ' ' + hop_as_list[0]
                else:
                    as_path = as_path + ' {' + ' '.join(hop_as_list) + '}'
        #wf.write(as_path + '\n')
        if get_link_flag:
            GetLinksFromTrace(CompressAsPathToMin(CompressAsPath(as_path)).split(' '), all_trace_links_set, all_possi_trace_links_set)
        else:
            all_trace_path_set.add(CompressAsPathToMin(CompressAsPath(as_path)))
        line_num += 1   
    #wf.close()
    # InitBdrCache_2(trace_file_name) #20210807临时补丁
    # CloseBdrMapItDb_2(trace_file_name) #20210807临时补丁

def ChgTrace2ASPathPerMonth(year, month, vp):
    print(vp + ' begin')
    all_trace_links_set = set()
    all_possi_trace_links_set = set()
    for root,dirs,files in os.walk('.'):
        for filename in files: #例：cdg-fr.20180125
            if not filename.startswith('back_'):
                if not filename.startswith(vp):
                    continue
                w_filename = 'as_' + filename
                if os.path.exists(w_filename) and os.path.getsize(w_filename) > 0:
                    continue
                print(filename)
                ChgTrace2ASPath_2(filename, w_filename, all_trace_links_set, all_possi_trace_links_set, None)
    with open(global_var.par_path + global_var.other_middle_data_dir + 'all_trace_links_from_' + vp, 'w') as wf:
        wf.write(','.join(list(all_trace_links_set)))
    with open(global_var.par_path + global_var.other_middle_data_dir + 'all_possi_trace_links_from_' + vp, 'w') as wf:
        wf.write(','.join(list(all_possi_trace_links_set)))
    print(vp + ' end')

def GetPathOrLinksPerMonth(year, month, vp, get_link_flag):
    print(vp + ' begin')
    all_trace_links_set = set()
    all_possi_trace_links_set = set()
    all_trace_path_set = set()
    for root,dirs,files in os.walk('.'):
        for filename in files: #例：cdg-fr.20180125            
            if not filename.startswith('back_'):
                if not filename.startswith(vp):
                    continue
                w_filename = 'as_' + filename
                if os.path.exists(w_filename) and os.path.getsize(w_filename) > 0:
                    continue
                print(filename)
                if get_link_flag:
                    ChgTrace2ASPath_2WithPadding(filename, w_filename, True, None, all_trace_links_set, all_possi_trace_links_set)
                else:
                    ChgTrace2ASPath_2WithPadding(filename, w_filename, False, all_trace_path_set, None, None)
    if get_link_flag:
        with open(global_var.par_path + global_var.other_middle_data_dir + 'all_trace_links_from_' + vp, 'w') as wf:
            wf.write(','.join(list(all_trace_links_set)))
        with open(global_var.par_path + global_var.other_middle_data_dir + 'all_possi_trace_links_from_' + vp, 'w') as wf:
            wf.write(','.join(list(all_possi_trace_links_set)))
    else:
        with open(global_var.par_path + global_var.other_middle_data_dir + 'all_trace_path_from_' + vp, 'w') as wf:
            wf.write('\n'.join(list(all_trace_path_set)))
    print(vp + ' end')

def GetPathOrLinksPerMonth_Ori(vp, only_stat_trace_flag):
    print(vp + ' begin')
    global bgp_ip_as_cache
    #global bdrmapit_date_cursor_dict
    #bdrmapit_date_db_dict = dict()
    all_trace_links_set = set()
    all_possi_trace_links_set = set()
    all_trace_path_set = set()

    for year in range(2018,2021):
        for month in range(1, 13):
            if (year == 2020 and month > 4): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
                continue
            date = str(year) + str(month).zfill(2)
            GetSibRel(year, month)
            GetIxpPfxDict(year, month)
            GetIxpAsSet()   #这条语句需要放在GetSibRel()之后，因为需要sib_dict，也就是as2org_dict
            GetPfx2ASByRv(year, month)
            bgp_ip_as_cache.clear()
            for root,dirs,files in os.walk('.'):
                for filename in files: #例：cdg-fr.20180125
                    (cur_vp, cur_date) = filename.split('.')
                    if cur_vp != vp or  date != cur_date[:6]:
                        continue
                    w_filename = 'as_' + filename
                    if os.path.exists(w_filename) and os.path.getsize(w_filename) > 0:
                        continue
                    print(filename)
                    if only_stat_trace_flag: #其实可以一次把as file写下来，同时统计trace和link。但是原来为了快，只统计了trace和link，现在还是需要写as file
                        ChgTrace2ASPath_2(filename, None, all_trace_links_set, all_possi_trace_links_set, all_trace_path_set)
                    else:
                        ChgTrace2ASPath_2(filename, w_filename, None, None, None)
            ClearIp2AsDict()
            ClearIxpAsSet()
            ClearIxpPfxDict()
            ClearSibRel()
    
    #for key in bdrmapit_date_cursor_dict.keys():
        # bdrmapit_date_cursor_dict[key].close()
        # bdrmapit_date_db_dict[key].close()

    if only_stat_trace_flag:
        with open(global_var.par_path + global_var.other_middle_data_dir + 'all_trace_links_2_from_' + vp, 'w') as wf:
            wf.write(','.join(list(all_trace_links_set)))
        with open(global_var.par_path + global_var.other_middle_data_dir + 'all_possi_trace_links_2_from_' + vp, 'w') as wf:
            wf.write(','.join(list(all_possi_trace_links_set)))
        with open(global_var.par_path + global_var.other_middle_data_dir + 'all_trace_path_2_from_' + vp, 'w') as wf:
            wf.write('\n'.join(list(all_trace_path_set)))
    print(vp + ' end')

def TmpPerProc(year, month, vp):
    global bgp_ip_as_cache
    #global bdrmapit_date_cursor_dict
    #bdrmapit_date_db_dict = dict()
    all_trace_links_set = set()
    all_possi_trace_links_set = set()
    all_trace_path_set = set()

    GetSibRel(year, month)
    GetIxpPfxDict(year, month)
    date = str(year) + str(month).zfill(2)
    bgp_ip_as_cache.clear()
    for root,dirs,files in os.walk('.'):
        for filename in files: #例：cdg-fr.20180125
            (cur_vp, cur_date) = filename.split('.')
            if cur_vp != vp or  date != cur_date[:6]:
                continue
            # w_filename = 'as_' + filename
            # if os.path.exists(w_filename) and os.path.getsize(w_filename) > 0:
            #     continue
            print(filename)
            ChgTrace2ASPath_2(filename, None, all_trace_links_set, all_possi_trace_links_set, all_trace_path_set)
    ClearSibRel()
    ClearIxpPfxDict()

    with open(global_var.par_path + global_var.other_middle_data_dir + 'all_trace_links_2_from_' + vp + '_' + date, 'w') as wf:
        wf.write(','.join(list(all_trace_links_set)))
    with open(global_var.par_path + global_var.other_middle_data_dir + 'all_possi_trace_links_2_from_' + vp + '_' + date, 'w') as wf:
        wf.write(','.join(list(all_possi_trace_links_set)))
    with open(global_var.par_path + global_var.other_middle_data_dir + 'all_trace_path_2_from_' + vp + '_' + date, 'w') as wf:
        wf.write('\n'.join(list(all_trace_path_set)))

def GetPathOrLinksPerMonth_Ori_MultiProc(vp, flag):
    print(vp + ' begin')
    proc_list = []
    all_trace_links_set = set()
    all_possi_trace_links_set = set()
    all_trace_path_set = set()

    for year in range(2018,2021):
        for month in range(1, 13):
            if (year == 2020 and month > 4): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
                continue
            proc_list.append(mp.Process(target=TmpPerProc, args=(year, month, vp)))
            #RunBdrmapitPermonthAllVps(year, month)
    for proc in proc_list:
        proc.start()
    for proc in proc_list:
        proc.join()
    
    output = os.popen('ls ' + global_var.par_path + global_var.other_middle_data_dir + 'all_trace_links_2_from_' + vp + '_*')
    data = output.read()
    for filename in data.strip('\n').split('\n'):
        with open(filename, 'r') as rf:            
            tmp_set = set(rf.read().strip(',').split(','))
            all_trace_links_set |= tmp_set
        os.system('rm -f %s' %filename)
    with open(global_var.par_path + global_var.other_middle_data_dir + 'all_trace_links_2_from_' + vp, 'w') as wf:
        wf.write(','.join(list(all_trace_links_set)))
    output = os.popen('ls ' + global_var.par_path + global_var.other_middle_data_dir + 'all_possi_trace_links_2_from_' + vp + '_*')
    data = output.read()
    for filename in data.strip('\n').split('\n'):
        with open(filename, 'r') as rf:
            tmp_set = set(rf.read().strip(',').split(','))
            all_possi_trace_links_set |= tmp_set
        os.system('rm -f %s' %filename)
    with open(global_var.par_path + global_var.other_middle_data_dir + 'all_possi_trace_links_2_from_' + vp, 'w') as wf:
        wf.write(','.join(list(all_possi_trace_links_set)))
    output = os.popen('ls ' + global_var.par_path + global_var.other_middle_data_dir + 'all_trace_path_2_from_' + vp + '_*')
    data = output.read()
    for filename in data.strip('\n').split('\n'):
        with open(filename, 'r') as rf:
            tmp_set = set(rf.read().strip(',').split(','))
            all_trace_path_set |= tmp_set
        os.system('rm -f %s' %filename)
    with open(global_var.par_path + global_var.other_middle_data_dir + 'all_trace_path_2_from_' + vp, 'w') as wf:
        wf.write(','.join(list(all_trace_path_set)))
    
    
    #for key in bdrmapit_date_cursor_dict.keys():
        # bdrmapit_date_cursor_dict[key].close()
        # bdrmapit_date_db_dict[key].close()
    print(vp + ' end')

def GetPathOrLinksPerMonth_Ori_Debug(year, month, vp):
    all_trace_links_set = set()
    all_possi_trace_links_set = set()
    all_trace_path_set = set()
    GetSibRel(year, month)
    GetIxpPfxDict(year, month)
    date = str(year) + str(month).zfill(2)
    for root,dirs,files in os.walk('.'):
        for filename in files: #例：cdg-fr.20180125
            (cur_vp, cur_date) = filename.split('.')
            if cur_vp != vp or  date != cur_date[:6]:
                continue
            # w_filename = 'as_' + filename
            # if os.path.exists(w_filename) and os.path.getsize(w_filename) > 0:
            #     continue
            #print(filename)
            ChgTrace2ASPath_2(filename, None, all_trace_links_set, all_possi_trace_links_set, all_trace_path_set)
    ClearSibRel()
    ClearIxpPfxDict()


def TmpGetIpsOfTraceByIp(obj_ip, files):    
    output = os.popen('grep ' + obj_ip + ' ' + files)
    data = output.readline()
    info_suc = dict()
    info_prev = dict()
    dbname = 'mty-tx'
    db_pathname = '../back/bdrmapit_mty-mx.20200216.db'
    ConnectToBdrMapItDb_2(dbname, db_pathname)
    ConstrBdrCache_2(dbname)
    while data:
        elems = data.strip('\n').split('\t')
        dst_ip = elems[2]
        suc_ip = ''
        prev_ip = ''
        for i in range(13, len(elems)):
            if elems[i].__contains__(obj_ip):
                if not prev_ip:
                    if i > 0:
                        prev_ip = elems[i - 1].split(',')[0]
                    else:
                        prev_ip = 'N'
                if i < len(elems) - 1:
                    suc_ip = elems[i + 1].split(',')[0]
                    if suc_ip != obj_ip:
                        break
                    else:
                        suc_ip = ''
                else:
                    break
        if suc_ip not in info_suc.keys():
            info_suc[suc_ip] = [0, []]
        info_suc[suc_ip][0] += 1
        info_suc[suc_ip][1].append(dst_ip)
        if prev_ip not in info_prev.keys():
            info_prev[prev_ip] = 0
        info_prev[prev_ip] += 1
        data = output.readline()
    sort_list_suc = sorted(info_suc.items(), key=lambda d:d[1][0], reverse = True)
    sort_list_prev = sorted(info_prev.items(), key=lambda d:d[1], reverse = True)
    print('next ip:')
    for elem in sort_list_suc:
        (key, val) = elem        
        if key != '' and key != 'q':
            print('%s (%s): %d' %(key, GetAsOfIpByBGPAndBdrmapit(key, dbname), val[0]))
        else:
            print('%s (): %d' %(key, val[0]))
        if key == '':
            dst_as_dict = dict()
            for elem in val[1]:
                as_str = GetAsOfIpByBGPAndBdrmapit(elem, dbname)
                if as_str:
                    if as_str not in dst_as_dict.keys():
                        dst_as_dict[as_str] = 0
                    dst_as_dict[as_str] += 1
            sort_list_local = sorted(dst_as_dict.items(), key=lambda d:d[1], reverse = True)
            print('\t', end='')
            for sub_elem in sort_list_local:
                (asn, num) = sub_elem
                print('%s(%d)' %(asn, num), end=',')
            print('')
    print('prev ip:')
    for elem in sort_list_prev:
        (key, val) = elem        
        if key != '':
            print('%s (%s): %d' %(key, GetAsOfIpByBGPAndBdrmapit(key, dbname), val))
        else:
            print('%s (): %d' %(key, val))
    InitBdrCache_2(dbname)
    CloseBdrMapItDb_2(dbname)

def IndexCompressPathToPath(vp, no_use_flag):
    comp_ori_dict = dict()
    for root,dirs,files in os.walk('.'):
        for filename in files: #例：cdg-fr.20180125
            if not filename.startswith('as_' + vp):
                continue
            print(filename)
            with open(filename, 'r') as rf:
                curline_ip = rf.readline()
                while curline_ip:
                    curline_trace = rf.readline()
                    trace = curline_trace.strip('\n').strip('\t').strip(' ')
                    compress_trace = CompressAsPath(trace)
                    if compress_trace not in comp_ori_dict.keys():
                        comp_ori_dict[compress_trace] = set()
                    comp_ori_dict[compress_trace].add(trace)
                    curline_ip = rf.readline()
    print(vp + ' done')
    with open(global_var.par_path + global_var.other_middle_data_dir + 'compress_trace_to_ori_trace_' + vp, 'w') as wf:
        for (comp, trace_set) in comp_ori_dict.items():
            wf.write(comp + ':' + ','.join(list(trace_set)) + '\n')

if __name__ == '__main__':
    PreGetSrcFilesInDirs()
    os.chdir(global_var.all_trace_par_path + global_var.all_trace_download_dir + 'result/')
    thread_list = []

    #Debug
    # tmp_set1 = set()
    # tmp_set2 = set()
    # with open('/mountdisk1/ana_c_d_incongruity/other_middle_data/temp', 'r') as rf:
    #     curline = rf.readline()
    #     while curline:
    #         GetLinksFromTrace(curline.strip('\n').split(' '), tmp_set1, tmp_set2)
    #         curline = rf.readline()    
    
    # for year in range(2018,2021):
    #     for month in range(1, 13):
    #         if (year == 2020 and month > 4): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
    #             continue
    #         thread_list.append(mp.Process(target=GetPathOrLinksPerMonth_Ori_Debug, args=(year, month, 'mty-mx')))
    # for thread in thread_list:
    #         thread.start()
    # for thread in thread_list:
    #     thread.join()
    #TmpGetIpsOfTraceByIp('63.223.15.174', 'mty-mx.20*')
    #TmpGetIpsOfTraceByIp('63.223.15.178', 'mty-mx.20*')
    # print('done')
    # vps = set()
    # for root,dirs,files in os.walk('.'):
    #     for filename in files:
    #         vps.add(filename.split('.')[0])
    # #print(vps)
    # tmp = {'ams2-nl', 'arn-se', 'bcn-es', 'bjl-gm', 'bma-se', 'bwi-us', 'cdg-fr', 'cjj-kr', 'dfw-us', 'dub-ie', 'eug-us', 'fnl-us', 'hel-fi', 'hkg-cn', 'hlz-nz', 'iad-us', 'jfk-us', 'lej-de', 'mel-au', 'mty-mx', 'nrt-jp', 'ord-us', 'osl-no', 'per-au', 'pna-es', 'pry-za', 'rno-us', 'sao-br', 'scl-cl', 'sea-us', 'sjc2-us', 'tpe-tw', 'wbu-us', 'yow-ca', 'yyz-ca', 'zrh2-ch'}
    # print(vps.difference(tmp))
    # while True:
    #     pass

    # for year in range(2018,2021):
    #     for month in range(1, 13):
    #         if (year == 2020 and month > 4): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
    #             continue
    #         date = str(year) + str(month).zfill(2)
    #         tmp_db_name = '../back/bdrmapit_%s.db' %date
    #         if os.path.exists(tmp_db_name) and os.path.getsize(tmp_db_name) > 0:
    #             # bdrmapit_date_db_dict[date] = sqlite3.connect('../back/bdrmapit_%s.db' %date)
    #             # if bdrmapit_date_db_dict[date]:
    #             #     bdrmapit_date_cursor_dict[date] = bdrmapit_date_db_dict[date].cursor()
    #             ConnectToBdrMapItDb_2(date, tmp_db_name)
    #             #InitBdrCache_2(date)
    #             ConstrBdrCache_2(date)

    vps = set()
    for root,dirs,files in os.walk('.'):
        for filename in files:
            if filename.startswith('as_'):
                continue
            vps.add(filename.split('.')[0])
    for vp in vps:
    #for vp in ['cbg-uk']:
        #GetPathOrLinksPerMonth_Ori_MultiProc(vp, True)
    # 2019.01 trace get
    #     #ChgTrace2ASPathPerMonth(year, month, vp)
    #     #thread_list.append(threading.Thread(target=ChgTrace2ASPathPerMonth,args=(year, month, vp)))
    #     #thread_list.append(mp.Process(target=ChgTrace2ASPathPerMonth, args=(year, month, vp)))
          #thread_list.append(mp.Process(target=GetPathOrLinksPerMonth, args=(year, month, vp, False)))
        #3 days/month trace get      
        #thread_list.append(mp.Process(target=GetPathOrLinksPerMonth_Ori, args=(vp, False)))
        thread_list.append(mp.Process(target=IndexCompressPathToPath, args=(vp, False)))
    for thread in thread_list:
        thread.start()
    for thread in thread_list:
        thread.join()
    # ClearIp2AsDict()
    # ClearIxpAsSet()
    # ClearIxpPfxDict()
    # ClearSibRel()
    
    # for year in range(2018,2021):
    #     for month in range(1, 13):
    #         if (year == 2020 and month > 4): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
    #             continue
    #         date = str(year) + str(month).zfill(2)
    #         tmp_db_name = '../back/bdrmapit_%s.db' %date
    #         if os.path.exists(tmp_db_name) and os.path.getsize(tmp_db_name) > 0:
    #             CloseBdrMapItDb_2(date)
    #             InitBdrCache_2(date)    
