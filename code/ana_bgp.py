
import os
import re
import socket
import struct
import multiprocessing as mp
import sys

import requests

import global_var
from gen_ip2as_command import GetCloseDateFile, PreGetSrcFilesInDirs
from download_irrdata import GetOrgAsnFromIRROnLine, GetCountryOrgAsnFromIrrOnline
from ana_inconformity import FindTraceAsInBgpPath, ClassifyAbTrace2, PrintDetourDict
from ana_prefix_traceroute_group_by_prefix_v2 import ChgTrace2ASPath, CompressAsPath, CompressAsPathToMin, CheckTracesByIRRData
from utils_v2 import ClearAsRel, ClearIxpAsSet, ClearSibRel, FindBgpAsInTracePath, GetSibRel, AsnInTracePathList, DropStarsInTraceList, IsSib, PathHasValley_2, GetBgpByPrefix, \
                    GetBgpPathFromBgpPrefixDict_2, ClearBGPByPrefix, GetAsRel, GetIxpAsSet, IsIxpAs, GetSibRelByMultiDataFiles, GetOrgByMultiDataFiles_2, PreLoadAsnInfoFromASNS, \
                    GetAsnInfoFromASNS, ClearAsnInfoFromASNS, ConstrPeerDbInfoDict, IsTwoAsPeerInIXP, GetNeighFromRipe, ClearPeerDbInfoDict, IsAsSet, GetAsRel_2, ClearAsRel_2, \
                    GetNeighOfAs, GetFuncLgDict, GetFuncOfLg, ClearFuncLgDict, GetAsPfxDict, GetRepIpsOfAs, ClearAsPfxDict, GetAsStrOfIpByRv, GetPfx2ASByRv, ClearIp2AsDict, \
                    GetPfx2ASByRvV6, ClearIp2AsDictV6, GetAsStrOfIpByRvV6, GetAsRankDict, ClearAsRankDict, GetAsRankFromDict, Get2AsRel, GetCCNums, GetCCNumsFromDict

all_traces_set = set()
def PreGetAllTraces():
    global all_traces_set
    obj_filename = global_var.all_trace_par_path + global_var.all_trace_out_data_dir + global_var.all_trace_out_all_trace_filename
    if not os.path.exists(obj_filename):
        print('Construct all_traces file')
        for year in range(2018,2021):
            for month in range(1,13):
                if (year == 2016 and month < 4) or (year == 2020 and month > 4): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
                    continue
                os.chdir(global_var.all_trace_par_path + global_var.all_trace_trace_as_res_dir + str(year) + '/' + str(month).zfill(2) + '/')
                for root,dirs,files in os.walk('.'):
                    for filename in files: #例：cdg-fr.20180125
                        with open(filename, 'r') as rf:
                            curline_trace = rf.readline()
                            while curline_trace:
                                ori_trace_path = curline_trace.strip('\n')
                                trace_path = CompressAsPathToMin(CompressAsPath(ori_trace_path))
                                all_traces_set.add(trace_path)
                                curline_trace = rf.readline()
        print(len(all_traces_set))
        with open(obj_filename, 'w') as wf:
            wf.write(';'.join(all_traces_set))
    else:
        print('Get all traces from file')
        with open(obj_filename, 'r') as rf:
            data = rf.read()
        all_traces_set = set(data.strip(';').split(';'))
        print(len(all_traces_set))

def PreGetAllTraces_Mini():
    global all_traces_set
    obj_filename = global_var.par_path + global_var.other_middle_data_dir + 'all_traces'
    if not os.path.exists(obj_filename):
    #if True:
        print('Construct all_traces file')
        for year in range(2018,2021):
            for month in range(1,13):
                if (year == 2016 and month < 4) or (year == 2020 and month > 4): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
                    continue
                month_str = str(month).zfill(2)
                date = str(year) + month_str + '15'
                for vp in global_var.vps:
                    parent_dir = global_var.par_path + global_var.out_my_anatrace_dir + '/' + vp + '_' + date + '/ribs_midar_bdrmapit/'
                    filenames = ['final_normal_fin', 'final_unmap_fin', 'final_ab_fin']
                    for filename in filenames:
                        path = parent_dir + filename
                        print(path)
                        if not os.path.exists(path):
                            continue
                        with open(path, 'r') as rf:
                            curline_trace = rf.readline()
                            while curline_trace:
                                #print(curline_trace)
                                curline_bgp = rf.readline()
                                curline_ip = rf.readline()
                                ori_trace_path = curline_trace[curline_trace.index(']') + 1:].strip('\n').strip(' ')
                                trace_path = CompressAsPathToMin(CompressAsPath(ori_trace_path))
                                all_traces_set.add(trace_path)
                                curline_trace = rf.readline()
        print(len(all_traces_set))
        with open(obj_filename, 'w') as wf:
        #with open(obj_filename, 'a') as wf:
            #wf.write(';')
            wf.write(';'.join(all_traces_set))
    else:
        print('Get all traces from file')
        with open(obj_filename, 'r') as rf:
            data = rf.read()
        all_traces_set = set(data.strip(';').split(';'))
        print(len(all_traces_set))

def TransOneTraceToUniASTraces(trace):
    #print(trace)
    if not trace.__contains__(' '):
        return set(trace.split('_'))
    res_trace_set = set()
    for elem in trace.split(' ')[0].split('_'):
        sub_trace_set = TransOneTraceToUniASTraces(trace[trace.index(' ') + 1:])
        for sub_trace in sub_trace_set:
            tmp = CompressAsPathToMin(CompressAsPath(elem + ' ' + sub_trace))
            res_trace_set.add(tmp)
    return res_trace_set

def PreTransAllTraceToUniASTraces():
    global all_traces_set
    res_trace_set = set()
    for trace in all_traces_set:
        #print(trace)
        cur_trace_set = TransOneTraceToUniASTraces(trace)
        res_trace_set |= cur_trace_set
    print(len(res_trace_set))
    #with open(global_var.all_trace_par_path + global_var.all_trace_out_data_dir + global_var.all_trace_out_all_trace_uni_as_filename, 'w') as wf:
    with open(global_var.par_path + global_var.other_middle_data_dir + 'all_traces_uni_as', 'w') as wf:
        wf.write(';'.join(res_trace_set))
    return res_trace_set

all_trace_set_uni_as = set()
def PreGetAllTracesWithUniAs():
    global all_trace_set_uni_as
    #with open(global_var.all_trace_par_path + global_var.all_trace_out_data_dir + global_var.all_trace_out_all_trace_uni_as_filename, 'r') as rf:
    with open(global_var.par_path + global_var.other_middle_data_dir + 'all_traces_uni_as', 'r') as rf:
        data = rf.read()
    all_trace_set_uni_as = data.split(';')
    print(len(all_trace_set_uni_as))

def GetLinkSetFromBgpPath(bgp_path):
    cur_path = CompressAsPathToMin(CompressAsPath(bgp_path))
    link_set = set()
    prev_elem = ''
    for elem in cur_path.split(' '):
        if prev_elem == '' or prev_elem.__contains__(',') or elem.__contains__(','):
            prev_elem = elem
            continue
        link_set.add(prev_elem + ' ' + elem)
        prev_elem = elem
    return link_set

g_bgp_paths_dict = dict()
g_bgp_link_path_dict = dict() #建立link到path的索引
def PreGetAllBgpPaths():
    global g_bgp_paths_dict
    global g_bgp_link_path_dict
    #g_bgp_paths_dict.clear()
    filepath = global_var.par_path + global_var.other_middle_data_dir + 'all_bgp_paths'
    if not os.path.exists(filepath):
        for year in range(2018,2021):
            for month in range(1,13):
                if (year == 2016 and month < 4) or (year == 2020 and month > 4): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
                    continue
                month_str = str(month).zfill(2)
                date = str(year) + month_str + '15'
                for vp in global_var.vps:
                    filename = 'bgp_' + global_var.trace_as_dict[vp] + '_' + date
                    print(filename)
                    with open(global_var.par_path + global_var.rib_dir + '/bgpdata/' + filename, 'r') as rf:
                        curline = rf.readline()
                        while curline:
                            elems = curline.split('|')
                            if len(elems) < 3:
                                print(curline)
                                curline = rf.readline()
                                continue
                            cur_path = CompressAsPathToMin(CompressAsPath(elems[2]))
                            # if cur_path.__contains__('3491 58593'):
                            #     print('h')
                            if cur_path not in g_bgp_paths_dict.keys():
                                g_bgp_paths_dict[cur_path] = dict()
                            locate = date + '_' + vp
                            if locate not in g_bgp_paths_dict[cur_path].keys():
                                g_bgp_paths_dict[cur_path][locate] = 0
                            g_bgp_paths_dict[cur_path][locate] += 1
                            curline = rf.readline()
        with open(filepath, 'w') as wf:
        #with open(filepath, 'a') as wf: #2021.10.13
            for (bgp_path, info) in g_bgp_paths_dict.items():
                wf.write(bgp_path + ':')
                for (locate, num) in info.items():
                    wf.write(locate + ' ' + str(num) + ',')
                wf.write(';')
    else:
        with open(filepath, 'r') as rf:
            data_list = rf.read().strip(';').split(';')
            for elem in data_list:
                items = elem.split(':')
                g_bgp_paths_dict[items[0]] = dict()
                for sub_elem in items[1].strip(',').split(','):
                    tmp = sub_elem.split(' ')
                    g_bgp_paths_dict[items[0]][tmp[0]] = int(tmp[1])
                    #g_bgp_paths_dict[cur_path][locate] += 1
                bgp_path = items[0]
                if bgp_path == '6939 701 5511 39386 41426 48237 35819 15802':
                    print('Pre here 1')
                link_set = GetLinkSetFromBgpPath(bgp_path)
                for link in link_set:
                    if link not in g_bgp_link_path_dict.keys():
                        g_bgp_link_path_dict[link] = set()
                    g_bgp_link_path_dict[link].add(bgp_path)
            print(len(g_bgp_paths_dict))
        # with open('test', 'w') as wf:
        #     for (bgp_path, info) in g_bgp_paths_dict.items():
        #         wf.write(bgp_path + ':')
        #         for (locate, num) in info.items():
        #             wf.write(locate + ' ' + str(num) + ',')
        #         wf.write('\n')
        # if '7660 2516 3491 58593' in g_bgp_paths_dict.keys():
        #     print('in bgp')
        # else:
        #     print('not in bgp')

#transient_bgp_date_count = 5
transient_bgp_date_count = 1
transient_bgp_occur_count = 20
transient_bgp_interval = 7200 #7200s, 2 hours
if_bgp_transient_dict = dict()
def BgpSegIsTransient(bgp_seg, wf_one_date_not_transient): #先找出多于一个date的BGP
    global g_bgp_paths_dict
    global if_bgp_transient_dict
    global g_bgp_link_path_dict
    debug_set = g_bgp_link_path_dict['48237 35819']
    if len(debug_set) == 1:
        print('Error occur')
    if bgp_seg in if_bgp_transient_dict.keys():
        return if_bgp_transient_dict[bgp_seg]
    occur_count = 0
    #step 1
    date_set = set()
    locate_set = set()
    if bgp_seg in g_bgp_paths_dict.keys(): #先快速查找
        info = g_bgp_paths_dict[bgp_seg]        
        #info: dict(locate: num)
        for locate in info.keys():
            date_set.add(locate[:locate.index('_')])
            locate_set.add(locate)
            if len(date_set) > transient_bgp_date_count:
                if_bgp_transient_dict[bgp_seg] = False
                return False
        # if sum(info.values()) > transient_bgp_occur_count: #2021.9.16 不能用出现的数量衡量，因为路由抖动会在某一瞬间带来大面积的影响
        #     if_bgp_transient_dict[bgp_seg] = False
        #     return False
    link_set = GetLinkSetFromBgpPath(bgp_seg)
    # if bgp_seg == '6939 701 5511 39386 41426 48237 35819 15802':
    #     print('Check here')
    tmp_path_set = set()
    start = True
    for link in link_set:
        if link not in g_bgp_link_path_dict.keys():
            print('This ought not happen. Link: %s, path: %s' %(link, bgp_seg))
            if_bgp_transient_dict[bgp_seg] = True
            return True
        if start:
            tmp_path_set = g_bgp_link_path_dict[link]
            start = False
        else:
            debug_path_set = g_bgp_link_path_dict[link]
            tmp_path_set1 = tmp_path_set & debug_path_set
            if not tmp_path_set1:
                print('This ought not happen. path: %s' %bgp_seg)
                if_bgp_transient_dict[bgp_seg] = True
                return True
            tmp_path_set = tmp_path_set1
    res_path_set = set()
    for tmp_path in tmp_path_set:
        if tmp_path.__contains__(bgp_seg):
            res_path_set.add(tmp_path)
    for res_path in res_path_set:
        if not res_path in g_bgp_paths_dict.keys(): 
            print('This ought not happen. res_path: %s' %res_path)
        info = g_bgp_paths_dict[res_path]        
        #info: dict(locate: num)
        for locate in info.keys():
            date_set.add(locate[:locate.index('_')])
            locate_set.add(locate)
            if len(date_set) > transient_bgp_date_count:
                if_bgp_transient_dict[bgp_seg] = False
                return False
    # for (bgp_path, info) in g_bgp_paths_dict.items():
    #     if bgp_path.__contains__(bgp_seg):
    #         for (locate, count) in info.items():
    #             date_set.add(locate[:locate.index('_')])
    #             locate_set.add(locate)
    #             if len(date_set) > transient_bgp_date_count:
    #                 if_bgp_transient_dict[bgp_seg] = False
    #                 return False
    #             occur_count += count
    #             # if occur_count > transient_bgp_occur_count:
    #             #     if_bgp_transient_dict[bgp_seg] = False
    #             #     return False
    if len(date_set) > transient_bgp_date_count:# or occur_count > transient_bgp_occur_count:
        if_bgp_transient_dict[bgp_seg] = False
        return False #not transient

    #2021.10.13 step 2, 对于只在一个日期里出现过的bgp path，进一步查找出现的时间
    search_path_dir = global_var.par_path + global_var.rib_dir + 'bgpdata/bgp_'
    for locate in locate_set:
        (date, vp) = locate.split('_')
        output = os.popen('grep \'' + bgp_seg + '\' ' + search_path_dir + global_var.trace_as_dict[vp] + '_' + date)
        if output:
            data = output.readline()
            min_timestamp = 0
            max_timestamp = 0
            while data:
                timestamp = int(float((data.split('|')[-2])))
                if min_timestamp == 0 or timestamp < min_timestamp:
                    min_timestamp = timestamp
                if timestamp > max_timestamp:
                    max_timestamp = timestamp
                data = output.readline()
            if (max_timestamp - min_timestamp) > transient_bgp_interval: #不认为是transient
                wf_one_date_not_transient.write(bgp_seg + '\n')
                if_bgp_transient_dict[bgp_seg] = False
                return False

    if_bgp_transient_dict[bgp_seg] = True
    return True

def GetAllBgpOfAbTrace(filename):
    os.chdir(global_var.par_path +  global_var.out_my_anatrace_dir + '/')
    wf_one_date_not_transient = open('one_date_not_transient_bgp', 'w') #2021.10.13 这里记录一下一个日期内但超过2小时的bgp path
    for year in range(2018,2021):
    #for year in range(2020,2021):
        for month in range(1,13):
        #for month in range(1,5):
            if (year == 2016 and month < 4) or (year == 2020 and month > 4): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
                continue
            date = str(year) + str(month).zfill(2) + '15'
            #print(date)
            for vp in global_var.vps:
                #print(vp)
                g_asn = global_var.trace_as_dict[vp]    
                bgp_filename = global_var.par_path + global_var.rib_dir + 'bgpdata/bgp_' + g_asn + '_' + date
                GetBgpByPrefix(bgp_filename)
                cur_sub_dir = vp + '_' + date + '/ribs_midar_bdrmapit/' #'nrt-jp.2019030115/'
                if not os.path.exists(cur_sub_dir):
                    continue
                wf = open(cur_sub_dir + filename + '_all_bgp', 'w')
                print(date + '_' + vp)
                with open(cur_sub_dir + filename, 'r') as rf:
                    curline_trace = rf.readline()
                    while curline_trace:
                        curline_bgp = rf.readline()
                        curline_ip = rf.readline()
                        dst_prefix = curline_trace[1:curline_trace.index(' ')]
                        #print(dst_prefix)
                        bgp_list = GetBgpPathFromBgpPrefixDict_2(dst_prefix)
                        wf.write(curline_trace)
                        for bgp_path in bgp_list:
                            if not BgpSegIsTransient(bgp_path, wf_one_date_not_transient): #只记录出现过多次的bgp path
                                wf.write("\t%s\n" %CompressAsPath(bgp_path))
                        wf.write(curline_ip)
                        curline_trace = rf.readline()
                ClearBGPByPrefix()
                wf.close()
    wf_one_date_not_transient.close()

g_all_trace_links_set = set()
def PreGetAllTraceLinks(): #ixp hop不算在link里
    global all_trace_set_uni_as
    global g_all_trace_links_set
    PreGetAllTracesWithUniAs()
    #filename = global_var.all_trace_par_path + global_var.all_trace_out_data_dir + global_var.all_trace_out_all_trace_links_filename
    filename = global_var.par_path + global_var.other_middle_data_dir + 'all_trace_links'
    if not os.path.exists(filename):
        for ori_trace in all_trace_set_uni_as:
            trace = CompressAsPathToMin(CompressAsPath(ori_trace))
            trace_list = trace.split(' ')
            pre_elem = None
            for i in range(0, len(trace_list)):
                cur_elem = trace_list[i]
                if (not pre_elem) or (pre_elem == '*') or (pre_elem == '?') or (pre_elem.__contains__('<')) or \
                    (cur_elem == '*') or (cur_elem == '?') or (cur_elem.__contains__('<')) or (pre_elem == cur_elem):
                    pre_elem = cur_elem
                    continue
                link = pre_elem + ' ' + cur_elem
                if link == '3491 58593':
                    print('here')
                if link not in g_all_trace_links_set:
                    g_all_trace_links_set.add(link)
                pre_elem = cur_elem
        with open(filename, 'w') as wf:
            wf.write('\n'.join(list(g_all_trace_links_set)))
    else:
        with open(filename, 'r') as rf:
            data = rf.read()
            g_all_trace_links_set = set(data.split('\n'))        

g_all_possi_trace_links_set = set()
def PreGetAllTraceLinks_AllVps(): #ixp hop不算在link里
    global g_all_trace_links_set
    global g_all_possi_trace_links_set
    os.chdir(global_var.par_path + global_var.other_middle_data_dir)
    #filename = global_var.all_trace_par_path + global_var.all_trace_out_data_dir + global_var.all_trace_out_all_trace_links_filename
    w_filename1 = 'all_trace_links_2_from_all_vps'
    w_filename2 = 'all_possi_trace_links_2_from_all_vps'
    if (not os.path.exists(w_filename1)) or (not os.path.exists(w_filename2)):
        for root,dirs,files in os.walk('.'):
            for filename in files: #例：cdg-fr.20180125
                if filename.startswith('all_trace_links_2_from_'):
                    with open(filename, 'r') as rf:
                        g_all_trace_links_set |= set(rf.read().split(','))
                if filename.startswith('all_possi_trace_links_2_from_'):
                    with open(filename, 'r') as rf:
                        g_all_possi_trace_links_set |= set(rf.read().split(','))
        with open(w_filename1, 'w') as wf1:
            wf1.write(','.join(list(g_all_trace_links_set)))
        with open(w_filename2, 'w') as wf2:
            wf2.write(','.join(list(g_all_possi_trace_links_set)))
    else:
        with open(w_filename1, 'r') as rf1:
            g_all_trace_links_set = set(rf1.read().split(','))  
        with open(w_filename2, 'r') as rf2:
            g_all_possi_trace_links_set = set(rf2.read().split(','))  
        print(len(g_all_trace_links_set))
        print(len(g_all_possi_trace_links_set))
    # if '3491 58593' in g_all_trace_links_set:
    #     print(1)
    # if '3491 58593' in g_all_possi_trace_links_set:
    #     print(2)
    # return

def CheckBgpLinkExistsInTrace(bgp_path, peer_link_set):
    global g_all_trace_links_set  
    global g_all_possi_trace_links_set  
    prev_elem = ''
    ab_link_set = set()
    for elem in bgp_path.split(' '):
        # if elem == '4635':
        #     print('h')
        if prev_elem != '' and (not IsIxpAs(elem)) and (not IsIxpAs(prev_elem)) and \
            (not IsAsSet(elem)) and (not IsAsSet(prev_elem)):
            link = prev_elem + ' ' + elem
            if link == '7660 4635':
                print('why here')
            if link in peer_link_set or link in ab_link_set:
                prev_elem = elem
                continue
            if (link not in g_all_trace_links_set) and (link not in g_all_possi_trace_links_set) and \
                (elem not in GetNeighFromRipe(prev_elem)) and (prev_elem not in GetNeighFromRipe(elem)): #abnormal link #2021.8.25 加入ripe的信息
                if IsTwoAsPeerInIXP(prev_elem, elem): #2021.8.25,加入peering db信息
                    peer_link_set.add(link)
                else:
                    ab_link_set.add(link) 
        prev_elem = elem
    return ab_link_set

def FilterValleyByNewestRel(filename): #发现不同时期的AS rel推断矛盾，用最新的rel再过滤一遍，只要有一次rel推断为non-valley即为non-valley
    PreGetSrcFilesInDirs()
    GetAsRankDict(2020, 4)
    GetAsRel(2020, 4)
    os.chdir(global_var.par_path +  global_var.out_my_anatrace_dir + '/')
    wf = open(filename + '_filter_by_newest_rel', 'w')
    wf_link = open(filename + '_filter_by_newest_rel_only_link', 'w')
    with open(filename, 'r') as rf:
        bgp_rec = []
        pre_elem = ''
        for elem in rf.read().split('\n'):
            if not elem.startswith('\t'): #bgp valley
                if pre_elem:
                    valley = pre_elem.split('(')[0]
                    if PathHasValley_2(valley): #re-assure valley
                        wf.write(pre_elem + '\n')
                        wf.write('\n'.join(bgp_rec) + '\n')
                        [as1, as2, as3] = valley.split(' ')
                        wf_link.write(pre_elem + '\n')
                        wf_link.write('%s(%s)%s(%s)%s\n' %(GetAsRankFromDict(as1), Get2AsRel(as1, as2), GetAsRankFromDict(as2), Get2AsRel(as2, as3), GetAsRankFromDict(as3)))
                bgp_rec.clear()
                pre_elem = elem
            else:
                bgp_rec.append(elem)
        if pre_elem and PathHasValley_2(pre_elem): #re-assure valley
            wf.write(pre_elem + '\n')
            wf.write('\n'.join(bgp_rec) + '\n')
            wf_link.write(pre_elem + '\n')
    wf.close()
    wf_link.close()
    ClearAsRel()
    ClearAsRankDict()

def GetSuspicBgp(filename):
    ab_link_dict = dict()
    valley_dict = dict()
    bgp_path_ab_link_set_dict = dict()
    peer_link_set = set()
    ixp_num = 0
    ripe_num = 0
    os.chdir(global_var.par_path +  global_var.out_my_anatrace_dir + '/')
    for year in range(2018,2021):
        for month in range(1,13):
        #for month in range(4,13):
            if (year == 2016 and month < 4) or (year == 2020 and month > 4): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
                continue
            date = str(year) + str(month).zfill(2) + '15'
            #print(date)
            GetSibRel(year, month)
            GetIxpAsSet()
            GetAsRel(year, month)
            ConstrPeerDbInfoDict(year, month)
            for vp in global_var.vps:
                g_asn = global_var.trace_as_dict[vp]    
                bgp_filename = global_var.par_path + global_var.rib_dir + 'bgpdata/bgp_' + g_asn + '_' + date
                GetBgpByPrefix(bgp_filename)
                cur_sub_dir = vp + '_' + date + '/ribs_midar_bdrmapit/' #'nrt-jp.2019030115/'
                if not os.path.exists(cur_sub_dir):
                    continue
                locate = date + '_' + vp
                print(locate)
                with open(cur_sub_dir + filename, 'r') as rf:
                    curline = rf.readline()
                    #dst_key = None
                    while curline:
                        if not curline.startswith('\t'):
                            #dst_key = curline[:curline.index(']') + 1]
                            curline = rf.readline()
                            continue
                        bgp_path = curline.strip('\n').strip('\t')
                        # if bgp_path.__contains__('34288 8757 262287'):
                        #     print('h')
                        ab_link_set = None
                        if bgp_path not in bgp_path_ab_link_set_dict.keys():
                            bgp_path_ab_link_set_dict[bgp_path] = CheckBgpLinkExistsInTrace(bgp_path, peer_link_set)
                        ab_link_set = bgp_path_ab_link_set_dict[bgp_path]
                        for ab_link in ab_link_set:
                            if ab_link not in ab_link_dict.keys():
                                ab_link_dict[ab_link] = [dict(), 0]
                            if bgp_path not in ab_link_dict[ab_link][0].keys():
                                ab_link_dict[ab_link][0][bgp_path] = dict()
                            if locate not in ab_link_dict[ab_link][0][bgp_path].keys():
                                ab_link_dict[ab_link][0][bgp_path][locate] = 0
                            ab_link_dict[ab_link][0][bgp_path][locate] += 1                            
                        valley_set = PathHasValley_2(bgp_path) #2021.9.7 原来有valley_set的缓存，不应该用，因为不同时期AS关系不一样
                        for valley in valley_set:
                            if valley not in valley_dict.keys():
                                valley_dict[valley] = [dict(), 0]
                            if bgp_path not in valley_dict[valley][0].keys():
                                valley_dict[valley][0][bgp_path] = dict()
                            if locate not in valley_dict[valley][0][bgp_path].keys():
                                valley_dict[valley][0][bgp_path][locate] = 0
                            valley_dict[valley][0][bgp_path][locate] += 1     
                        curline = rf.readline()
                ClearBGPByPrefix()
            ClearAsRel()
            ClearIxpAsSet()
            ClearSibRel()
            ClearPeerDbInfoDict()
    print(len(ab_link_dict.keys()))
    for (ab_link, info) in ab_link_dict.items():
        total_count = 0
        for (bgp_path, sub_info) in info[0].items():
            for (locate, count) in sub_info.items():
                total_count += count
        ab_link_dict[ab_link][1] = total_count
    for (valley, info) in valley_dict.items():
        total_count = 0
        for (bgp_path, sub_info) in info[0].items():
            for (locate, count) in sub_info.items():
                total_count += count
        valley_dict[valley][1] = total_count
    ab_link_sort_list = sorted(ab_link_dict.items(), key=lambda d:d[1][1], reverse = True)
    valley_sort_list = sorted(valley_dict.items(), key=lambda d:d[1][1], reverse = True)
    with open('bgp_ab_link_2', 'w') as wf1:
        print('ab link num: %d' %len(ab_link_dict))
        for elem in ab_link_sort_list:
            wf1.write("%s(%d)\n" %(elem[0], elem[1][1]))
            for (bgp_path, sub_info) in elem[1][0].items():
                wf1.write("\t%s\n" %bgp_path)
                for (locate, count) in sub_info.items():
                    wf1.write("\t\t%s(%d)\n" %(locate, count))
    with open('bgp_valley_2', 'w') as wf2:
        for elem in valley_sort_list:
            wf2.write("%s(%d)\n" %(elem[0], elem[1][1]))
            for (bgp_path, sub_info) in elem[1][0].items():
                wf2.write("\t%s\n" %bgp_path)
                for (locate, count) in sub_info.items():
                    wf2.write("\t\t%s(%d)\n" %(locate, count))

    with open('peer_links', 'w') as wf3:
        print('peer link num: %d' %len(peer_link_set))
        wf3.write('\n'.join(list(peer_link_set)))

def IsSibLink(link):
    (asn1, asn2) = link.split(' ')
    org1 = GetOrgByMultiDataFiles_2(asn1)
    org2 = GetOrgByMultiDataFiles_2(asn2)
    return org1 & org2

def FilterSibLinkInAbBgp(pathname):
    PreGetSrcFilesInDirs()
    GetSibRelByMultiDataFiles(2018, 1)
    #GetSibRel(year, month)
    total_num = 0
    ab_num = 0
    wf = open(pathname + '_1_filtersib', 'w') #filter sibling
    with open(pathname, 'r') as rf:
        group_data = None
        curline = rf.readline()
        while curline:
            if not curline.startswith('\t'): #link，记录上一组数据
                total_num += 1
                if group_data:
                    wf.write(group_data)
                    group_data = None
                if not IsSibLink(curline[:curline.index('(')]):
                    ab_num += 1
                    group_data = curline
            else:
                if group_data:
                    group_data += curline
            curline = rf.readline()
        if group_data:
            wf.write(group_data)
    wf.close()
    print("Total num: %d, still ab num: %d" %(total_num, ab_num))

def PerProcFindNeighInTrace(asn, vp, mode, queue):
    if mode == 'moas':
        output = os.popen('grep _' + asn + ' back_as_' + vp + '*')
    else:
        output = os.popen('grep ' + asn + ' back_as_' + vp + '*')
    data = output.readline()
    res_set = set()
    while data:
        data_list = CompressAsPathToMin(CompressAsPath(data.strip('\n'))).split(' ')
        if mode == 'moas':
            for elem in data_list:
                if elem.__contains__('_' + asn):
                    res_set |= set(elem.split('_'))
        else:
            if asn not in data_list:
                data = output.readline()
                continue
            #index = FindBgpAsInTracePath(asn, data_list)
            index = data_list.index(asn)
            if mode == 'prev_neigh' or mode == 'neigh':
                if index > 1:
                    res_set.add(data_list[index - 1])
            if mode == 'next_neigh' or mode == 'neigh':
                if index < len(data_list) - 1:
                    res_set.add(data_list[index + 1])
        data = output.readline()
    if res_set:
        #print(res_set)
        queue.put(' '.join(list(res_set)))

def TmpFindNeighInTrace(asn, mode):
    os.chdir(global_var.all_trace_par_path + global_var.all_trace_download_dir + '2019/01/result/')
    queue = mp.Queue()
    proc_list = []
    for vp in ['ams-nl', 'arn-se', 'bcn-es', 'bjl-gm', 'bwi-us', 'cbg-uk', 'cjj-kr', 'dub-ie', 'eug-us', 'fnl-us', 'hel-fi', 'hkg-cn', 'hlz-nz', 'mty-mx', 'nrt-jp', 'per-au', 'pna-es', 'pry-za', 'sao-br', 'scl-cl', 'sjc2-us', 'syd-au', 'wbu-us', 'yyz-ca', 'zrh2-ch', 'zrh-ch', 'ord-us', 'osl-no', 'rno-us', 'sea-us']:
        proc_list.append(mp.Process(target=PerProcFindNeighInTrace, args=(asn, vp, mode, queue)))
    for elem in proc_list:
        elem.start()
    for elem in proc_list:
        elem.join()
    res_set = set()
    while not queue.empty():
        res_set |= set(queue.get().split(' '))
    # print('trace: ', end='')
    # print(res_set)
    return res_set

def PerProcFindNeighInBgp(queue, asn, filename, mode):
    output = os.popen('grep ' + asn + ' ' + filename)
    data = output.readline()
    res_set = set()
    while data:
        data_list = CompressAsPath(data.split('|')[2]).split(' ')
        if asn not in data_list:
            data = output.readline()
            continue
        #index = FindBgpAsInTracePath(asn, data_list)
        index = data_list.index(asn)
        if mode == 'prev_neigh' or mode == 'neigh':
            if index > 1:
                res_set.add(data_list[index - 1])
        if mode == 'next_neigh' or mode == 'neigh':
            if index < len(data_list) - 1:
                res_set.add(data_list[index + 1])
        data = output.readline()
    if res_set:
        #print(res_set)
        queue.put(' '.join(list(res_set)))

def TmpFindNeighInBgp(asn, mode):
    os.chdir(global_var.par_path + global_var.rib_dir + 'all_collectors_one_date_bgpdata/')
    queue = mp.Queue()
    proc_list = []
    for root,dirs,files in os.walk('.'):
        for filename in files: #例：cdg-fr.20180125
            if (not filename.endswith('gz')) and (not filename.endswith('bz2')):
                proc_list.append(mp.Process(target=PerProcFindNeighInBgp, args=(queue, asn, filename, mode)))
    for elem in proc_list:
        elem.start()
    for elem in proc_list:
        elem.join()
    res_set = set()
    while not queue.empty():
        res_set |= set(queue.get().split(' '))
    #print('bgp: ', end='')
    #print(res_set)
    return res_set

def TmpStat():
    num = 0
    with open('/mountdisk1/ana_c_d_incongruity/out_my_anatrace/bgp_ab_link', 'r') as rf:
        curline = rf.readline()
        while curline:
            if not curline.startswith('\t'):
                tmp = int(curline[curline.index('(') + 1:curline.index(')')])
                if tmp == 1:
                    break
                num += 1
            curline = rf.readline()
    print(num)

def GetTraceSegOfAbBgpLink(link, all_trace_path_list, as_trace_dict):
    res_set = set()
    (asn1, asn2) = link.split(' ')
    if (not asn1 in as_trace_dict.keys()) or (not asn2 in as_trace_dict.keys()):
        print('asn not in trace')
        return res_set
    join_index = as_trace_dict[asn1] & as_trace_dict[asn2]
    if not join_index:
        print('bgp link not found in trace')
        return res_set
    for index in join_index:
        trace = all_trace_path_list[index]
        trace_list = trace.split(' ')
        index1 = FindBgpAsInTracePath(asn1, trace_list)
        index2 = FindBgpAsInTracePath(asn2, trace_list)
        if index1 != -1 and index2 != -1 and index1 < index2:
            trace_seg = ' '.join(trace_list[index1:index2 + 1])
            res_set.add(trace_seg)
    return res_set

def AnaAbBgpLink():
    all_trace_path_set = set()
    os.chdir(global_var.par_path + global_var.other_middle_data_dir)
    all_trace_path_filename = 'all_trace_path_2_from_all_vp'
    if not os.path.exists(all_trace_path_filename):
        for root,dirs,files in os.walk('.'):
            for filename in files: #例：cdg-fr.20180125
                if filename.startswith('all_trace_path_2_from_'):
                    print(filename)
                    with open(filename, 'r') as rf:
                        all_trace_path_set |= set(rf.read().strip('\n').split('\n'))
        with open(all_trace_path_filename, 'w') as wf:
            wf.write('\n'.join(list(all_trace_path_set)))
    else:
        with open(all_trace_path_filename, 'r') as rf:
            all_trace_path_set = set(rf.read().strip('\n').split('\n'))
    print(len(all_trace_path_set))
    all_trace_path_list = list(all_trace_path_set)
    all_trace_path_set.clear()
    as_trace_dict = dict()
    for i in range(0, len(all_trace_path_list)):
        cur_trace = all_trace_path_list[i]
        for elem in cur_trace.split(' '):
            for asn in elem.split('_'):
                if asn not in as_trace_dict.keys():
                    as_trace_dict[asn] = set()
                as_trace_dict[asn].add(i)
    # prev_link_trace_dict = dict()
    # with open(global_var.par_path + global_var.out_my_anatrace_dir + '/bgp_ab_link_2_rel_trace', 'r') as rf:
    #     prev_link = None
    #     trace_set = set()
    #     curline = rf.readline()
    #     while curline:
    #         if not curline.startswith('\t'): #cur_link, deal prev_link
    #             prev_link_trace_dict[prev_link] = trace_set
    #             prev_link = curline
    #             trace_set = set()
    #         else:
    #             trace_set.add(curline.strip('\n').strip('\t'))
    #         curline = rf.readline()
    #     prev_link_trace_dict[prev_link] = trace_set
    # last_dealed_link = ''
    # with open(global_var.par_path + global_var.out_my_anatrace_dir + '/bgp_ab_link_2_rel_trace', 'r') as rf:
    #     elems = rf.read().split('\n')
    #     for i in range(len(elems) - 1, 0, -1):
    #         if not elems[i].startswith('\t'):
    #             last_dealed_link = elems[i]
    #             break
    # print('last_dealed_link: %s' %last_dealed_link)
    wf = open(global_var.par_path + global_var.out_my_anatrace_dir + '/bgp_ab_link_2_rel_trace', 'w')
    begin_flag = False
    with open(global_var.par_path + global_var.out_my_anatrace_dir + '/bgp_ab_link_2_1_filtersib', 'r') as rf:
        curline = rf.readline()
        #work_flag = False
        while curline:
            # if curline.__contains__('4755 37934'):
            #     work_flag = True
            # if not work_flag:
            #     curline = rf.readline()
            #     continue
            if not curline.startswith('\t'):
                #if begin_flag:
                if True:
                    trace_set = None
                    # if curline in prev_link_trace_dict.keys():
                    #     trace_set = prev_link_trace_dict[curline]
                    if False:
                        pass
                    else:
                        print(curline)
                        link = curline[:curline.index('(')]
                        trace_set = GetTraceSegOfAbBgpLink(link, all_trace_path_list, as_trace_dict)
                    wf.write(curline)
                    for elem in trace_set:
                        wf.write('\t' + elem + '\n')
                # if not begin_flag and curline.__contains__(last_dealed_link):
                #     begin_flag = True
            curline = rf.readline()
    wf.close()


def GetNoTraceLink():
    stub_threshhold = 5
    PreLoadAsnInfoFromASNS()
    GetCCNums(2021, 4)
    os.chdir(global_var.par_path + global_var.out_my_anatrace_dir)
    wf_no_trace = open('bgp_ab_link_2_rel_trace_no_trace', 'w')
    all_num = 0
    stub_link_num = 0
    with open('bgp_ab_link_2_rel_trace', 'r') as rf:
        prev_link = ''
        rel_traces = []
        curline = rf.readline()
        while curline:
            if not curline.startswith('\t'): #link
                if prev_link: #analysize prev_link
                    if len(rel_traces) == 0: #no trace
                        (asn1, asn2) = prev_link.split(' ')
                        cc_num_1 = GetCCNumsFromDict(asn1)
                        cc_num_2 = GetCCNumsFromDict(asn2)
                        if cc_num_1 <= stub_threshhold or cc_num_2 <= stub_threshhold:
                            stub_link_num += 1
                        wf_no_trace.write('%s(%s %s)\n' %(prev_link, cc_num_1, cc_num_2))
                        all_num += 1
                rel_traces = []
                prev_link = curline[:curline.index('(')]
            else: #trace
                rel_traces.append(curline)
            curline = rf.readline()
    wf_no_trace.close()
    print('stub_link_num: %d' %stub_link_num)
    print('all_num: %d' %all_num)

def GetInfoOfAbLink():
    PreLoadAsnInfoFromASNS()
    os.chdir(global_var.par_path + global_var.out_my_anatrace_dir)
    all_trace_path_filename = 'bgp_ab_link_2_rel_trace'
    wf = open('bgp_ab_link_2_rel_trace_with_info', 'w')
    wf_only_link = open('bgp_ab_link_2_rel_trace_with_info_only_link', 'w')
    fst_as_dict = dict()
    snd_as_dict = dict()
    num_has_path_trace = 0
    num_has_no_path_trace = 0
    with open(all_trace_path_filename, 'r') as rf:
        prev_link = ''
        rel_traces = []
        curline = rf.readline()
        while curline:
            if not curline.startswith('\t'): #link
                if prev_link: #analysize prev_link
                    if len(rel_traces) > 0:
                        num_has_path_trace += 1
                        (asn1, asn2) = prev_link.split(' ')
                        (org1, country1, rank1) = GetAsnInfoFromASNS(asn1)
                        (org2, country2, rank2) = GetAsnInfoFromASNS(asn2)
                        if asn1 not in fst_as_dict.keys():
                            fst_as_dict[asn1] = [0, org1, country1, rank1]
                        fst_as_dict[asn1][0] += 1
                        if asn2 not in snd_as_dict.keys():
                            snd_as_dict[asn2] = [0, org2, country2, rank2, []]
                        snd_as_dict[asn2][0] += 1
                        snd_as_dict[asn2][4].append(asn1)
                        wf.write("%s:%s,%s,%s\n" %(asn1, org1.replace(',', '_'), country1, rank1))
                        wf.write("%s:%s,%s,%s\n" %(asn2, org2.replace(',', '_'), country2, rank2))
                        wf_only_link.write("%s:%s,%s,%s\n" %(asn1, org1.replace(',', '_'), country1, rank1))
                        wf_only_link.write("%s:%s,%s,%s\n\n" %(asn2, org2.replace(',', '_'), country2, rank2))
                        for trace in rel_traces:
                            wf.write(trace)
                    else:
                        num_has_no_path_trace += 1
                rel_traces = []
                prev_link = curline[:curline.index('(')]
            else: #trace
                rel_traces.append(curline)
            curline = rf.readline()
    
    sort_list_1 = sorted(fst_as_dict.items(), key=lambda d:d[1][0], reverse = True)    
    with open('bgp_ab_link_2_fst_as_info', 'w') as wf_ab_as:
        for elem in sort_list_1:
            wf_ab_as.write("%s(%d):%s,%s,%s\n" %(elem[0], elem[1][0], elem[1][1], elem[1][2], elem[1][3]))
    sort_list_2 = sorted(snd_as_dict.items(), key=lambda d:d[1][0], reverse = True)
    with open('bgp_ab_link_2_snd_as_info', 'w') as wf_ab_as:
        for elem in sort_list_2:
            wf_ab_as.write("%s(%d):%s,%s,%s\n" %(elem[0], elem[1][0], elem[1][1], elem[1][2], elem[1][3]))
            wf_ab_as.write('\t%s\n' %(' '.join(elem[1][4])))
    print('num_has_path_trace: %d' %num_has_path_trace)
    print('num_has_no_path_trace: %d' %num_has_no_path_trace)
    ClearAsnInfoFromASNS()

def GetInfoOfPriorAbLink():
    os.chdir(global_var.par_path + global_var.out_my_anatrace_dir)
    fst_as_dict = dict()
    snd_as_dict = dict()    
    with open('bgp_ab_link_2_rel_trace_with_info_prior_ab', 'r') as rf:
        curline_ab_fst = rf.readline()
        curline_ab_snd = rf.readline()
        while True:
            wf = None
            curline = rf.readline()
            while curline and curline.startswith('\t'):
                curline = rf.readline()
            asn1 = curline_ab_fst.split(':')[0]
            (org1, country1, rank1) = curline_ab_fst.strip('\n').split(':')[1].split(',')
            asn2 = curline_ab_snd.split(':')[0]
            (org2, country2, rank2) = curline_ab_snd.strip('\n').split(':')[1].split(',')
            if asn1 not in fst_as_dict.keys():
                fst_as_dict[asn1] = [0, org1, country1, rank1]
            fst_as_dict[asn1][0] += 1
            if asn2 not in snd_as_dict.keys():
                snd_as_dict[asn2] = [0, org2, country2, rank2, []]
            snd_as_dict[asn2][0] += 1
            snd_as_dict[asn2][4].append(asn1)
            if curline:
                curline_ab_fst = curline
                curline_ab_snd = rf.readline()
            else:
                break    
    sort_list_1 = sorted(fst_as_dict.items(), key=lambda d:d[1][0], reverse = True)    
    with open('bgp_ab_link_2_prior_ab_fst_as_info', 'w') as wf_ab_as:
        for elem in sort_list_1:
            wf_ab_as.write("%s(%d):%s,%s,%s\n" %(elem[0], elem[1][0], elem[1][1], elem[1][2], elem[1][3]))
    sort_list_2 = sorted(snd_as_dict.items(), key=lambda d:d[1][0], reverse = True)
    with open('bgp_ab_link_2_prior_ab_snd_as_info', 'w') as wf_ab_as:
        for elem in sort_list_2:
            wf_ab_as.write("%s(%d):%s,%s,%s\n" %(elem[0], elem[1][0], elem[1][1], elem[1][2], elem[1][3]))
            wf_ab_as.write('\t%s\n' %(' '.join(elem[1][4])))

minimum_trace_num = 3
def ClassifyAbLink():
    os.chdir(global_var.par_path + global_var.out_my_anatrace_dir)
    r_filename = 'bgp_ab_link_2_rel_trace_with_info'
    wf_one_trace = open(r_filename + '_4_one_trace', 'w')
    wf_one_aster = open(r_filename + '_4_one_aster', 'w')
    wf_all_aster_same_country = open(r_filename + '_3_all_aster_same_country', 'w')
    wf_all_aster_diff_country = open(r_filename + '_2_all_aster_diff_country', 'w')
    wf_one_ab_same_country = open(r_filename + '_3_one_ab_same_country', 'w')
    wf_same_country = open(r_filename + '_2_same_country', 'w')
    wf_other_ab = open(r_filename + '_1_other_ab', 'w')
    count_prior_ab = 0
    with open(r_filename, 'r') as rf:
        curline_ab_fst = rf.readline()
        curline_ab_snd = rf.readline()
        while True:
            wf = None
            curline = rf.readline()
            trace_list = []
            while curline and curline.startswith('\t'):
                trace_list.append(curline.strip('\t').strip('\n'))
                curline = rf.readline()
            has_other_asn = False
            has_only_one_ab = False
            if len(trace_list) < minimum_trace_num:
                wf = wf_one_trace
            else:
                for trace in trace_list:
                    elem_list = trace.split(' ')
                    #step 1, if trace has only one * or ? in between
                    if len(elem_list) == 3:
                        if (elem_list[1] == '*' or elem_list[1] == '?'):
                            wf = wf_one_aster
                            break
                        else:
                            has_only_one_ab = True
                    #step 2, if trace has other asn in between
                    if not has_other_asn:
                        for elem in elem_list[1:-1]:
                            if elem != '*' and elem != '?':
                                has_other_asn = True
                                break
            fst_country = curline_ab_fst.split(',')[-2]
            snd_country = curline_ab_snd.split(',')[-2]
            #step 2, if trace has only one asn in between and src and dst in the same country
            if not wf:
                if has_only_one_ab and fst_country == snd_country:
                    wf = wf_one_ab_same_country
            #step 3, if trace has other asn in between
            if not wf:
                if has_other_asn == False:
                    #step 3.1, if in the same country
                    if fst_country == snd_country:
                        wf = wf_all_aster_same_country
                    #step 3.2, if not in the same country
                    else:
                        wf = wf_all_aster_diff_country
                        count_prior_ab += 1
            #step 4, if has other asn in between, but fst_ab and snd_ab are in the same country
            if not wf:
                if fst_country == snd_country:
                    wf = wf_same_country
                    count_prior_ab += 1
            #step 4, highest possibility of ab
            if not wf:
                wf = wf_other_ab
                count_prior_ab += 1

            wf.write('%s%s' %(curline_ab_fst, curline_ab_snd))
            for cur_trace in trace_list:
                wf.write('\t%s\n' %cur_trace)

            if curline:
                curline_ab_fst = curline
                curline_ab_snd = rf.readline()
            else:
                break

    wf_one_trace.close()
    wf_one_aster.close()
    wf_all_aster_same_country.close()
    wf_all_aster_diff_country.close()
    wf_one_ab_same_country.close()
    wf_same_country.close()
    wf_other_ab.close()
    os.system('cat bgp_ab_link_2_rel_trace_with_info_1_other_ab bgp_ab_link_2_rel_trace_with_info_2_same_country bgp_ab_link_2_rel_trace_with_info_2_all_aster_diff_country > bgp_ab_link_2_rel_trace_with_info_prior_ab')
    print('count_prior_ab: %d' %count_prior_ab)

def ClassifyAbLink_2():
    os.chdir(global_var.par_path + global_var.out_my_anatrace_dir)
    r_filename = 'bgp_ab_link_2_rel_trace_with_info'
    wf_one_aster = open(r_filename + '_v2_3_one_aster', 'w')
    wf_all_aster = open(r_filename + '_v2_2_all_aster', 'w')
    wf_same_country = open(r_filename + '_v2_2_same_country', 'w')
    wf_other_ab = open(r_filename + '_v2_1_other_ab', 'w')
    count_one_aster = 0
    count_all_aster = 0
    count_same_country = 0
    count_other_ab = 0
    with open(r_filename, 'r') as rf:
        curline_ab_fst = rf.readline()
        curline_ab_snd = rf.readline()
        while True:
            wf = None
            curline = rf.readline()
            trace_list = []
            while curline and curline.startswith('\t'):
                trace_list.append(curline.strip('\t').strip('\n'))
                curline = rf.readline()
            has_other_asn = False
            for trace in trace_list:
                elem_list = trace.split(' ')
                #step 1, if trace has only one * or ? in between
                if len(elem_list) == 3:
                    if (elem_list[1] == '*' or elem_list[1] == '?'):
                        wf = wf_one_aster
                        count_one_aster += 1
                        break
                #step 2, if trace has other asn in between
                if not has_other_asn:
                    for elem in elem_list[1:-1]:
                        if elem != '*' and elem != '?':
                            has_other_asn = True
                            break
            fst_country = curline_ab_fst.split(',')[-2]
            snd_country = curline_ab_snd.split(',')[-2]
            #step 2, if trace has only one asn in between and src and dst in the same country
            if not wf:
                if has_other_asn == False:
                    count_all_aster += 1
                    wf = wf_all_aster
                if fst_country == snd_country:
                    wf = wf_same_country
                    count_same_country += 1
            #step 4, highest possibility of ab
            if not wf:
                wf = wf_other_ab
                count_other_ab += 1

            wf.write('%s%s' %(curline_ab_fst, curline_ab_snd))
            for cur_trace in trace_list:
                wf.write('\t%s\n' %cur_trace)

            if curline:
                curline_ab_fst = curline
                curline_ab_snd = rf.readline()
            else:
                break

    wf_one_aster.close()
    wf_same_country.close()
    wf_all_aster.close()
    wf_other_ab.close()
    print('count_one_aster: %d' %count_one_aster)
    print('count_all_aster: %d' %count_all_aster)
    print('count_same_country: %d' %count_same_country)
    print('count_other_ab: %d' %count_other_ab)

def CheckAbLinkCheckedByEmails(filename, email_dict, all_content):
    print(filename)
    with open(filename) as rf:
        curline_ab_fst = rf.readline()
        curline_ab_snd = rf.readline()
        while True:
            curline = rf.readline()
            while curline and curline.startswith('\t'):
                curline = rf.readline()
            ab_snd = curline_ab_snd.split(':')[0]
            if all_content.__contains__(ab_snd):
                for (from_, content) in email_dict.items():
                    if content.__contains__(ab_snd):
                        print('\t' + ab_snd + ':' + from_)
                        break
            if curline:
                curline_ab_fst = curline
                curline_ab_snd = rf.readline()
            else:
                break

def CheckEmails():
    email_dict = dict()
    all_content = ''
    with open('email_contents', 'r') as rf:
        data = rf.read()
        for elem in data.strip('<MyEmailDelimiter>').split('<MyEmailDelimiter>'):
            (from_, content) = elem.split('<MyEmailDelimiterSub>')
            email_dict[from_] = content
            all_content += content
    CheckAbLinkCheckedByEmails(global_var.par_path + global_var.out_my_anatrace_dir + '/bgp_ab_link_2_rel_trace_with_info_1_other_ab', email_dict, all_content)
    CheckAbLinkCheckedByEmails(global_var.par_path + global_var.out_my_anatrace_dir + '/bgp_ab_link_2_rel_trace_with_info_2_same_country', email_dict, all_content)
    CheckAbLinkCheckedByEmails(global_var.par_path + global_var.out_my_anatrace_dir + '/bgp_ab_link_2_rel_trace_with_info_2_all_aster_diff_country', email_dict, all_content)

def ChgOneIpTrace2ASPath(ip_trace):
    as_trace = ''
    for elem in ip_trace.split(' '):
        if elem == '*':
            as_trace += ' *'
        else:
            asn = ''
            if elem.__contains__('.'): #ipv4 address
                asn = GetAsStrOfIpByRv(elem)
            elif elem.__contains__(':'): #ipv6 address
                asn = GetAsStrOfIpByRvV6(elem)
            else:
                print('False ip form: %s' %asn)
            if not asn:
                as_trace += ' ?'
            else:
                as_trace += ' ' + asn

def LinkInTrace(link, trace):
    trace_set = TransOneTraceToUniASTraces(trace)
    for elem in trace_set:
        if elem.__contains__(link):
            return True
    return False

def CheckAbLinkByOnlineTraceroute():
    GetAsRel_2(global_var.par_path + global_var.rel_cc_dir + '20210801.as-rel2.txt')
    os.chdir('/home/slt/code/ana_c_d_incongruity/')
    GetFuncLgDict()
    GetPfx2ASByRv()
    GetPfx2ASByRvV6()
    os.chdir(global_var.par_path + global_var.out_my_anatrace_dir + '/')
    wf = open('bgp_ab_link_2_rel_trace_with_info_prior_ab_after_online', 'w')
    with open('bgp_ab_link_2_rel_trace_with_info_prior_ab', 'r') as rf:
        curline_ab_fst = rf.readline()
        curline_ab_snd = rf.readline()
        while True:
            curline = rf.readline()
            existed_trace_list = []
            while curline and curline.startswith('\t'):
                existed_trace_list.append(curline.strip('\n').strip('\t'))
                curline = rf.readline()
            as1 = curline_ab_fst.split(':')[0]
            as2 = curline_ab_snd.split(':')[0]
            dst_ips = GetRepIpsOfAs(as2)
            as1_neighs = GetNeighOfAs(as1)
            as1_neighs.add(as1)
            trace_list = []
            trace_as_list = []
            req = requests.session()
            for asn in as1_neighs:
                funcs = GetFuncOfLg(asn)
                for func in funcs:
                    for dst_ip in dst_ips:                        
                        tmp_trace_list = func(dst_ip, req) #调用函数
                        for tmp_trace in tmp_trace_list:
                            trace_list.append(tmp_trace)
            link = as1 + ' ' + as2
            link_norm = False
            for trace in trace_list:
                as_trace = ChgOneIpTrace2ASPath(trace)
                if LinkInTrace(link, as_trace):
                    link_norm = True
                    break
                if AsnInTracePathList(as1, as_trace.split(' ')) and AsnInTracePathList(as2, as_trace.split(' ')): #path as1->as2 exists
                    existed_trace_list.append(as_trace)
            if link_norm: #find by online traceroute
                pass
            else:
                wf.write(curline_ab_fst)
                wf.write(curline_ab_snd)
                for trace in existed_trace_list:
                    wf.write('\t' + trace + '\n')
            if curline:
                curline_ab_fst = curline
                curline_ab_snd = rf.readline()
            else:
                break
    ClearAsRel_2()
    ClearFuncLgDict()
    ClearIp2AsDict()
    ClearIp2AsDictV6()
    wf.close()

link_trace_index_dict = dict()
bgp_path_in_trace_set_dict = dict()
def CheckIfBgpPathInTraces(bgp_path, extra_dict):
    global bgp_path_in_trace_set_dict
    global link_trace_index_dict
    if bgp_path in bgp_path_in_trace_set_dict.keys():
        return bgp_path_in_trace_set_dict[bgp_path]
    elems = bgp_path.split(' ')
    if len(elems) < 2:
        return 0
    link = elems[0] + ' ' + elems[1]
    if link in link_trace_index_dict.keys():
        for trace in link_trace_index_dict[link]:
            if trace.__contains__(bgp_path):
                bgp_path_in_trace_set_dict[bgp_path] = 1
                extra_dict[bgp_path] = 1
                return 1
    bgp_path_in_trace_set_dict[bgp_path] = 0
    extra_dict[bgp_path] = 0
    return 0
    # for elem in bgp_path_in_trace_set_dict.keys():
    #     if elem.__contains__(bgp_path):
    #         bgp_path_in_trace_set_dict[bgp_path] = bgp_path_in_trace_set_dict[elem]
    #         extra_dict[bgp_path] = bgp_path_in_trace_set_dict[elem]
    #         return bgp_path_in_trace_set_dict[bgp_path]
    # if bgp_path in all_trace_path_set:
    #     bgp_path_in_trace_set_dict[bgp_path] = 1
    #     extra_dict[bgp_path] = 1
    #     return 1
    # for trace in all_trace_path_set:
    #     if trace.__contains__(bgp_path):
    #         bgp_path_in_trace_set_dict[bgp_path] = 1
    #         extra_dict[bgp_path] = 1
    #         return 1
    # bgp_path_in_trace_set_dict[bgp_path] = 0
    # extra_dict[bgp_path] = 0
    # return 0

def GetLinkTraceIndex():
    link_trace_index_filename = global_var.par_path + global_var.other_middle_data_dir + 'link_trace_index_2_from_all_vp'
    if not os.path.exists(link_trace_index_filename):
        all_trace_path_set = set()
        with open(global_var.par_path + global_var.other_middle_data_dir + 'all_trace_path_2_from_all_vp', 'r') as rf:
            all_trace_path_set = set(rf.read().strip('\n').split('\n'))
        for trace_path in all_trace_path_set:
            prev_elem = '*'
            for elem in trace_path.split(' '):
                if prev_elem != '*' and prev_elem != '?' and (not prev_elem.__contains__('<')) and \
                    elem != '*' and elem != '?' and (not elem.__contains__('<')):
                    link = prev_elem + ' ' + elem
                    if link not in link_trace_index_dict.keys():
                        link_trace_index_dict[link] = set()
                    link_trace_index_dict[link].add(trace_path)
                prev_elem = elem
        with open(link_trace_index_filename, 'w') as wf:
            for (link, trace_set) in link_trace_index_dict.items():
                wf.write(link + ':' + ','.join(list(trace_set)) + '\n')
    else:
        with open(link_trace_index_filename, 'r') as rf:
            curline = rf.readline()
            while curline:
                (link, trace_str) = curline.strip('\n').split(':')
                link_trace_index_dict[link] = set(trace_str.split(','))
                curline = rf.readline()

def GetAbBgpPath(filename):
    PreGetSrcFilesInDirs()
    global bgp_path_in_trace_set_dict
    global link_trace_index_dict
    GetLinkTraceIndex()
    # all_trace_path_set = set()
    # with open(global_var.par_path + global_var.other_middle_data_dir + 'all_trace_path_2_from_all_vp', 'r') as rf:
    #     all_trace_path_set = set(rf.read().strip('\n').split('\n'))
    os.chdir(global_var.par_path +  global_var.out_my_anatrace_dir + '/')
    bgp_path_in_trace_set_dict_filename = 'bgp_path_in_trace_set_dict'
    bgp_path_in_trace_set_dict.clear() 
    if os.path.exists(bgp_path_in_trace_set_dict_filename):
        with open(bgp_path_in_trace_set_dict_filename, 'r') as rf:
            for elem in rf.read().split('\n'):
                if not elem:
                    continue
                (bgp_path, val) = elem.split(':')
                bgp_path_in_trace_set_dict[bgp_path] = int(val)
    for year in range(2018,2021):
        for month in range(1,13):
            if (year == 2016 and month < 4) or (year == 2020 and month > 4): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
                continue
            date = str(year) + str(month).zfill(2) + '15'
            #print(date)
            # GetSibRel(year, month)
            # GetIxpAsSet()
            # GetAsRel(year, month)
            for vp in global_var.vps:
                g_asn = global_var.trace_as_dict[vp]    
                #bgp_filename = global_var.par_path + global_var.rib_dir + 'bgpdata/bgp_' + g_asn + '_' + date
                #GetBgpByPrefix(bgp_filename)
                cur_sub_dir = vp + '_' + date + '/ribs_midar_bdrmapit/' #'nrt-jp.2019030115/'
                if not os.path.exists(cur_sub_dir):
                    continue
                locate = date + '_' + vp
                print(locate)
                with open(cur_sub_dir + filename, 'r') as rf:
                    extra_dict = dict()
                    curline = rf.readline()
                    #dst_key = None
                    while curline:
                        if not curline.startswith('\t'):
                            #dst_key = curline[:curline.index(']') + 1]
                            curline = rf.readline()
                            continue
                        bgp_path = curline.strip('\n').strip('\t')
                        CheckIfBgpPathInTraces(bgp_path, extra_dict)
                        curline = rf.readline()
                    print('begin add data')
                    with open(bgp_path_in_trace_set_dict_filename, 'a') as wf:
                        for (bgp_path, val) in extra_dict.items():
                            wf.write(bgp_path + ':' + str(val) + '\n')
                    print('end add data')
                #ClearBGPByPrefix()
            # ClearAsRel()
            # ClearIxpAsSet()
            # ClearSibRel()
            # ClearPeerDbInfoDict()
    ab_count = 0
    total_count = 0
    with open('ab_bgp_path', 'w') as wf:
        for (bgp_path, val) in bgp_path_in_trace_set_dict.items():
            total_count += 1
            if val == 0:
                wf.write(bgp_path + '\n')
                ab_count += 1
    print('total count: %d' %total_count)
    print('ab count: %d' %ab_count)

def CountAbPathNum():
    bgp_path_set = set()
    with open(global_var.par_path + global_var.out_my_anatrace_dir + '/bgp_ab_link_2', 'r') as rf:
        for elem in rf.read().split('\n'):
            if elem.startswith('\t') and not elem.startswith('\t\t'):
                bgp_path_set.add(elem.strip('\t').strip('\n'))
    print(len(bgp_path_set))
    bgp_path_set.clear()
    with open(global_var.par_path + global_var.out_my_anatrace_dir + '/bgp_valley_2', 'r') as rf:
        for elem in rf.read().split('\n'):
            if elem.startswith('\t') and not elem.startswith('\t\t'):
                bgp_path_set.add(elem.strip('\t').strip('\n'))
    print(len(bgp_path_set))
    
def CountNonTransientBgpPath(filename):
    bgp_path_set = set()
    os.chdir(global_var.par_path +  global_var.out_my_anatrace_dir + '/')
    for year in range(2018,2021):
        for month in range(1,13):
            if (year == 2016 and month < 4) or (year == 2020 and month > 4): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
                continue
            date = str(year) + str(month).zfill(2) + '15'
            for vp in global_var.vps:
                cur_sub_dir = vp + '_' + date + '/ribs_midar_bdrmapit/' #'nrt-jp.2019030115/'
                if not os.path.exists(cur_sub_dir):
                    continue
                with open(cur_sub_dir + filename, 'r') as rf:
                    curline = rf.readline()
                    while curline:
                        if not curline.startswith('\t'):
                            #dst_key = curline[:curline.index(']') + 1]
                            curline = rf.readline()
                            continue
                        bgp_path = curline.strip('\n').strip('\t')
                        bgp_path_set.add(bgp_path)
    print(len(bgp_path_set))

def GetCoreASInAbLink(filename):
    wf_core_as = open(filename + '_core_as', 'w')
    wf_other_as = open(filename + '_other_as', 'w')
    num_core_as = 0
    num_other_as = 0
    with open(filename, 'r') as rf:
        curline_ab_fst = rf.readline()
        curline_ab_snd = rf.readline()
        while True:
            curline = rf.readline()
            existed_trace_list = []
            while curline and curline.startswith('\t'):
                existed_trace_list.append(curline.strip('\n').strip('\t'))
                curline = rf.readline()
            as_rank_1_str = curline_ab_fst.split(',')[-1].strip('\n')
            as_rank_1 = 0xFFFF
            if as_rank_1_str.isdigit():
                as_rank_1 = int(as_rank_1_str)
            wf = wf_other_as
            if as_rank_1 <= 15:
                wf = wf_core_as
                num_core_as += 1
            else:
                num_other_as += 1
            wf.write(curline_ab_fst)
            wf.write(curline_ab_snd)
            for trace in existed_trace_list:
                wf.write('\t' + trace + '\n')
            if curline:
                curline_ab_fst = curline
                curline_ab_snd = rf.readline()
            else:
                break
    print('num_core_as:%d' %num_core_as)
    print('num_other_as:%d' %num_other_as)
    wf_core_as.close()
    wf_other_as.close()


def StatLinkNumInSuspicBgp(filename):
    link_set = set()
    os.chdir(global_var.par_path +  global_var.out_my_anatrace_dir + '/')
    for year in range(2018,2021):
        for month in range(1,13):
        #for month in range(4,13):
            if (year == 2016 and month < 4) or (year == 2020 and month > 4): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
                continue
            date = str(year) + str(month).zfill(2) + '15'
            for vp in global_var.vps:
                cur_sub_dir = vp + '_' + date + '/ribs_midar_bdrmapit/' #'nrt-jp.2019030115/'
                if not os.path.exists(cur_sub_dir):
                    continue
                with open(cur_sub_dir + filename, 'r') as rf:
                    curline = rf.readline()
                    #dst_key = None
                    while curline:
                        if not curline.startswith('\t'):
                            #dst_key = curline[:curline.index(']') + 1]
                            curline = rf.readline()
                            continue
                        bgp_path = curline.strip('\n').strip('\t')
                        prev_elem = ''
                        for elem in bgp_path.split(' '):
                            if prev_elem != '':
                                link = prev_elem + ' ' + elem
                                link_set.add(link)
                            prev_elem = elem
                        curline = rf.readline()
    print(len(link_set))

def PathHasLoop(path):
    elems = path.split(' ')
    start = 0
    while elems[start] == '*' or elems[start] == '?' or elems[start] == '<>':
        start += 1
    for i in range(start + 1, len(elems)):
        if elems[i] == '*' or elems[i] == '?' or elems[start] == '<>': #记成前面相同的元素，不处理
            elems[i] = elems[i - 1]
        if elems[i] != elems[i - 1] and elems[i] in elems[:i - 1]:
            return True
    return False

def TryDealTPAdrress():
    filename = '/mountdisk1/ana_c_d_incongruity/out_my_anatrace/bgp_ab_link_2_rel_trace_with_info_v2_2_same_country'
    wf = open(filename + '_modi_tpadrr', 'w')
    search_path = '/mountdisk1/ana_c_d_incongruity/other_middle_data/compress_trace_to_ori_trace_*'
    with open(filename, 'r') as rf:
        curline = rf.readline()
        while curline:
            if not curline.startswith('\t'):
                wf.write(curline)
                curline = rf.readline()
                continue
            comp_bgp_path = curline.strip('\n').strip('\t')
            elems = comp_bgp_path.split(' ')
            if len(elems) != 3 or elems.__contains__('*') or elems.__contains__('?'):
                wf.write(curline)
                curline = rf.readline()
                continue
            mid_elem = elems[1]
            output = os.popen('grep \'' + comp_bgp_path + ':\' ' + search_path)
            if output:
                data = output.readline()
                tp_address_flag = True
                while data:
                    path_list = data[data.rindex(':') + 1:].split(',')
                    for path in path_list:
                        if PathHasLoop(path):
                            continue
                        count = path.split(' ').count(mid_elem)
                        if count != 1: #只要有一个不是，就应该不是
                            tp_address_flag = False
                            break                
                    data = output.readline()
                if tp_address_flag: #改写记录
                    elems[1] = '?'
                    wf.write('\t' + ' '.join(elems) + '\n')
                    print(curline)
                else:
                    wf.write(curline)
            curline = rf.readline()
    wf.close()
    StripMidAllAsterPath('/mountdisk1/ana_c_d_incongruity/out_my_anatrace/bgp_ab_link_2_rel_trace_with_info_v2_2_same_country_modi_tpadrr')    
    os.chdir(global_var.par_path + global_var.out_my_anatrace_dir + '/')
    os.system('cat bgp_ab_link_2_rel_trace_with_info_v2_1_other_ab bgp_ab_link_2_rel_trace_with_info_v2_2_same_country_modi_tpadrr_strip_aster > bgp_ab_link_2_rel_trace_with_info_v2_prior_ab')
    os.system('cat bgp_ab_link_2_rel_trace_with_info_v2_2_all_aster bgp_ab_link_2_rel_trace_with_info_v2_2_same_country_modi_tpadrr_stripped_asters > bgp_ab_link_2_rel_trace_with_info_v2_2_all_aster_2')

def StripMidAllAsterPath(filename):
    wf = open(filename + '_strip_aster', 'w')
    wf1 = open(filename + '_stripped_asters', 'w')
    strip_num = 0
    with open(filename, 'r') as rf:
        curline_fst_as = rf.readline()        
        while curline_fst_as:
            curline_snd_as = rf.readline()
            trace_list = []
            curline = rf.readline()
            while curline and curline.startswith('\t'):
                trace_list.append(curline.strip('\n').strip('\t'))
                curline = rf.readline()
            mid_all_aster = True
            for trace in trace_list:
                elems = trace.split(' ')
                elem_set = set(elems[1:-1])
                if elem_set and '*' in elem_set:
                    elem_set.remove('*')
                if elem_set and '?' in elem_set:
                    elem_set.remove('?')
                if elem_set and '<>' in elem_set:
                    elem_set.remove('<>')
                if elem_set:
                    mid_all_aster = False
                    break
            if mid_all_aster:
                #print(trace_list[0])
                strip_num += 1
                wf1.write(curline_fst_as)
                wf1.write(curline_snd_as)
                for trace in trace_list:
                    wf1.write('\t' + trace + '\n')
            else:
                wf.write(curline_fst_as)
                wf.write(curline_snd_as)
                for trace in trace_list:
                    wf.write('\t' + trace + '\n')
            curline_fst_as = curline
    print('strip_num: %d' %strip_num)
    wf.close()

def CheckSibInAbTraces():
    os.chdir(global_var.par_path + global_var.out_my_anatrace_dir + '/')
    PreGetSrcFilesInDirs()
    GetSibRelByMultiDataFiles(2018, 1)
    wf = open('bgp_ab_link_2_rel_trace_with_info_v2_prior_ab_filter_sib', 'w')
    num_all_aster = 0
    num_still_ab = 0
    with open('bgp_ab_link_2_rel_trace_with_info_v2_prior_ab', 'r') as rf:
        curline_fst_as = rf.readline()        
        while curline_fst_as:
            curline_snd_as = rf.readline()
            trace_list = []
            curline = rf.readline()
            while curline and curline.startswith('\t'):
                trace_list.append(curline.strip('\n').strip('\t'))
                curline = rf.readline()
            fst_as = curline_fst_as[:curline_fst_as.index(':')]
            snd_as = curline_snd_as[:curline_snd_as.index(':')]
            fst_org = GetOrgByMultiDataFiles_2(fst_as)
            snd_org = GetOrgByMultiDataFiles_2(snd_as)
            new_trace_list = []
            link_found = False
            has_other_as = False
            for trace in trace_list:
                elems = trace.split(' ')
                new_elems = []
                for i in range(1, len(elems) - 1):
                    sib_as = False
                    if elems[i] != '*' and elems[i] != '?' and elems[i] != '<>':
                        for asn in elems[i].split('_'):
                            tmp_org = GetOrgByMultiDataFiles_2(asn)
                            if (tmp_org & fst_org) or (tmp_org & snd_org):
                                sib_as = True
                                break
                    if not sib_as:
                        new_elems.append(elems[i])
                        if elems[i] != '*' and elems[i] != '?' and elems[i] != '<>':
                            has_other_as = True
                if not new_elems:
                    link_found = True
                    break
                else:
                    new_trace_list.append(fst_as + ' ' + ' '.join(new_elems) + ' ' + snd_as)
            if not link_found:
                if has_other_as:
                    num_still_ab += 1
                    wf.write(curline_fst_as)
                    wf.write(curline_snd_as)
                    for trace in new_trace_list:
                        wf.write('\t' + trace + '\n')
                else:
                    num_all_aster += 1
            curline_fst_as = curline
    wf.close()
    print('num_all_aster: %d' %num_all_aster)
    print('num_still_ab: %d' %num_still_ab)

def StatLinkCountInBgp():
    for vp in global_var.vps:
        link_set = set()
        with open(global_var.par_path + global_var.rib_dir + 'bgpdata/bgp_' + global_var.trace_as_dict[vp] + '_20200415', 'r') as rf:
            curline = rf.readline()
            while curline:
                elems = curline.split('|')
                if len(elems) < 3:
                    curline = rf.readline()
                    continue
                link_set |= GetLinkSetFromBgpPath(elems[2])
                curline = rf.readline()
        print(vp + ':%d' %len(link_set))

def Tmp():
    bgp_seg = '6939 701 5511 39386 41426 48237 35819 15802'
    link_set1 = GetLinkSetFromBgpPath(bgp_seg)
    tmp_dict = dict()
    for link in link_set1:
        if link not in g_bgp_link_path_dict.keys():
            tmp_dict[link] = set()
        tmp_dict[link].add(bgp_seg)
    link_set2 = GetLinkSetFromBgpPath(bgp_seg)
    tmp_path_set = set()
    start = True
    for link in link_set2:
        if start:
            tmp_path_set = tmp_dict[link]
            start = False
        else:
            debug_path_set = tmp_dict[link]
            tmp_path_set &= debug_path_set
            if not tmp_path_set:
                print('This ought not happen. path: %s' %bgp_seg)

if __name__ == '__main__':
    tmp = True
    if tmp:
        #Tmp()
        #StatLinkCountInBgp()

        #StatLinkNumInSuspicBgp('ana_ab_5_all_bgp')
        #GetCCNums(2021, 4)
        GetNoTraceLink()
        #ClassifyAbLink_2()
        #GetLinkTraceIndex()

        #GetCoreASInAbLink(global_var.par_path + global_var.out_my_anatrace_dir + '/bgp_ab_link_2_rel_trace_with_info_v2_prior_ab_filter_sib')
        #CountNonTransientBgpPath('ana_ab_5_all_bgp')
        
        # PreGetAllTraceLinks_AllVps()
        # PreGetSrcFilesInDirs()
        # CheckBgpLinkExistsInTrace('34288 8757 262287 30081', set())

        # PreGetSrcFilesInDirs()
        # GetSibRelByMultiDataFiles(2018, 1)
        # print(IsSibLink('31133 3450'))

        #CountAbPathNum()
        # PreGetSrcFilesInDirs()
        # GetAsRel(2020, 1)
        # PathHasValley_2('7575 11537 20965 21274 21320')
        #CheckEmails()
        print('done')
        while True:
            pass
        #TmpStat()
        #PreGetAllBgpPaths()
        for year in range(2018,2021):
            for month in range(1,13):
                if (year == 2016 and month < 4) or (year == 2020 and month > 4): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
                    continue
                ConstrPeerDbInfoDict(year, month)
                ClearPeerDbInfoDict()
        print('done')
        while True:
            pass
        trace_set = TmpFindNeighInTrace(sys.argv[1], 'neigh') #'44244'
        bgp_set = TmpFindNeighInBgp(sys.argv[1], 'neigh')
        tmp_set = trace_set & bgp_set
        print('common (%d): ' %len(tmp_set), end='')
        print(tmp_set)
        tmp_set = trace_set.difference(bgp_set)
        print('trace unique (%d): ' %len(tmp_set), end='')
        print(tmp_set)
        tmp_set = bgp_set.difference(trace_set)
        print('bgp unique (%d): ' %len(tmp_set), end='')
        print(tmp_set)
        #FilterSibLinkInAbBgp(global_var.par_path +  global_var.out_my_anatrace_dir + '/bgp_ab_link')
    else:
        pre_constr_trace_set = False
        if pre_constr_trace_set: #准备工作是建立所有trace集合，最后生成'all_traces_uni_as'文件
            #PreGetAllTraces()

            PreGetAllTraces_Mini()
            PreTransAllTraceToUniASTraces()
            #PreGetAllTracesWithUniAs() #以后每次正常工作需要调用这个句子，获取all_trace_set_uni_as
            pass
        else: #正常工作
            PreGetAllTraceLinks() #得到g_all_trace_links_set
            PreGetAllTraceLinks_AllVps() #得到g_all_trace_links_set, 前提条件
            #step 0
            # print('step 0')
            # PreGetAllBgpPaths() #这句和下面一句GetAllBgpOfAbTrace是成对出现的            
            # GetAllBgpOfAbTrace('ana_ab_5')
            #step 1
            # print('step 1')
            # PreGetSrcFilesInDirs()
            # GetSuspicBgp('ana_ab_5_all_bgp')
            # #step 2
            # print('step 2')
            # FilterSibLinkInAbBgp(global_var.par_path +  global_var.out_my_anatrace_dir + '/bgp_ab_link_2')
            # AnaAbBgpLink()
            # #step 3
            # print('step 3')
            # GetInfoOfAbLink()
            #step 4, 筛选出prior ab link
            print('step 4')
            # ClassifyAbLink_2()
            TryDealTPAdrress()
            # GetInfoOfPriorAbLink()
            # CheckSibInAbTraces()
            #step 5, 通过在线traceroute筛选ab_link
            #CheckAbLinkByOnlineTraceroute()      
            #step 6
            #FilterValleyByNewestRel('bgp_valley_2')
            #step 7 #补
            #GetAbBgpPath('ana_ab_5_all_bgp')
