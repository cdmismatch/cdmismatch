
#from code.ana_c_d_incongruity.ana_bgp_deleted import BgpSegInTraceList
import re
import socket
import struct
import os
import operator
import copy
import shutil

import global_var
from ana_prefix_traceroute_group_by_prefix_v2 import FilterSimilarPathAndRecord, \
                                                    TracePathListEqualsBgpPathList, GetDiffList, TracePathIsNormal, \
                                                    TagStepInRecordFile, CheckTracesByIRRData, FindSimilarestBgpPath
from utils_v2 import GetAsRel, GetSibRel, IsSib_2, IsPeer_2, IsPc_2, GetBgp_1, DebugGetBgpRoute, \
                    GetAsStrOfIpByRv, GetAsOfIpByMi, PreOpenRouterIpFiles, CloseRouterIpFiles, \
                    PreOpenAsRouterFiles, CloseAsRouterFiles, GetPfx2ASByRv, DebugGetBgpLink, \
                    GetRouterOfIpByMi, GetGeoOfRouterByMi, GetAsOfRouterByMi, Get2AsRel, GetDistOfIps, \
                    AsnInBgpPathList, FstPathContainedInSnd, GetAsConnDegree, GetAsRel, ClearBGP_1, \
                    GetAsRank, ClearSibRel, AsIsEqual, ClearAsRel, GetVpNeighborFromBgp, ClearVpNeighbor, \
                    AsnIsVpNeighbor, AsnInTracePathList, GetAsRankDict, ClearAsRankDict, GetAsRankFromDict, \
                    GetAsRankStrFromDict, FindBgpAsInTracePath, FstPathContainedInSnd, FindTraceAsInBgpPath, \
                    CountAsnInTracePathList, Get2AsRel_2, GetBgpByPrefix, GetBgpPathFromBgpPrefixDict, ClearBGPByPrefix, \
                    GetDstIpIntSet, ClearDstIpIntSet, GetPathAsDict, GetBgpPathByAs, ClearPathAsDict, ConnectToDb, \
                    FindPathInDb, CloseDb, GetAsCountryDict, ClearAsCountryDict, GetAsCountry, GetAsRelAndTranslate, \
                    GetIxpPfxDict, GetIxpAsSet, SetCurMidarTableDate, GetPfx2ASByBgp, ClearIp2AsDict, FindTraceAsSetInBgpPath, \
                    ClearIxpAsSet, ClearIxpPfxDict, GetBgpPathFromBgpPrefixDict_2, GetCommonAsInMoasList, TranslateAsRel, \
                    GetAsRankFromDict_2, GetLongestMatchPrefixByRv, DropStarsInTraceList, CompressAsPathToMin, CompressAsPath

from gen_ip2as_command import PreGetSrcFilesInDirs

def ChgAbPathFileToCmpFormat(filename):
    rf = open(filename, 'r')
    wf = open(filename + '_observe', 'w')
    curline_trace_as = rf.readline()
    while curline_trace_as:
        discard = rf.readline()
        curline_ip = rf.readline()
        elems = curline_trace_as.split(']')
        trace_as = ""
        dst_as = elems[0].split(' ')[1]
        if len(elems) > 1:
            trace_as = elems[1].strip('\n').strip(' ')
        trace_as = CompressAsPathToMin(CompressAsPath(trace_as))
        bgp_path_list = DebugGetBgpRoute(dst_as)
        wf.write("%s (%s)\n" %(curline_trace_as.strip('\n'), trace_as))
        for bgp_path in bgp_path_list:
            wf.write("\t\t\t\t\t\t%s\n" %bgp_path)
        wf.write(curline_ip)
        curline_trace_as = rf.readline()
    rf.close()
    wf.close()
 
def ChgAbPathFileToCmpFormat_2(filename):
    rf = open(filename, 'r')
    wf = open(filename + '_observe_ab', 'w')
    curline_difstart = rf.readline()
    while curline_difstart:
        curline_trace_as = rf.readline()
        discard = rf.readline()
        curline_ip = rf.readline()
        elems = curline_trace_as.split(']')
        #trace_as = ""
        dst_as = elems[0].split(' ')[1]
        #if len(elems) > 1:
            #trace_as = elems[1].strip('\n').strip(' ')
        #trace_as = CompressAsPathToMin(CompressAsPath(trace_as))
        bgp_path_list = DebugGetBgpRoute(dst_as)
        #wf.write("%s%s\n" %(elems[0] + ']', trace_as))
        wf.write("%s" %curline_difstart)
        wf.write("%s" %curline_trace_as)
        for bgp_path in bgp_path_list:
            wf.write("\t\t\t\t\t\t%s\n" %bgp_path)
        wf.write("%s" %curline_ip)
        curline_difstart = rf.readline()
    rf.close()
    wf.close()

#step 1, 先把trace里多出最后一跳的检出来
rel_kind = ['customer', 'provider', 'peer', 'sibling', 'unknown']
#rel_kind = ['customer', 'provider', 'peer', 'sib']
def GetOneExtraLastHopRel(trace_path, bgp_path):
    #front_trace_path = trace_path[0:trace_path.rindex(' ')]
    #if bgp_path.__contains__(front_trace_path):
    dst_as = ''
    if not bgp_path.__contains__(' '):
        print('NOTE in GetOneExtraLastHopRel() bgp_path: %s' %bgp_path)
        dst_as = bgp_path
    else:
        dst_as = bgp_path[bgp_path.rindex(' ') + 1:]
    extra_as = trace_path[trace_path.rindex(' ') + 1:]
    if IsSib_2(dst_as, extra_as):
        return rel_kind[3] #'sib'
    res = IsPc_2(dst_as, extra_as)
    if res == -1: #is customer
        return rel_kind[0] #'customer'
    elif res == 1:
        return rel_kind[1] #'provider'
    if IsPeer_2(dst_as, extra_as):
        return rel_kind[2] #'peer'
    return rel_kind[4] #'not_known'
    #return '' #'not_known'

'''
def TraceHasExtraLastHops(dst_as_key, trace_path, bgp_path):
    trace_path_list = trace_path.split(' ')
    bgp_path_list = bgp_path.split(' ')
    dst_as = bgp_path_list[-1]
    index = -1
    find = False
    for index in range(0, len(trace_path_list)):
        if dst_as in trace_path_list[index].split('_'): #find dst_as:
            find = True
            break
    if not find: #dst_as not found
        return (False, 0)

    for i in range(0, index):
        cur_hop = trace_path_list[i]
        find = False
        for dst_as in cur_hop.split('_'):
            if dst_as in bgp_path_list:
                find = True
                break
        if not find:
            return (False, 0)
    return (True, len(trace_path_list) - index - 1)
'''
        
def ExtractExtraLastHop(filename, wf, not_decided_filename, record_file_name):
    rf = open(filename, 'r')
    count_total = 0
    count = dict()
    for elem in wf.keys():
        count[elem] = 0
    wf_not_decided = open(not_decided_filename, 'w')
    curline_trace_as = rf.readline()

    while curline_trace_as:
        #if curline_trace_as.__contains__('[192.54.53.0/24 393567] 7660 * 2516 2516 2516 2516 3549_4323'):
            #print('')
        curline_bgp_as = rf.readline()
        curline_ip = rf.readline()
        elems = curline_trace_as.split(']')
        if len(elems) < 2:
            curline_trace_as = rf.readline()
            continue
        count_total += 1
        trace_path = elems[1].strip('\n').strip(' ')
        trace_path = CompressAsPathToMin(CompressAsPath(trace_path))
        trace_path_list = trace_path.split(' ')
        dst_as_key = elems[0].split(' ')[1]
        bgp_path_list = CompressAsPath(curline_bgp_as.strip('\n').strip('\t')).split(' ')
        dst_as = bgp_path_list[-1]
        last_norm_index = -1
        ab_hop_set = set()
        for last_norm_index in range(len(trace_path_list) - 1, -1, -1):
            cur_hop = trace_path_list[last_norm_index]
            if cur_hop.__contains__('*') or cur_hop.__contains__('?') or cur_hop.__contains__('<'):
                continue
            if dst_as not in cur_hop.split('_'):
                ab_hop_set.add(cur_hop)
            else:
                break #last normal found
        if not ab_hop_set:
            print('NOTE! ab_hop_set empty!')
            #print("%s%s%s" %(curline_trace_as, curline_bgp_as, curline_ip))
            wf_not_decided.write("%s%s%s" %(curline_trace_as, curline_bgp_as, curline_ip))
            curline_trace_as = rf.readline()
            continue
        common_ab_as_set = GetCommonAsInMoasList(list(ab_hop_set))
        if common_ab_as_set: #有共同的mapping AS, 认为异常跳属于同一个AS
            max_rel = -100
            for elem in common_ab_as_set:
                cur_rel = Get2AsRel(elem, dst_as)
                if cur_rel > max_rel:   #有更近的关系
                    max_rel = cur_rel
            rel_str = TranslateAsRel(max_rel)
            wf[rel_str].write("%s%s%s" %(curline_trace_as, curline_bgp_as, curline_ip))
            count[rel_str] += 1
        else: #没有共同的mapping AS, 认为异常跳属于不同的AS
            wf['multi'].write("%s%s%s" %(curline_trace_as, curline_bgp_as, curline_ip))
            count['multi'] += 1
        curline_trace_as = rf.readline()
    rf.close()

    wf_record = open(record_file_name, 'a')
    for elem in count.keys():
        wf_record.write("In reach_dst_last_extra, %s: %d, percent: %.2f\n" %(elem, count[elem], count[elem] / count_total))
        print("In reach_dst_last_extra, %s: %d, percent: %.2f\n" %(elem, count[elem], count[elem] / count_total))
        wf[elem].close()
    wf_not_decided.close()
    wf_record.close()

def ExtractLastAb(filename):
    rf = open(filename, 'r')
    count_total = 0
    count_last_ab = dict()
    count_seg_ab = 0
    wf_last_ab = dict()
    for elem in rel_kind:
        wf_last_ab[elem] = open(filename + '_last_ab_' + elem, 'w')
        count_last_ab[elem] = 0
    wf_seg_ab = open(filename + '_seg_ab', 'w')
    curline_trace = rf.readline()

    while curline_trace:
        count_total += 1
        curline_bgp = rf.readline()
        curline_ip = rf.readline()
        ori_trace_path = curline_trace.split(']')[1].strip('\n').strip(' ')
        trace_list = ori_trace_path.split(' ')
        #这里发现有的bgp path不是最相似的bgp_path，重新筛选一下
        #bgp_list = curline_bgp.strip('\n').strip('\t').split(' ')
        dst_prefix = curline_trace[1:curline_trace.index(' ')]
        ori_bgp_path_list = GetBgpPathFromBgpPrefixDict_2(dst_prefix)
        bgp_path = FindSimilarestBgpPath(CompressAsPathToMin(CompressAsPath(ori_trace_path)), ori_bgp_path_list)
        bgp_list = bgp_path.split(' ')
        ab_as_set = None
        ab_as = None
        last_ab = True
        last_norm_index = None
        bifurc = True
        for i in range(len(trace_list) - 1, -1, -1):
            cur_hop = trace_list[i]
            if cur_hop.__contains__('*') or cur_hop.__contains__('?') or cur_hop.__contains__('<'):
                continue
            if not AsnInBgpPathList(cur_hop, bgp_list):
                if last_norm_index:
                    bifurc = False
                    break
                if not ab_as_set:
                    ab_as_set = set(cur_hop.split('_'))
                else:
                    ab_as_set = set(cur_hop.split('_')) & ab_as_set
                    if not ab_as_set:
                        last_ab = False
                        break
            else:
                if not last_norm_index:
                    ab_as = '_'.join(list(ab_as_set))
                    last_norm_index = FindTraceAsInBgpPath(cur_hop, bgp_list)
                    for j in range(i + 1, len(trace_list)):
                        tmp = trace_list[j]
                        if tmp.__contains__('*') or tmp.__contains__('?') or tmp.__contains__('<'):
                            continue
                        trace_list[j] = ab_as #把多map的hop改为同一的map
        if not bifurc:
            print("NOTE, ought to be detour")
            print('\t' + ori_trace_path)
            print('\t' + bgp_path)
            curline_trace = rf.readline()
            continue
        if last_ab:
            max_rel = Get2AsRel(ab_as, bgp_list[last_norm_index])
            rel_2 = Get2AsRel(ab_as, bgp_list[last_norm_index + 1])
            if rel_2 > max_rel:   #有更近的关系
                max_rel = rel_2
            rel_str = TranslateAsRel(max_rel)
            wf_last_ab[rel_str].write("%s%s%s" %(curline_trace[:curline_trace.index(']') + 2] + ' '.join(trace_list) + '\n', '\t' + bgp_path + '\n', curline_ip))
            count_last_ab[rel_str] += 1
        else:
            wf_seg_ab.write("%s%s%s" %(curline_trace, curline_bgp, curline_ip))
            count_seg_ab += 1
        curline_trace = rf.readline()
    rf.close()

    print("Total count: %d" %count_total)
    print("Last ab count: %d" %(count_total - count_seg_ab))
    for elem in rel_kind:
        wf_last_ab[elem].close()
        print("Last ab %s count: %d" %(elem, count_last_ab[elem]))
    print("Seg ab count: %d" %count_seg_ab)
    wf_seg_ab.close()
   
def ExtractLastAb_2(filename, record_filename):
    rf = open(filename, 'r')
    count_total = 0
    wf_last_ab = open(filename + '_last_hop_ab_unknown', 'w')
    count_dict = {'same':0, 'sibling':0, 'provider':0, 'customer':0, 'peer':0, 'unknown':0, 'others':0}
    wf_others = open(filename + '_others', 'w')
    curline_trace = rf.readline()

    while curline_trace:
        curline_bgp = rf.readline()
        curline_ip = rf.readline()
        ori_trace_path = curline_trace.split(']')[1].strip('\n').strip(' ')
        trace_list = ori_trace_path.split(' ')
        #这里发现有的bgp path不是最相似的bgp_path，重新筛选一下
        #bgp_list = curline_bgp.strip('\n').strip('\t').split(' ')
        dst_prefix = curline_trace[1:curline_trace.index(' ')]
        ori_bgp_path_list = GetBgpPathFromBgpPrefixDict_2(dst_prefix)
        compress_trace_path = CompressAsPathToMin(CompressAsPath(ori_trace_path))
        fin_trace_path_list = DropStarsInTraceList(compress_trace_path.split(' '))
        bgp_path = FindSimilarestBgpPath(compress_trace_path, ori_bgp_path_list)
        bgp_list = bgp_path.split(' ')
        #判断方法1：
        # if set(fin_trace_path_list[:-1]).issubset(set(bgp_list[:-1])) and \
        #     AsnInTracePathList(bgp_list[-2], fin_trace_path_list):
        #     rel = Get2AsRel(fin_trace_path_list[-1], bgp_list[-1])
        #判断方法2：
        if set(fin_trace_path_list[:-1]).issubset(set(bgp_list)):
            index = FindTraceAsInBgpPath(fin_trace_path_list[-2], bgp_list)
            rel = GetAsRelAndTranslate(fin_trace_path_list[-1], bgp_list[index + 1])
            count_dict[rel] += 1
            count_total += 1
            wf_last_ab.write(curline_trace)
            wf_last_ab.write('\t' + bgp_path + '\n')
            wf_last_ab.write(curline_ip)
        else:
            count_dict['others'] += 1
            count_total += 1
            wf_others.write(curline_trace)
            wf_others.write('\t' + bgp_path + '\n')
            wf_others.write(curline_ip)
        curline_trace = rf.readline()

    wf_last_ab.close()
    wf_others.close()
    with open(record_filename, 'a') as wf:
        wf.write("In (sub1)last_ab, ")
        for (key, val) in count_dict.items():
            print('\t%s: %d, percent: %.2f\n' %(key, val, val / count_total))
   
def ExtractLastAb_3(filename, count_last_ab_dict):
    rf = open(filename, 'r')
    curline_trace = rf.readline()

    while curline_trace:
        curline_bgp = rf.readline()
        curline_ip = rf.readline()
        ori_trace_path = curline_trace.split(']')[1].strip('\n').strip(' ')
        trace_list = ori_trace_path.split(' ')
        #这里发现有的bgp path不是最相似的bgp_path，重新筛选一下
        #bgp_list = curline_bgp.strip('\n').strip('\t').split(' ')
        dst_prefix = curline_trace[1:curline_trace.index(' ')]
        ori_bgp_path_list = GetBgpPathFromBgpPrefixDict_2(dst_prefix)
        compress_trace_path = CompressAsPathToMin(CompressAsPath(ori_trace_path))
        fin_trace_path_list = DropStarsInTraceList(compress_trace_path.split(' '))
        bgp_path = FindSimilarestBgpPath(compress_trace_path, ori_bgp_path_list)
        bgp_list = bgp_path.split(' ')
        #判断方法1：
        # if set(fin_trace_path_list[:-1]).issubset(set(bgp_list[:-1])) and \
        #     AsnInTracePathList(bgp_list[-2], fin_trace_path_list):
        #     rel = Get2AsRel(fin_trace_path_list[-1], bgp_list[-1])
        #判断方法2：
        #if set(fin_trace_path_list[:-1]).issubset(set(bgp_list)):
        for elem in fin_trace_path_list[-1].split('_'):
            if elem not in count_last_ab_dict.keys():
                count_last_ab_dict[elem] = 0
            count_last_ab_dict[elem] += 1
        curline_trace = rf.readline()
             
def CheckForSpecLink(filename):
    as1 = '3257'
    as2 = '5580'
    key = as1 + ' ' + as2
    '''
    rf = open(filename, 'r')
    wf = open('susp_ip_list', 'w')
    curline_trace = rf.readline()
    while curline_trace:
        discard = rf.readline()
        curline_ip = rf.readline()
        if curline_trace.__contains__(key):
            pos = curline_trace.index(key)
            list_index = curline_trace[0:pos + 1].count(' ') - 2
            elems = curline_ip.split(']')
            if len(elems) > 1:
                ip_list = elems[1].strip('\n').strip(' ').split(' ')
                ip1 = ip_list[list_index]
                ip2 = ip_list[list_index + 1]
                #print(ip1)
                #print(ip2)
                #print('')
                rv_as1 = GetAsStrOfIpByRv(ip1)
                mi_as1 = GetAsOfIpByMi(ip1)
                rv_as2 = GetAsStrOfIpByRv(ip2)
                mi_as2 = GetAsOfIpByMi(ip2)
                wr_as1 = rv_as1 + '|' + mi_as1 #_'.join(set(rv_as1.split('_')) | set(mi_as1.split('_')))
                wr_as2 = rv_as2 + '|' + mi_as2 #'_'.join(set(rv_as2.split('_')) | set(mi_as2.split('_')))
                wf.write("%s, %s" %(wr_as1, wr_as2))
                wf.write("\t%s" %curline_trace)
        curline_trace = rf.readline()
    rf.close()
    wf.close()
    '''
    DebugGetBgpLink(key)

def GetSpecIpInfo(filename, key):
    rf = open(filename, 'r')
    ip_tuple_list = []
    curline_trace = rf.readline()
    (fst_as, snd_as) = key.split(' ')    
    while curline_trace:
        curline_bgp = rf.readline()
        curline_ip = rf.readline()
        if not curline_trace.__contains__(snd_as) and \
            not curline_trace.__contains__(fst_as):
            curline_trace = rf.readline()
            continue
        elems = curline_trace.split(']')
        if len(elems) < 2:
            curline_trace = rf.readline()
            continue
        trace_path_list = elems[1].strip('\n').strip(' ').split(' ')
        i = 0
        find = False
        for i in range(0, len(trace_path_list) - 1):
            if fst_as in trace_path_list[i].split('_') and \
                snd_as in trace_path_list[i + 1].split('_'):
                find = True
                break
        if not find:
            curline_trace = rf.readline()
            continue
        elems = curline_ip.split(']')
        if len(elems) < 2:
            curline_trace = rf.readline()
            continue
        #print("%s %s" %(trace_path_list[i], trace_path_list[i + 1]))
        ip_list = elems[1].strip('\n').strip(' ').split(' ')
        ip_tuple_list.append([ip_list[i], ip_list[i + 1]])
        curline_trace = rf.readline()
    return ip_tuple_list

    
def CheckIfLastHopAb(filename):
    rf = open(filename, 'r')
    wf_last_hop_ab_list = []
    ab_num_list = [ 0 for i in range(0, 2)]
    for i in range(0,2):
        wf = open(filename + "_last_hop_ab_" + str(i+1), 'w')
        wf_last_hop_ab_list.append(wf)
    wf_not_last_hop_ab = open(filename + "_not_last_hop_ab", 'w')
    cur_num_ab = 0

    curline_trace = rf.readline()
    while curline_trace:
        curline_bgp = rf.readline()
        curline_ip = rf.readline()
        elems = curline_trace.strip('\n').strip(' ').split(']')
        if len(elems) < 2:
            curline_trace = rf.readline()
            continue
        ori_trace_path = elems[1].strip('\n').strip(' ')
        trace_path = CompressAsPathToMin(CompressAsPath(ori_trace_path))
        trace_list = trace_path.split(' ')
        dst_key = elems[0][1:]
        dst_as_str = dst_key.split(' ')[1]
        find = False
        for elim_num in range(1, 3):
            former_trace_list = trace_list[0:len(trace_list) - elim_num]
            for dst_as in dst_as_str.split('_'):
                bgp_path_list = DebugGetBgpRoute(dst_as)
                if bgp_path_list:                    
                    for bgp_path in bgp_path_list:
                        match = True
                        for elem in former_trace_list:
                            if not AsnInBgpPathList(elem, bgp_path):
                                match = False
                                break
                        if match:
                            wf_last_hop_ab_list[elim_num - 1].write("%s" %curline_trace)
                            wf_last_hop_ab_list[elim_num - 1].write("\t\t%s\n" %bgp_path)
                            wf_last_hop_ab_list[elim_num - 1].write("%s" %curline_ip)
                            ab_num_list[elim_num - 1] += 1
                            find = True
                            break
                if find:
                    break
            if find:
                break
        if not find:
            wf_not_last_hop_ab.write("%s" %curline_trace)
            wf_not_last_hop_ab.write("%s" %curline_bgp)
            wf_not_last_hop_ab.write("%s" %curline_ip)
        curline_trace = rf.readline()
        
    print("[%s]num_ab: %d" %(filename, cur_num_ab))    
    for i in range(0,2):
        wf_last_hop_ab_list[i].close()
        print("[%s last hop]num_ab: %d" %(i + 1, ab_num_list[i]))
    wf_not_last_hop_ab.close()
    rf.close()

def GetLastAbAsRel(filename, ab_num): #ab_num == 1 或 ab_num == 2
    rf = open(filename, 'r')
    wf = open(filename + "_ab_rel", 'w')
    wf_ab = open(filename + "_ab_no_rel", 'w')
    subs_rel_num = [0 for i in range(3)] #0: cust; 1: peer; 2:prov
    prev_rel_num = [0 for i in range(3)]
    subs_rel_num_dict = dict()
    prev_rel_num_dict = dict()
    remain_ab_num = 0
    
    curline_trace = rf.readline()
    while curline_trace:
        curline_bgp = rf.readline()
        curline_ip = rf.readline()
        elems = curline_trace.split(']')
        if len(elems) < 2:
            curline_trace = rf.readline()
            continue
        ori_trace_path = elems[1].strip('\n').strip(' ')
        trace_path = CompressAsPathToMin(CompressAsPath(ori_trace_path))
        trace_list = trace_path.split(' ')
        last_ab_as = trace_list[-1]
        last_ab_as_list = last_ab_as.split('_')
        last_next_ab_as = ''
        last_next_ab_as_list = []
        former_as = ''
        if ab_num == 2:
            last_next_ab_as = trace_list[-2]
            last_next_ab_as_list = last_next_ab_as.split('_')
            former_as = trace_list[-3]
        else:
            former_as = trace_list[-2]
        former_as_sig = ''
        bgp_list = curline_bgp.strip('\n').strip('\t').split(' ')
        for asn in former_as.split('_'):
            if asn in bgp_list:
                former_as_sig = asn
                break
        if not former_as_sig:
            print(curline_trace, end='')
            print(curline_bgp, end='')
            curline_trace = rf.readline()
            continue
        bgp_start_index = bgp_list.index(former_as_sig) + 1
        prev_rel_dict = dict()
        subs_rel_dict = dict()
        prev_rel_dict_2 = dict()
        subs_rel_dict_2 = dict()
        omit = False
        omit_2 = False
        if ab_num != 2:
            omit_2 = True
        max_prev_rel = -2
        max_subs_rel = -2
        max_prev_rel_2 = -2
        max_subs_rel_2 = -2
        for asn in last_ab_as_list:
            rel = Get2AsRel(former_as_sig, asn)
            if rel >= 2:
                omit = True
                break
            prev_rel_dict[(former_as_sig, asn)] = rel
            if rel > max_prev_rel:
                max_prev_rel = rel
        for asn in last_next_ab_as_list:
            rel = Get2AsRel(former_as_sig, asn)
            if rel >= 2:
                omit_2 = True
                break
            prev_rel_dict_2[(former_as_sig, asn)] = rel
            if rel > max_prev_rel_2:
                max_prev_rel_2 = rel
        if not omit:
            for i in range(bgp_start_index, len(bgp_list)):
                for asn in last_ab_as_list:
                    rel = Get2AsRel(former_as_sig, asn)
                    if rel >= 2:
                        omit = True
                        break
                    subs_rel_dict[(bgp_list[i], asn)] = rel
                    if rel > max_subs_rel:
                        max_subs_rel = rel
                if omit:
                    break
        if not omit_2:
            for i in range(bgp_start_index, len(bgp_list)):
                for asn in last_next_ab_as_list:
                    rel = Get2AsRel(former_as_sig, asn)
                    if rel >= 2:
                        omit_2 = True
                        break
                    subs_rel_dict_2[(bgp_list[i], asn)] = rel
                    if rel > max_subs_rel_2:
                        max_subs_rel_2 = rel
                if omit_2:
                    break
        if omit and omit_2:
            curline_trace = rf.readline()
            continue
        write_in_rel = True
        if ab_num == 2:
            if max_subs_rel > -2 and max_subs_rel_2 > -2:
                key = (max_subs_rel, max_subs_rel_2)
                if key not in subs_rel_num_dict.keys():
                    subs_rel_num_dict[key] = 0
                subs_rel_num_dict[key] += 1
            elif max_prev_rel > -2 and max_prev_rel_2 > -2:
                key = (max_prev_rel, max_prev_rel_2)
                if key not in prev_rel_num_dict.keys():
                    prev_rel_num_dict[key] = 0
                prev_rel_num_dict[key] += 1
            else:
                remain_ab_num += 1
                write_in_rel = False
        else:
            if max_subs_rel > -2:
                subs_rel_num[max_subs_rel + 1] += 1 #0: cust; 1: peer; 2:prov
            elif max_prev_rel > -2:
                prev_rel_num[max_prev_rel + 1] += 1 #0: cust; 1: peer; 2:prov
            else:
                remain_ab_num += 1
                write_in_rel = False
        if write_in_rel:
            wf.write(curline_trace)
            wf.write(curline_bgp)
        else:
            wf_ab.write(curline_trace)
            wf_ab.write(curline_bgp)
            wf_ab.write(curline_ip)
        if not omit and write_in_rel:
            for (key, rel) in prev_rel_dict.items():
                (as1, as2) = key
                wf.write("%s(%s)%s " %(as1, rel, as2))
            for (key, rel) in subs_rel_dict.items():
                (as1, as2) = key
                wf.write("%s(%s)%s " %(as1, rel, as2))
            wf.write('\n')
        if not omit_2 and write_in_rel:
            for (key, rel) in prev_rel_dict_2.items():
                (as1, as2) = key
                wf.write("%s(%s)%s " %(as1, rel, as2))
            for (key, rel) in subs_rel_dict_2.items():
                (as1, as2) = key
                wf.write("%s(%s)%s " %(as1, rel, as2))
            wf.write('\n')
        curline_trace = rf.readline()
    rf.close()
    wf.close()
    wf_ab.close()
    if ab_num == 2:
        print("subs:")
        for (key, num) in subs_rel_num_dict.items():
            (rel1, rel2) = key
            print("\t(%s %s) %d" %(rel1, rel2, num))
        print("prev:")
        for (key, num) in prev_rel_num_dict.items():
            (rel1, rel2) = key
            print("\t(%s %s) %d" %(rel1, rel2, num))
    else:
        print("subs rel is customer: %d" %subs_rel_num[0])
        print("subs rel is peer: %d" %subs_rel_num[1])
        print("subs rel is provider: %d" %subs_rel_num[2])
        print("prev rel is customer: %d" %prev_rel_num[0])
        print("prev rel is peer: %d" %prev_rel_num[1])
        print("prev rel is provider: %d" %prev_rel_num[2])
    print("no rel: %d" %remain_ab_num)

def GetLastAbAsRel_2(filename, record_filename, open_mode):
    rf = open(filename, 'r')
    wf = open(filename + "_rel", 'w')
    wf_ab = open(filename + "_no_rel", 'w')
    count_total = 0
    count_rel = 0
    count_no_rel = 0
    
    curline_trace = rf.readline()
    while curline_trace:
        curline_bgp = rf.readline()
        curline_ip = rf.readline()
        elems = curline_trace.split(']')
        if len(elems) < 2:
            curline_trace = rf.readline()
            continue
        count_total += 1
        ori_trace_path = elems[1].strip('\n').strip(' ')
        trace_path = CompressAsPathToMin(CompressAsPath(ori_trace_path))
        trace_list = trace_path.split(' ')
        last_ab_as = trace_list[-1]
        last_ab_as_list = last_ab_as.split('_')
        former_as = trace_list[-2]
        former_as_sig = ''
        bgp_list = curline_bgp.strip('\n').strip('\t').split(' ')
        for asn in former_as.split('_'):
            if asn in bgp_list:
                former_as_sig = asn
                break
        if not former_as_sig:
            print(curline_trace)
            print(curline_bgp)
            curline_trace = rf.readline()
            continue
        rel_dict = dict()
        omit = False
        max_rel = -2
        for asn in last_ab_as_list:
            rel = Get2AsRel(former_as_sig, asn)
            if rel >= 2:
                omit = True
                break
            rel_dict[(former_as_sig, asn)] = rel
            if rel > max_rel:
                max_rel = rel        
        if not omit:
            for asn in last_ab_as_list:
                rel = Get2AsRel(bgp_list[-1], asn)
                if rel >= 2:
                    omit = True
                    break
                rel_dict[(bgp_list[-1], asn)] = rel
                if rel > max_rel:
                    max_rel = rel
        if omit:
            curline_trace = rf.readline()
            continue
        if max_rel != -2:
            wf.write(curline_trace)
            wf.write(curline_bgp)
            wf.write(curline_ip)
            count_rel += 1
        else:
            wf_ab.write(curline_trace)
            wf_ab.write(curline_bgp)
            wf_ab.write(curline_ip)
            count_no_rel += 1
        curline_trace = rf.readline()
    rf.close()
    wf.close()
    wf_ab.close()

    wf_record = open(record_filename, open_mode)
    wf_record.write("In not_reach_dst_last_ab, last ab has rel: %d, percent: %.2f\n" %(count_rel, count_rel / count_total))
    wf_record.write("In not_reach_dst_last_ab, last ab has no rel: %d, percent: %.2f\n" %(count_no_rel, count_no_rel / count_total))
    wf_record.close()

def AnaGeoOfLastAbAs(filename, ab_num):
    rf = open(filename, 'r')
    wf = open('dist_' + filename, 'w')

    curline_trace = rf.readline()
    while curline_trace:
        curline_bgp = rf.readline()
        curline_ip = rf.readline()

        elems = curline_trace.split(']')
        if len(elems) < 2:
            curline_trace = rf.readline()
            continue
        dst_key = elems[0].strip('[')
        ori_trace_path = elems[1].strip('\n').strip(' ')
        ori_trace_list = ori_trace_path.split(' ')
        trace_path = CompressAsPathToMin(CompressAsPath(ori_trace_path))
        trace_list = trace_path.split(' ')
        last_ab_index = ori_trace_list.index(trace_list[-1 * ab_num])
        last_normal_index = last_ab_index - 1
        while ori_trace_list[last_normal_index] == '*' or ori_trace_list[last_normal_index] == '?':
            last_normal_index -= 1
        temp = curline_ip.split(']')
        if len(temp) < 2:
            curline_trace = rf.readline()
            continue
        ip_path = temp[1].strip('\n').strip(' ')
        if not ip_path:
            curline_trace = rf.readline()
            continue
        ip_list = ip_path.split(' ')
        ip_last_normal = ip_list[last_normal_index]
        ip_first_ab = ip_list[last_ab_index]
        ip_last_ab = ''
        for i in range(len(ip_list) - 1, -1, -1):
            if ip_list[i] != '*':
                ip_last_ab = ip_list[i]
                break
        ip_dst = dst_key.split('/')[0]
        (accurate1, dist1) = GetDistOfIps(ip_last_normal, ip_first_ab)
        (accurate2, dist2) = GetDistOfIps(ip_last_ab, ip_dst)
        if (accurate1 and dist1 < 100) or (accurate2 and dist2 < 100): #认为正常
            curline_trace = rf.readline()
            continue
        wf.write(curline_trace)
        wf.write(curline_bgp)
        wf.write(curline_ip)
        if not accurate1:
            wf.write("?")
        wf.write("%d " %dist1)
        if not accurate2:
            wf.write("?")
        wf.write("%d\n" %dist2)
        curline_trace = rf.readline()
    rf.close()
    wf.close()

def TransDistToDistClass(dist_str):
    pre = ''
    if dist_str.__contains__('?'):
        pre = '?'
    dist = int(dist_str.strip('?'))
    dist_class = ''
    if dist == 0:
        dist_class = '0'
    elif dist < 100:
        dist_class = '<100'
    else:
        dist_class = '>100'
    return pre + dist_class

def TmpAnaDist(filename):
    rf = open(filename, 'r')
    curline = rf.readline()
    dist_count_dict = dict()
    while curline:
        if curline.__contains__('[') or curline.__contains__('\t'):
            curline = rf.readline()
            continue
        (dist1_str, dist2_str) = curline.strip('\n').split(' ')
        dist1_class = TransDistToDistClass(dist1_str)
        dist2_class = TransDistToDistClass(dist2_str)
        key = dist1_class + ' ' + dist2_class
        if key not in dist_count_dict.keys():
            dist_count_dict[key] = 0
        dist_count_dict[key] += 1
        curline = rf.readline()
    total_num = 0
    for (key, count) in dist_count_dict.items():
        print("%s:%d" %(key, count))  
        total_num += count
    print("count: %d" %total_num)      

def CheckAbStartAs(filename, top_num): #找出每条路径一开始分叉的地方(AS)
    rf = open(filename, 'r')
    wf = open(filename + '_top_ab_start_as', 'w')
    wf_pre_ab = open(filename + '_pre_ab_as', 'w')

    curline_trace = rf.readline()
    ab_pre_as_dict = dict()
    ab_as_dict = dict()
    while curline_trace:
        curline_bgp = rf.readline()
        curline_ip = rf.readline()

        elems = curline_trace.split(']')
        if len(elems) < 2:
            curline_trace = rf.readline()
            continue
        ori_trace_path = elems[1].strip('\n').strip(' ')
        trace_path = CompressAsPathToMin(CompressAsPath(ori_trace_path))
        trace_list = trace_path.split(' ')
        bgp_list = curline_bgp.strip('\n').strip('\t').split(' ')
        for i in range(0, len(trace_list)):
            if not AsnInBgpPathList(trace_list[i], bgp_list):
                if trace_list[i - 1] not in ab_pre_as_dict.keys():
                    ab_pre_as_dict[trace_list[i - 1]] = 0
                ab_pre_as_dict[trace_list[i - 1]] += 1
                if trace_list[i] not in ab_as_dict.keys():
                    ab_as_dict[trace_list[i]] = 0
                ab_as_dict[trace_list[i]] += 1
                break
        curline_trace = rf.readline()
    rf.close()
    sort_list = sorted(ab_as_dict.items(), key=lambda d:d[1], reverse = True)
    sort_list_1 = sorted(ab_pre_as_dict.items(), key=lambda d:d[1], reverse = True)
    asn_list = []
    asn_list_1 = []
    for elem in sort_list:
        asn_list.append(elem[0])
    for elem in sort_list_1:
        asn_list_1.append(elem[0])
    rank_list = GetAsRank(asn_list[:top_num])
    rank_list_1 = GetAsRank(asn_list_1)
    for i in range(top_num):
        print("%s %s %s" %(sort_list[i][0], sort_list[i][1], rank_list[i]))
        wf.write("%s %s %s\n" %(sort_list[i][0], sort_list[i][1], rank_list[i]))
    for i in range(0, len(sort_list_1)):
        wf_pre_ab.write("%s %s %s\n" %(sort_list_1[i][0], sort_list_1[i][1], rank_list_1[i]))
    wf.close()
    wf_pre_ab.close()

def TmpExtractSet():
    rf = open('res2_has_set_nrt-jp.20190301', 'r')
    wf_set = open('has_set', 'w')
    wf = open('not_has_set', 'w')
    curline_trace = rf.readline()
    
    while curline_trace:
        curline_ip = rf.readline()
        if curline_trace.__contains__('{'):
            if curline_ip.__contains__('{'):
                section_trace = re.findall('(\{.*?\})', curline_trace)
                section_ip = re.findall('(\{.*?\})', curline_ip)
                i = 0
                for seg in section_trace:
                    as_list = seg.strip('{').strip('}').split(' ')
                    ip_list = section_ip[i].strip('{').strip('}').split(' ')
                    trace_str = ''
                    if len(set(as_list)) == 1:
                        trace_str = str(' ') + as_list[0]
                        #print(curline_trace)
                        curline_trace = curline_trace.replace(seg, trace_str, 1)
                        #print(curline_trace)
                        #print(curline_ip)
                        ip_str = str(' ') + ip_list[0]
                        curline_ip = curline_ip.replace(section_ip[i], ip_str, 1)
                        #print(curline_ip)
                    #else: #暂时不处理
                    i += 1
                if curline_trace.__contains__('{'):
                    wf_set.write(curline_trace)
                    wf_set.write(curline_ip)
                else:
                    wf.write(curline_trace)
                    wf.write(curline_ip)
            else:
                print(curline_trace, end='')
                print(curline_ip, end='')
        else:
            if curline_ip.__contains__('{'):
                print(curline_trace, end='')
                print(curline_ip, end='')
            else:
                wf.write(curline_trace)
                wf.write(curline_ip)
        curline_trace = rf.readline()
    rf.close()
    wf.close()
    wf_set.close()

def TmpClassiMultiSingle():
    rf = open('not_has_set', 'r', encoding='utf-8')
    wf_single = open('single_not_has_set', 'w', encoding='utf-8')
    wf_multi = open('multi_not_has_set', 'w', encoding='utf-8')

    trace_dict = dict()
    curline_trace = rf.readline()
    while curline_trace:
        curline_ip = rf.readline()
        dst_key = re.match('(\[.*?\])', curline_trace).group(1).strip('[').strip(']')
        '''
        as_path = curline_trace.split(']')[1].strip('\n').strip(' ')
        as_path = CompressAsPathToMin(CompressAsPath(as_path))
        if dst_key not in trace_dict.keys():
            trace_dict[dst_key] = dict()
        find = False
        for cur_min_path in trace_dict[dst_key].keys():
            if as_path == cur_min_path or FstPathContainedInSnd(as_path, cur_min_path):
                find = True
                break
            elif FstPathContainedInSnd(cur_min_path, as_path):
                trace_dict[dst_key][as_path] = [curline_trace, curline_ip]
                trace_dict[dst_key].pop(cur_min_path)
                find = True
                break
        if not find:
            trace_dict[dst_key][as_path] = [curline_trace, curline_ip]
        '''
        if dst_key not in trace_dict.keys():
            trace_dict[dst_key] = []
        trace_dict[dst_key].append([curline_ip.split(']')[1].strip('\n').strip(' '), curline_trace.split(']')[1].strip('\n').strip(' ')])
        curline_trace = rf.readline()        
    rf.close()

    '''
    for (dst_key, path_dict) in trace_dict.items():
        if len(path_dict) == 1:
            for as_path in path_dict.keys():
                wf_single.write(path_dict[as_path][0])
                wf_single.write(path_dict[as_path][1])
        else:
            for as_path in path_dict.keys():
                wf_multi.write(path_dict[as_path][0])
                wf_multi.write(path_dict[as_path][1])
    '''
    multi_num = 0
    for (dst_key, ip_as_path_list) in trace_dict.items():    
        multi_num += FilterSimilarPathAndRecord(dst_key, ip_as_path_list, wf_single, wf_multi, False)
    wf_single.close()
    wf_multi.close()
    print("multi_num: %d" %multi_num)

bgp_prefix_as_dict = dict()
def GetBgpPrefixAsDict(asn):
    rf = open('bgp_' + asn, 'r', encoding='utf-8')
    curline = rf.readline()
    while curline:
        elems = curline.split('|')
        prefix = elems[1]
        as_path = elems[2]
        if as_path:
            dst_as = as_path.split(' ')[-1]
        if prefix not in bgp_prefix_as_dict.keys():
            bgp_prefix_as_dict[prefix] = dst_as
        else:
            cur_as = bgp_prefix_as_dict[prefix]
            if dst_as not in cur_as.split('_'):
                bgp_prefix_as_dict[prefix] += '_' + dst_as
        curline = rf.readline()
    rf.close()

def SubGetMatchPrefixInBgpRoute(ip_int, mask_len):
    mask = 0xFFFFFFFF - (1 << (32 - mask_len)) + 1
    cur_prefix_int = ip_int & mask
    cur_prefix = str(socket.inet_ntoa(struct.pack('I',socket.htonl(cur_prefix_int))))
    cur_prefix = cur_prefix + '/' + str(mask_len)
    #print(cur_prefix)
    if cur_prefix in bgp_prefix_as_dict.keys():
        return (cur_prefix, bgp_prefix_as_dict[cur_prefix])
    return (None, None)
    
def GetMatchPrefixInBgpRoute(prefix): #已知prefix不在bgp_prefix_as_dict中
    elems = prefix.split('/')
    ip = elems[0]
    slash = int(elems[1])
    ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton(ip))[0])
    if slash > 7 and slash < 25:
        for mask_len in range(slash - 1, 7, -1):
            (matched_prefix, asn) = SubGetMatchPrefixInBgpRoute(ip_int, mask_len)
            if matched_prefix:
                return (matched_prefix, asn)
        for mask_len in range(slash + 1, 25):
            (matched_prefix, asn) = SubGetMatchPrefixInBgpRoute(ip_int, mask_len)
            if matched_prefix:
                return (matched_prefix, asn)
    return (None, None)

def FstAsnIsInSndAsn(as1, as2):
    as2_list = as2.split('_')
    for elem in as1.split('_'):
        if elem in as2_list:
            return True
    return False

def AnaNoBgpTrace(filename):
    rf = open(filename, 'r')
    wf_similar_prefix = open(filename + '_similar_prefix', 'w')
    wf_no_prefix = open(filename + '_no_prefix', 'w')
    wf_dif_as = open(filename + '_dif_as', 'w')
    cur_num_similar_prefix = 0
    cur_num_no_prefix = 0
    cur_num_dif_as = 0
    dst_as_list = []

    curline_trace = rf.readline()
    while curline_trace:
        curline_ip = rf.readline()
        dst_key = curline_trace[1:curline_trace.index(']')]
        elems = dst_key.split(' ')
        prefix = elems[0]
        dst_as = elems[1]
        if dst_as not in dst_as_list:
            dst_as_list.append(dst_as)
        if prefix in bgp_prefix_as_dict.keys():
            cur_as = bgp_prefix_as_dict[prefix]
            if FstAsnIsInSndAsn(dst_as, cur_as):
                print("NOTE (%s %s) found in (%s %s)" %(prefix, dst_as, prefix, cur_as))
            else:
                wf_dif_as.write("(%s)\n" %cur_as)
                wf_dif_as.write(curline_trace)
                wf_dif_as.write(curline_ip)
                cur_num_dif_as += 1
        else:
            (matched_prefix, asn) = GetMatchPrefixInBgpRoute(prefix)
            if matched_prefix:
                wf_similar_prefix.write("(%s %s)\n" %(matched_prefix, asn))
                wf_similar_prefix.write(curline_trace)
                wf_similar_prefix.write(curline_ip)
                cur_num_similar_prefix += 1
            else:
                wf_no_prefix.write(curline_trace)
                wf_no_prefix.write(curline_ip)
                cur_num_no_prefix += 1
        curline_trace = rf.readline()
    rf.close()
    
    wf_similar_prefix.close()
    wf_dif_as.close()
    wf_no_prefix.close()
    print("cur_num_dif_as: %d" %cur_num_dif_as)
    print("cur_num_similar_prefix: %d" %cur_num_similar_prefix)
    print("cur_num_no_prefix: %d" %cur_num_no_prefix)
    print("dst_as num: %d" %len(dst_as_list))

def TmpCmpIxpPathFiles():
    rf = open('5_has_ixp_ip', 'r')

    key_set = set()
    curline = rf.readline()
    while curline:
        curline = rf.readline()
        key = curline[1:curline.index(']')]
        key_set.add(key)
        curline = rf.readline()
    rf.close

    rf = open('cmp', 'r')
    wf = open('tmp', 'w')
    curline_trace = rf.readline()
    while curline_trace:
        curline_ip = rf.readline()
        #if not curline_ip or curline_ip.count(']') == 0:
        key = curline_ip[1:curline_ip.index(']')]
        if key not in key_set:
            wf.write(curline_trace)
            wf.write(curline_ip)
        curline_trace = rf.readline()
    rf.close()
    wf.close()

def CalKeyPercentInIxpPath(src_file, ixp_path_file, has_bgp_path):
    rf = open(ixp_path_file)

    key_set = set()
    curline = rf.readline()
    while curline:
        curline = rf.readline()
        key = curline[1:curline.index(']')]
        key_set.add(key)
        curline = rf.readline()
    rf.close()

    rf = open(src_file, 'r')
    curline_trace = rf.readline()
    total_num = 0
    find_num = 0
    while curline_trace:
        if has_bgp_path:
            curline = rf.readline()
        curline_ip = rf.readline()
        total_num += 1
        #if not curline_ip or curline_ip.count(']') == 0:
        key = curline_ip[1:curline_ip.index(']')]
        if key in key_set:
            find_num += 1
        curline_trace = rf.readline()
    rf.close()
    print(find_num)
    print(total_num)
    print("percent: %.2f" %(find_num / total_num))


def AnaAsDegree(filename):
    rf = open(filename, 'r')
    as_conn_dict = dict()
    num_0_customer_1_provider_0_peer = 0
    num_0_customer_1_provider_less_peer = 0
    num_0_customer_1_provider_more_peer = 0
    num_0_customer_less_provider = 0
    num_0_customer_more_provider = 0
    num_has_customer = 0

    curline_trace = rf.readline()
    while curline_trace:
        curline_ip = rf.readline()
        dst_key = curline_trace[1:curline_trace.index(']')]
        dst_as = dst_key.split(' ')[1]
        for cur_as in dst_as.split('_'):
            if cur_as not in as_conn_dict.keys():
                (provider_num, customer_num, peer_num) = GetAsConnDegree(cur_as, False)
                as_conn_dict[cur_as] = [provider_num, customer_num, peer_num]
        curline_trace = rf.readline()
    rf.close()

    for (key, val) in as_conn_dict.items():
        [provider_num, customer_num, peer_num] = val
        if customer_num == 0:
            if provider_num == 1:
                if peer_num == 0:
                    num_0_customer_1_provider_0_peer += 1
                elif peer_num < 3:
                    num_0_customer_1_provider_less_peer += 1
                else:
                    num_0_customer_1_provider_more_peer += 1
            elif provider_num < 3:
                num_0_customer_less_provider += 1
            else:
                num_0_customer_more_provider += 1
        else:
            num_has_customer += 1
    
    total = len(as_conn_dict)
    num_0_customer_1_provider = num_0_customer_1_provider_0_peer + num_0_customer_1_provider_less_peer + num_0_customer_1_provider_more_peer
    print("num_0_customer_1_provider_0_peer: %d %.2f" %(num_0_customer_1_provider_0_peer, num_0_customer_1_provider_0_peer / total))
    print("num_0_customer_1_provider_less_peer: %d %.2f" %(num_0_customer_1_provider_less_peer, num_0_customer_1_provider_less_peer / total))
    print("num_0_customer_1_provider_more_peer: %d %.2f" %(num_0_customer_1_provider_more_peer, num_0_customer_1_provider_more_peer / total))
    print("num_0_customer_1_provider: %d %.2f" %(num_0_customer_1_provider, num_0_customer_1_provider / total))
    print("num_0_customer_less_provider: %d %.2f" %(num_0_customer_less_provider, num_0_customer_less_provider / total))
    print("num_0_customer_more_provider: %d %.2f" %(num_0_customer_more_provider, num_0_customer_more_provider / total))
    print("num_has_customer: %d %.2f" %(num_has_customer, num_has_customer / total))
    print("total: %d" %total)

def GetDifStartAFromB(list1, list2):
    dif = False
    dif_index_list = []
    for i in range(0, len(list1)):
        elem = list1[i]
        if elem == '*' or elem == '?' or AsnInBgpPathList(elem, list2):
            dif = False
        elif dif == True:
            pass
        else:
            dif_index_list.append(i)
            dif = True
    return dif_index_list

ixp_index_dict_1 = dict()
def GetIxpIndexDict_Obselete():
    rf = open('5_has_ixp_ip_index', 'r')

    curline_ixp_index = rf.readline()
    while curline_ixp_index:
        discard = rf.readline()
        curline_ip = rf.readline()
        elems = curline_ixp_index.split(']')
        if len(elems) < 2:
            curline_ixp_index = rf.readline()
            continue
        dst_prefix = elems[0].split(' ')[0][1:]
        ixp_index_str = elems[1].strip('\n').strip(' ')
        ip_path = curline_ip.split(']')[1].strip('\n').strip(' ')
        if dst_prefix not in ixp_index_dict_1.keys():
            ixp_index_dict_1[dst_prefix] = []
        ixp_index_dict_1[dst_prefix].append([ixp_index_str, ip_path])
        curline_ixp_index = rf.readline()
          
    rf.close()
    print("ixp_index_dict_1 len: %d" %len(ixp_index_dict_1))

    '''
    num = 0
    for (key, val) in ixp_index_dict.items():
        if len(val) > 1:
            for elem in val:
                print("%s %s" %(key, elem[1]))
            num += 1
    print(num)
    '''

ixp_index_dict = dict()
def GetIxpIndexDict():
    rf = open('5_has_ixp_ip', 'r')

    curline_trace = rf.readline()
    while curline_trace:
        curline_ip = rf.readline()
        elems = curline_trace.split(']')
        if len(elems) < 2:
            curline_trace = rf.readline()
            continue
        dst_prefix = elems[0].split(' ')[0][1:]
        trace_list = elems[1].strip('\n').strip(' ').split(' ')
        index_list = []
        for i in range(0, len(trace_list)):
            if trace_list[i] == '<>':
                index_list.append(str(i))
        ip_path = curline_ip.split(']')[1].strip('\n').strip(' ')
        ip_path = re.sub('\<.*?\>[ ]', '', ip_path)
        ip_path = re.sub('\<.*?\>', '', ip_path)
        ip_path = ip_path.strip(' ')
        #ip_path = ip_path.replace('<>', '')
        if dst_prefix not in ixp_index_dict.keys():
            ixp_index_dict[dst_prefix] = []
        ixp_index_dict[dst_prefix].append([' '.join(index_list), ip_path])
        curline_trace = rf.readline()
          
    rf.close()
    print("ixp_index_dict len: %d" %len(ixp_index_dict))

def GetDifStart(filename):
    rf = open(filename, 'r')
    wf_difstrat_not_in_ixp = open(filename + '_difstart_not_in_ixp', 'w')
    wf_difstrat_1_dif_same_w_ixp = open(filename + '_difstart_1_dif_same_w_ixp', 'w')
    wf_difstrat_1_dif_dif_w_ixp = open(filename + '_difstart_1_dif_dif_w_ixp', 'w')
    wf_difstrat_mul_dif_fst_same_w_ixp = open(filename + '_difstart_mul_dif_fst_same_w_ixp', 'w')
    wf_difstrat_mul_dif_all_same_w_ixp = open(filename + '_difstart_mul_dif_all_same_w_ixp', 'w')
    wf_difstrat_mul_dif_dif_w_ixp = open(filename + '_difstart_mul_dif_dif_w_ixp', 'w')
    num_total = 0
    num_not_in_ixp = 0
    num_1_dif_same_w_ixp = 0
    num_1_dif_dif_w_ixp = 0    
    num_mul_dif_fst_same_w_ixp = 0
    num_mul_dif_all_same_w_ixp = 0
    num_mul_dif_dif_w_ixp = 0

    curline_trace = rf.readline()
    while curline_trace:
        curline_bgp = rf.readline()
        curline_ip = rf.readline()
        elems = curline_trace.split(']')
        if len(elems) < 2:
            curline_trace = rf.readline()
            continue
        num_total += 1
        dst_key = elems[0] + ']'
        dst_prefix = dst_key.split(' ')[0][1:]
        ip_path = curline_ip.split(']')[1].strip('\n').strip(' ')
        ori_trace_path = elems[1].strip('\n').strip(' ')
        ori_trace_list = ori_trace_path.split(' ')
        bgp_list = curline_bgp.strip('\n').strip('\t').split(' ')
        dif_index_list = GetDifStartAFromB(ori_trace_list, bgp_list)
        find = False
        error = False
        tmp_wf = None
        if dst_prefix in ixp_index_dict.keys():
            for elem in ixp_index_dict[dst_prefix]: #elem: [ixp_index_str, ip_path]
                if ip_path == elem[1]:
                    find = True
                    ixp_index_list = elem[0].split(' ')
                    if len(dif_index_list) == 0:
                        print('NOTE: no difference')
                        error = True                        
                        break
                    elif len(dif_index_list) == 1:
                        if str(dif_index_list[0]) in ixp_index_list:
                            tmp_wf = wf_difstrat_1_dif_same_w_ixp
                            num_1_dif_same_w_ixp += 1
                        else:
                            tmp_wf = wf_difstrat_1_dif_dif_w_ixp
                            num_1_dif_dif_w_ixp += 1
                    else:
                        if str(dif_index_list[0]) in ixp_index_list:
                            all_match = True
                            for tmp in dif_index_list:
                                if str(tmp) not in ixp_index_list:
                                    all_match = False
                                    break
                            if all_match:
                                tmp_wf = wf_difstrat_mul_dif_all_same_w_ixp
                                num_mul_dif_all_same_w_ixp += 1
                            else:
                                tmp_wf = wf_difstrat_mul_dif_fst_same_w_ixp
                                num_mul_dif_fst_same_w_ixp += 1
                        else:
                            tmp_wf = wf_difstrat_mul_dif_dif_w_ixp
                            num_mul_dif_dif_w_ixp += 1
        if error:
            curline_trace = rf.readline()
            continue
        if not find:
            tmp_wf = wf_difstrat_not_in_ixp
            num_not_in_ixp += 1
        if tmp_wf:
            tmp_wf.write("%s" %dst_key)
            for elem in dif_index_list:
                tmp_wf.write(" %d" %elem)
            tmp_wf.write('\n')
            tmp_wf.write(curline_trace)
            tmp_wf.write(curline_bgp)
            tmp_wf.write(curline_ip)
        curline_trace = rf.readline()
          
    rf.close()
    wf_difstrat_not_in_ixp.close()
    wf_difstrat_1_dif_same_w_ixp.close()
    wf_difstrat_1_dif_dif_w_ixp.close()
    wf_difstrat_mul_dif_fst_same_w_ixp.close()
    wf_difstrat_mul_dif_all_same_w_ixp.close()
    wf_difstrat_mul_dif_dif_w_ixp.close()
    print("num_total: %d" %num_total)
    print("num_not_in_ixp: %d" %num_not_in_ixp)
    print("num_1_dif_same_w_ixp: %d" %num_1_dif_same_w_ixp)
    print("num_1_dif_dif_w_ixp: %d" %num_1_dif_dif_w_ixp)
    print("num_mul_dif_fst_same_w_ixp: %d" %num_mul_dif_fst_same_w_ixp)
    print("num_mul_dif_all_same_w_ixp: %d" %num_mul_dif_all_same_w_ixp)
    print("num_mul_dif_dif_w_ixp: %d" %num_mul_dif_dif_w_ixp)

def TmpChg():
    rf = open('5_has_ixp_ip_index', 'r')
    wf = open('cmp1', 'w')
    curline_ixp_index = rf.readline()
    while curline_ixp_index:
        discard = rf.readline()
        discard = rf.readline()
        dst_key = curline_ixp_index.split(']')[0]
        wf.write("%s]\n" %dst_key)
        curline_ixp_index = rf.readline()          
    rf.close()
    wf.close()

    rf = open('5_has_ixp_ip', 'r')
    wf = open('cmp2', 'w')
    curline_trace = rf.readline()
    while curline_trace:
        discard = rf.readline()
        dst_key = curline_trace.split(']')[0]
        wf.write("%s]\n" %dst_key)
        curline_trace = rf.readline()          
    rf.close()
    wf.close()

def AnaIxpAbPathWithAllBgp():
    rf = open('5_has_ixp_ip_ab_ab', 'r')
    wf = open('5_has_ixp_ip_ab_ab_observe', 'w')

    curline_trace = rf.readline()
    while curline_trace:
        discard = rf.readline()
        curline_ip = rf.readline()
        trace_list = curline_trace.split('<')
        ip_list = curline_ip.split('<')
        for i in range(1, len(ip_list)):
            seg = ip_list[i]
            ip = seg[:seg.index('>')]
            as_str = GetAsStrOfIpByRv(ip)
            asn = GetAsOfIpByMi(ip, True)
            if asn not in as_str.split('_'):
                as_str += '_' + asn
            as_str = as_str.strip('_')
            trace_list[i] = as_str + trace_list[i]    
        wf.write("%s" %('<'.join(trace_list)))        
        dst_as = curline_trace.split(']')[0].split(' ')[1]
        bgp_path_list = DebugGetBgpRoute(dst_as)
        for bgp_path in bgp_path_list:
            wf.write("\t\t\t\t\t\t%s\n" %bgp_path)
        #if trace_list[1].__contains__(' '):
            #next_as = trace_list[1][trace_list[1].index('>') + 2:trace_list[1].index(' ')]
            #bgp_path_list = DebugGetBgpRoute(next_as)
            #for bgp_path in bgp_path_list:
                #wf.write("\t\t\t\t\t\t%s\n" %bgp_path)
        wf.write("%s" %curline_ip)
        curline_trace = rf.readline()
    
    rf.close()
    wf.close()

def FilterAb_2_NotUse(filename):
    rf = open(filename, 'r')
    wf = open(filename + '_filter2', 'w')
    curline_trace = rf.readline()

    while curline_trace:
        curline_bgp = rf.readline()
        curline_ip = rf.readline()
        dst_as = curline_trace.split(']')[0].split(' ')[1]
        ori_trace = curline_trace.split(']')[1].strip('\n').strip(' ')
        ori_trace_list = ori_trace.split(' ')
        ori_as = ori_trace_list[0]
        bgp_list = curline_bgp.strip('\t').strip('\n').split(' ')
        dif_index_seg_list = []
        dif_index_seg = []
        for i in range(0, len(ori_trace_list)):
            cur_as = ori_trace_list[i]
            if AsnInBgpPathList(cur_as, bgp_list):
                if dif_index_seg:
                    dif_index_seg_list.append(dif_index_seg)
                    dif_index_seg = []
            else:
                dif_index_seg.append(i)
        can_filter = True
        for cur_dif_index_seg in dif_index_seg_list:
            cur_filter = False
            if len(cur_dif_index_seg) == 1: #只有一个hop不对，可能是mapping error，忽略掉
                cur_filter = True
            else:
                fst_dif_index = cur_dif_index_seg[0]
                start_as = ori_trace_list[fst_dif_index - 1]
                if start_as != ori_as:
                    new_bgp_path_list = DebugGetBgpRoute(start_as)
                    if new_bgp_path_list:                    
                        for new_bgp_path in new_bgp_path_list:
                            temp_flag = True
                            for elem in cur_dif_index_seg:
                                if not AsnInBgpPathList(elem, new_bgp_path):
                                    temp_flag = False
                                    break
                            if temp_flag:
                                cur_filter = True
            if not cur_filter:
                can_filter = False
                break
        if not can_filter:
            wf.write(curline_trace)
            wf.write(curline_bgp)
            wf.write(curline_ip)
        curline_trace = rf.readline()
    rf.close()
    wf.close()
    
#为了和ana_prefix_traceroute_group_by_prefix_v2.py中的GetSimilarAs()区分
#在/24中，如果有和意想中的AS匹配的，就认为是可以匹配到新的AS。具体的方法可以再定
def GetSimilarAs_2(ip, new_as_set):
    #if GetAsOfIpByMi(ip) == asn:
        #print("NOTICE in GetSimilarAs: ip %s, asn %s" %(ip, asn))
        #return asn
    
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
        return None
    temp_list = content[start_index + 1:].split(',')
    for info in temp_list:
        elems = info.split(' ')
        if len(elems) < 2:
            continue
        tmp_mid_ip = elems[0]
        if mid == tmp_mid_ip[0:tmp_mid_ip.rfind('.')]:
            tmp_asn = GetAsOfRouterByMi(elems[1])
            if tmp_asn in new_as_set:
                return tmp_asn
        else:
            break
    return None

def FilterOnlyOneAbAndSimilarHop(filename, write_filename):
    rf = open(filename, 'r')
    wf_can_remap = open(filename + '_oneab_can_remap', 'w')
    #wf_cannot_remap = open(filename + '_oneab_cannot_remap', 'w')
    #wf_mulabs = open(filename + '_mulabs', 'w')
    wf_rem = open(write_filename, 'w')

    curline_trace = rf.readline()
    while curline_trace:
        curline_bgp = rf.readline()
        curline_ip = rf.readline()
        ori_trace_list = curline_trace.split(']')[1].strip('\n').strip(' ').split(' ')
        bgp_list = curline_bgp.strip('\t').strip('\n').split(' ')
        ip_list = curline_ip.split(']')[1].strip('\n').strip(' ').split(' ')
        ab_index_list = []
        filter = False
        for i in range(0, len(ori_trace_list)):
            if ori_trace_list[i] != '*' and ori_trace_list[i] != '?' and not ori_trace_list[i].startswith('<') and not AsnInBgpPathList(ori_trace_list[i], bgp_list):
                ab_index_list.append(i)
        if len(ab_index_list) == 1:
            index = ab_index_list[0]
            bgp_as_set = set()
            pre_index = index - 1
            while pre_index >= 0 and (ori_trace_list[pre_index] == '*' or ori_trace_list[pre_index] == '?'):
                pre_index -= 1
            pre_as = ori_trace_list[pre_index]
            bgp_as_set.add(pre_as)
            bgp_index = len(bgp_list)
            for cur_as in pre_as.split('_'):
                if cur_as in bgp_list:
                    bgp_index = bgp_list.index(cur_as)
                    break
            if bgp_index < len(bgp_list) - 1:
                bgp_as_set.add(bgp_list[bgp_index + 1])
            new_as = GetSimilarAs_2(ip_list[index].strip('<').strip('>'), bgp_as_set)
            if new_as:
                wf_can_remap.write(curline_trace)
                wf_can_remap.write(curline_bgp)
                wf_can_remap.write(curline_ip)
                wf_can_remap.write("%d %s %s <%s>\n" %(index, ip_list[index], ori_trace_list[index], new_as))
                filter = True
        if not filter:
            wf_rem.write(curline_trace)
            wf_rem.write(curline_bgp)
            wf_rem.write(curline_ip)
        curline_trace = rf.readline()
    rf.close()
    wf_can_remap.close()
    wf_rem.close()

bgp_dict_group = dict()
def GetBgpDictGroup(as_list): #从原始格式中提取数据
    global bgp_dict_group
    for asn in as_list:
        cur_bgp_dict = dict()
        rf = open('bgp_' + asn, 'r')
        curline = rf.readline()
        while curline:
            as_path = curline.split('|')[2]
            if as_path:
                dst_as = as_path.split(' ')[-1]
                if dst_as not in cur_bgp_dict.keys():
                    cur_bgp_dict[dst_as] = []
                compress_as_path = CompressAsPath(as_path)
                if compress_as_path not in cur_bgp_dict[dst_as]:
                    cur_bgp_dict[dst_as].append(compress_as_path)
            curline = rf.readline()
        rf.close()
        bgp_dict_group[asn] = cur_bgp_dict

def ClearBgpDictGroup():
    global bgp_dict_group
    for (key, item) in bgp_dict_group.items():
        item.clear()
    bgp_dict_group.clear()

def FilterMightLegalTrace(filename, write_filename):
    rf = open(filename, 'r')
    wf = open(write_filename, 'w')
    curline_trace = rf.readline()

    while curline_trace:
        curline_bgp = rf.readline()
        curline_ip = rf.readline()        
        ori_trace_path = curline_trace.split(']')[1].strip('\n').strip(' ')
        dst_as = curline_trace.split(']')[0].split(' ')[1]
        trace_path = CompressAsPathToMin(CompressAsPath(ori_trace_path))
        bgp_path = CompressAsPathToMin(curline_bgp.strip('\n').strip('\t'))
        if trace_path == bgp_path:
            print('NOTE: trace_path == bgp_path')
            return
        trace_list = trace_path.split(' ')
        bgp_list = bgp_path.split(' ')
        if TracePathListEqualsBgpPathList(trace_list, bgp_list):
            print('NOTE: TracePathListEqualsBgpPathList() returns True')
            return
        dif_list = GetDiffList(trace_list, bgp_list)
        normal = True
        for dif_seg in dif_list:
            (dif_trace_range, dif_bgp) = dif_seg
            if not dif_trace_range: #bgp多出来几跳，认为正常
                continue
            find = False
            dif_trace = [x for x in dif_trace_range]
            left_index = dif_trace[0] - 1
            right_index = dif_trace[-1] + 1
            tmp_src_as = trace_list[left_index]
            tmp_dst_as = ''
            if right_index == len(trace_list):
                tmp_dst_as = dst_as
            else:
                tmp_dst_as = trace_list[right_index]
            for cur_src_as in tmp_src_as.split('_'):
                if cur_src_as not in bgp_dict_group.keys():
                    continue
                for cur_dst_as in tmp_dst_as.split('_'):
                    for bgp_path in bgp_dict_group[cur_src_as][cur_dst_as]:
                        (find, not_use) = TracePathIsNormal(' '.join(trace_list[left_index:right_index]), bgp_path, '', False)
                        if find:
                            break
                    if find:
                        break
                if find:
                    break
            if not find:
                normal = False
                break
        if not normal:
            wf.write(curline_trace)     
            wf.write(curline_bgp)
            wf.write(curline_ip)
        curline_trace = rf.readline()
    rf.close()
    wf.close()

#这里，把bgp_path重新选择一遍，选使前面尽可能多trace hop合法的bgp_path
def CheckStartAb(filename, start_ab_from_begin_filename, start_ab_not_from_begin_filename, case_name, record_file_name, open_mode):
    rf = open(filename, 'r')
    wf_start_ab_from_begin = open(start_ab_from_begin_filename, 'w')
    count_start_ab_from_begin = 0
    wf_start_ab_not_from_begin = None
    count_start_ab_not_from_begin = 0
    if start_ab_not_from_begin_filename: #对于未到达dst_as的trace，不分析interc的情况
        wf_start_ab_not_from_begin = open(start_ab_not_from_begin_filename, 'w')
    curline_trace = rf.readline()
    count_total = 0

    while curline_trace:
        curline_bgp = rf.readline()
        curline_ip = rf.readline()   
        elems = curline_trace.split(']')  
        ori_trace_path = elems[1].strip('\n').strip(' ')
        dst_key = elems[0][1:]
        dst_prefix = dst_key.split(' ')[0]
        trace_path = CompressAsPathToMin(CompressAsPath(ori_trace_path))
        trace_list = trace_path.split(' ')
        bgp_path_list = GetBgpPathFromBgpPrefixDict_2(dst_prefix)
        if not bgp_path_list:
            print("NOTICE: CheckStartAb() no bgp route for prefix: %s" %dst_prefix)
            print(curline_ip)
            curline_trace = rf.readline()
            continue
        count_total += 1
        #选一个合适的bgp_path
        max_ab_index = 0
        best_bgp_path = ''
        for bgp_path in bgp_path_list:
            bgp_list = bgp_path.split(' ')
            for i in range(0, len(trace_list)):
                if not AsnInBgpPathList(trace_list[i], bgp_list):
                    if i > max_ab_index:
                        max_ab_index = i
                        best_bgp_path = bgp_path
                    break
        bgp_list = best_bgp_path.split(' ')
        src_as = bgp_list[0]
        for i in range(0, len(trace_list)):
            if not AsnInBgpPathList(trace_list[i], bgp_list):
                if AsIsEqual(trace_list[i - 1], src_as): #从src_as的下一步就开始不一样
                    wf_start_ab_from_begin.write(curline_trace)
                    wf_start_ab_from_begin.write("\t%s\n" %best_bgp_path)
                    wf_start_ab_from_begin.write(curline_ip)
                    count_start_ab_from_begin += 1
                elif wf_start_ab_not_from_begin:
                    wf_start_ab_not_from_begin.write(curline_trace)
                    wf_start_ab_not_from_begin.write("\t%s\n" %best_bgp_path)
                    wf_start_ab_not_from_begin.write(curline_ip)
                    count_start_ab_not_from_begin += 1
                break
        curline_trace = rf.readline()
    
    rf.close()
    wf_start_ab_from_begin.close()
    if wf_start_ab_not_from_begin:
        wf_start_ab_not_from_begin.close()
    wf_record = open(record_file_name, open_mode)
    wf_record.write("In %s, start_ab_from_begin: %d, percent: %.2f\n" %(case_name, count_start_ab_from_begin, count_start_ab_from_begin / count_total))
    print("In %s, start_ab_from_begin: %d, percent: %.2f" %(case_name, count_start_ab_from_begin, count_start_ab_from_begin / count_total))
    wf_record.write("In %s, start_ab_not_from_begin: %d, percent: %.2f\n" %(case_name, count_start_ab_not_from_begin, count_start_ab_not_from_begin / count_total))
    wf_record.close()

def AnaSpecInterc(filename, prev_ab_dict):
    rf = open(filename, 'r')
    curline_trace = rf.readline()

    while curline_trace:
        curline_bgp = rf.readline()
        curline_ip = rf.readline()     
        ori_trace_path = curline_trace.split(']')[1].strip('\n').strip(' ')
        compress_trace_path_1 = re.sub('[\*\?]', '', ori_trace_path)
        compress_trace_path_2 = re.sub('\<.*?\>', '', compress_trace_path_1)
        compress_trace_path = re.sub('  *', ' ', compress_trace_path_2)
        bgp_path = CompressAsPathToMin(curline_bgp.strip('\n').strip('\t'))
        trace_list = compress_trace_path.split(' ')
        bgp_list = bgp_path.split(' ')
        ori_as = trace_list[0]
        if not AsnInTracePathList(bgp_list[-1], trace_list): #异常，此函数只分析dst_as到达的情况
            print("NOTICE: AnaSpecInterc() dst_as not in bgp_path")
            curline_trace = rf.readline()
            continue
        prev_as = ''
        prev_index = 0
        for i in range(0, len(trace_list)):
            if not AsnInBgpPathList(trace_list[i], bgp_list):
                if not prev_as: #一段不一致路径的开始
                    prev_hop = trace_list[i - 1]
                    if AsIsEqual(prev_hop, ori_as): #一开始就不一致，不管这种情况
                        print("NOTICE: AnaSpecInterc() start ab from begin")
                        break
                    last_index = 0
                    for elem in prev_hop.split('_'):
                        if elem in bgp_list:
                            index = bgp_list.index(elem)
                            if index > last_index:
                                last_index = index
                    prev_as = bgp_list[last_index]
                    prev_index = i - 1
                else: #一段不一致路径的中间
                    pass
            else:
                if prev_as: #一段不一致路径的结束
                    if prev_as not in prev_ab_dict.keys():
                        prev_ab_dict[prev_as] = [0, GetAsRankFromDict(prev_as), []]
                    end_index = FindTraceAsInBgpPath(trace_list[i], bgp_list)
                    trace_seg = trace_list[prev_index:i]
                    trace_seg[0] = bgp_list[bgp_list.index(prev_as)] #如果第一跳是moas，将其改为bgp_list中确定的as
                    trace_seg.append(bgp_list[end_index])
                    path_info = [CompressAsPathToMin(CompressAsPath(' '.join(trace_seg))), ' '.join(bgp_list[bgp_list.index(prev_as):(end_index+1)])]
                    prev_ab_dict[prev_as][0] += 1
                    if path_info not in prev_ab_dict[prev_as][2]:
                        prev_ab_dict[prev_as][2].append(path_info)
                    prev_as = ''
                else: #一段一致路径
                    pass
        curline_trace = rf.readline()
    
    rf.close()
    return prev_ab_dict

def ClassifyAbTrace(filename, reach_dst_last_extra_filename, reach_dst_mid_ab_filename, reach_dst_mid_ab_and_last_extra_filename, reach_dst_uncertain_filename, not_reach_dst_last_ab_filename, not_reach_dst_other_filename, record_filename, open_mode):
    rf = open(filename, 'r')
    wf_reach_dst_last_extra = open(reach_dst_last_extra_filename, 'w')
    count_reach_dst_last_extra = 0
    wf_reach_dst_mid_ab = open(reach_dst_mid_ab_filename, 'w')
    count_reach_dst_mid_ab = 0
    wf_reach_dst_mid_ab_and_last_extra = open(reach_dst_mid_ab_and_last_extra_filename, 'w')
    count_reach_dst_mid_ab_and_last_extra = 0
    wf_reach_dst_uncertain = open(reach_dst_uncertain_filename, 'w')
    count_reach_dst_uncertain = 0
    wf_not_reach_dst_last_ab = open(not_reach_dst_last_ab_filename, 'w')
    count_not_reach_dst_last_ab = 0
    wf_not_reach_dst_other = open(not_reach_dst_other_filename, 'w')
    count_not_reach_dst_other = 0
    curline_trace = rf.readline()
    count_total = 0

    while curline_trace:
        curline_bgp = rf.readline()
        curline_ip = rf.readline()   
        #if curline_trace.__contains__('[103.90.76.0/24 136493] 3257 3257 174 174 6939\n'):
            #print("")
        ori_trace_path = curline_trace.split(']')[1].strip('\n').strip(' ')
        trace_path = CompressAsPathToMin(CompressAsPath(ori_trace_path))
        bgp_path = CompressAsPathToMin(curline_bgp.strip('\n').strip('\t'))
        trace_list = trace_path.split(' ')
        bgp_list = bgp_path.split(' ')
        if len(bgp_list) == 1:
            curline_trace = rf.readline()
            continue
        count_total += 1
        dst_as_index = FindBgpAsInTracePath(bgp_list[-1], trace_list)
        if dst_as_index != -1: #dst_as在trace_list中
            if FstPathContainedInSnd(' '.join(trace_list[:dst_as_index]), bgp_path): #dst_as前的trace hop全正常
                if not AsIsEqual(trace_list[-1], bgp_list[-1]): #末尾跳和dst_as不同
                #1.结尾多出几跳
                    wf_reach_dst_last_extra.write(curline_trace)
                    wf_reach_dst_last_extra.write(curline_bgp)
                    wf_reach_dst_last_extra.write(curline_ip)
                    count_reach_dst_last_extra += 1
                else: #末尾跳也正常，异常情况，多是由于last hop匹配到多个AS，其中有的和上一跳一致，有的和bgp最后一跳（但是是trace的前几跳）一致，此时trace中倒数几跳（除了最后一跳）是abnormal的
                    wf_reach_dst_uncertain.write(curline_trace)
                    wf_reach_dst_uncertain.write(curline_bgp)
                    wf_reach_dst_uncertain.write(curline_ip)
                    count_reach_dst_uncertain += 1
            else: #dst_as前有些trace hop不正常，这里由interc的可能，但是先全部记下来，下一步再过滤疑似interc的情况
                if not AsIsEqual(trace_list[-1], bgp_list[-1]): #末尾跳和dst_as不同
                #2.中间不一致，结尾又多出几跳
                    wf_reach_dst_mid_ab_and_last_extra.write(curline_trace)
                    wf_reach_dst_mid_ab_and_last_extra.write(curline_bgp)
                    wf_reach_dst_mid_ab_and_last_extra.write(curline_ip)
                    count_reach_dst_mid_ab_and_last_extra += 1
                else: #末尾跳正常
                #3.只有中间不一致
                    wf_reach_dst_mid_ab.write(curline_trace)
                    wf_reach_dst_mid_ab.write(curline_bgp)
                    wf_reach_dst_mid_ab.write(curline_ip) 
                    count_reach_dst_mid_ab += 1                   
        else: #dst_as不在trace_list中
            if FstPathContainedInSnd(' '.join(trace_list[0:-1]), ' '.join(bgp_list[0:-1])): #除了最后一跳其它trace hop都正常
                if not AsIsEqual(trace_list[-1], bgp_list[-1]):
                #4. 最后一跳不同
                    wf_not_reach_dst_last_ab.write(curline_trace)
                    wf_not_reach_dst_last_ab.write(curline_bgp)
                    wf_not_reach_dst_last_ab.write(curline_ip)
                    count_not_reach_dst_last_ab += 1
                else: #异常
                    print("NOTICE: ClassifyAbTrace() trace_path normal 2")
                    print(trace_path)
                    print(bgp_path)
            else:
                #5. 其它异常，统共放入一类
                wf_not_reach_dst_other.write(curline_trace)
                wf_not_reach_dst_other.write(curline_bgp)
                wf_not_reach_dst_other.write(curline_ip)
                count_not_reach_dst_other += 1           
        curline_trace = rf.readline()
        continue

    rf.close()
    wf_reach_dst_last_extra.close()
    wf_reach_dst_mid_ab_and_last_extra.close()
    wf_reach_dst_mid_ab.close()
    wf_not_reach_dst_last_ab.close()
    wf_not_reach_dst_other.close()

    wf = open(record_filename, open_mode)
    wf.write("Total num: %d\n" %count_total)
    wf.write("reach_dst_last_extra num: %d, percent: %.2f\n" %(count_reach_dst_last_extra, count_reach_dst_last_extra / count_total))
    wf.write("reach_dst_mid_ab_and_last_extra num: %d, percent: %.2f\n" %(count_reach_dst_mid_ab_and_last_extra, count_reach_dst_mid_ab_and_last_extra / count_total))
    wf.write("reach_dst_mid_ab num: %d, percent: %.2f\n" %(count_reach_dst_mid_ab, count_reach_dst_mid_ab / count_total))
    wf.write("not_reach_dst_last_ab num: %d, percent: %.2f\n" %(count_not_reach_dst_last_ab, count_not_reach_dst_last_ab / count_total))
    wf.write("not_reach_dst_other num: %d, percent: %.2f\n" %(count_not_reach_dst_other, count_not_reach_dst_other / count_total))
    wf.close()

def ClassifyAbTrace2(filename, last_extra_filename, first_hop_ab_filename, detour_filename, bifurc_filename, record_file_name, open_mode, detour_dict):
    rf = open(filename, 'r')
    wf_last_extra = open(last_extra_filename, 'w')
    count_last_extra = 0
    wf_first_hop_ab = 0
    if first_hop_ab_filename:
        wf_first_hop_ab = open(first_hop_ab_filename, 'w')
    count_first_hop_ab = 0
    wf_detour = open(detour_filename, 'w')
    count_detour = 0
    wf_bifurc = open(bifurc_filename, 'w')
    count_bifurc = 0
    count_total = 0
    curline_trace = rf.readline()

    while curline_trace:
        curline_bgp = rf.readline()
        curline_ip = rf.readline()   
        ori_trace_path = curline_trace.split(']')[1].strip('\n').strip(' ')
        trace_path = CompressAsPathToMin(CompressAsPath(ori_trace_path))
        bgp_path = CompressAsPathToMin(curline_bgp.strip('\n').strip('\t').strip(' '))
        trace_list = DropStarsInTraceList(trace_path.split(' '))
        bgp_list = bgp_path.split(' ')
        if len(bgp_list) == 1:
            curline_trace = rf.readline()
            continue
        count_total += 1
        #1.先检查是否是first_hop_ab
        start_as = bgp_list[0]
        next_as = bgp_list[1]
        is_first_hop_ab = False
        for i in range(0, len(trace_list)):
            cur_hop = trace_list[i]
            if start_as not in cur_hop.split('_'): #出源AS
                if next_as not in cur_hop.split('_'): #first_hop_ab
                    wf_first_hop_ab.write("%s%s%s" %(curline_trace, curline_bgp, curline_ip))
                    count_first_hop_ab += 1
                    is_first_hop_ab = True
                break #停止分析
        # if is_first_hop_ab: #2021.9.27 first_hop_ab和其它的分类不在同一个维度上，所以它独立统计，不影响其它情况的统计
        #     curline_trace = rf.readline()
        #     continue
        #2.检查是否是reach_dst_last_extra
        dst_as = bgp_list[-1]
        is_last_extra = False
        if AsnInTracePathList(dst_as, trace_list):
            for i in range(0, len(trace_list)):
                cur_hop = trace_list[i]
                if dst_as in cur_hop.split('_'): #到达dst_as，后面的不一致
                    wf_last_extra.write("%s%s%s" %(curline_trace, curline_bgp, curline_ip))
                    count_last_extra += 1
                    is_last_extra = True
                    break
                elif not AsnInBgpPathList(cur_hop, bgp_list): #没到dst_as的时候就发生了不一致，转下一步分析
                    break
        if is_last_extra:
            curline_trace = rf.readline()
            continue
        #3.分析是绕路还是分叉
        begin_ab = False
        prev_bgp_index = 0
        prev_trace_index = 0
        detour = False
        for i in range(0, len(trace_list)):
            cur_hop = trace_list[i]
            possi_index = FindTraceAsInBgpPath(cur_hop, bgp_list)
            if possi_index != -1: #cur_hop合法
                if begin_ab: #前面有不一致，这里又一致了，说明绕路
                    if not detour: #加上这个判断条件是为了防止一条路上有多段绕路
                        wf_detour.write("%s%s%s" %(curline_trace, curline_bgp, curline_ip))
                        count_detour += 1
                        detour = True
                    #修正possi_index
                    #if prev_bgp_index:
                    possi_index = FindTraceAsInBgpPath(cur_hop, bgp_list[prev_bgp_index + 1:])
                    if possi_index == -1: #有loop，这里也记录下来
                        pair = bgp_list[prev_bgp_index] + ' ' + bgp_list[prev_bgp_index]
                        bgp_seg = bgp_list[prev_bgp_index]
                    else:
                        possi_index += prev_bgp_index + 1
                        pair = bgp_list[prev_bgp_index] + ' ' + bgp_list[possi_index]
                        bgp_seg = ' '.join(bgp_list[prev_bgp_index:(possi_index + 1)])
                    if pair not in detour_dict.keys():
                        detour_dict[pair] = [dict(), 0]
                    if bgp_seg not in detour_dict[pair][0].keys():
                        detour_dict[pair][0][bgp_seg] = [dict(), 0]
                    #trace_seg = ' '.join(trace_list[prev_trace_index:(i + 1)])
                    prev_trace_hop = trace_list[prev_trace_index]
                    trace_seg = ''
                    find_cur_hop = ' ' + cur_hop
                    if i < len(trace_list) - 1:
                        find_cur_hop += ' '
                    find_prev_hop = prev_trace_hop + ' '
                    if prev_trace_index > 0:
                        find_prev_hop = ' ' + find_prev_hop
                    trace_seg = ori_trace_path[ori_trace_path.rindex(find_prev_hop):(ori_trace_path.index(find_cur_hop) + len(cur_hop) + 1)]
                    if not trace_seg:
                        print('trace_seg none. ori_trace:')
                        print(ori_trace_path)
                        #print(prev_trace_hop)
                    if trace_seg not in detour_dict[pair][0][bgp_seg][0].keys():
                        detour_dict[pair][0][bgp_seg][0][trace_seg] = 0
                    detour_dict[pair][0][bgp_seg][0][trace_seg] += 1
                    detour_dict[pair][0][bgp_seg][1] += 1
                    detour_dict[pair][1] += 1
                prev_bgp_index = possi_index
                prev_trace_index = i
                begin_ab = False
            else:
                begin_ab = True
        if not detour: #分叉路
            wf_bifurc.write("%s%s%s" %(curline_trace, curline_bgp, curline_ip))
            count_bifurc += 1
        curline_trace = rf.readline()
        continue

    rf.close()
    wf_last_extra.close()
    if wf_first_hop_ab:
        wf_first_hop_ab.close()
    wf_detour.close()
    wf_bifurc.close()
    if record_file_name:
        wf = open(record_file_name, open_mode)
        wf.write("Total num: %d\n" %count_total)
        wf.write("last_extra num: %d, percent: %.2f\n" %(count_last_extra, count_last_extra / count_total))
        wf.write("first_hop_ab num: %d, percent: %.2f\n" %(count_first_hop_ab, count_first_hop_ab / count_total))
        wf.write("detour num: %d, percent: %.2f\n" %(count_detour, count_detour / count_total))
        wf.write("bifurc num: %d, percent: %.2f\n" %(count_bifurc, count_bifurc / count_total))
        wf.close()
    print("last_extra num: %d, percent: %.2f\n" %(count_last_extra, count_last_extra / count_total))
    if wf_first_hop_ab:
        print("first_hop_ab num: %d, percent: %.2f\n" %(count_first_hop_ab, count_first_hop_ab / count_total))
    print("detour num: %d, percent: %.2f\n" %(count_detour, count_detour / count_total))
    print("bifurc num: %d, percent: %.2f\n" %(count_bifurc, count_bifurc / count_total))   
    return detour_dict

def GetOtherLoopTraces(filename, writefilename, record_file_name):
    rf = open(filename, 'r')
    wf = open(writefilename, 'w')
    wf_loop = open(filename + '_loop', 'w')
    count_not_loop = 0
    count_total = 0
    curline_trace = rf.readline()

    while curline_trace:
        count_total += 1
        curline_bgp = rf.readline()
        curline_ip = rf.readline()
        ori_trace_path = curline_trace.split(']')[1].strip('\n').strip(' ')
        trace_path = CompressAsPathToMin(CompressAsPath(ori_trace_path))
        trace_list = DropStarsInTraceList(trace_path.split(' '))
        #print(trace_path)
        bgp_set = set(CompressAsPathToMin(curline_bgp.strip('\n').strip('\t')).split(' '))
        loop = False
        existed_asn_set_list = []
        prev_asn_set = set()
        for elem in trace_list:
            cur_set = set(elem.split('_'))
            #print(cur_set)
            #print(prev_asn_set)
            #print(existed_asn_set_list)
            #print('')
            match_set = bgp_set & cur_set #2021.6.24 如果match_set不为空，用这个去做比较，而不是cur_set
            if not match_set:
                match_set = cur_set
            #if prev_asn_set and (not FstAsnIsInSndAsn(elem, '_'.join(list(prev_asn_set)))):
            #print(prev_asn_set)
            #print(match_set)
            #print(existed_asn_set_list)
            if not (prev_asn_set & match_set):
                for existed_set in existed_asn_set_list:                    
                    #join_set = match_set & existed_set
                    #if join_set and (join_set & bgp_set): #loop                        
                    if existed_set & match_set: #原来写的是相等，如果一个hop匹配了多个bgp path中的hop，只求相等可能还是会漏掉一些loop
                        loop = True
                        wf_loop.write(curline_trace)
                        wf_loop.write(curline_bgp)
                        wf_loop.write(curline_ip)
                        break
            if not loop:
                if prev_asn_set:
                    existed_asn_set_list.append(prev_asn_set)
                prev_asn_set = copy.deepcopy(match_set)
            else:
                break
        if not loop:
            wf.write(curline_trace)
            wf.write(curline_bgp)
            wf.write(curline_ip)
            count_not_loop += 1
        curline_trace = rf.readline()
    rf.close()
    wf.close()
    wf_loop.close()
    with open(record_file_name, 'w') as f:
        f.write("In loop check, total num: %d, not loop: %d, percent: %.2f\n" %(count_total, count_not_loop, count_not_loop / count_total))
        print("In loop check, total num: %d, not loop: %d, percent: %.2f" %(count_total, count_not_loop, count_not_loop / count_total))

def AnaStartAb(filename, start_ab_as_freq_dict):
    rf = open(filename, 'r')
    curline_trace = rf.readline()

    while curline_trace:
        curline_bgp = rf.readline()
        curline_ip = rf.readline()
        if not curline_trace.__contains__(']'):
            print("NOTICE: AnaStartAb() curline_trace error: %s" %curline_trace)
            curline_trace = rf.readline()
            continue
        ori_trace_path = curline_trace.split(']')[1].strip('\n').strip(' ')
        trace_path = CompressAsPathToMin(CompressAsPath(ori_trace_path))
        bgp_path = CompressAsPathToMin(curline_bgp.strip('\n').strip('\t'))
        trace_list = trace_path.split(' ')
        bgp_list = bgp_path.split(' ')
        for elem in trace_list:
            if not AsnInBgpPathList(elem, bgp_list):
                for asn in trace_list[1].split('_'):
                    if AsnIsVpNeighbor(asn):
                        if asn not in start_ab_as_freq_dict.keys():
                            start_ab_as_freq_dict[asn] = [0, True]
                        start_ab_as_freq_dict[asn][0] += 1
                    else:
                        if asn not in start_ab_as_freq_dict.keys():
                            start_ab_as_freq_dict[asn] = [0, False]
                        start_ab_as_freq_dict[asn][0] += 1
                break
        curline_trace = rf.readline()
    rf.close()
    return start_ab_as_freq_dict

def AnaExtraAs(filename, filename_extra_as_ana):
    rf = open(filename, 'r')
    wf_extra_as_ana = open(filename_extra_as_ana, 'w')
    curline_trace = rf.readline()
    as_freq_dict = dict()

    while curline_trace:
        curline_bgp = rf.readline()
        curline_ip = rf.readline()   
        ori_trace_path = curline_trace.split(']')[1].strip('\n').strip(' ')
        trace_path = CompressAsPathToMin(CompressAsPath(ori_trace_path))
        bgp_path = CompressAsPathToMin(curline_bgp.strip('\n').strip('\t'))
        trace_list = trace_path.split(' ')
        bgp_list = bgp_path.split(' ')
        for i in range(0, len(trace_list)):
            cur_as = trace_list[i]
            if not AsnInBgpPathList(cur_as, bgp_list):
                for asn in cur_as.split('_'):
                    as_rank = GetAsRankFromDict(asn)
                    if asn not in as_freq_dict.keys():
                        as_freq_dict[asn] = [0, as_rank, dict()]
                    as_freq_dict[asn][0] += 1
                    prev_as_rank = GetAsRankStrFromDict(trace_list[i - 1])
                    next_as_rank = ''
                    if i < len(trace_list) - 1:
                        next_as_rank = GetAsRankStrFromDict(trace_list[i + 1])
                    if prev_as_rank == as_rank or next_as_rank == as_rank:
                        print('')
                    rank_rel = prev_as_rank + ',' + as_rank + ',' + next_as_rank
                    if rank_rel not in as_freq_dict[asn][2].keys():
                        as_freq_dict[asn][2][rank_rel] = 0
                    as_freq_dict[asn][2][rank_rel] += 1
        curline_trace = rf.readline()

    sort_list = sorted(as_freq_dict.items(), key=lambda d:d[1][0], reverse = True)
    for elem in sort_list:
        wf_extra_as_ana.write("%s %d %s\n" %(elem[0], elem[1][0], elem[1][1]))
        sub_sort_list = sorted(elem[1][2].items(), key=lambda d:d[1], reverse = True)
        for sub_elem in sub_sort_list:
            wf_extra_as_ana.write("\t<%s> %d\n" %(sub_elem[0], sub_elem[1]))
    
    rf.close()
    wf_extra_as_ana.close()

def FilterOnlyOneAbRelWithNeigh(filename, w_filename):
    rf = open(filename, 'r')
    wf = open(w_filename, 'w')
    curline_trace = rf.readline()

    while curline_trace:
        curline_bgp = rf.readline()
        curline_ip = rf.readline()   
        #if curline_trace.__contains__('[194.158.192.0/19 6697]'):
            #print('')
        ori_trace_path = curline_trace.split(']')[1].strip('\n').strip(' ')
        trace_path = CompressAsPathToMin(CompressAsPath(ori_trace_path))
        bgp_path = CompressAsPathToMin(curline_bgp.strip('\n').strip('\t'))
        ori_trace_list = ori_trace_path.split(' ')
        trace_list = trace_path.split(' ')
        bgp_list = bgp_path.split(' ')
        dst_as = bgp_list[-1]
        dif_list = GetDiffList(trace_list, bgp_list)
        normal = True
        for dif_seg in dif_list:
            (dif_trace_range, dif_bgp) = dif_seg
            if not dif_trace_range: #bgp多出来几跳，认为正常
                continue
            dif_trace = [x for x in dif_trace_range]
            if len(dif_trace) > 1:
                normal = False
                break
            (sel_asn, max_count) = CountAsnInTracePathList(trace_list[dif_trace[0]], ori_trace_list)
            if max_count > 1: #在原始trace中也只能出现一次，否则不认为正常
                normal = False #如果不要这个限制条件，还可以过滤一半的trace为正常
                break         
            left_index = dif_trace[0] - 1
            right_index = dif_trace[0] + 1
            left_as = trace_list[left_index]
            right_as = ''
            if right_index == len(trace_list):
                right_as = dst_as
            else:
                right_as = trace_list[right_index]
            normal = False
            cur_as = trace_list[dif_trace[0]]
            if cur_as.__contains__('20764') and left_as.__contains__('6939'):
                print('')
            if Get2AsRel_2(left_as, cur_as) != -2 or Get2AsRel_2(right_as, cur_as) != -2:
                normal = True
            else:
                for elem in dif_bgp:
                    if Get2AsRel_2(elem, cur_as) != -2:
                        normal = True
                        break
            if not normal:
                break
        if not normal:
            wf.write(curline_trace)     
            wf.write(curline_bgp)
            wf.write(curline_ip)
        curline_trace = rf.readline()
        
    rf.close()
    wf.close()

def StatisSuspIntercAna(filename, ana_dict):
    rf = open(filename, 'r')
    curline = rf.readline()
    cur_as = ''

    while curline:
        if curline.__contains__('<'):
            if cur_as not in ana_dict.keys():
                print("NOTICE: StatisSuspIntercAna() cur_as not found in ana_dict")
                return None
            trace_match = re.findall('<(.*?)>', curline, re.DOTALL)
            trace_path = trace_match[0]
            bgp_path = trace_match[1]
            if trace_path not in ana_dict[cur_as][2].keys():
                ana_dict[cur_as][2][trace_path] = bgp_path
        else:
            elems = curline.strip('\n').split(' ') #elems: asn, freq, path_num, as_rank
            cur_as = elems[0]
            if cur_as not in ana_dict.keys():
                ana_dict[cur_as] = [0, elems[3], dict()]
            ana_dict[cur_as][0] += int(elems[1]) #freq
        curline = rf.readline()
    
    rf.close()
    return ana_dict

def FilterTraceWithSpecBgp(r_filename, w_filename):
    rf = open(r_filename, 'r')
    asn = ''
    curline = rf.readline()
    check = False
    ana_dict = dict()

    while curline:
        if not curline.__contains__('<'): #new ab as
            elems = curline.strip('\n').split(' ')
            asn = elems[0]
            freq = elems[1]
            as_rank = elems[3]
            if int(elems[2]) > 1:
                ClearBGP_1()
                if os.path.exists('bgp_' + asn):
                    GetBgp_1(asn)
                    check = True
                else:
                    check = False
            else:
                check = False
            if asn not in ana_dict.keys():
                ana_dict[asn] = [freq, dict(), as_rank]
        else:
            if curline.__contains__('<6939 52320 262761>'):
                print('')
            trace_match = re.findall('<(.*?)>', curline, re.DOTALL)
            trace_path = trace_match[0]
            bgp_path = trace_match[1]
            if check:
                dst_as = trace_path.split(' ')[-1]
                bgp_path_list = DebugGetBgpRoute(dst_as)
                if not TracePathIsNormal(trace_path, ' '.join(bgp_path_list), '', False): #还是ab trace
                    ana_dict[asn][1][trace_path] = bgp_path
            else:
                ana_dict[asn][1][trace_path] = bgp_path
        curline = rf.readline()

    wf = open(w_filename, 'w')
    sort_list = sorted(ana_dict.items(), key=lambda d:len(d[1][1]), reverse = True)
    for elem in sort_list:
        wf.write("%s %s %d %s\n" %(elem[0], elem[1][0], len(elem[1][1]), elem[1][2]))
        if len(elem[1][1]) > 1:
            print("%s %s %d %s" %(elem[0], elem[1][0], len(elem[1][1]), elem[1][2]))
        for (trace_path, bgp_path) in elem[1][1].items():
            wf.write("\t<%s> <%s>\n" %(trace_path, bgp_path))
    wf.close()

def GetFileLineNum(filename):
    return len(open(filename, 'r').readlines())

def AnaAb(filename, w_filename):
    rf = open(filename, 'r')
    wf = open(w_filename, 'w')
    curline_trace = rf.readline()
    as_dict = dict()

    while curline_trace:
        curline_bgp = rf.readline()
        curline_ip = rf.readline()   
        ori_trace_path = curline_trace.split(']')[1].strip('\n').strip(' ')
        trace_path = CompressAsPathToMin(CompressAsPath(ori_trace_path))
        prefix = curline_trace.split(' ')[0][1:]
        #candidate_ip = curline_ip.split(' ')[-1]
        (bgp_prefix, bgp_path) = GetBgpPathFromBgpPrefixDict(prefix)
        if bgp_path:
            bgp_path = CompressAsPathToMin(CompressAsPath(bgp_path))
            trace_list = trace_path.split(' ')
            bgp_list = bgp_path.split(' ')
            for elem in trace_list:
                if not AsnInBgpPathList(elem, bgp_list):
                    for asn in elem.split('_'):
                        if asn not in as_dict.keys():
                            as_dict[asn] = 0
                        as_dict[asn] += 1
                    break
        wf.write("%s" %curline_trace)
        wf.write("[%s]%s\n" %(bgp_prefix, bgp_path))
        curline_trace = rf.readline()
    rf.close()
    wf.close()

    sort_list = sorted(as_dict.items(), key=lambda d:d[1], reverse = True)
    for elem in sort_list:
        print("%s %d" %(elem[0], elem[1]))
    print("\nlen: %d" %len(sort_list))

def CheckAbPathIsPartNormal(filename, w_filename, case_name, record_file_name, open_mode):
    rf = open(filename, 'r')
    wf = open(w_filename, 'w')
    curline_trace= rf.readline()
    count_total = 0
    count_ab = 0

    while curline_trace:
        count_total += 1
        curline_bgp = rf.readline()
        curline_ip = rf.readline()
        elems = curline_trace.strip('\n').split(']')
        trace_list = CompressAsPathToMin(CompressAsPath(elems[1].strip('\n').strip(' '))).split(' ')
        bgp_list = CompressAsPath(curline_bgp.strip('\n').strip('\t')).split(' ')
        dst_as = elems[0].split(' ')[1]
        start_index = 0
        while start_index < len(trace_list) and AsnInBgpPathList(trace_list[start_index], bgp_list):
            start_index += 1
        start_index -= 1
        part_trace_list = trace_list[start_index:]
        for asn in dst_as.split('_'):
            if AsnInTracePathList(asn, trace_list):
                end_index = FindBgpAsInTracePath(asn, trace_list) + 1
                part_trace_list = trace_list[start_index:end_index] #不一致的路径段截取为：一开始不一致（的前一跳）到dst_as（当tracepath经过dst_as）或tracepath末尾（当tracepath不经过dst_as）
                break
        if not FindPathInDb(part_trace_list):
            wf.write(curline_trace)
            wf.write(curline_bgp)
            wf.write(curline_ip)
            count_ab += 1
        curline_trace = rf.readline()
    rf.close()
    wf.close()
    
    wf_record = open(record_file_name, open_mode)
    wf_record.write("In %s, still ab: %d, percent: %.2f\n" %(case_name, count_ab, count_ab / count_total))
    wf_record.close()

def ClassifyLastAb(filename):
    rf = open(filename, 'r')
    trace_line = rf.readline()
    rel_filename_list = ['unknown', 'peer', 'customer', 'provider', 'sibling']
    path_filename_list = ['normpath', 'detour']
    wf = dict()
    count = dict()
    rel_priority = dict()
    for path in path_filename_list:
        for rel in rel_filename_list:
            wf[path, rel] = open(filename + '_' + path + '_rel_' + rel, 'w')
            count[path, rel] = 0
    i = 0
    for rel in rel_filename_list:
        rel_priority[rel] = i
        i += 1

    while trace_line:
        bgp_line = rf.readline()
        ip_line = rf.readline()
        trace_elems = trace_line.strip('\n').split(' ')
        ab_as = ''
        for i in range(len(trace_elems) - 1, -1, -1):
            cur_hop = trace_elems[i]
            if cur_hop.__contains__('*') or cur_hop.__contains__('?') or cur_hop.__contains__('<'):
                continue
            ab_as = trace_elems[i]
            break        
        if ab_as.__contains__('_'): #还没想好怎么处理，先跳过
            trace_line = rf.readline()
            continue
        bgp_elems = bgp_line.strip('\n').strip('\t').split(' ')
        last_norm_as_index_in_bgp = -1
        for i in range(len(trace_elems) - 1, -1, -1):
            if trace_elems[i] in bgp_elems:
                last_norm_as_index_in_bgp = bgp_elems.index(trace_elems[i])
                break
        last_norm_as_country = GetAsCountry(bgp_elems[last_norm_as_index_in_bgp])
        rem_bgp_as_country_set = set()
        #for cur_ab_as in ab_as.split('_'):
        cur_ab_as = ab_as #为了和后面处理多个AS的兼容
        ab_as_country = GetAsCountry(cur_ab_as)
        max_rel_priority = 0
        for cur_as in bgp_elems[last_norm_as_index_in_bgp:]:
            rem_bgp_as_country_set.add(GetAsCountry(cur_as))
            rel = GetAsRelAndTranslate(cur_as, cur_ab_as)
            if rel_priority[rel] > max_rel_priority:
                max_rel_priority = rel_priority[rel]
        path = 'normpath'
        if len(rem_bgp_as_country_set) == 1:
            for elem in rem_bgp_as_country_set:
                if elem == last_norm_as_country:
                    if last_norm_as_country != ab_as_country:   #detour
                        path = 'detour'
        cur_wf = wf[path, rel_filename_list[max_rel_priority]]
        cur_wf.write(trace_line)
        cur_wf.write(bgp_line)
        cur_wf.write(ip_line)
        for cur_as in bgp_elems[last_norm_as_index_in_bgp:]:
            cur_wf.write("%s(%s, %s) %s(%s, %s): %s\n" %(cur_as, GetAsRankFromDict(cur_as), GetAsCountry(cur_as), cur_ab_as, GetAsRankFromDict(cur_ab_as), GetAsCountry(cur_ab_as), GetAsRelAndTranslate(cur_as, cur_ab_as)))
        for i in range(last_norm_as_index_in_bgp, len(bgp_elems) - 1):
            cur_wf.write("%s(%s, %s) %s(%s, %s): %s\n" %(bgp_elems[i], GetAsRankFromDict(bgp_elems[i]), GetAsCountry(bgp_elems[i]), bgp_elems[i + 1], GetAsRankFromDict(bgp_elems[i + 1]), GetAsCountry(bgp_elems[i + 1]), GetAsRelAndTranslate(bgp_elems[i], bgp_elems[i + 1])))
        cur_wf.write('\n')
        count[path, rel_filename_list[max_rel_priority]] += 1
        trace_line = rf.readline()
    rf.close()
    for path in path_filename_list:
        for rel in rel_filename_list:
            wf[path, rel].close()
            print("%s %s: %d" %(path, rel, count[path, rel]))

def StatDstAsFreq(files, w_filename):
    as_freq = dict()
    total_num = 0
    for cur_file in files:
        #print(cur_file)
        rf = open(cur_file, 'r')
        curline_trace= rf.readline()
        while curline_trace:
            curline_bgp = rf.readline()
            curline_ip = rf.readline()
            dst_as = curline_trace.split(']')[0].split(' ')[1]
            for elem in dst_as.split('_'):
                if elem not in as_freq.keys():
                    as_freq[elem] = 0
                as_freq[elem] += 1
            total_num += 1
            curline_trace= rf.readline()
        rf.close()
    sort_list = sorted(as_freq.items(), key=lambda d:d[1], reverse = True)
    wf = open(w_filename, 'w')
    wf.write("total trace num: %d\n" %total_num)
    wf.write("total asn num: %d\n" %len(sort_list))
    for (asn, freq) in sort_list:
        wf.write("%s %d\n" %(asn, freq))
    wf.close()

#检查一下情况（主要是违反无谷原则）：
#trace path: A C B
#bgp path: A B
#(1)
#如果：A是B的provider，B是C的provider，A和C无商业关系，as_rank(A) < as_rank(B) < as_rank(C)，path A C B违反无谷原则
#那么：C应该是宣告了B的地址空间，属于mapping错误
#(2)
#如果：B是A的provider，A是C的provider，B和C无商业关系，as_rank(B) < as_rank(A) < as_rank(C)，path A C B违反无谷原则
#那么：C应该是宣告了A的地址空间，属于mapping错误
def CheckAbLink(filename, replace_ip_map_dict):
    rf = open(filename, 'r')
    curline_trace= rf.readline()

    while curline_trace:
        curline_bgp = rf.readline()
        curline_ip = rf.readline()
        elems = curline_trace.strip('\n').split(']')
        ori_trace = elems[1].strip(' ')
        ori_trace_list = ori_trace.split(' ')
        ip_list = curline_ip.strip('\n').split(']')[1].strip(' ').split(' ')
        #trace_list = CompressAsPathToMin(CompressAsPath(ori_trace)).split(' ')
        bgp_path = CompressAsPath(curline_bgp.strip('\n').strip('\t'))
        bgp_list = bgp_path.split(' ')
        prev_hop = None
        prev_ab_set = None
        replace_dict = dict()
        #for i in range(0, len(trace_list)):
        for i in range(0, len(ori_trace_list)):
            #cur_hop = trace_list[i]
            cur_hop = ori_trace_list[i]
            if cur_hop.__contains__('*') or cur_hop.__contains__('?') or cur_hop.__contains__('<'):
                continue
            if AsnInBgpPathList(cur_hop, bgp_list):
                prev_hop = cur_hop
                prev_ab_set = None
                continue
            ab_as_set = set(cur_hop.split('_'))
            if prev_ab_set and (len(prev_ab_set & ab_as_set) == 0): #前一跳就异常，且前一跳和当前跳不同，不符合条件
                continue
            #cur_hop not in bgp_list
            bgp_prev_set = None
            bgp_next_set = None
            if prev_hop: #看左边
                bgp_prev_set = FindTraceAsSetInBgpPath(prev_hop, bgp_list)
            if i < (len(ori_trace_list) - 1):
                for j in range(i + 1, len(ori_trace_list)):
                    next_hop = ori_trace_list[j]
                    if next_hop.__contains__('*') or next_hop.__contains__('?') or next_hop.__contains__('<'):
                        continue
                    if AsnInBgpPathList(next_hop, bgp_list): #找到bgp_next
                        bgp_next_set = FindTraceAsSetInBgpPath(next_hop, bgp_list)
                        break
                    else: #next_hop也是abnormal hop，此时要求它和cur_hop有相同的map AS
                        ab_as_set &= set(next_hop.split('_'))
                        if not ab_as_set:
                            break #当有不同的ab hop时，bgp_next为空，意即这种情况不符合条件
            if bgp_prev_set and bgp_next_set:
                if len(bgp_prev_set) > 1 or len(bgp_next_set) > 1: #这种情况不知道怎么处理，跳过
                    pass
                else:
                    bgp_prev = list(bgp_prev_set)[0]
                    bgp_next = list(bgp_next_set)[0]
                    #print(bgp_prev)
                    #print(bgp_next)
                    if bgp_path.__contains__(bgp_prev + ' ' + bgp_next): #bgp_prev和bgp_next在bgp path中必须邻接
                        cur_hop = '_'.join(list(ab_as_set)) #修正一下cur_hop
                        #print(cur_hop)
                        if (Get2AsRel_2(bgp_prev, bgp_next) == -1) and (Get2AsRel_2(bgp_next, cur_hop) == -1) and \
                        (Get2AsRel_2(bgp_prev, cur_hop) == -100) and (GetAsRankFromDict(bgp_prev) < GetAsRankFromDict(bgp_next)) and \
                        (GetAsRankFromDict(bgp_next) < GetAsRankFromDict_2(cur_hop)): #符合条件1
                            #2021.7.18 这里不对，应该修正单个的ip，而不是prefix
                            # for elem in cur_hop.split('_'):
                            #     replace_dict[elem] = bgp_next #应该map到bgp_next上
                            if ip_list[i] in replace_ip_map_dict.keys():
                                if replace_ip_map_dict[ip_list[i]] != bgp_next: #map不一样，这时对校正产生怀疑，舍弃该mapping
                                    del replace_ip_map_dict[ip_list[i]]
                            else:
                                replace_ip_map_dict[ip_list[i]] = bgp_next
                        elif (Get2AsRel_2(bgp_next, bgp_prev) == -1) and (Get2AsRel_2(bgp_prev, cur_hop) == -1) and \
                        (Get2AsRel_2(bgp_next, cur_hop) == -100) and (GetAsRankFromDict(bgp_next) < GetAsRankFromDict(bgp_prev)) and \
                        (GetAsRankFromDict(bgp_prev) < GetAsRankFromDict_2(cur_hop)): #符合条件2
                            #2021.7.18 这里不对，应该修正单个的ip，而不是prefix
                            # for elem in cur_hop.split('_'):
                            #     replace_dict[elem] = bgp_prev #应该map到bgp_prev上
                            if ip_list[i] in replace_ip_map_dict.keys():
                                if replace_ip_map_dict[ip_list[i]] != bgp_prev: #map不一样，这时对校正产生怀疑，舍弃该mapping
                                    del replace_ip_map_dict[ip_list[i]]
                            else:
                                replace_ip_map_dict[ip_list[i]] = bgp_prev
            prev_ab_set = ab_as_set
        #2021.7.18 这里不对，应该修正单个的ip，而不是prefix
        # if len(replace_dict) > 0:
        #     ip_list = curline_ip.strip('\n').split(']')[1].strip(' ').split(' ')
        #     for i in range(0, len(ori_trace_list)):
        #         cur_hop = ori_trace_list[i]
        #         if cur_hop.__contains__('*') or cur_hop.__contains__('?') or cur_hop.__contains__('<'):
        #             continue
        #         if AsnInBgpPathList(cur_hop, bgp_list):
        #             continue
        #         for key in replace_dict.keys():
        #             if cur_hop.__contains__(key):
        #                 prefix = GetLongestMatchPrefixByRv(ip_list[i])
        #                 if prefix in replace_ip_map_dict.keys():
        #                     if replace_ip_map_dict[prefix] != replace_dict[key]: #map不一样，这时对校正产生怀疑，舍弃该mapping
        #                         del replace_ip_map_dict[prefix]
        #                 else:
        #                     replace_ip_map_dict[prefix] = replace_dict[key]
        curline_trace = rf.readline()
    rf.close()
    print("replace_ip_map_dict len: %d" %len(replace_ip_map_dict))
    return replace_ip_map_dict

g_replace_ip_map_dict = dict()
def FilterAbLink(filename, w_filename, record_filename):
    global g_replace_ip_map_dict
    rf = open(filename, 'r')
    wf = open(w_filename, 'w')
    total_num = 0
    ab_num = 0
    curline_trace= rf.readline()

    while curline_trace:
        total_num += 1
        curline_bgp = rf.readline()
        curline_ip = rf.readline()
        elems = curline_trace.strip('\n').split(']')
        ori_trace = elems[1].strip(' ')
        ori_trace_list = ori_trace.split(' ')
        bgp_path = CompressAsPath(curline_bgp.strip('\n').strip('\t'))
        bgp_list = bgp_path.split(' ')        
        ip_list = curline_ip.strip('\n').split(']')[1].strip(' ').split(' ')
        #print(curline_trace)
        #print(curline_ip)
        for i in range(0, len(ori_trace_list)):
            cur_hop = ori_trace_list[i]
            if cur_hop.__contains__('*') or cur_hop.__contains__('?') or cur_hop.__contains__('<'):
                continue
            if AsnInBgpPathList(cur_hop, bgp_list):
                continue
            #prefix = GetLongestMatchPrefixByRv(ip_list[i])
            if ip_list[i] in g_replace_ip_map_dict.keys():
                new_asn = g_replace_ip_map_dict[ip_list[i]] #重新匹配
                if AsnInBgpPathList(new_asn, bgp_list): #重新匹配后正常
                    continue
            #还是异常
            wf.write("%s%s%s" %(curline_trace, curline_bgp, curline_ip))
            ab_num += 1
            break
        curline_trace = rf.readline()
    rf.close()

    wf_record = open(record_filename, 'a')
    wf_record.write("After filter_ab_link, still ab: %d, percent: %.2f\n" %(ab_num, ab_num / total_num))
    print("After filter_ab_link, still ab: %d, percent: %.2f\n" %(ab_num, ab_num / total_num))
    wf_record.close()

def CheckDetourOfTwoPaths(trace_path, bgp_path):
    trace_list = []
    for elem in trace_path.split(' '):
        if elem != '*' and elem != '?' and elem != '<>' and (not AsnInTracePathList(elem, trace_list)):
            trace_list.append(elem)
    bgp_list = []
    for elem in bgp_path.split(' '):
        if elem not in bgp_list:
            bgp_list.append(elem)
    prev_i = 0 #bgp list iterator
    prev_j = 0 #trace list iterator
    while prev_i < len(bgp_list) - 1:
        i = prev_i + 1
        j = 0
        while i < len(bgp_list):
            j = FindBgpAsInTracePath(bgp_list[i], trace_list)
            if j != -1:
                break
            i += 1
        if (i - prev_i) == 1: #bgp link
            if (j - prev_j) != 1: #trace detour
                return 'linkab'
            else: #trace_link
                prev_i = i
                prev_j = j #move a step
        else: #bgp seg path
            if ((i - prev_i) == 2) and ((j - prev_j) == 2): #one hop ab
                return 'oneab'
            else:
                return 'others'
        
def ClassifyDetour(filename, count_dict):
    with open(filename, 'r') as rf:
        curline_trace = rf.readline()
        while curline_trace:
            curline_bgp = rf.readline()
            curline_ip = rf.readline()
            detour_type = CheckDetourOfTwoPaths(curline_trace[curline_trace.index(']') + 2:].strip('\n'), curline_bgp.strip('\n').strip('\t'))
            count_dict[detour_type] += 1
            curline_trace = rf.readline()   

def StatLastAsInLastExtra():    
    last_as_in_last_extra_dict = dict()
    for year in range(2018,2021):
        for month in range(1,13):
            date = str(year) + str(month).zfill(2) + '15'
            for vp in global_var.vps:
                filename = global_var.par_path +  global_var.out_my_anatrace_dir + '/' + vp + '_' + date + '/ribs_midar_bdrmapit/ana_ab_4_last_extra_one_provider'
                if os.path.exists(filename):
                    with open(filename, 'r') as rf:
                        curline_trace = rf.readline()
                        while curline_trace:
                            curline_bgp = rf.readline()
                            curline_ip = rf.readline()
                            last_bgp_hop = curline_bgp.strip('\n').strip('\t').split(' ')[-1]
                            if last_bgp_hop not in last_as_in_last_extra_dict.keys():
                                last_as_in_last_extra_dict[last_bgp_hop] = 0
                            last_as_in_last_extra_dict[last_bgp_hop] += 1
                            curline_trace = rf.readline()
    sort_list = sorted(last_as_in_last_extra_dict.items(), key=lambda d:d[1], reverse = True)
    with open(global_var.par_path +  global_var.out_my_anatrace_dir + '/last_as_in_last_extra', 'w') as wf:
        for elem in sort_list:
            wf.write('%s: %d\n' %(elem[0], elem[1]))

def AnaOneDate(vps, year, month, map_methods, detour_dict = None, detour_count_dict = None, bifurc_count_last_ab_dict = None):
    #steps = ['rename', 'filter_loop', 'filter_ab_link', 'classify_ab', '(sub1)last_extra', '(sub1)bifurc', '(sub1)detour', 'group_ab_for_further_ana']
    steps = ['rename', 'filter_loop', 'filter_ab_link']
    need_prepare_data_dict = dict()
    need_prepare_data_dict['rename'] = False
    need_prepare_data_dict['filter_loop'] = False
    need_prepare_data_dict['filter_ab_link'] = True
    need_prepare_data_dict['classify_ab'] = False
    need_prepare_data_dict['(sub1)last_extra'] = True
    #need_prepare_data_dict['(sub1)bifurc'] = True
    need_prepare_data_dict['(sub1)bifurc'] = False
    need_prepare_data_dict['(sub1)detour'] = False
    need_prepare_data_dict['group_ab_for_further_ana'] = False

    need_prepare_data = False
    for elem in steps:
        if need_prepare_data_dict[elem]:
            need_prepare_data = True
            break
    
    year_str = str(year)
    month_str = str(month).zfill(2)
    date = year_str + month_str + '15'
    
    if need_prepare_data:
        GetSibRel(year, month)
        GetAsRel(year, month)
        GetIxpPfxDict(year, month)
        GetIxpAsSet()   #这条语句需要放在GetSibRel()之后，因为需要sib_dict，也就是as2org_dict
        ConnectToDb()
        SetCurMidarTableDate(year, month)
        GetAsRankDict(year, month)
    
    for vp in vps:
        cur_dir = global_var.par_path +  global_var.out_my_anatrace_dir + '/' + vp + '_' + date + '/' #'nrt-jp.2019030115/'
        if not os.path.exists(cur_dir):
            continue
        print(cur_dir)
        os.chdir(cur_dir)
        
        g_asn = global_var.trace_as_dict[vp]    
        bgp_filename = global_var.par_path + global_var.rib_dir + 'bgpdata/bgp_' + g_asn + '_' + date
        if need_prepare_data:
            #os.system("copy ..\\%s %s" %(trace_file_name, trace_file_name))
            #os.system("copy ..\\bgp_%s bgp_%s" %(g_asn, g_asn))
            GetPfx2ASByBgp(bgp_filename) #2021.3.2使用VP本身的bgp表进行匹配，而不是用routeviews-rv2-20190301-1200.pfx2as_coalescedGetSibRel()
            GetBgpByPrefix(bgp_filename) #step 2 #get bgp_path to dst_prefix
            GetPathAsDict(bgp_filename) #step 3 #get bgp_path to dst_as
        if steps.__contains__('(sub1)bifurc'):
            GetBgpByPrefix(bgp_filename) #step 2 #get bgp_path to dst_prefix
        
        global cur_map_method
        cur_map_method = 'ribs_midar_bdrmapit'
        print(cur_map_method)
        #if cur_map_method.__contains__('ribs'):
        os.chdir(cur_map_method)
        record_file_name = 'ana_record_' + vp + '.' + date
        
        if steps.__contains__('step debug'):
            print('step debug')
            GetBgp_1('3257')
            ChgAbPathFileToCmpFormat('hkg-cn.20190301\\ab_filter4_other_ab_start_ab_from_begin_link_exists')
            ClearBGP_1()

        if steps.__contains__('rename'):
            print('rename')
            shutil.copyfile('final_ab_fin', 'ana_ab_1')
            
        if steps.__contains__('filter_loop'): #过滤考虑moas的情况下含loop的trace
            print('filter_loop')
            if os.path.exists(record_file_name):
                os.remove(record_file_name)
            TagStepInRecordFile(record_file_name, 'filter_loop')
            GetOtherLoopTraces('ana_ab_1', 'ana_ab_2', record_file_name)
                
        if steps.__contains__('filter_ab_link'): 
            print('filter_ab_link')
            if False:
                FilterOnlyOneAbAndSimilarHop('ana_ab_2', 'ana_ab_3')
            elif False:   #不做这一步, 2021.3.4
                os.system("cp ana_ab_2 ana_ab_3")
            else: #2021.5.30
                TagStepInRecordFile(record_file_name, 'filter_ab_link')
                FilterAbLink('ana_ab_2', 'ana_ab_3', record_file_name)

        #这一步先不做
        if steps.__contains__('undo'):
            print('undo')
            if False:
                GetSibRel()
                as_list = ['3356', '3257', '1299', '174', '2914', '6762', '6939', '6453', '6461', '3549', '3491', '1273', '9002', '4637', '12956']
                GetBgpDictGroup(as_list)
                FilterMightLegalTrace(cur_dir + 'ab_filter3', cur_dir + 'ab_filter4')
                ClearBgpDictGroup()
                ClearSibRel()
            else:
                os.system("cp ana_ab_3 ana_ab_4")
        
        if steps.__contains__('classify_ab'): #将ab trace分类
            os.system("cp ana_ab_3 ana_ab_4")
            if False:
                print('step 4')
                ClassifyAbTrace('ana_ab_4', 'ana_ab_4_reach_dst_last_extra', 'ana_ab_4_reach_dst_mid_ab', 'ana_ab_4_reach_dst_mid_ab_and_last_extra', 'ana_ab_4_reach_dst_not_decided', 'ana_ab_4_not_reach_dst_last_ab', 'ana_ab_4_not_reach_dst_other', record_file_name, 'a')
            else:
                print('classify_ab')
                TagStepInRecordFile(record_file_name, 'classify_ab')
                ClassifyAbTrace2('ana_ab_4', 'ana_ab_4_last_extra', 'ana_ab_4_first_hop_ab', 'ana_ab_4_detour', 'ana_ab_4_bifurc', record_file_name, 'a', detour_dict)
                #ClassifyAbTrace2('ana_ab_4', 'ana_ab_4_last_extra', None, 'ana_ab_4_detour', 'ana_ab_4_bifurc', record_file_name, 'a', detour_dict)

        if steps.__contains__('(sub1)last_extra'): #处理正常到了dst_as但后面多出n跳的trace
            print('(sub1)last_extra')
            TagStepInRecordFile(record_file_name, '(sub1)last_extra')
            wf = dict()
            for elem in rel_kind:
                wf[elem] = open('ana_ab_4_last_extra_one_' + elem, 'w')
            wf['multi'] = open('ana_ab_4_last_extra_multi', 'w')
            ExtractExtraLastHop('ana_ab_4_last_extra', wf, 'ana_ab_4_last_extra_not_decided', record_file_name)

        if steps.__contains__('(sub1)bifurc'):
            print('(sub1)bifurc')
            TagStepInRecordFile(record_file_name, '(sub1)bifurc')
            #ExtractLastAb_2('ana_ab_4_bifurc', record_file_name)
            ExtractLastAb_3('ana_ab_4_bifurc', bifurc_count_last_ab_dict[vp])

        if steps.__contains__('(sub1)detour'):
            print('(sub1)detour')
            TagStepInRecordFile(record_file_name, '(sub1)detour')
            ClassifyDetour('ana_ab_4_detour', detour_count_dict)            

        if steps.__contains__('step stat_dst_as_freq'):
            print('step stat_dst_as_freq')
            files = ['ana_ab_4_reach_dst_last_extra']
            StatDstAsFreq(files, 'ana_ab_4_reach_dst_last_extra_dst_as_freq')
        
        if steps.__contains__('group_ab_for_further_ana'):
            os.system('cat ana_ab_4_detour ana_ab_4_bifurc > ana_ab_5')
        
        # if steps.__contains__('step fst_ab'):
        #     print('step fst_ab')
        #     TagStepInRecordFile(record_file_name, 'step fst_ab')
        #     CheckStartAb('ana_ab_4_reach_dst_mid_ab', 'ana_ab_4_reach_dst_mid_ab_start_ab_from_begin', 'ana_ab_4_reach_dst_mid_ab_start_ab_not_from_begin', 'reach_dst_mid_ab', record_file_name, 'a')
        #     CheckStartAb('ana_ab_4_reach_dst_mid_ab_and_last_extra', 'ana_ab_4_reach_dst_mid_ab_and_last_extra_start_ab_from_begin', 'ana_ab_4_reach_dst_mid_ab_and_last_extra_start_ab_not_from_begin', 'reach_dst_mid_ab_and_last_extra', record_file_name, 'a')
        #     CheckStartAb('ana_ab_4_not_reach_dst_other', 'ana_ab_4_not_reach_dst_other_start_ab_from_begin', 'ana_ab_4_not_reach_dst_other_start_ab_not_from_begin', 'not_reach_dst_other', record_file_name, 'a')
              
        # if steps.__contains__('step 8'):
        #     print('step 8')
        #     TagStepInRecordFile(record_file_name, 'step 8')
        #     filenames = ['ab_filter4_reach_dst_mid_ab_start_ab_from_begin', 'ab_filter4_reach_dst_mid_ab_start_ab_not_from_begin',\
        #                 'ab_filter4_reach_dst_mid_ab_and_last_extra_start_ab_from_begin', 'ab_filter4_reach_dst_mid_ab_and_last_extra_start_ab_not_from_begin',\
        #                 'ab_filter4_not_reach_dst_other_start_ab_from_begin', 'ab_filter4_not_reach_dst_other_start_ab_not_from_begin']
        #     for filename in filenames:
        #         print(filename)
        #         w_filename = re.sub('4', '5', filename)
        #         CheckAbPathIsPartNormal(filename, w_filename, filename[filename.index('4') + 2:], record_file_name, 'a')

        # if steps.__contains__('step 9'):
        #     GetAsCountryDict()
        #     GetAsRankDict()
        #     GetAsRel()
        #     ClassifyLastAb('ab_filter4_not_reach_dst_last_ab')    
        #     ClearAsCountryDict() 
        #     ClearAsRankDict()
        #     ClearAsRel()     

        '''        
        if steps.__contains__('step 6'): #处理最后一跳不是dst_as的trace
            print('step 6')
            TagStepInRecordFile(record_file_name, 'step 6')
            GetSibRel()
            GetAsRel()   
            GetLastAbAsRel_2('ab_filter4_not_reach_dst_last_ab', record_file_name, 'a')
            ClearSibRel()
            ClearAsRel()

        #if steps.__contains__('step 7'):
            #print('step 7')
            #GetAsRankDict()
            #AnaExtraAs('ab_filter4_reach_dst_mid_ab', 'ab_filter4_reach_dst_mid_ab_and_last_extra')
            #ClearAsRankDict()

        if steps.__contains__('step 9'): #分析start_ab_from_begin时第一个ab hop的特征
            print('step 9')
            asn = global_var.trace_as_dict[cur_dir.split('.')[0]]
            GetAsRankDict()
            GetVpNeighborFromBgp(asn)
            filenames = ['ab_filter4_reach_dst_mid_ab_start_ab_from_begin', 'ab_filter4_reach_dst_mid_ab_and_last_extra_start_ab_from_begin', 'ab_filter4_not_reach_dst_other_start_ab_from_begin']
            start_ab_as_freq_dict = dict()
            for filename in filenames:
                start_ab_as_freq_dict = AnaStartAb(filename, start_ab_as_freq_dict)   
            sort_list = sorted(start_ab_as_freq_dict.items(), key=lambda d:d[1][0], reverse = True)             
            wf = open('ab_filter4.1_start_ab_from_begin_ana', 'w')
            for elem in sort_list:
                wf.write("%s %d %s %s\n" %(elem[0], elem[1][0], GetAsRankFromDict(elem[0]), elem[1][1]))
            wf.close()        
            ClearVpNeighbor()
            ClearAsRankDict()            
        
        if steps.__contains__('step 10'):
            print('step 10')
            GetAsRankDict()
            filenames = ['ab_filter4_reach_dst_mid_ab_start_ab_not_from_begin', 'ab_filter4_reach_dst_mid_ab_and_last_extra_start_ab_not_from_begin']
            prev_ab_dict = dict()
            for filename in filenames:
                prev_ab_dict = AnaSpecInterc(filename, prev_ab_dict)
            wf_susp_interc_prev_ab_ana = open('ab_filter4.1_susp_interc_ana', 'w')
            #prev_ab_dict[prev_as] = [0, GetAsRankFromDict(prev_as), set()]
            sort_list = sorted(prev_ab_dict.items(), key=lambda d:d[1][0], reverse = True)
            for elem in sort_list:
                wf_susp_interc_prev_ab_ana.write("%s %d %d %s\n" %(elem[0], elem[1][0], len(elem[1][2]), elem[1][1]))
                for path_info in elem[1][2]:
                    wf_susp_interc_prev_ab_ana.write("\t<%s> <%s>\n" %(path_info[0], path_info[1]))
            wf_susp_interc_prev_ab_ana.close()
            ClearAsRankDict()

        if steps.__contains__('step 15'): #要检查，是否有一些目的地址就不在匹配的前缀里
            print('step 15')
            GetSibRel()
            GetAsRel()   
            asn = global_var.trace_as_dict[cur_dir.split('.')[0]]
            GetDstIpIntSet(cur_dir.strip('\\'))
            GetBgpByPrefix(asn)
            filenames = ['ab_filter4_reach_dst_mid_ab']
            for filename in filenames:
                AnaAb(filename, filename + '_ana_ab')
            ClearBGPByPrefix()
            ClearDstIpIntSet()

        if steps.__contains__('step 11'):
            print('step 11')
            GetSibRel()
            GetAsRel()   
            filenames = ['ab_filter4_reach_dst_mid_ab', 'ab_filter4_reach_dst_mid_ab_and_last_extra', 'ab_filter4_not_reach_dst_other', 'ab_filter4_reach_dst_mid_ab_start_ab_not_from_begin', 'ab_filter4_reach_dst_mid_ab_and_last_extra_start_ab_not_from_begin']
            for filename in filenames:
                w_filename = re.sub('4', '5', filename)
                FilterOnlyOneAbRelWithNeigh(filename, w_filename)
            ClearSibRel()
            ClearAsRel()
            GetAsRankDict()
            filenames = ['ab_filter5_reach_dst_mid_ab_start_ab_not_from_begin', 'ab_filter5_reach_dst_mid_ab_and_last_extra_start_ab_not_from_begin']
            prev_ab_dict = dict()
            for filename in filenames:
                prev_ab_dict = AnaSpecInterc(filename, prev_ab_dict)
            wf_susp_interc_prev_ab_ana = open('ab_filter5_susp_interc_ana', 'w')
            #prev_ab_dict[prev_as] = [0, GetAsRankFromDict(prev_as), set()]
            sort_list = sorted(prev_ab_dict.items(), key=lambda d:d[1][0], reverse = True)
            for elem in sort_list:
                wf_susp_interc_prev_ab_ana.write("%s %d %d %s\n" %(elem[0], elem[1][0], len(elem[1][2]), elem[1][1]))
                for path_info in elem[1][2]:
                    wf_susp_interc_prev_ab_ana.write("\t<%s> <%s>\n" %(path_info[0], path_info[1]))
            wf_susp_interc_prev_ab_ana.close()
            ClearAsRankDict()

        if steps.__contains__('step 12'):
            print('step 12')
            GetAsRankDict()
            ana_dict = StatisSuspIntercAna('ab_filter5_susp_interc_ana', ana_dict)
            ClearAsRankDict()

        if steps.__contains__('step 14'):
            print('step 14')
            filenames = ['ab_filter4_reach_dst_last_extra', 'ab_filter4_reach_dst_mid_ab', 'ab_filter4_reach_dst_mid_ab_and_last_extra', 'ab_filter4_not_reach_dst_last_ab', 'ab_filter4_not_reach_dst_other',\
                        'ab_filter4_reach_dst_last_extra_one_not_known', 'ab_filter4_reach_dst_last_extra_multi', 'ab_filter4_not_reach_dst_last_ab_no_rel', \
                        'ab_filter4_reach_dst_mid_ab_start_ab_from_begin', 'ab_filter4_reach_dst_mid_ab_start_ab_not_from_begin', \
                        'ab_filter4_reach_dst_mid_ab_and_last_extra_start_ab_from_begin', 'ab_filter4_reach_dst_mid_ab_and_last_extra_start_ab_not_from_begin', \
                        'ab_filter4_not_reach_dst_other_start_ab_from_begin', \
                        'ab_filter5_reach_dst_mid_ab', 'ab_filter5_reach_dst_mid_ab_and_last_extra', 'ab_filter5_not_reach_dst_other', 'ab_filter5_reach_dst_mid_ab_start_ab_not_from_begin', 'ab_filter5_reach_dst_mid_ab_and_last_extra_start_ab_not_from_begin']
            for filename in filenames:
                print("%s: %d" %(filename, GetFileLineNum(filename) / 3))
        
        if steps.__contains__('delete step'):
            os.remove()
        '''

        '''
        if steps.__contains__('step 12'):
            wf_step12 = open('ab_filter5_susp_interc_ana_statis', 'w')
            sort_list = sorted(ana_dict.items(), key=lambda d:len(d[1][2]), reverse = True)
            for elem in sort_list:
                wf_step12.write("%s %d %d %s\n" %(elem[0], elem[1][0], len(elem[1][2]), elem[1][1]))
                #if len(elem[1][2]) > 1:
                print("%s %d %d %s" %(elem[0], elem[1][0], len(elem[1][2]), elem[1][1]))
                for (trace_path, bgp_path) in elem[1][2].items():
                    wf_step12.write("\t<%s> <%s>\n" %(trace_path, bgp_path))
            wf_step12.close()
        
        if steps.__contains__('step 13'):
            FilterTraceWithSpecBgp('ab_filter5_susp_interc_ana_statis', 'ab_filter6_susp_interc_ana_statis')
        '''
        
        if need_prepare_data:
            ClearPathAsDict() #get bgp_path to dst_as
            ClearBGPByPrefix() #get bgp_path to dst_prefix
            ClearIp2AsDict()
        os.chdir('../..')
        
    
    if need_prepare_data:
        CloseDb()
        ClearIxpAsSet()
        ClearIxpPfxDict()
        ClearSibRel()
        ClearAsRel()    
        ClearAsRankDict()
    


#AnaGeoOfLastAbAs('test')
#AnaGeoOfLastAbAs('last_hop_ab_no_rel_1', 1)
#AnaGeoOfLastAbAs('last_hop_ab_no_rel_2', 2)
#TmpAnaDist('dist_last_hop_ab_no_rel_1')
#TmpAnaDist('dist_last_hop_ab_no_rel_2')

#GetBgpPrefixAsDict('7660')
#AnaNoBgpTrace('3-2_single_path_no_bgp')
#TmpCmpIxpPathFiles() #正常不用
#CalKeyPercentInIxpPath('3-1_single_path_ab_ab', '5_has_ixp_ip', True)

#GetAsRel()
#AnaAsDegree('3-2_single_path_no_bgp')
            
'''
GetIxpIndexDict_Obselete()
GetIxpIndexDict()
wf = open('temp', 'w')
num_key_not_in_1 = 0
num_val_not_in = 0
num_val_not_in_1 = 0
for (key, val) in ixp_index_dict.items():
    if key not in ixp_index_dict_1.keys():
        num_key_not_in_1 += 1
    else:
        for elem in ixp_index_dict[key]:
            if elem not in ixp_index_dict_1[key]:
                num_val_not_in_1 += 1
        for elem in ixp_index_dict_1[key]:
            if elem not in ixp_index_dict[key]:
                wf.write("[%s](%s)%s\n" %(key, elem[0], elem[1])) #elem: [ixp_index_str, ip_path]
                for elem1 in ixp_index_dict[key]:
                    wf.write("\t\t\t\t(%s)%s\n" %(elem1[0], elem1[1])) #elem1: [ixp_index_str, ip_path]
                num_val_not_in += 1
#for (key, val) in ixp_index_dict_1.items():
    #if key not in ixp_index_dict.keys():
        #num_key_not_in += 1
#print("num_key_not_in: %d" %num_key_not_in)
print("num_key_not_in_1: %d" %num_key_not_in_1)
print("num_val_not_in: %d" %num_val_not_in)
print("num_val_not_in_1: %d" %num_val_not_in_1)
wf.close()
#GetDifStart('3-1_single_path_ab_ab')
'''

#GetBgp_1('7660')
#ChgAbPathFileToCmpFormat_2('3-1_single_path_ab_ab_difstart_1_dif_same_w_ixp')

#GetBgp_1('7660')
#res = DebugGetBgpRoute('7660')
#AnaIxpAbPathWithAllBgp()

def FilenameContainsVP(filename):
    for elem in global_var.vps:
        if filename.__contains__(elem):
            return True
    return False

def GlobalAna():
    steps = ['step stat_dst_as_freq']

    par_dir = global_var.par_path +  global_var.out_my_anatrace_dir
    os.chdir(par_dir)
    g_files = []
    if steps.__contains__('step stat_dst_as_freq'):
        print('step stat_dst_as_freq')
        dir_list = os.listdir(par_dir)
        for vp in global_var.vps:
            files = []
            for cur_dir in dir_list:
                if os.path.isdir(os.path.join(par_dir, cur_dir)) and cur_dir.__contains__(vp) and \
                (cur_dir.__contains__('2018') or cur_dir.__contains__('2019')):
                    #print(cur_dir)
                    #continue
                    if cur_dir.__contains__(vp):
                        files.append(cur_dir + '/ribs_midar_bdrmapit/ana_ab_4_reach_dst_last_extra')
                        g_files.append(cur_dir + '/ribs_midar_bdrmapit/ana_ab_4_reach_dst_last_extra')
            StatDstAsFreq(files, 'statistics/dstasfreq_stat/ana_ab_4_reach_dst_last_extra_dst_as_freq_' + vp)
        StatDstAsFreq(g_files, 'statistics/dstasfreq_stat/ana_ab_4_reach_dst_last_extra_dst_as_freq')

def PrepareInfo():
    steps = ['replace_ab_link_ip_map']

    cur_dir = global_var.par_path +  global_var.out_my_anatrace_dir
    if steps.__contains__('replace_ab_link_ip_map'):
        global g_replace_ip_map_dict
        filepath = global_var.par_path + global_var.other_middle_data_dir + 'replace_ab_link_ip_map'
        if os.path.exists(filepath):
            with open(filepath, 'r') as rf:
                curline = rf.readline()
                elems = curline.strip('\n').split(' ')
                g_replace_ip_map_dict[elems[0]] = elems[1]
        else:
            for year in range(2018,2021):
                for month in range(1,13):
                    if (year == 2016 and month < 4) or (year == 2020 and month > 4): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
                        continue
                    year_str = str(year)
                    month_str = str(month).zfill(2)
                    date = year_str + month_str + '15'

                    GetSibRel(year, month)
                    GetAsRel(year, month)
                    GetAsRankDict(year, month)
                    
                    for vp in global_var.vps:
                        g_asn = global_var.trace_as_dict[vp]    
                        bgp_filename = global_var.par_path + global_var.rib_dir + 'bgpdata/bgp_' + g_asn + '_' + date
                        GetPfx2ASByBgp(bgp_filename) #2021.3.2使用VP本身的bgp表进行匹配，而不是用routeviews-rv2-20190301-1200.pfx2as_coalescedGetSibRel()
                        filename = cur_dir + '/' + vp + '_' + date + '/ribs_midar_bdrmapit/ana_ab_2'
                        print(filename)
                        if os.path.exists(filename):
                            g_replace_ip_map_dict = CheckAbLink(filename, g_replace_ip_map_dict)                    
                        ClearIp2AsDict()

                    ClearSibRel()
                    ClearAsRel()    
                    ClearAsRankDict()   
            wf = open(filepath, 'w')
            for (ip, asn) in g_replace_ip_map_dict.items():
                wf.write("%s %s\n" %(ip, asn))
            wf.close()

    #print(g_replace_ip_map_dict)

def PrintDetourDict(detour_dict, w_filename):
    wf = open(w_filename, 'w')
    sum = 0
    for (key, val) in detour_dict.items():
        sum += val[1]
    sort_list = sorted(detour_dict.items(), key=lambda d:d[1][1], reverse=True)
    for pair_elem in sort_list:
        wf.write("%s: %d(%.2f)\n" %(pair_elem[0], pair_elem[1][1], pair_elem[1][1]/sum))
        sort_list_2 = sorted(pair_elem[1][0].items(), key=lambda d:d[1][1], reverse=True)
        for bgp_seg_elem in sort_list_2:
            wf.write("\t%s: %d(%.2f)\n" %(bgp_seg_elem[0], bgp_seg_elem[1][1], bgp_seg_elem[1][1]/sum))
            sort_list_3 = sorted(bgp_seg_elem[1][0].items(), key=lambda d:d[1], reverse=True)
            for trace_seg_elem in sort_list_3:
                wf.write("\t\t%s: %d(%.2f)\n" %(trace_seg_elem[0], trace_seg_elem[1], trace_seg_elem[1]/sum))
    wf.close()

def tmp():
    detour_dict = dict()
    #ClassifyAbTrace2('tmp_test', 'tmp_last_extra', 'tmp_first_hop_ab', 'tmp_detour', 'tmp_bifurc', None, 'w', detour_dict)
    #GetOtherLoopTraces('tmp_test', 'tmp_last_extra', 'tmp_first_hop_ab')

def main_func():
    PreGetSrcFilesInDirs()
    PrepareInfo() #PrepareInfo目前只为filter_ab_link提供数据

    detour_dict = dict()
    detour_count_dict = {'linkab': 0, 'oneab': 0, 'others': 0}
    bifurc_count_last_ab_dict = dict()
    for vp in global_var.vps:
        bifurc_count_last_ab_dict[vp] = dict()
       
    for year in range(2018,2021):
    #for year in range(2018,2019):
        for month in range(1,13):
        #for month in range(8, 9):
            if (year == 2016 and month < 4) or (year == 2020 and month > 4): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
                continue
            AnaOneDate(global_var.vps, year, month, global_var.map_methods, detour_dict, detour_count_dict, bifurc_count_last_ab_dict)
            '''
            GetSibRel(year, month)
            year_str = str(year)
            month_str = str(month).zfill(2)
            date = year_str + month_str + '15'
            for vp in global_var.vps:
                g_asn = global_var.trace_as_dict[vp]    
                bgp_filename = global_var.par_path + global_var.rib_dir + 'bgpdata/bgp_' + g_asn + '_' + date
                GetPfx2ASByBgp(bgp_filename) #2021.3.2使用VP本身的bgp表进行匹配，而不是用routeviews-rv2-20190301-1200.pfx2as_coalescedGetSibRel()
                filename = global_var.par_path +  global_var.out_my_anatrace_dir + '/' + vp + '_' + date + '/ribs_midar_bdrmapit/ana_ab_4_detour'
                print(filename)
                CheckTracesByIRRData(filename, True, False)
                ClearIp2AsDict()
            '''
    if len(detour_dict) > 0:
        PrintDetourDict(detour_dict, global_var.par_path +  global_var.out_my_anatrace_dir + '/detour_dict')
    if detour_count_dict['linkab'] > 0:
        for (key, val) in detour_count_dict.items():
            print('%s:%s' %(key, val))
    if len(bifurc_count_last_ab_dict['nrt-jp']) > 0:
        for vp in global_var.vps:
            sort_list = sorted(bifurc_count_last_ab_dict[vp].items(), key=lambda d:d[1], reverse = True)
            total_count = 0
            for elem in sort_list:
                total_count += elem[1]
            with open('tmp_bifurc_' + vp, 'w') as wf:
                wf.write('%d\n' %total_count)
                for elem in sort_list:
                    wf.write('%s:%d,%.2f\n' %(elem[0], elem[1], elem[1]/total_count))
    #GlobalAna()


if __name__ == '__main__':
    #tmp()
    main_func()
    #StatLastAsInLastExtra()
