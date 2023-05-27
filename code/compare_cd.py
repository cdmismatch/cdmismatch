
from collections import defaultdict, Counter
import socket
import struct
import os
import sys
import glob
import datetime
import json
import re
from multiprocessing import Process, Queue, Pool

from find_vp_v2 import CompressBGPPath
from get_ip2as_from_bdrmapit import ConnectToBdrMapItDb, GetIp2ASFromBdrMapItDb, CloseBdrMapItDb, InitBdrCache, \
                                    ConstrBdrCache
from utils_v2 import GetIxpPfxDict_2, IsIxpIp, ClearIxpPfxDict, SetCurMidarTableDate_2, GetAsOfIpByMi, ConnectToDb, \
                    InitMidarCache, CloseDb, GetSibRel_2, IsSib_2, GetOrgOfAS, GetPeerASNByIp, GetIxpAsSet, ClearIxpAsSet, \
                    IsIxpAs
from traceutils.ixps import AbstractPeeringDB, create_peeringdb

g_parell_num = os.cpu_count()
filter_snd_mm = True#False#

def InitPref2ASInfo(filename, pref2as_info): #适用于：/mountdisk1/ana_c_d_incongruity/out_ip2as_data/
    with open(filename, 'r') as rf:
        curlines = rf.readlines(100000)
        while curlines:
            for curline in curlines:
                (prefix, asn) = curline.strip('\n').split(' ')
                if prefix not in pref2as_info.keys():
                    pref2as_info[prefix] = set()
                pref2as_info[prefix].add(asn)
            curlines = rf.readlines(100000)

def InitPref2ASInfo_2(filename, pref2as_info): #适用于：/mountdisk1/ana_c_d_incongruity/rib_data/rv_prefix2as/*.pfx2as
    with open(filename, 'r') as rf:        
        for curline in rf:
            (prefix_val, prefix_len, asn) = curline.strip('\n').split('\t')
            pref2as_info[prefix_val + '/' + prefix_len] = set(asn.split('_'))

def InitBGPPathInfo(bgp_filename, bgp_path_info):    
    with open(bgp_filename, 'r') as rf:
        curlines = rf.readlines(100000)
        while curlines:
            for curline in curlines:
                (prefix, path) = curline.strip('\n').split('|')
                if prefix not in bgp_path_info.keys():
                    bgp_path_info[prefix] = [set(), set()]
                new_path = CompressBGPPath(path)
                if not new_path:
                    print(prefix)
                bgp_path_info[prefix][0].add(new_path)
                bgp_path_info[prefix][1].add(new_path.split(' ')[-1])
            curlines = rf.readlines(100000)
    bgp_path_info[prefix][0] = list(bgp_path_info[prefix][0])

def GetBGPPath_Or_OriASN(bgp_path_info, _ip, mode):
    ip_int = None
    try:
        ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton(_ip))[0])
    except Exception as e:
        print('err: {}'.format(_ip))
        return (None, None)
    res = []
    bgp_paths = set()
    paths_prefs = []
    for mask_len in range(32, 7, -1):
        # mask = 0xFFFFFFFF - (1 << (32 - mask_len)) + 1
        # cur_prefix_int = ip_int & mask
        # cur_prefix = str(socket.inet_ntoa(struct.pack('I',socket.htonl(cur_prefix_int))))
        if mask_len < 32:
            mask = ~(1 << (31 - mask_len))
            ip_int = ip_int & mask
        cur_prefix = str(socket.inet_ntoa(struct.pack('I',socket.htonl(ip_int))))
        cur_prefix = cur_prefix + '/' + str(mask_len)
        #print(cur_prefix)
        if cur_prefix in bgp_path_info.keys():
            if mode == 'get_path':
                #return bgp_path_info[cur_prefix][0]
                bgp_paths = bgp_paths | set(bgp_path_info[cur_prefix][0])
            elif mode == 'get_paths_prefs':
                #return bgp_path_info[cur_prefix][0]
                paths_prefs.append((cur_prefix, bgp_path_info[cur_prefix][0]))
            elif mode == 'get_orias':
                return '_'.join(sorted(list(bgp_path_info[cur_prefix][1])))
            elif mode == 'get_prefix':
                return cur_prefix
            elif mode == 'get_all_prefixes':
                res.append([cur_prefix, '_'.join(sorted(list(bgp_path_info[cur_prefix][1])))])
            elif mode == 'get_all':
                return (cur_prefix, '_'.join(sorted(list(bgp_path_info[cur_prefix][1]))), bgp_path_info[cur_prefix][0])
            elif mode == 'get_orias_2':
                return '_'.join(sorted(list(bgp_path_info[cur_prefix])))
            elif mode == 'get_all_2':
                return (cur_prefix, '_'.join(sorted(list(bgp_path_info[cur_prefix]))))
            elif mode == 'get_all_prefixes_2':
                res.append([cur_prefix, '_'.join(sorted(list(bgp_path_info[cur_prefix])))])
    if mode == 'get_path':
        #return []
        return list(bgp_paths)
    elif mode == 'get_paths_prefs':
        return paths_prefs
    elif mode == 'get_orias' or mode == 'get_orias_2':
        return '?'
    elif mode == 'get_prefix':
        return ''
    elif mode == 'get_all':
        return ('', '?', [])
    elif mode == 'get_all_2':
        return ('', '?')
    elif mode == 'get_all_prefixes' or mode == 'get_all_prefixes_2':
        return res
    return None

def MapTrace(bgp_path_info, ip_list, map_method, ori_asn_cache): #ori_asn_cache只在rib-based中有用
    global pfx2as_info
    trace_list = []
    for _ip in ip_list:
        if _ip == '*':
            trace_list.append('*')
            continue
        res = ''
        if map_method == 'rib_based' or map_method == 'coa_rib_based' or map_method == 'rib_peeringdb':
            if _ip in ori_asn_cache.keys():
                res = ori_asn_cache[_ip]
            else: 
                if map_method == 'rib_based':
                    res = GetBGPPath_Or_OriASN(bgp_path_info, _ip, 'get_orias')
                    ori_asn_cache[_ip] = res
                else:
                    res = GetBGPPath_Or_OriASN(pfx2as_info, _ip, 'get_orias_2')
                    ori_asn_cache[_ip] = res
        elif map_method.__contains__('bdr') or map_method == 'midar':
            if map_method == 'sxt_bdr' and _ip in ori_asn_cache.keys():
                res = ori_asn_cache[_ip]
            else:
                res = GetIp2ASFromBdrMapItDb(_ip)
        # elif map_method == 'midar':
        #     res = GetAsOfIpByMi(_ip)
        elif map_method.__contains__('ml_map'):
            res = ori_asn_cache.get(_ip, '?')
            # if _ip == '27.68.250.249':
            #     print('debug. res: %s' %res)
        # if res == '7473':
        #     print('here')
        if not res:
            res = '?'
        # elif IsIxpAs(res):
        #     res = '<' + res + '>'
        trace_list.append(res)
    return trace_list

# def CompressTrace(trace_list, ip_list): #如果是MOAS，要求不同AS之间按字母排序
#     loop_flag = False
#     new_list = []
#     pre_hop = ''
#     trace_to_ip_info = {}
#     for i in range(0, len(trace_list)):
#         hop = trace_list[i]
#         if hop != pre_hop:
#             if hop != '*' and hop != '?' and hop != '<>' and hop in new_list:
#                 for elem in new_list[new_list.index(hop) + 1:]:
#                     if elem != '*' and elem != '?' and elem != '<>':
#                         loop_flag = True
#             new_list.append(hop)
#             pre_hop = hop
#         if hop not in trace_to_ip_info.keys():
#             trace_to_ip_info[hop] = []
#         if ip_list[i] not in trace_to_ip_info[hop]:
#             trace_to_ip_info[hop].append(ip_list[i]) #ip要保持有序，后面miss bgp hop的时候用到
#     return (new_list, trace_to_ip_info, loop_flag)

def CompressTrace(trace_list, ip_list, first_asn): #如果是MOAS，要求不同AS之间按字母排序
    #2022.2.9重写该函数，解决trace中出现A * A导致ip_accur不准的问题
    #分为两步: 分解复杂问题
    new_list = []
    trace_to_ip_info = {}
    loop_flag = True
    #step 0 如果第一跳是'*'，加入一个初始跳
    if trace_list[0] == '*':
        trace_list.insert(0, first_asn)
        ip_list.insert(0, '^')
    #step 1 去除相邻重复的AS，同时构建trace_to_ip_info
    pre_hop = ''
    tmp_list = []
    for i in range(0, len(trace_list)):
        hop = trace_list[i]
        if hop != pre_hop:
            tmp_list.append(hop)
            pre_hop = hop
        if hop != '*':
            if hop not in trace_to_ip_info.keys():
                trace_to_ip_info[hop] = []
            if ip_list[i] not in trace_to_ip_info[hop]:
                trace_to_ip_info[hop].append(ip_list[i]) #ip要保持有序，后面miss bgp hop的时候用到
    #step 2 解决loop和"A * A"问题
    for i in range(0, len(tmp_list)):
        hop = tmp_list[i]
        if hop == '*' or hop == '?' or hop.startswith('<'):
            new_list.append(hop)
        elif hop not in new_list:
            new_list.append(hop)
        else:
            pre_index = new_list.index(hop)
            for mid_hop in new_list[pre_index + 1:]:
                if mid_hop != '*' and mid_hop != '?' and not mid_hop.startswith('<'):
                    if ip_list[0] == '^':
                        del trace_list[0]
                        del ip_list[0]
                    return (None, None, True) #loop
            #"A * A"
            new_list = new_list[:pre_index + 1] #去除两个A中间的"*"
    if ip_list[0] == '^':
        del trace_list[0]
        del ip_list[0]
    return (new_list, trace_to_ip_info, False)

#return: (find_flag, pos)  当find_flag == True and pos == 255时，表明有mal_pos
def FindTraceHopInBGP(bgp_list, hop, pre_pos):
    find = False
    pos = 255
    for asn in hop.split('_'):
        if asn in bgp_list:
            find = True
            cur_pos = bgp_list.index(asn)
            if cur_pos >= pre_pos:  #cur_pos < pre_pos这个map按出错处理
                pos = min(cur_pos, pos)
    if not find:
        return (False, 255)
    return (True, pos)

def CheckAbHopCountAndMalPos(bgp, trace_list, trace_to_ip_info): #trace没有compress
    bgp_list = bgp.split(' ')
    pre_pos = 0
    ab_count = 0
    mal_pos_flag = False
    for trace_hop in trace_list:
        if trace_hop == '*' or trace_hop == '?' or trace_hop.startswith('<'): #兼容后面修正IXP, IXP hop忽略
            continue
        (find, pos) = FindTraceHopInBGP(bgp_list, trace_hop, pre_pos)
        if pos < 255: #正常
            pre_pos = pos
        else:
            ab_count += len(trace_to_ip_info[trace_hop])
            if find:
                mal_pos_flag = True
    return (ab_count, mal_pos_flag)

def SelCloseBGP(bgps, trace_list, trace_to_ip_info):
    sel_bgp = ''
    min_ab_count = 255
    malpos_bgp = ''
    for bgp in bgps:
        (ab_count, mal_pos_flag) = CheckAbHopCountAndMalPos(bgp, trace_list, trace_to_ip_info)
        if mal_pos_flag:
            malpos_bgp = bgp
        elif ab_count < min_ab_count:
            (sel_bgp, min_ab_count) = (bgp, ab_count)
    if sel_bgp:
        return (sel_bgp, min_ab_count, False)
    else:
        return (malpos_bgp, 255, True)


mode_info = {'match': 0, 'mismatch': 1, 'partial_match': 2, 'unmap': 3, 'ixp_as': 4}
def UpdateIPMapAccur(ip_accur_info, _ip, mode, ip_list):
    global mode_info
    # if _ip == '77.67.76.34' and mode == 'mismatch':
    #     if _ip in ip_accur_info.keys():
    #         print(ip_accur_info[_ip])
    # if _ip == '65.223.57.18':
    #     print('')
    ind = ip_list.index(_ip)
    if _ip not in ip_accur_info.keys():
        ip_accur_info[_ip] = [[0, 0.0, set()], [0, 0.0, set()], [0, 0.0, set()], [0, 0.0, set()], [0, 0.0, set()]]
    ip_accur_info[_ip][mode_info[mode]][0] += 1
    ip_accur_info[_ip][mode_info[mode]][1] += round((ind + 1) / len(ip_list), 2)
    prev_ip = ip_list[ind - 1] if ind > 0 else '^'
    next_ip = ip_list[ind + 1] if ind < len(ip_list) - 1 else '$'
    ip_accur_info[_ip][mode_info[mode]][2].add((prev_ip, next_ip))

def TestGetSpecIP(filename, sel_num):
    with open(filename, 'r') as rf:
        temp_info = json.load(rf)
        sort_list = sorted(temp_info.items(), key=lambda d:d[1][3][0], reverse=True)
        for i in range(0, sel_num):
            print(sort_list[i][0] + ':' + str(sort_list[i][1][3][0]))

#CompareCD_PerTrace:
#   1.bgp要求已是最简状态; 
#   2.mal_pos的情况先不处理，此函数不考虑; 
#   3. trace AS_PATH loop先不处理，此函数不考虑;
def CompareCD_PerTrace(bgp, trace_list, ip_list, trace_to_ip_info, ip_accur_info, mm_ips, pm_ips, ip_remap):
    bgp_list = bgp.split(' ')
    bgp_list.append('$')
    trace_list.append('$')
    segs = []
    pre_bgp_index = pre_trace_index = 0
    match_flag = True
    for trace_index in range(1, len(trace_list)): #默认BGP和traceroute第一跳相同
        trace_hop = trace_list[trace_index]
        if trace_hop == '*' or trace_hop == '?' or trace_hop == '-1' or trace_hop.startswith('<'): #兼容后面修正IXP, IXP hop忽略
            continue
        (find, bgp_index) = FindTraceHopInBGP(bgp_list, trace_hop, pre_bgp_index)
        if find:
            if bgp_index == 255: # 不应该出现这个情况，函数外已过滤
                # print('Mal_pos should already be filtered')
                # print(bgp)
                # print(trace_list)
                while trace_list[-1] == '$': trace_list.pop(-1)
                return
            segs.append([bgp_list[pre_bgp_index:bgp_index + 1], trace_list[pre_trace_index:trace_index + 1]])
            pre_bgp_index = bgp_index
            pre_trace_index = trace_index
    #print(segs)
    # if ip_list[-1] == '173.214.140.1':
    #     print(trace_list)
    #     print(bgp_list)
    #     print('segs:')
    #     print(segs)

    ip_list_len = len(ip_list)
    # for _ip in trace_to_ip_info[trace_list[0]]: #trace_seg最左边的匹配hop记作match
    #     UpdateIPMapAccur(ip_accur_info, _ip, 'match', (ip_list.index(_ip) + 1) / ip_list_len)
    prev_extra_bgps = []
    for seg in segs:
        [bgp_seg, trace_seg] = seg
        ##trace_seg最左边的匹配hop已在上一轮记了一遍match，这里不再重复记录 
        #更新对missing BGP hop的处理，每个segment只记录最左边匹配的hop，最右边匹配的hop留到下一个segment处理
        for elem in trace_seg[1:-1]: #trace has extra elem
            if elem != '*' and elem != '?' and not elem.startswith('<'): #mismatch
                match_flag = False
                for _ip in trace_to_ip_info[elem]:
                    if _ip != '^':
                        UpdateIPMapAccur(ip_accur_info, _ip, 'mismatch', ip_list)
                        mm_ips.append(_ip)
                        ip_remap[_ip][trace_seg[0]] += 1
                        ip_remap[_ip][trace_seg[-1]] += 1
        if trace_seg[-1] == '$':
            _ip = trace_to_ip_info[trace_seg[0]][0]
            if prev_extra_bgps:
                if _ip != '^':
                    UpdateIPMapAccur(ip_accur_info, _ip, 'partial_match', ip_list)
                    pm_ips.append(_ip)
                    if len(trace_to_ip_info[trace_seg[0]]) > 1:
                        ip_remap[_ip][prev_extra_bgps[-1]] += 1
            else:
                if _ip != '^':
                    UpdateIPMapAccur(ip_accur_info, _ip, 'match', ip_list) #match的不记录所在位置
            for _ip in trace_to_ip_info[trace_seg[0]][1:]: #处理最后匹配的hop
                if _ip != '^':
                    UpdateIPMapAccur(ip_accur_info, _ip, 'match', ip_list)
            break
        #开始处理missing BGP hop
        cur_extra_bgps = []
        # if len(bgp_seg) > 2: #bgp has extra elem            
        #     cur_partial_match = True
        #     for elem in trace_seg[1:-1]:
        #         if elem == '*' or elem == '?' or elem.startswith('<'): #trace中间有"*"或"?"，将trace_seg最右边的匹配hop置为match，否则将该hop的第一个IP置为partial_match
        #             cur_partial_match = False
        #             break
        #     if cur_partial_match:
        #         cur_extra_bgps = bgp_seg[1:-1]
        if len(bgp_seg) > len(trace_seg):  #bgp has extra elem, 20220412 modify
            cur_extra_bgps = bgp_seg[1:-1]
        #_ip = trace_to_ip_info[trace_seg[-1]][0]
        # if trace_seg[0] == '*':
        #     print(trace_list)
        #     print(bgp_list)
        #     print(segs)
        _ip = trace_to_ip_info[trace_seg[0]][-1]
        if cur_extra_bgps:
            match_flag = False
            if _ip != '^':
                UpdateIPMapAccur(ip_accur_info, _ip, 'partial_match', ip_list)
                pm_ips.append(_ip)
                if len(trace_to_ip_info[trace_seg[0]]) > 1:
                    ip_remap[_ip][cur_extra_bgps[0]] += 1
        else:
            if _ip != '^':
                #print(_ip)
                UpdateIPMapAccur(ip_accur_info, _ip, 'match', ip_list) #match的不记录所在位置
        _ip = trace_to_ip_info[trace_seg[0]][0]
        if prev_extra_bgps:
            if _ip != '^':
                UpdateIPMapAccur(ip_accur_info, _ip, 'partial_match', ip_list)
                pm_ips.append(_ip)
                if len(trace_to_ip_info[trace_seg[0]]) > 1:
                    ip_remap[_ip][prev_extra_bgps[-1]] += 1
        else:
            if _ip != '^':
                UpdateIPMapAccur(ip_accur_info, _ip, 'match', ip_list) #match的不记录所在位置
        prev_extra_bgps = cur_extra_bgps
        # if len(trace_to_ip_info[trace_seg[-1]]) > 1:
        #     for _ip in trace_to_ip_info[trace_seg[-1]][1:]:
        if len(trace_to_ip_info[trace_seg[0]]) > 2:
            for _ip in trace_to_ip_info[trace_seg[0]][1:-1]:  
                if _ip != '^':   
                    UpdateIPMapAccur(ip_accur_info, _ip, 'match', ip_list)
    for _key in trace_to_ip_info.keys():
        if _key == '?' or _key.startswith('<'):
            mode = 'unmap' if _key == '?' else 'ixp_as'
            for _ip in trace_to_ip_info[_key]:
                if _ip != '^':
                    UpdateIPMapAccur(ip_accur_info, _ip, mode, ip_list)
    while trace_list[-1] == '$': trace_list.pop(-1)
    return match_flag

def extract_ip2as_from_peeringdb(date, ori_asn_cache):
    ixp = create_peeringdb('/mountdisk1/ana_c_d_incongruity/peeringdb_data/peeringdb_%s_%s_%s.json' %(date[:4], date[4:6], date[6:8]))
    for val in ixp.netixlans.values():
        ori_asn_cache[val.ipaddr4] = str(val.asn)
        
map_method = 'ml_map_0.3_0.5' #'sxt_bdr'#'midar'#'sxt_bdr'#'sxt_bdr'#'rib_peeringdb'#'coa_rib_based'#'hoiho_s_bdr'#
pfx2as_info = {}
def CompareCD(bgp_filename, trace_filename, first_asn, filter_dsts):
    global map_method
    global pfx2as_info

    bgp_path_info = {}
    ip_accur_info = {}
    suffix = trace_filename[trace_filename.rindex('/') + 1:]
    suffix = suffix[len('trace'):]
    wf_no_bgp = open('nobgp' + suffix, 'w')
    wf_mal_pos = open('malpos' + suffix, 'w')
    wf_loop = open('loop' + suffix, 'w')
    wf_ab = open('mm' + suffix, 'w')
    wf_match = open('match' + suffix, 'w')
    (vp, date) = suffix[1:].split('.')
    trace_links = set()
    bgp_links = set()
    trace_link_info = {}
    # start_time = datetime.datetime.now()
    count_nobgp = count_malpos = count_loop = count_match = count_total = 0
    GetIxpAsSet(date)
    InitBGPPathInfo(bgp_filename, bgp_path_info)
    ori_asn_cache = {} #ori_asn_cache只在rib-based方法中有用，为了函数参数统一，其它方法中只是占位符
    if map_method.__contains__('bdr'):     
        if map_method == 'sxt_bdr_before_rect':
            print('open db: /mountdisk1/ana_c_d_incongruity/out_bdrmapit/sxt_bdr/before_rect_bdrmapit_%s_%s.db' %(vp, date))   
            ConnectToBdrMapItDb('/mountdisk1/ana_c_d_incongruity/out_bdrmapit/sxt_bdr/before_rect_bdrmapit_%s_%s.db' %(vp, date))
        else:
            print('open db: /mountdisk1/ana_c_d_incongruity/out_bdrmapit/%s/bdrmapit_%s_%s.db' %(map_method, vp, date))   
            ConnectToBdrMapItDb('/mountdisk1/ana_c_d_incongruity/out_bdrmapit/%s/bdrmapit_%s_%s.db' %(map_method, vp, date))
        ConstrBdrCache()
    elif map_method == 'midar':
        # ConnectToDb()
        # SetCurMidarTableDate_2(date)
        # InitMidarCache()
        tmp_date = date[:6]+'15' if date[6:8] == '16' else date
        db_name = '/mountdisk1/ana_c_d_incongruity/out_bdrmapit/midar/bdrmapit_%s_%s.db' %(vp, tmp_date)
        if os.path.exists(db_name):
            ConnectToBdrMapItDb(db_name)
            ConstrBdrCache()
        else:
            print('{} not exist!'.format(db_name))
            return
    elif map_method.__contains__('rib'):
        #InitPref2ASInfo('/mountdisk1/ana_c_d_incongruity/out_ip2as_data/' + date + '.ip2as.prefixes', pfx2as_info)
        InitPref2ASInfo_2('/mountdisk1/ana_c_d_incongruity/rib_data/rv_prefix2as/' + date + '.pfx2as', pfx2as_info)
        if map_method == 'rib_peeringdb':
            extract_ip2as_from_peeringdb(date, ori_asn_cache) #在rib中加入peeringdb的内容
    elif map_method == 'ml_map':
        if filter_snd_mm:
            with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/model_modified_ip_mappings_%s_%s.json' %(vp, vp, date), 'r') as rf:
                ori_asn_cache = json.load(rf)
        else:
            with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map/model_modified_ip_mappings_%s_%s.json' %(vp, vp, date), 'r') as rf:
                ori_asn_cache = json.load(rf)
    elif map_method.__contains__('ml_map_'):
        _, _, suffix1, suffix2 = map_method.split('_')
        with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/model_modified_ip_mappings_%s_%s_%s_%s.json' %(vp, vp, date, suffix1, suffix2), 'r') as rf:
            ori_asn_cache = json.load(rf)
    # end_time = datetime.datetime.now()
    # print((end_time - start_time).seconds)
    
    # start_time = datetime.datetime.now()
    ip_remap = defaultdict(Counter)
    with open(trace_filename, 'r') as rf:
        curlines = rf.readlines(100000)
        while curlines:
            #print(len(ip_accur_info))
            for curline in curlines:
                (dst_ip, trace_ip_path) = curline.strip('\n').split(':')
                if dst_ip in filter_dsts:
                    continue
                if not trace_ip_path: #有的dst_ip没有trace
                    continue
                debug_flag = True if dst_ip == '103.137.128.1' else False
                count_total += 1
                ip_list = trace_ip_path.split(',')
                # if trace_ip_path.__contains__('120.88.53.74'):
                #     print('')
                bgps = GetBGPPath_Or_OriASN(bgp_path_info, dst_ip, 'get_path')
                if not bgps:
                    wf_no_bgp.write(curline)
                    count_nobgp += 1
                    continue
                if debug_flag: print('bgps: {}'.format(bgps))
                ori_trace_list = MapTrace(bgp_path_info, ip_list, map_method, ori_asn_cache)
                (trace_list, trace_to_ip_info, loop_flag) = CompressTrace(ori_trace_list, ip_list, first_asn)
                if loop_flag: #FIX-ME. AS PATH loop的情况暂不解决
                    wf_loop.write("[%s]%s\n" %(dst_ip, ' '.join(ori_trace_list)))
                    wf_loop.write("%s\n" %' '.join(ip_list))
                    count_loop += 1
                    continue
                (sel_bgp, min_ab_count, mal_pos_flag) = SelCloseBGP(bgps, trace_list, trace_to_ip_info)
                if debug_flag: 
                    print('sel_bgp: {}'.format(sel_bgp))
                    print('mal_pos_flag: {}'.format(mal_pos_flag))
                if mal_pos_flag: #FIX-ME. mal pos的情况暂不解决
                    wf_mal_pos.write("[%s]%s\n" %(dst_ip, ' '.join(ori_trace_list)))
                    wf_mal_pos.write("\t\t%s\n" %sel_bgp)
                    wf_mal_pos.write("%s\n" %' '.join(ip_list))
                    count_malpos += 1
                    continue
                mm_ips = []
                pm_ips = []
                match_flag = CompareCD_PerTrace(sel_bgp, trace_list, ip_list, trace_to_ip_info, ip_accur_info, mm_ips, pm_ips, ip_remap)
                #if min_ab_count == 0:
                if match_flag:
                    count_match += 1
                    wf_match.write("[%s]%s\n" %(dst_ip, ' '.join(ori_trace_list)))
                    wf_match.write("\t\t%s\n" %sel_bgp)
                    wf_match.write("%s\n" %' '.join(ip_list))
                else:
                    wf_ab.write("[%s]%s\n" %(dst_ip, ' '.join(ori_trace_list)))
                    wf_ab.write("\t\t%s\n" %sel_bgp)
                    wf_ab.write("[%s][%s]%s\n" %(','.join(mm_ips), ','.join(pm_ips), ' '.join(ip_list)))
                prev_elem = None
                trace_list = trace_list[:-1]
                for elem in trace_list:
                    if elem == '*' or elem == '?':
                        prev_elem = None
                    elif elem.startswith('<'): #ixp as的跳过，认为前后跳可以组成peer关系
                        pass
                    else:
                        if prev_elem:
                            trace_link = prev_elem + '|' + elem
                            trace_links.add(trace_link)
                            if trace_link not in trace_link_info:
                                trace_link_info[trace_link] = set()
                            trace_link_info[trace_link].add((trace_to_ip_info[prev_elem][-1], trace_to_ip_info[elem][0]))
                        prev_elem = elem
                bgp_list = sel_bgp.split(' ')
                prev_elem = bgp_list[0]
                for elem in bgp_list[1:]:
                    bgp_links.add(prev_elem + '|' + elem)
                    prev_elem = elem
            curlines = rf.readlines(100000)
    ori_asn_cache.clear()
    if map_method.__contains__('bdr') or map_method == 'midar':
        CloseBdrMapItDb()
        InitBdrCache()
    # elif map_method == 'midar':
    #     CloseDb()
    elif map_method == 'coa_rib_based':
        pfx2as_info.clear()
    # end_time = datetime.datetime.now()
    # print((end_time - start_time).seconds)
    wf_no_bgp.close()
    wf_loop.close()
    wf_mal_pos.close()
    wf_ab.close()
    wf_match.close()
    with open('mm_ip_remap' + suffix + '.json', 'w') as wf:
        json.dump(ip_remap, wf, indent = 1)
    #ip_accur_info[_ip] = [[0, 0.0, set()], [0, 0.0, set()], [0, 0.0, set()], [0, 0.0, set()], [0, 0.0, set()]]
    for _ip, val in ip_accur_info.items():
        for elem in val:
            elem[2] = list(elem[2])
    with open('ipaccur' + suffix + '.json', 'w') as wf:
        json.dump(ip_accur_info, wf, indent=1)  # 写为多行
    with open('trace_stat' + suffix, 'w') as wf:
        wf.write('total:%d\n' %count_total)
        wf.write('nobgp:%.2f\n' %(count_nobgp / count_total))
        count_cancompare = count_total - count_nobgp
        wf.write('can_compare: %d, among them:\n' %count_cancompare)
        wf.write('loop:%.2f\n' %(count_loop / count_cancompare))
        wf.write('malpos:%.2f\n' %(count_malpos / count_cancompare))
        wf.write('match:%.2f\n' %(count_match / count_cancompare))
    with open('link_stat' + suffix, 'w') as wf:
        link_total = len(bgp_links)
        tp = len(trace_links & bgp_links) / link_total
        fp = len(trace_links.difference(bgp_links)) / link_total
        fn = len(bgp_links.difference(trace_links)) / link_total
        wf.write('total bgp link num: %d' %link_total)
        wf.write('total trace link num: %d' %len(trace_links))
        wf.write('tp: %.2f, fp: %.2f, fn: %.2f' %(tp, fp, fn))
    with open('falselink' + suffix, 'w') as wf:
        ip_link_info = defaultdict(set)
        for link in trace_links.difference(bgp_links):
            for (ip1, ip2) in trace_link_info[link]:
                ip_link_info[ip1].add((0, link))
                ip_link_info[ip2].add((1, link))
        sorted_ips = dict(sorted(ip_link_info.items(), key=lambda d:len(d[1]), reverse=True))
        for (_ip, links) in sorted_ips.items():
            wf.write('{}[{}]\n'.format(_ip, links))
    with open('missinglink' + suffix, 'w') as wf:
        wf.write('\n'.join(list(bgp_links.difference(trace_links))))
    # print('here:', end='')
    # print(ip_accur_info['77.67.76.34'])
    os.system('cat ' + 'trace_stat' + suffix)
    ip_accur_info.clear()
    bgp_path_info.clear()
    ClearIxpAsSet()

def GetBgpByPrefix(filename, bgp_by_prefix_dict): #从原始格式中提取数据
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
        as_path = CompressBGPPath(elems[2])
        for prefix in prefix_list:
            if prefix not in bgp_by_prefix_dict:
                bgp_by_prefix_dict[prefix] = []
            if as_path not in bgp_by_prefix_dict[prefix]:
                bgp_by_prefix_dict[prefix].append(as_path)
        curline = rf.readline()
    rf.close()

from find_vp_v2 import CompressBGPPathListAndTestLoop
def CheckBGPPathLegal(curline):
    if curline.__contains__('{'): #has set
        return False
    path_list = curline.split(' ')
    has_private_asn = False
    for private_asn in range(64512, 65536):
        if str(private_asn) in path_list:
            has_private_asn = True
            break
    if has_private_asn:
        return False
    comp_path_list = CompressBGPPathListAndTestLoop(path_list)
    if not comp_path_list: #has loop
        return False
    return True

def GetLegalPaths(paths):
    legal_paths = []
    for path in paths:
        if CheckBGPPathLegal(path):
            legal_paths.append(path)
    return legal_paths

def CheckTwoBGPMethods():
    bgp_dict1 = {}
    GetBgpByPrefix('/mountdisk1/ana_c_d_incongruity/rib_data/bgpdata/back/bgp_7575_20190215', bgp_dict1)
    bgp_dict2 = {}
    InitBGPPathInfo('/mountdisk1/ana_c_d_incongruity/rib_data/bgpdata/bgp_198.32.176.177_20190215', bgp_dict2)
    len1 = len(bgp_dict1)
    len2 = len(bgp_dict2)
    for (prefix, paths) in bgp_dict1.items():
        if prefix not in bgp_dict2.keys():
            legal_paths = GetLegalPaths(paths)
            if len(legal_paths) > 0:
                print('[+]' + prefix + ':' + ','.join(legal_paths))
        else:            
            if sorted(paths) != sorted(bgp_dict2[prefix][0]):
                legal_paths = GetLegalPaths(paths)
                if sorted(legal_paths) != sorted(bgp_dict2[prefix][0]):
                    print('[*1]' + prefix + ':' + ','.join(paths))
                    print('[*2]' + prefix + ':' + ','.join(bgp_dict2[prefix][0]))
    for (prefix, val) in bgp_dict2.items():
        if prefix not in bgp_dict1.keys():
            print('[-]' + prefix + ':' + ','.join(val[0]))

def FindIncongPrefix():
    bgp_path_info = {}
    InitBGPPathInfo('/mountdisk1/ana_c_d_incongruity/rib_data/bgpdata/bgp_203.181.248.168_20190115', bgp_path_info)
    data_compare = ''
    with open('/mountdisk1/ana_c_d_incongruity/tmp_out_my_anatrace/nrt-jp_20190115/ribs/final_ab', 'r') as rf:
        data_compare = rf.read()
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/nrt-jp/rib_based/mm_nrt-jp.20190115', 'r') as rf:
        data = rf.read()
        find_res = re.findall(r'\[(.*?)\]', data)
        for elem in find_res:
            prefix = GetBGPPath_Or_OriASN(bgp_path_info, elem, 'get_prefix')
            if not prefix in data_compare:
                print(elem + ',' + prefix)

def StatIP(spec_data_name):
    ip_accur_info = {}
    with open('ipaccur_' + spec_data_name + '.json', 'r') as rf:
        ip_accur_info = json.load(rf)
    stat_info = {'succ': {}, 'fail': {}, 'other': {}, 'unmap': {}, 'ixp_as': {}}
    span = {'succ': 0, 'fail': 0, 'other': 0, 'unmap': 0, 'ixp_as': 0}
    mismatch_count = 0
    
    # mode_info = {'match': 0, 'mismatch': 1, 'partial_match': 2, 'unmap': 3}
    # ip_accur_info[_ip] = [[0, 0.0], [0, 0.0], [0, 0.0], [0, 0.0]]
    for (_ip, val) in ip_accur_info.items():
        if val[3][0] > 0: #unmap
            stat_info['unmap'][_ip] = [val[3][0], val[3][1] / val[3][0]]
            span['unmap'] += val[3][0]
            continue
        if val[4][0] > 0: #ixp_as
            stat_info['ixp_as'][_ip] = [val[4][0], val[4][1] / val[4][0]]
            span['ixp_as'] += val[4][0]
            continue
        (match, mismatch, partial_match) = (val[0][0], val[1][0], val[2][0])
        tmp_total = match + mismatch + partial_match
        if mismatch > 0:
            mismatch_count += 1
        pos_avg = (val[0][1] + val[1][1] + val[2][1]) / tmp_total
        if (match / tmp_total) > ((mismatch / tmp_total) * 10):
            stat_info['succ'][_ip] = [tmp_total, pos_avg]
            span['succ'] += tmp_total
        elif (mismatch / tmp_total) > ((match / tmp_total) * 10):
            stat_info['fail'][_ip] = [tmp_total, pos_avg]
            span['fail'] += tmp_total
        elif (partial_match / tmp_total) > ((match / tmp_total) * 10):
            stat_info['fail'][_ip] = [tmp_total, pos_avg]
            span['fail'] += tmp_total
        else:
            stat_info['other'][_ip] = [tmp_total, pos_avg]
            span['other'] += tmp_total
    count = {}
    for _type in stat_info.keys():
        count[_type] = len(stat_info[_type])
    total_count = sum(count.values())
    
    with open('ip_stat_' + spec_data_name, 'w') as wf:
        for (_type, val) in count.items():
            if val > 0:
                wf.write('%s: %.2f(%d), average span: %d\n' %(_type, val / total_count, val, span[_type] / val))
            else:
                wf.write('%s: 0.0, average span: #\n' %_type)
        wf.write('total_count: %d\n' %total_count)
    os.system('cat ip_stat_' + spec_data_name)
    for _type in stat_info.keys():
        stat_info[_type] = dict(sorted(stat_info[_type].items(), key=lambda d:d[1][0], reverse=True))
    with open('ipclass_' + spec_data_name + '.json', 'w') as wf:
        json.dump(stat_info, wf, indent=1)  # 写为多行

def StatIP_StripDstIP(spec_data_name):
    ipaccur_info = {}
    with open('ipaccur_' + spec_data_name + '.json', 'r') as rf:
        ipaccur_info = json.load(rf)
    stat_info = {}
    with open('ipclass_' + spec_data_name + '.json', 'r') as rf:
        stat_info = json.load(rf)
    stat_info_modi = {}
    ipaccur_info_modi = {}
    sub_stat_info = {'rigor_succ': {}}
    count = {}
    span = {'succ': 0, 'fail': 0, 'other': 0, 'unmap': 0, 'ixp_as': 0}
    multi_occur_ip_num = 0
    #stat_info[_type][_ip] = [span, pos_avg]
    for (_type, info) in stat_info.items():
        stat_info_modi[_type] = {}
        for (_ip, sub_info) in info.items():
            if _ip.split('.')[-1] == '1' and sub_info[0] == 1 and sub_info[1] == 1:
                continue
            if sub_info[0] > 1: multi_occur_ip_num += 1
            stat_info_modi[_type][_ip] = sub_info
            ipaccur_info_modi[_ip] = ipaccur_info[_ip]
            span[_type] += sub_info[0]
            if _type == 'succ':
                if len(ipaccur_info_modi[_ip][0][2]) > 2 and len(ipaccur_info_modi[_ip][0][2]) > 2 * (len(ipaccur_info_modi[_ip][1][2]) + len(ipaccur_info_modi[_ip][2][2])):
                    sub_stat_info['rigor_succ'][_ip] = len(ipaccur_info_modi[_ip][0][2])
        count[_type] = len(stat_info_modi[_type])
    total = sum(count.values())
    with open('ipclass_nodstip_' + spec_data_name + '.json', 'w') as wf:
        json.dump(stat_info_modi, wf, indent=1)    
    with open('ipaccur_nodstip_' + spec_data_name + '.json', 'w') as wf:
        json.dump(ipaccur_info_modi, wf, indent=1)
    stat_filename = 'ip_stat_nodstip_' + spec_data_name
    #print(stat_filename)
    if total > 0:
        with open(stat_filename, 'w') as wf:
            for (_type, val) in count.items():
                if val > 0:
                    wf.write('%s: %.2f(%d), average span: %d\n' %(_type, val / total, val, span[_type] / val))
                else:
                    wf.write('%s: 0.0, average span: #\n' %_type)
            wf.write('rigor_succ rate: {}\n'.format(len(sub_stat_info['rigor_succ']) / total))
            wf.write('total_count: %d\n' %total)
            wf.write('multi_occur_ips: %.2f(%d)\n' %(multi_occur_ip_num / total, total))
    os.system('cat ' + stat_filename)

def PerTask(bgp_dir, vp, date, trace_filename, filter_dsts):
    #tracevp_bgpvp_info = {'ams-nl': '80.249.208.34', 'jfk-us': '198.32.160.61', 'sjc2-us': '64.71.137.241', 'syd-au': '45.127.172.46', 'zrh2-ch': '109.233.180.32', 'nrt-jp': '203.181.248.168'}
    tracevp_bgpvp_info = {'ams-nl': '80.249.208.34', 'jfk-us': '198.32.160.61', 'sjc2-us': '64.71.137.241', 'syd-au': '45.127.172.46', 'nrt-jp': '203.181.248.168', 'sao-br': '187.16.217.17'}
    #tracevp_as_info = {'ams-nl': '1103', 'jfk-us': '6939', 'sjc2-us': '6939', 'syd-au': '7575', 'zrh2-ch': '34288', 'nrt-jp': '7660'}
    tracevp_as_info = {'ams-nl': '1103', 'jfk-us': '6939', 'sjc2-us': '6939', 'syd-au': '7575', 'nrt-jp': '7660', 'sao-br': '22548'}
    
    if filter_snd_mm:
        os.chdir('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/' + vp + '/' + map_method + '_filtersndmm/')
    else:
        os.chdir('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/' + vp + '/' + map_method + '/')
    if not os.path.exists(bgp_dir + 'bgp_' + tracevp_bgpvp_info[vp] + '_' + date):
        print(bgp_dir + 'bgp_' + tracevp_bgpvp_info[vp] + '_' + date)
        return
    CompareCD(bgp_dir + 'bgp_' + tracevp_bgpvp_info[vp] + '_' + date, trace_filename, tracevp_as_info[vp], filter_dsts)
    #if not os.path.exists('stat_' + ipaccur_filename) or os.path.getsize('stat_' + ipaccur_filename) == 0:
    spec_data_name = vp + '.' + date
    StatIP(spec_data_name)
    StatIP_StripDstIP(spec_data_name)

def debug_1():
    bgp_path_info = {}
    InitPref2ASInfo_2('/mountdisk1/ana_c_d_incongruity/rib_data/rv_prefix2as/20220726.pfx2as', bgp_path_info)
    #InitPref2ASInfo('/mountdisk1/ana_c_d_incongruity/out_ip2as_data/20220726.ip2as.prefixes', bgp_path_info)
    with open('/mountdisk2/collector_vps/c_ips_20220726.json', 'r') as rf:
        data = json.load(rf)
    ip_nextasns = {}
    with open('/mountdisk2/collector_vps/c_ips_20220726.json', 'r') as rf:
        ip_nextasns = json.load(rf)
    ip4s = {ip for val in data.values() for ip in val if ip.__contains__('.')}
    ip6s = {ip for val in data.values() for ip in val if ip.__contains__(':')}
    print(len(ip4s))
    print(len(ip6s))
    asns = set()
    for ip in ip4s:
        #res = GetBGPPath_Or_OriASN(bgp_path_info, ip, 'get_all_2')
        # if res and res[1]:
        #     for asn in res[1].split('_'):
        #         asns.add(asn)
        asn = GetBGPPath_Or_OriASN(bgp_path_info, ip, 'get_orias_2')
        asns.add(asn)            
    print(len(asns))

def debug_9():
    vp_c_asns = {}
    with open('/mountdisk2/collector_vps/ip_c_asns_2.json', 'r') as rf:
        vp_c_asns = json.load(rf)
    concerns = set()
    for vp, val in vp_c_asns.items():
        tmp = set()
        for c, asns in val.items():
            tmp = tmp | set(asns)
        #if len(tmp) > 1 and vp.__contains__('.'):
        if len(tmp) == 1 and vp.__contains__('.'):
            concerns.add(vp)
    bgp_path_info = {}
    InitPref2ASInfo('/mountdisk1/ana_c_d_incongruity/out_ip2as_data/20220726.ip2as.prefixes', bgp_path_info)
    #prefs = set()
    #asns = set()
    asn_ips = defaultdict(list)
    err_cnt = 0
    for tmp in concerns:
        res = GetBGPPath_Or_OriASN(bgp_path_info, tmp, 'get_all_2')
        #prefs.add(res[0])
        #asns = asns | set(res[1].split('_'))
        if not res[1]:
            err_cnt += 1
            print(tmp)
        #elif not res[1].startswith('-'):
        else:
            asn_ips[res[1]].append(tmp)
    # print(len(concerns))
    # print(len(prefs))
    # print(len([asn for asn in asns if int(asn) > 0]))
    print(err_cnt)
    print(len(asn_ips))
    with open('/mountdisk2/collector_vps/asn_ips.json', 'w') as wf:
        json.dump(asn_ips, wf, indent=1)

def get_common_vp():
    bgp_asn_ips = {}
    with open('/mountdisk2/collector_vps/asn_ips.json', 'r') as rf:
        bgp_asn_ips = json.load(rf)
    atlas_asn_ips = {}
    with open('/mountdisk2/atlas/builtin_src.json', 'r') as rf:
        atlas_asn_ips = json.load(rf)
    common_asns = set(bgp_asn_ips.keys()) & set(atlas_asn_ips.keys())
    print(len(common_asns))
    #print(common_asns)
    common_asn_ips = {}
    for asn in common_asns:
        common_asn_ips[asn] = [bgp_asn_ips[asn], atlas_asn_ips[asn]]
        for ip in bgp_asn_ips[asn]:
            #os.system('nslookup %s >> dns.%s.bgp' %(ip, asn))
            os.system('nslookup %s' %ip)
        # for ip in atlas_asn_ips[asn]:
        #     os.system('nslookup %s >> dns.%s.atlas' %(ip, asn))
    print(1)
    # bgp_ixp_num = 0
    # for asn, ips in bgp_asn_ips.items():
    #     if asn.startswith('-'):
    #         bgp_ixp_num += len(ips)
    # atlas_ixp_num = 0
    # atlas_ip_num = 0
    # for asn, ips in atlas_asn_ips.items():
    #     if asn.startswith('-'):
    #         atlas_ixp_num += len(ips)
    #     atlas_ip_num += len(ips)
    # print(bgp_ixp_num)
    # print(atlas_ixp_num)
    # print(atlas_ip_num)

def debug_11():
    bgp_asn_dns = {}
    with open('/mountdisk2/collector_vps/asn_ips_nds.json', 'r') as rf:
        bgp_asn_dns = json.load(rf)
    atlas_asn_ips = {}
    with open('/mountdisk2/atlas/builtin_src.json', 'r') as rf:
        atlas_asn_ips = json.load(rf)
    #cnt = 0
    for asn in bgp_asn_dns.keys():
        #cnt += len(atlas_asn_ips[asn])
        for ip in atlas_asn_ips[asn]:
            os.system('nslookup %s' %ip)
    #print(cnt)

def debug_10():
    asn_ips = {}
    # with open('/mountdisk2/collector_vps/asn_ips.json', 'r') as rf:
    with open('/mountdisk2/atlas/builtin_src.json', 'r') as rf:
        asn_ips = json.load(rf)
    ip_asn = {}
    for asn, ips in asn_ips.items():
        for ip in ips:
            ip_asn[ip] = asn
    asn_dns = defaultdict(defaultdict)
    #with open('raw_bgp_vp_dns', 'r') as rf:
    with open('raw_atlas_vp_dns', 'r') as rf:
        for line in rf:
            elems = line.strip('\n').split(' ')
            arpa = elems[0]
            tmps = arpa.split('.')
            ip = tmps[3] + '.' + tmps[2] + '.' + tmps[1] + '.' + tmps[0]
            if ip not in ip_asn.keys():
                print('err: %s' %ip)
            asn_dns[ip_asn[ip]][ip] = elems[-1]
    #with open('/mountdisk2/collector_vps/bgp_asn_ips_nds.json', 'w') as wf:
    with open('/mountdisk2/atlas/atlas_asn_ips_nds.json', 'w') as wf:
        json.dump(asn_dns, wf, indent=1)

def debug_12():
    atlas_asn_ips_nds = {}
    with open('/mountdisk2/atlas/atlas_asn_ips_nds.json', 'r') as rf:
        atlas_asn_ips_nds = json.load(rf)
    bgp_asn_ips_nds = {}
    with open('/mountdisk2/collector_vps/bgp_asn_ips_nds.json', 'r') as rf:
        bgp_asn_ips_nds = json.load(rf)
    data = {}
    for asn, atlas_val in atlas_asn_ips_nds.items():
        bgp_val = bgp_asn_ips_nds[asn]
        data[asn] = [bgp_val, atlas_val]
    with open('/mountdisk2/collector_vps/common_asn_bgp-atlas_asn_ips_nds.json', 'w') as wf:
        json.dump(data, wf, indent=1)

def main_func():
    # db_fn = '/mountdisk1/ana_c_d_incongruity/out_bdrmapit/ori_bdr/bdrmapit_nrt-jp_20210316.db'
    # print(db_fn)
    # ConnectToBdrMapItDb(db_fn)
    # ConstrBdrCache()
    # c = 0
    # t = 0
    # with open('/mountdisk1/ana_c_d_incongruity/hoiho/out/202103.hoiho_small.csv', 'r') as rf:
    #     for line in rf:
    #         addr, asn = line.strip('\n').split(',')
    #         if not asn.isdigit():
    #             continue
    #         bdrmap = GetIp2ASFromBdrMapItDb(addr)
    #         if bdrmap != asn:
    #             c += 1
    #         t += 1
    # CloseBdrMapItDb()
    # print(c)
    # print(t)
    # return
    
    
    if len(sys.argv) > 1:
    #if False:
        # CheckTwoBGPMethods()
        #FindIncongPrefix()        
        tracevp_bgpvp_info = {'ams-nl': '80.249.208.34', 'jfk-us': '198.32.160.61', 'sjc2-us': '64.71.137.241', 'syd-au': '198.32.176.177', 'per-au': '198.32.176.177', 'zrh2-ch': '109.233.180.32', 'nrt-jp': '203.181.248.168'}
        bgp_path_info = {}
        vp = sys.argv[1]
        date = sys.argv[2]
        mode = 'get_oriasn_coa'
        if sys.argv[3] == 'a':
            mode = 'get_oriasn_outip2as'
        elif sys.argv[3] == 'v':
            mode = 'get_bgp_all'
            vp = sys.argv[5]
        elif sys.argv[3] == 'p':
            mode = 'check_peer_addr'
        elif sys.argv[3] == 'b':
            mode = 'get_oriasn_bdrmapit'
        _ip = sys.argv[4]

        if mode == 'get_bgp':
            InitBGPPathInfo('/mountdisk1/ana_c_d_incongruity/rib_data/bgpdata/bgp_%s_%s' %(tracevp_bgpvp_info[vp], date), bgp_path_info)
            print(GetBGPPath_Or_OriASN(bgp_path_info, _ip, 'get_all'))
        elif mode == 'get_bgp_all':
            InitBGPPathInfo('/mountdisk1/ana_c_d_incongruity/rib_data/bgpdata/bgp_%s_%s' %(tracevp_bgpvp_info[vp], date), bgp_path_info)
            print(GetBGPPath_Or_OriASN(bgp_path_info, _ip, 'get_all_prefixes'))
            #print(GetBGPPath_Or_OriASN(bgp_path_info, _ip, 'get_path'))
        elif mode == 'get_oriasn_outip2as':
            InitPref2ASInfo('/mountdisk1/ana_c_d_incongruity/out_ip2as_data/%s.ip2as.prefixes' %date, bgp_path_info)
            print(GetBGPPath_Or_OriASN(bgp_path_info, _ip, 'get_all_2'))
            #print(GetBGPPath_Or_OriASN(bgp_path_info, _ip, 'get_orias_2'))
        elif mode == 'get_all_oriasn_outip2as':
            InitPref2ASInfo('/mountdisk1/ana_c_d_incongruity/out_ip2as_data/%s.ip2as.prefixes' %date, bgp_path_info)
            print(GetBGPPath_Or_OriASN(bgp_path_info, _ip, 'get_all_prefixes_2'))    
        elif mode == 'get_oriasn_coa':
            InitPref2ASInfo_2('/mountdisk1/ana_c_d_incongruity/rib_data/rv_prefix2as/' + date + '.pfx2as', bgp_path_info)        
            print(GetBGPPath_Or_OriASN(bgp_path_info, _ip, 'get_orias_2')) 
        elif mode == 'get_oriasn_bdrmapit':
            db_fn = '/mountdisk1/ana_c_d_incongruity/out_bdrmapit/sxt_bdr/bdrmapit_%s_%s.db' %(vp, date)
            print(db_fn)
            ConnectToBdrMapItDb(db_fn)
            ConstrBdrCache()
            print(GetIp2ASFromBdrMapItDb(_ip))
            CloseBdrMapItDb()
            InitBdrCache()
        elif mode == 'check_peer_addr':
            GetIxpPfxDict_2(int(date[:4]), int(date[4:6]))
            print(IsIxpIp(_ip))            
            ClearIxpPfxDict()
        elif mode == 'get_peer_asn':
            GetIxpPfxDict_2(int(date[:4]), int(date[4:6]))
            print(GetPeerASNByIp(_ip))       
            ClearIxpPfxDict()
        # with open('test3', 'w') as wf:
        #     with open('test2', 'r') as rf:
        #         for data in rf.readlines():
        #             _ip = data.split('(')[0][:-1]
        #             if IsIxpIp(_ip):
        #                 print(data, end='')
        #             else:
        #                 wf.write(data)
        # ClearIxpPfxDict()
        
        #GetSibRel_2(2018, 2)
        # with open('test4', 'w') as wf:
        #     with open('test3', 'r') as rf:
        #         for data in rf.readlines():
        #             find_res = re.findall(r'AS1: (.*?), AS2: (.*?)\)', data)
        #             #print(find_res)
        #             (as1, as2) = (find_res[0][0], find_res[0][1])
        #             if IsSib_2(as1, as2):
        #                 print(data, end='')
        #             else:
        #                 wf.write(data)

        # with open('test4', 'r') as rf:
        #     for data in rf.readlines():
        #         find_res = re.findall(r'AS1: (.*?), AS2: (.*?)\)', data)
        #         #print(find_res)
        #         (as1, as2) = (find_res[0][0], find_res[0][1])
        #         print(as1 + ':' + GetOrgOfAS(as1) + '\t\t' + as2 + ':' + GetOrgOfAS(as2))
        
        # TestGetSpecIP('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/ams-nl/rib_based/', sel_num)
        pass
    else:
        os.chdir('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/')
        tracevp_bgpvp_info = {'ams-nl': '80.249.208.34', 'sjc2-us': '64.71.137.241', 'syd-au': '198.32.176.177', 'nrt-jp': '203.181.248.168', 'sao-br': '187.16.217.17'}
        #tracevp_bgpvp_info = {'sjc2-us': '64.71.137.241'}
        #tracevp_as_info = {'ams-nl': '1103', 'jfk-us': '6939', 'sjc2-us': '6939', 'syd-au': '7575', 'zrh2-ch': '34288', 'nrt-jp': '7660'}
        tracevp_as_info = {'ams-nl': '1103', 'sjc2-us': '6939', 'syd-au': '7575', 'nrt-jp': '7660', 'sao-br': '22548'}
        #bgp_dir = '/mountdisk1/ana_c_d_incongruity/rib_data/compress_files/temp_work5/'
        bgp_dir = '/mountdisk1/ana_c_d_incongruity/rib_data/bgpdata/'
        #bgp_dir = '/mountdisk1/ana_c_d_incongruity/rib_data/compress_files/dir_ribs/ams_fst_bfk/'
        #trace_dir = '/mountdisk1/ana_c_d_incongruity/traceroute_data/back/temp5/'
        trace_dir = '/mountdisk1/ana_c_d_incongruity/traceroute_data/result/'
        print(map_method)
        paras = []
        filter_dsts = None
        if filter_snd_mm:
            with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/filter_dsts', 'r') as rf:
                filter_dsts = json.load(rf)                            
        #for vp in tracevp_bgpvp_info.keys():
        for vp in ['sao-br']:
            # if vp != 'ams-nl':
            #     continue
            task_list = []
            if not os.path.exists(vp):
                os.mkdir(vp)
            
            if filter_snd_mm:
                if not os.path.exists(vp + '/' + map_method + '_filtersndmm/'):
                    os.mkdir(vp + '/' + map_method + '_filtersndmm/')
                os.chdir(vp + '/' + map_method + '_filtersndmm/')
            else:
                if not os.path.exists(vp + '/' + map_method + '/'):
                    os.mkdir(vp + '/' + map_method + '/')
                os.chdir(vp + '/' + map_method + '/')
            trace_filenames = glob.glob(r'%strace_%s*' %(trace_dir, vp))
            for trace_filename in trace_filenames:
                date = trace_filename[trace_filename.rindex('.') + 1:]
                if date != '20220215':
                    continue
                if map_method == 'ml_map':
                    if filter_snd_mm:
                        if not os.path.exists('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/model_modified_ip_mappings_%s_%s.json' %(vp, vp, date)):
                            continue
                    else:
                        if not os.path.exists('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map/model_modified_ip_mappings_%s_%s.json' %(vp, vp, date)):
                            continue
                cur_filter_dsts = set(filter_dsts[date][vp]) if filter_dsts and date in filter_dsts and vp in filter_dsts[date] else set()
                #print(date[:6])
                if map_method == 'snmp_bdr' and date[:4] != '2021':
                    continue
                ipstat_filename = 'ip_stat_' + vp + '.' + date
                if not os.path.exists(ipstat_filename) or os.path.getsize(ipstat_filename) == 0:
                #if True:
                    print(trace_filename)
                    #PerTask(bgp_dir, vp, date, trace_filename)
                    # task = Process(target=PerTask, args=(bgp_dir, vp, date, trace_filename, cur_filter_dsts))
                    # task_list.append(task)
                    # task.start()
                    paras.append((bgp_dir, vp, date, trace_filename, cur_filter_dsts))
                    #PerTask(bgp_dir, vp, date, trace_filename, ipaccur_filename)
                #break
            os.chdir('../../')
        pool = Pool(processes=g_parell_num)
        pool.starmap(PerTask, paras)
        pool.close()
        pool.join()


if __name__ == '__main__':
    main_func()
