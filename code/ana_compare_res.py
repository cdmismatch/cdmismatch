
import os
import json
import socket
import struct
import pandas as pd
import glob
from collections import Counter, defaultdict
from multiprocessing import Process, Pool
from traceutils.bgp.bgp import BGP
from compare_cd import InitBGPPathInfo, InitPref2ASInfo, CompressTrace, GetBGPPath_Or_OriASN, CompareCD_PerTrace, FindTraceHopInBGP, \
                        CheckAbHopCountAndMalPos, InitPref2ASInfo_2
from utils_v2 import GetIxpPfxDict_2, IsIxpAs, IsIxpIp, GetIxpAsSet
from rect_bdrmapit import CheckSiblings
from find_vp_v2 import CompressBGPPath, TestLoopInTrace
from get_ip2as_from_bdrmapit import ConnectToBdrMapItDb, GetIp2ASFromBdrMapItDb, CloseBdrMapItDb, InitBdrCache, \
                                    ConstrBdrCache, GetBdrCache
import numpy as np
import datetime

tracevp_bgpvp_info = {'ams-nl': '80.249.208.34', 'jfk-us': '198.32.160.61', 'sjc2-us': '64.71.137.241', 'syd-au': '45.127.172.46', 'zrh2-ch': '109.233.180.32', 'nrt-jp': '203.181.248.168', 'sao-br': '187.16.217.17'}
g_parell_num = os.cpu_count()

def drop_stars_in_trace(trace_list):
    new_trace_list = []
    for hop in trace_list:
        if hop == '*' or hop == '?' or hop.startswith('<') or (new_trace_list and new_trace_list[-1] == hop):
            continue
        new_trace_list.append(hop)
    return new_trace_list

def classify_mm_trace(filename, vp, date):
    vp_rib_info = {}
    coa_rib_info = {}
    InitBGPPathInfo('/mountdisk1/ana_c_d_incongruity/rib_data/bgpdata/bgp_%s_%s' %(tracevp_bgpvp_info[vp], date), vp_rib_info)
    InitPref2ASInfo('/mountdisk1/ana_c_d_incongruity/out_ip2as_data/%s.ip2as.prefixes' %(date), coa_rib_info)
    GetIxpPfxDict_2(int(date[:4]), int(date[4:6]))
    checksiblings = CheckSiblings(date)
    ip_accur_info = {}

    data = {'diff_path': {}, 'single_mm': {}, 'others': []}
    data['diff_path'] = {'first_exit_mm': {'moas': [], 'others': []}, 'middle_mm': {'moas': [], 'others': []}}
    data['single_mm'] = {'ixp': [], 'sibling': [], 'neigh': [], 'others': []}
    total = 0
    with open(filename, 'r') as rf:
        lines = [rf.readline() for _ in range(3)]
        while lines[0]:
            #print(lines[0])
            total += 1
            (dst_ip, trace) = lines[0][1:].strip('\n').split(']')
            if dst_ip == '179.61.109.1':
                print('')
            bgp_path = lines[1].strip('\n').strip('\t')
            bgp_list = bgp_path.split(' ')
            ip_list = lines[2].strip('\n').split(']')[-1].split(' ')
            (trace_list, trace_to_ip_info, _) = CompressTrace(trace.split(' '), ip_list, 'n')
            #trace_list1 = drop_stars_in_trace(trace_list)
            coa_asn = GetBGPPath_Or_OriASN(coa_rib_info, dst_ip, 'get_orias_2')
            vp_asn = GetBGPPath_Or_OriASN(vp_rib_info, dst_ip, 'get_orias')
            # #1. check extra_tail_diff_orias
            # if coa_asn != '?' and coa_asn != vp_asn:
            #     #print('coa_asn:{}, vp_asn：{}'.format(coa_asn, vp_asn))
            #     last_bgp_hop = bgp_path.split(' ')[-1]
            #     if last_bgp_hop in trace_list:
            #         stop_index = trace_list.index(last_bgp_hop)
            #         # print(bgp_path)
            #         # print(trace_list[:stop_index + 1])
            #         # print(lines[2].strip('\n').split(' '))
            #         if CompareCD_PerTrace(bgp_path, trace_list[:stop_index + 1], ip_list, trace_to_ip_info, ip_accur_info):
            #             data['extra_tail_diff_orias'] += lines
            #             lines = [rf.readline() for _ in range(3)]
            #             continue
            #2. check diff path
            comp_trace_list = drop_stars_in_trace(trace_list)
            # for trace_hop in comp_trace_list[::-1]:
            #     if trace_hop in bgp_list:
            #         bgp_list = bgp_list[:bgp_list.index(trace_hop) + 1]
            #         break
            if comp_trace_list[-1] in bgp_list:
                bgp_list = bgp_list[:bgp_list.index(comp_trace_list[-1]) + 1]
            mm_trace_hops = {x for x in comp_trace_list if x not in bgp_list}
            mm_trace_hops_num = len(mm_trace_hops)
            mm_bgp_hops = {x for x in bgp_list if x not in comp_trace_list}
            mm_bgp_hops_num = len(mm_bgp_hops)
            if mm_trace_hops_num >= 2 or mm_trace_hops_num > len(comp_trace_list) / 2 or \
                mm_bgp_hops_num >= 2 or mm_bgp_hops_num > len(bgp_list) / 2:
                if (len(bgp_list) == 1) or (comp_trace_list[1] not in bgp_list and bgp_list[1] not in comp_trace_list): #一开始即不一样
                    if coa_asn != vp_asn: data['diff_path']['first_exit_mm']['moas'] += lines
                    else: data['diff_path']['first_exit_mm']['others'] += lines
                else:
                    if coa_asn != vp_asn: data['diff_path']['middle_mm']['moas'] += lines
                    else: data['diff_path']['middle_mm']['others'] += lines
                lines = [rf.readline() for _ in range(3)]
                continue
            #3. single mm hop, might-be mapping errors
            if (mm_trace_hops_num + mm_bgp_hops_num) == 1 or (mm_trace_hops_num == 1 and mm_bgp_hops_num == 1):
                mm_bgp_hop = list(mm_bgp_hops)[0] if mm_bgp_hops_num == 1 else None
                if mm_trace_hops_num == 1:
                    mm_trace_hop = list(mm_trace_hops)[0]
                    mm_trace_index = comp_trace_list.index(mm_trace_hop)
                    neigh_hops = {comp_trace_list[mm_trace_index - 1] if mm_trace_index > 0 else None, comp_trace_list[mm_trace_index + 1] if mm_trace_index < len(comp_trace_list) - 1 else None}
                    if None in neigh_hops: neigh_hops.remove(None)
                    mm_ip_hops = trace_to_ip_info[mm_trace_hop]
                    #print(mm_ip_hops)
                    if IsIxpAs(mm_trace_hop) or all(IsIxpIp(_ip) for _ip in mm_ip_hops):
                        data['single_mm']['ixp'] += lines
                    elif any(checksiblings.check_sibling(neigh, mm_trace_hop) for neigh in neigh_hops) or \
                        (mm_bgp_hop and checksiblings.check_sibling(mm_bgp_hop, mm_trace_hop)):
                        data['single_mm']['sibling'] += lines
                    elif any(checksiblings.bgp.rel(int(neigh), int(mm_trace_hop)) for neigh in neigh_hops) or \
                        (mm_bgp_hop and checksiblings.bgp.rel(int(mm_bgp_hop), int(mm_trace_hop))):
                        data['single_mm']['neigh'] += lines
                    else:
                        data['single_mm']['others'] += lines
                else:
                    mm_bgp_index = bgp_list.index(mm_bgp_hop)
                    neigh_hops = {bgp_list[mm_bgp_index - 1] if mm_bgp_index > 0 else None, bgp_list[mm_bgp_index + 1] if mm_bgp_index < len(bgp_list) - 1 else None}
                    if IsIxpAs(mm_bgp_hop):
                        data['single_mm']['ixp'] += lines
                    elif any(checksiblings.check_sibling(neigh, mm_bgp_hop) for neigh in neigh_hops):
                        data['single_mm']['sibling'] += lines
                    elif any(checksiblings.bgp.rel(int(neigh), int(mm_bgp_hop)) for neigh in neigh_hops):
                        data['single_mm']['neigh'] += lines
                    else:
                        data['single_mm']['others'] += lines
                lines = [rf.readline() for _ in range(3)]
                continue
            #4. others
            data['others'] += lines
            lines = [rf.readline() for _ in range(3)]
    
    if not os.path.exists('ana_compare_res/'): os.mkdir('ana_compare_res')
    count = {}
    for _type, val in data.items():
        with open('ana_compare_res/' + _type, 'w') as wf:
            if isinstance(val, list):
                wf.write(''.join(val))
                count[_type] = [len(val) / 3, len(val) / 3 / total]
            elif isinstance(val, dict):
                json.dump(val, wf, indent=1)
                count[_type] = [0, 0, {}]
                for subtype, subval in val.items():
                    if isinstance(subval, list):
                        count[_type][2][subtype] = [len(subval) / 3, len(subval) / 3 / total]
                        count[_type][0] += len(subval) / 3                        
                    elif isinstance(subval, dict):
                        count[_type][2][subtype] = [0, 0, {}]
                        for subsubtype, subsubval in subval.items():
                            count[_type][2][subtype][2][subsubtype] = [len(subsubval) / 3, len(subsubval) / 3 / total]
                            count[_type][2][subtype][0] += len(subsubval) / 3
                            count[_type][0] += len(subsubval) / 3
                        count[_type][2][subtype][1] = count[_type][2][subtype][0] / total
                count[_type][1] = count[_type][0] / total
    with open('ana_compare_res/' + filename + '_stat', 'w') as wf:
        json.dump(count, wf, indent=1)
    os.system('cat ana_compare_res/' + filename + '_stat')

def get_mm_ips_from_mm_trace(vp, date):
    mm_ip_trace = defaultdict(list)
    pm_ip_trace = defaultdict(list)
    with open('mm_' + vp + '.' + date, 'r') as rf:
        lines = [rf.readline() for _ in range(3)]
        while lines[0]:
            (mm_ips, pm_ips, _) = lines[2].split(']')
            for _ip in mm_ips[1:].split(','):
                mm_ip_trace[_ip].append(lines)
            for _ip in pm_ips[1:].split(','):
                pm_ip_trace[_ip].append(lines)
            lines = [rf.readline() for _ in range(3)]
    return mm_ip_trace, pm_ip_trace

def get_mm_trace_by_succ_ip(vp, date):
    classips = {}
    ipaccur = {}
    mm_ip_trace, pm_ip_trace = get_mm_ips_from_mm_trace(vp, date)
    with open('ipaccur_nodstip_' + vp + '.' + date + '.json', 'r') as rf:
        ipaccur = json.load(rf)
    with open('ipclass_nodstip_' + vp + '.' + date + '.json', 'r') as rf:
        classips = json.load(rf)
    mm_dst_ip_lines = {}
    for _ip in classips['succ'].keys():
        if ipaccur[_ip][1][0] > 0: #has failed cases in some traces
            for lines in mm_ip_trace[_ip]:
                dst_ip = lines[0].split(']')[0]
                if dst_ip not in mm_dst_ip_lines.keys():
                    mm_dst_ip_lines[dst_ip] = lines
    with open('dedu_succip_failedtrace', 'w') as wf:
        for lines in mm_dst_ip_lines.values():
            wf.write(''.join(lines))

def get_mm_trace_by_succ_ip_v2(vp, date, step, total=0):
    classips = {}
    # ipaccur = {}
    # mm_ip_trace, pm_ip_trace = get_mm_ips_from_mm_trace(vp, date)
    # with open('ipaccur_nodstip_' + vp + '.' + date + '.json', 'r') as rf:
    #     ipaccur = json.load(rf)
    succ_ip_fail_num = 0
    with open('ipclass_nodstip_' + vp + '.' + date + '.json', 'r') as rf:
        classips = json.load(rf)
    filename = 'mm_' + vp + '.' + date if step == 0 else '%d_remain.%s.%s' %(step, vp, date)
    wf_succip_failed = open('%d_succip_failed.%s.%s' %(step + 1, vp, date), 'w')
    wf_remain = open('%d_remain.%s.%s' %(step + 1, vp, date), 'w')
    first_stat = True if total == 0 else False
    with open(filename, 'r') as rf:
        lines = [rf.readline() for _ in range(3)]
        while lines[0]:
            (mm_ips, pm_ips, _) = lines[2].split(']')
            if any(mm_ip in classips['succ'].keys() and classips['succ'][mm_ip][0] > 1 for mm_ip in mm_ips[1:].split(',')):
                wf_succip_failed.write(''.join(lines))
                succ_ip_fail_num += 1
            #if any(mm_ip in classips['succ'].keys() for mm_ip in mm_ips[1:].split(',')):
            # for mm_ip in mm_ips[1:].split(','):
            #     if mm_ip in classips['succ'].keys():
            #         a = classips['succ'][mm_ip]
            #         if classips['succ'][mm_ip][0] > 1:
            #             wf_succip_failed.write(''.join(lines))
            #             succ_ip_fail_num += 1
            #         else:
            #             print('')
            else:
                wf_remain.write(''.join(lines))
            if first_stat: total += 1
            lines = [rf.readline() for _ in range(3)]
    wf_succip_failed.close()
    wf_remain.close()    
    update_stat_file('discrimin_stat.%s.%s' %(vp, date), step, 'succ_ip_fail_trace rate: {}\n'.format(succ_ip_fail_num / total))
    print('succ_ip_fail_trace rate: {}'.format(succ_ip_fail_num / total))
    return total

def get_extra_moas_trail_mm_trace(vp, date, step, total=0):
    vp_rib_info = {}
    coa_rib_info = {}
    InitBGPPathInfo('/mountdisk1/ana_c_d_incongruity/rib_data/bgpdata/bgp_%s_%s' %(tracevp_bgpvp_info[vp], date), vp_rib_info)
    InitPref2ASInfo('/mountdisk1/ana_c_d_incongruity/out_ip2as_data/%s.ip2as.prefixes' %(date), coa_rib_info)
    checksiblings = CheckSiblings(date)
    ip_accur_info = {}
    mm_ips = []
    pm_ips = []
    ip_remap = defaultdict(Counter) #没用，适配接口
    extra_tail_num = 0
    filename = 'mm_' + vp + '.' + date if step == 0 else '%d_remain.%s.%s' %(step, vp, date)
    wf_extra_moas_trail = open('%d_extra_moas_trail.%s.%s' %(step + 1, vp, date), 'w')
    wf_remain = open('%d_remain.%s.%s' %(step + 1, vp, date), 'w')
    first_stat = True if total == 0 else False
    stat_info = {}
    with open(filename, 'r') as rf:
        lines = [rf.readline() for _ in range(3)]
        while lines[0]:
            (dst_ip, trace) = lines[0][1:].strip('\n').split(']')
            bgp_path = lines[1].strip('\n').strip('\t')
            ip_list = lines[2].strip('\n').split(']')[-1].split(' ')
            (trace_list, trace_to_ip_info, _) = CompressTrace(trace.split(' '), ip_list, 'n')
            #trace_list1 = drop_stars_in_trace(trace_list)
            coa_asn = GetBGPPath_Or_OriASN(coa_rib_info, dst_ip, 'get_orias_2')
            #vp_asn = GetBGPPath_Or_OriASN(vp_rib_info, dst_ip, 'get_orias')
            vp_asn = bgp_path.split(' ')[-1]
            #print('{}, {}'.format(coa_asn, vp_asn))
            if coa_asn != '?' and coa_asn !='-1' and int(coa_asn) > 0 and int(coa_asn) < 0xFFFFFF and \
                int(vp_asn) > 0 and int(vp_asn) < 0xFFFFFF and \
                coa_asn != vp_asn and checksiblings.bgp.reltype(int(coa_asn), int(vp_asn)) == 2:
                #print('coa_asn:{}, vp_asn：{}'.format(coa_asn, vp_asn))
                last_bgp_hop = bgp_path.split(' ')[-1]
                if last_bgp_hop in trace_list:
                    stop_index = trace_list.index(last_bgp_hop)
                    if CompareCD_PerTrace(bgp_path, trace_list[:stop_index + 1], ip_list, trace_to_ip_info, ip_accur_info, mm_ips, pm_ips, ip_remap):
                        wf_extra_moas_trail.write(''.join(lines))
                        extra_tail_num += 1
                        lines = [rf.readline() for _ in range(3)]
                        if step == 0: total += 1
                        continue
            if first_stat: total += 1
            wf_remain.write(''.join(lines))
            lines = [rf.readline() for _ in range(3)]
    wf_extra_moas_trail.close()
    wf_remain.close()
    update_stat_file('discrimin_stat.%s.%s' %(vp, date), step, 'extra_tail rate: {}\n'.format(extra_tail_num / total))
    print('extra_tail rate: {}'.format(extra_tail_num / total))
    return total

def get_all_rib_paths(date, bgp_path_info):
    rib_dir = '/mountdisk1/ana_c_d_incongruity/rib_data/bgpdata/'
    for filename in os.listdir(rib_dir):
        if filename.endswith(date):
            InitBGPPathInfo(rib_dir + filename, bgp_path_info)

def trace_seg_in_any_rib(trace_list, dst_ip, bgp_path_info, ip_list, trace_to_ip_info):
    fst_trace_hop = trace_list[0]
    ip_accur_info = {} #没用，为了适配接口
    mm_ips = [] #没用，为了适配接口
    pm_ips = [] #没用，为了适配接口
    ip_remap = defaultdict(Counter) #没用，适配接口
    for bgp_path in GetBGPPath_Or_OriASN(bgp_path_info, dst_ip, 'get_path'):
        if fst_trace_hop in bgp_path:
            bgp_seg = bgp_path[bgp_path.index(fst_trace_hop):]
            if CompareCD_PerTrace(bgp_seg, trace_list, ip_list, trace_to_ip_info, ip_accur_info, mm_ips, pm_ips, ip_remap):
                return True
            trace_list.remove('$')
    return False

def get_all_arrival_paths_from_vp(vp, date, arrival_segs, asn_paths):
    GetIxpAsSet(date)
    bgp_path_info = {}
    InitBGPPathInfo('/mountdisk1/ana_c_d_incongruity/rib_data/bgpdata/bgp_' + tracevp_bgpvp_info[vp] + '_' + date, bgp_path_info)
    for pref, val in bgp_path_info.items():
        for path in val[0]:
            pos = path.find(' ')
            start = 0
            while pos != -1:
                asn_paths[path[start:pos]].add(path[:pos])
                arrival_segs.add(path[:pos])
                start = pos + 1
                pos = path.find(' ', pos + 1)
            asn_paths[path[start:]].add(path)
            arrival_segs.add(path)

def find_longest_head_mm_hop(comp_trace_list, arrival_segs):    
    last_hop = None
    while comp_trace_list:
        if ' '.join(comp_trace_list) in arrival_segs:
            #return last_hop
            return comp_trace_list[-1]
        last_hop = comp_trace_list[-1]
        comp_trace_list = comp_trace_list[:-1]
    return None

def find_longest_head_mm_hop_v2(comp_trace_list, bgp_path):    
    last_hop = None
    while comp_trace_list:
        if ' '.join(comp_trace_list) in bgp_path:
            #return last_hop
            return comp_trace_list[-1]
        last_hop = comp_trace_list[-1]
        comp_trace_list = comp_trace_list[:-1]
    return None

def update_stat_file(filename, line_index, data):
    lines = []
    if os.path.exists(filename):
        with open(filename, 'r') as rf:
            lines = rf.readlines()
            print(lines)
            print(line_index)
            if line_index >= len(lines):
                lines.append(data)
            else:
                lines[line_index] = data
    else:
        lines.append(data)
    with open(filename, 'w') as wf:
        wf.write(''.join(lines))

def filter_ixpips_in_mm_trace(vp, date, step, total=0):
    # classips = {}
    # with open('ipclass_nodstip_' + vp + '.' + date + '.json', 'r') as rf:
    #     classips = json.load(rf)
    GetIxpPfxDict_2(int(date[:4]), int(date[4:6]))
    #fail_ixp_ips = {_ip for _ip in classips['fail'].keys() if IsIxpIp(_ip)}
    ixp_fail_num = 0
    filename = 'mm_' + vp + '.' + date if step == 0 else '%d_remain.%s.%s' %(step, vp, date)
    wf_fail_ixpip = open('%d_ixpip_failed.%s.%s' %(step + 1, vp, date), 'w')
    wf_remain = open('%d_remain.%s.%s' %(step + 1, vp, date), 'w')
    with open(filename, 'r') as rf:
        lines = [rf.readline() for _ in range(3)]
        while lines[0]:
            (dst_ip, trace) = lines[0][1:].strip('\n').split(']')
            (mm_ips, pm_ips, ips) = lines[2].strip('\n').split(']')
            ip_list = ips.split(' ')
            mm_ips = mm_ips[1:].split(',')
            pm_ips = pm_ips[1:].split(',')
            ixp = True
            if mm_ips and any(not IsIxpIp(_ip) for _ip in mm_ips if _ip):
                ixp = False
            else:
                i = 0
                while i < len(pm_ips): 
                    if i < len(pm_ips) - 1 and ip_list.index(pm_ips[i + 1]) - ip_list.index(pm_ips[i]) == 1:
                        if pm_ips[i] and pm_ips[i + 1] and not IsIxpIp(pm_ips[i]) and not IsIxpIp(pm_ips[i + 1]):
                            ixp = False
                            break
                        i += 2
                    else:
                        if pm_ips[i] and not IsIxpIp(pm_ips[i]):
                            ixp = False
                            break
                        i += 1
            # elif pm_ips and any(not IsIxpIp(_ip) for _ip in pm_ips if _ip):
            #     ixp = False
            if ixp:
                wf_fail_ixpip.write(''.join(lines))
                ixp_fail_num += 1
            else:
                trace_list = trace.split( )
                ip_list = ips.split(' ')
                for i in range(0, len(ip_list)):
                    if ip_list[i] != '*' and IsIxpIp(ip_list[i]):
                        if not trace_list[i].startswith('<'):
                            trace_list[i] = '<' + trace_list[i] + '>'
                            pass
                lines[0] = '[' + dst_ip + ']' + ' '.join(trace_list) + '\n'
                wf_remain.write(''.join(lines))
            if step == 0: total += 1
            lines = [rf.readline() for _ in range(3)]
    wf_fail_ixpip.close()
    wf_remain.close()
    update_stat_file('discrimin_stat.%s.%s' %(vp, date), step, 'ixp fail rate: {}\n'.format(ixp_fail_num / total))
    print('ixp fail rate: {}'.format(ixp_fail_num / total))
    return total

def collect_one_date_all_bgp_paths(date):
    os.chdir('/mountdisk1/ana_c_d_incongruity/rib_data/all_collectors_one_date_bgpdata/' + date)
    if not os.path.exists('prefix_path.json'):
        prefix_paths = defaultdict(set)
        for filename in os.listdir('.'):
            # if not filename.endswith('gz'):
            #     continue
            print('begin ' + filename)
            os.system('bgpdump -M ' + filename + ' > test')
            if os.path.getsize('test') > 0:
                print('parse ' + filename)
                with open('test', 'r') as rf:
                    for line in rf:
                        if not line.startswith('TABLE_DUMP2'):
                            continue
                        #TABLE_DUMP2|12/15/20 00:00:00|B|202.249.2.169|2497|1.0.0.0/24|2497 13335|IGP
                        (_, date, _, _, _, prefix, path, _) = line.split('|')
                        prefix_paths[prefix].add(CompressBGPPath(path))
                os.system('rm -f test')
            print('{} done'.format(filename))
        for prefix, paths in prefix_paths.items():
            prefix_paths[prefix] = list(paths)
        with open('prefix_path.json', 'w') as wf:
            json.dump(prefix_paths, wf, indent = 1)
        return prefix_paths
    else:
        with open('prefix_path.json', 'r') as rf:
            prefix_paths = json.load(rf)
        return prefix_paths

# def get_dstasn_path_from_vp(vp, date, asn_paths):
#     bgp_path_info = {}
#     InitBGPPathInfo('/mountdisk1/ana_c_d_incongruity/rib_data/bgpdata/bgp_' + tracevp_bgpvp_info[vp] + '_' + date, bgp_path_info)
#     for pref, val in bgp_path_info.items():
#         for path in val[0]:
#             pos = path.find(' ')
#             start = 0
#             while pos != -1:
#                 asn_paths[path[start:pos]].add(path[:pos])
#                 start = pos + 1
#                 pos = path.find(' ', pos + 1)
#             asn_paths[path[start:]].add(path)

def get_parent_prefs(ip, prefs):
    res = set()
    ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton(ip))[0])
    for pref in prefs:
        if not pref.__contains__('.'):
            continue
        prefix, pref_len = pref.split('/')
        mask = ~(1 << (31 - pref_len))
        if str(socket.inet_ntoa(struct.pack('I',socket.htonl(ip_int & mask)))) == prefix:
            res.add(pref)
    return res

def get_dstasn_path_from_all_vps(prefix_paths, all_asn_paths):
    for pref, paths in prefix_paths.items():
        for path in paths:
            pos = path.find(' ')
            start = 0
            while pos != -1:
                all_asn_paths[path[start:pos]].add(path[:pos])
                start = pos + 1
                pos = path.find(' ', pos + 1)
            all_asn_paths[path[start:]].add(path)

def find_covered_ip(concerned_ips, pref):    
    res = set()
    prefix, pref_len = pref.split('/')
    mask = 0xFFFFFFFF - (1 << (32 - int(pref_len))) + 1
    pref_int = socket.ntohl(struct.unpack("I",socket.inet_aton(prefix))[0])
    for ip, ip_int in concerned_ips.items():
        if ip_int & mask == pref_int:
            res.add(ip)
    return res

def get_concerned_data_from_all_vps(prefix_paths, concerned_ips, pref2path, pref2asn, asn2path, existed_ip2pref):
    tmp_pref2ip = defaultdict(set)
    for ip, prefs in concerned_ips.items():
        for pref in prefs:
            tmp_pref2ip[pref].add(ip)
    all_concerned_prefixes = tmp_pref2ip.keys()
    print('all concerned prefixes num: {}'.format(len(all_concerned_prefixes)))
    # progress = 0
    # with open('/mountdisk1/ana_c_d_incongruity/rib_data/all_collectors_one_date_bgpdata/' + date + '/prefix_path_compress.json', 'r') as rf:
    #     #prefix_paths = json.load(rf)
    #     pref = None
    #     rf.readline()
    #     lines = rf.readlines(1000000)
    #     while lines:
    #         for line in lines:
    #             if line.startswith('}') or line[1] == ']':
    #                 continue
    #             if line.__contains__('['):                    
    #                 pref = line.split('"')[1]
    #                 if pref not in all_concerned_prefixes:
    #                     pref = None
    #                 else:
    #                     if progress % 10000 == 0: print(progress)
    #                     progress += 1
    #                     for ip in tmp_pref2ip[pref]:
    #                         existed_ip2pref[ip].add(pref)
    #                 continue
    #             if pref:
    #                 path = line.split('"')[1]
    #                 pref2path[pref].add(path)
    #                 dst_asn = path.split(' ')[-1]
    #                 pref2asn[pref].add(dst_asn)
    #         lines = rf.readlines(1000000)
    #         #if len(pref2path) > 10: break
    for pref, paths in prefix_paths.items():
        if pref not in all_concerned_prefixes:
            continue
        for ip in tmp_pref2ip[pref]:
            existed_ip2pref[ip].add(pref)
        for path in paths:
            pref2path[pref].add(path)
            dst_asn = path.split(' ')[-1]
            pref2asn[pref].add(dst_asn)
    print('parse prefix_path_compress.json fst turn done')

    concerned_asns = {asn for asns in pref2asn.values() for asn in asns}
    print('len of concerned_asns: {}'.format(len(concerned_asns)))
    # progress = 0
    # with open('/mountdisk1/ana_c_d_incongruity/rib_data/all_collectors_one_date_bgpdata/' + date + '/prefix_path_compress.json', 'r') as rf:
    #     #prefix_paths = json.load(rf)
    #     pref = None
    #     rf.readline()
    #     lines = rf.readlines(1000000)
    #     while lines:
    #         for line in lines:                
    #             if line.startswith('}') or line[1] == ']' or line.__contains__('['):
    #                 continue
    #             path = line.split('"')[1]
    #             asn = path.split(' ')[-1]
    #             if asn in concerned_asns:
    #                 if progress % 10000 == 0: print(progress)
    #                 progress += 1
    #                 asn2path[asn].add(path)
    #         lines = rf.readlines(1000000)
    #         #if len(asn2path) > 3: break
    # print('parse prefix_path_compress.json snd turn done')
    i = 0
    for pref, paths in prefix_paths.items():
        if i % 1000 == 0: print(i)
        for path in paths:
            asn = path.split(' ')[-1]
            if asn in concerned_asns:
                asn2path[asn].add(path)

def get_concerned_data_from_all_vps_v2(prefix_paths, concerned_ips, dst2path):
    tmp_pref2ip = defaultdict(set)
    for ip, prefs in concerned_ips.items():
        for pref in prefs:
            tmp_pref2ip[pref].add(ip)
    all_concerned_prefixes = tmp_pref2ip.keys()
    tmp_asn2dst = defaultdict(set)
    for pref, paths in prefix_paths.items():
        if pref not in all_concerned_prefixes:
            continue
        for ip in tmp_pref2ip[pref]:
            for path in paths:
                dst2path[ip].add(path)
                tmp_asn2dst[path.split(' ')[-1]].add(ip)
    print('parse prefix_path_compress.json fst turn done')

    #concerned_asns = tmp_asn2dst.keys()
    #print('len of concerned_asns: {}'.format(len(concerned_asns)))
    i = 0
    all_path = {path for paths in prefix_paths.values() for path in paths}
    for path in all_path:
        i += 1
        if i % 10000000 == 0: print(i)
        asn = path.split(' ')[-1]
        if asn in tmp_asn2dst.keys():
            for ip in tmp_asn2dst[asn]:
                dst2path[ip].add(path)
    all_path.clear()

def get_all_prefs(ip):
    prefs = set()
    ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton(ip))[0])
    for mask_len in range(32, 7, -1):
        if mask_len < 32:
            mask = ~(1 << (31 - mask_len))
            ip_int = ip_int & mask
        prefs.add(str(socket.inet_ntoa(struct.pack('I',socket.htonl(ip_int)))) + '/' + str(mask_len))
    return prefs

def check_diff_path_in_other_bgp_part1(vp, date):
    os.chdir('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/' %vp)
    arrival_segs = set()
    asn_paths = defaultdict(set)
    get_all_arrival_paths_from_vp(vp, date, arrival_segs, asn_paths)

    res = {'partial_trace': [], 'conform_srcVp_dstAsn': [], 'others': []}
    ip_accur_info = {} #没用，为了适配接口
    mm_ips = [] #没用，为了适配接口
    pm_ips = [] #没用，为了适配接口
    ip_remap = defaultdict(Counter) #没用，适配接口

    data = {} #{'first_exit_mm': {'moas': [], 'others': []}, 'middle_mm': {'moas': [], 'others': []}}
    with open('ana_compare_res/continuous_mm.%s.%s' %(vp, date), 'r') as rf:
        lines = rf.readlines()
        for i in range(0, len(lines), 3):
            (dst_ip, trace) = lines[i][1:].strip('\n').split(']')
            bgp_list = lines[i + 1].strip('\t').strip('\n').split(' ')
            dst_asn = bgp_list[-1]
            ip_list = lines[i + 2].strip('\n').split(']')[-1].split(' ')
            (trace_list, trace_to_ip_info, _) = CompressTrace(trace.split(' '), ip_list, 'n')
            conform = False
            for t_path in asn_paths[dst_asn]:
                if CompareCD_PerTrace(t_path, trace_list, ip_list, trace_to_ip_info, ip_accur_info, mm_ips, pm_ips, ip_remap):
                    res['conform_srcVp_dstAsn'] += lines[i:i+2]
                    res['conform_srcVp_dstAsn'].append('matched bgp: ' + t_path + '\n')
                    res['conform_srcVp_dstAsn'].append(lines[i+2])
                    conform = True
                    break
            if conform:
                continue
            res['others'] += lines[i:i+3]
    total = 0
    for type, val in res.items():
        with open('ana_compare_res/diff_path_%s.%s.%s' %(type, vp, date), 'w') as wf:
            wf.write(''.join(val))
        total += len(val)
    print('total: {}'.format(total))
    for type, val in res.items():
        print('{} rate: {}'.format(type, len(val) / total))      


# def check_diff_path_in_other_bgp_part2(prefix_paths, vp, date):
#     os.chdir('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/' %vp)
#     tracevp_as_info = {'ams-nl': '1103', 'jfk-us': '6939', 'sjc2-us': '6939', 'syd-au': '7575', 'zrh2-ch': '34288', 'nrt-jp': '7660'}
#     lines = None
#     concerned_ips = {}
#     with open('ana_compare_res/diff_path_others.%s.%s' %(vp, date), 'r') as rf:
#         lines = rf.readlines()
#         for i in range(0, len(lines), 3):
#             (dst_ip, trace) = lines[i][1:].strip('\n').split(']')
#             #concerned_ips[dst_ip] = socket.ntohl(struct.unpack("I",socket.inet_aton(dst_ip))[0])
#             concerned_ips[dst_ip] = get_all_prefs(dst_ip)
#     pref2path = defaultdict(set)
#     pref2asn = defaultdict(set)
#     asn2path = defaultdict(set)
#     existed_ip2pref = defaultdict(set)
#     get_concerned_data_from_all_vps(prefix_paths, concerned_ips, pref2path, pref2asn, asn2path, existed_ip2pref)
#     print('get_concerned_data_from_all_vps() done')

#     arrival_segs = set()
#     asn_paths = defaultdict(set)
#     get_all_arrival_paths_from_vp(vp, date, arrival_segs, asn_paths)
#     print('get_all_arrival_paths_from_vp() done')

#     res = {'conform_dst_pref_all_vp': [], 'conform_mm_seg_all_vp': [], 'others1': []}
#     ip_accur_info = {} #没用，为了适配接口
#     mm_ips = [] #没用，为了适配接口
#     pm_ips = [] #没用，为了适配接口
#     ip_remap = defaultdict(Counter) #没用，适配接口
#     for i in range(0, len(lines), 3):
#         print(i)
#         (dst_ip, trace) = lines[i][1:].strip('\n').split(']')
#         ori_bgp_list = lines[i + 1].strip('\t').strip('\n').split(' ')
#         ip_list = lines[i + 2].strip('\n').split(']')[-1].split(' ')
#         (trace_list, trace_to_ip_info, _) = CompressTrace(trace.split(' '), ip_list, 'n')
#         # print('trace_list: {}'.format(trace_list))
#         # print('ori_bgp_list: {}'.format(ori_bgp_list))
#         comp_trace_list = drop_stars_in_trace(trace_list)
#         hop = find_longest_head_mm_hop(comp_trace_list, arrival_segs)
#         if hop == comp_trace_list[-1] or hop == None: #error. 这种情况应该在conform_dst_asn_single_vp中出现
#             print('error')
#             continue
#         remain_list = trace_list[trace_list.index(hop):]  
#         # print('remain_list: {}'.format(remain_list))
#         conform = False
#         for pref in existed_ip2pref[dst_ip]:
#             for dst_asn in pref2asn[pref]:
#                 for bgp_path in asn2path[dst_asn]:
#                     #print('bgp_path: {}'.format(bgp_path))
#                     bgp_list = bgp_path.split(' ')
#                     if tracevp_as_info[vp] in bgp_list:
#                         #if CompareCD_PerTrace(' '.join(bgp_list[bgp_list.index(tracevp_as_info[vp]):]), trace_list, ip_list, trace_to_ip_info, ip_accur_info, mm_ips, pm_ips, ip_remap):
#                         if cal_mm_segs(' '.join(bgp_list[bgp_list.index(tracevp_as_info[vp]):]), trace_list) == 0:
#                             res['conform_dst_pref_all_vp'] += lines[i:i+2]
#                             res['conform_dst_pref_all_vp'].append('matched bgp: ' + bgp_path + '\n')
#                             res['conform_dst_pref_all_vp'].append(lines[i + 2])
#                             conform = True
#                             # print('1 bgp_path: {}'.format(bgp_path))
#                             # print('conform')
#                             break
#                     if hop in bgp_list:
#                         #if CompareCD_PerTrace(' '.join(bgp_list[bgp_list.index(hop):]), remain_list, ip_list, trace_to_ip_info, ip_accur_info, mm_ips, pm_ips, ip_remap):
#                         if cal_mm_segs(' '.join(bgp_list[bgp_list.index(hop):]), remain_list) == 0:
#                             res['conform_mm_seg_all_vp'] += lines[i:i+2]
#                             res['conform_mm_seg_all_vp'].append('matched bgp: ' + bgp_path + '\n')
#                             res['conform_mm_seg_all_vp'].append(lines[i + 2])
#                             conform = True
#                             # print('2 bgp_path: {}'.format(bgp_path))
#                             # print('conform')
#                             break
#                 if conform:
#                     break
#             if conform:
#                 break
#         if not conform:
#             res['others1'] += lines[i:i+3]

#     total = 0
#     for type, val in res.items():
#         with open('ana_compare_res/diff_path_%s.%s.%s' %(type, vp, date), 'w') as wf:
#             wf.write(''.join(val))
#         total += len(val)
#     print('total: {}'.format(total))
#     for type, val in res.items():
#         print('{} rate: {}'.format(type, len(val) / total))  
#     #os.system('rm -f ana_compare_res/diff_path_others.%s.%s' %(type, vp, date))


def check_diff_path_in_other_bgp_part2_v2(prefix_paths, vp, date):
    os.chdir('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/' %vp)
    tracevp_as_info = {'ams-nl': '1103', 'jfk-us': '6939', 'sjc2-us': '6939', 'syd-au': '7575', 'zrh2-ch': '34288', 'nrt-jp': '7660'}
    lines = None
    concerned_ips = {}
    #trace_lines_info = defaultdict(list)
    #with open('ana_compare_res/diff_path_others.%s.%s' %(vp, date), 'r') as rf:
    with open('ana_compare_res/continuous_mm.%s.%s' %(vp, date), 'r') as rf:
        lines = rf.readlines()
        for i in range(0, len(lines), 3):
            (dst_ip, trace) = lines[i][1:].strip('\n').split(']')
            #concerned_ips[dst_ip] = socket.ntohl(struct.unpack("I",socket.inet_aton(dst_ip))[0])
            concerned_ips[dst_ip] = get_all_prefs(dst_ip)
            # ip_list = lines[i + 2].strip('\n').split(']')[-1].split(' ')
            # (trace_list, trace_to_ip_info, _) = CompressTrace(trace.split(' '), ip_list, 'n')
            # trace_lines_info[' '.join(trace_list)].append(i)
    dst2path = defaultdict(set)
    get_concerned_data_from_all_vps_v2(prefix_paths, concerned_ips, dst2path)
    print('get_concerned_data_from_all_vps() done')

    arrival_segs = set()
    asn_paths = defaultdict(set)
    get_all_arrival_paths_from_vp(vp, date, arrival_segs, asn_paths)
    print('get_all_arrival_paths_from_vp() done')

    res = {'conform_dst_pref_all_vp': [], 'conform_mm_seg_all_vp': [], 'others1': []}
    j = 0
    for i in range(0, len(lines), 3):
        (dst_ip, trace) = lines[i][1:].strip('\n').split(']')
        ip_list = lines[i + 2].strip('\n').split(']')[-1].split(' ')
        (trace_list, trace_to_ip_info, _) = CompressTrace(trace.split(' '), ip_list, 'n')
        # print('trace_list: {}'.format(trace_list))
        # print('ori_bgp_list: {}'.format(ori_bgp_list))
        j += 1
        print(j)
        conform = False
        comp_trace_list = drop_stars_in_trace(trace_list) 
        for bgp_path in dst2path[dst_ip]:
            #print('bgp_path: {}'.format(bgp_path))
            bgp_list = bgp_path.split(' ')
            if all(hop in bgp_list for hop in comp_trace_list):
                #if CompareCD_PerTrace(' '.join(bgp_list[bgp_list.index(tracevp_as_info[vp]):]), trace_list, ip_list, trace_to_ip_info, ip_accur_info, mm_ips, pm_ips, ip_remap):
                (ab_count, mal_pos_flag) = CheckAbHopCountAndMalPos(bgp_path, trace_list, trace_to_ip_info)
                if mal_pos_flag:
                    continue
                # print(bgp_list[bgp_list.index(tracevp_as_info[vp]):])
                # print(trace_list)
                (mm_seg_num, _) = cal_mm_segs(' '.join(bgp_list[bgp_list.index(tracevp_as_info[vp]):]), trace_list)
                if mm_seg_num == 0:
                    conform = True
                    res['conform_dst_pref_all_vp'] += lines[i:i+2]
                    res['conform_dst_pref_all_vp'].append('matched bgp: ' + bgp_path + '\n')
                    res['conform_dst_pref_all_vp'].append(lines[i + 2])
                    # print('1 bgp_path: {}'.format(bgp_path))
                    print('1 conform')
                    break
        # if conform:
        #     continue        
        # hop = find_longest_head_mm_hop(comp_trace_list, arrival_segs)
        # if hop == comp_trace_list[-1] or hop == None: #error. 这种情况应该在conform_dst_asn_single_vp中出现
        #     print('error')
        #     continue
        # remain_list = trace_list[trace_list.index(hop):] 
        # for bgp_path in dst2path[dst_ip]:
        #     #print('bgp_path: {}'.format(bgp_path))
        #     bgp_list = bgp_path.split(' ')
        #     if hop in bgp_list:
        #         #if CompareCD_PerTrace(' '.join(bgp_list[bgp_list.index(hop):]), remain_list, ip_list, trace_to_ip_info, ip_accur_info, mm_ips, pm_ips, ip_remap):
        #         if cal_mm_segs(' '.join(bgp_list[bgp_list.index(hop):]), remain_list) == 0:
        #             conform = True
        #             res['conform_mm_seg_all_vp'] += lines[i:i+2]
        #             res['conform_mm_seg_all_vp'].append('matched bgp: ' + bgp_path + '\n')
        #             res['conform_mm_seg_all_vp'].append(lines[i + 2])
        #             # print('2 bgp_path: {}'.format(bgp_path))
        #             print('2 conform')
        #             break
        if not conform:
           res['others1'] += lines[i:i+3]

    total = 0
    for type, val in res.items():
        with open('ana_compare_res/diff_path_%s.%s.%s' %(type, vp, date), 'w') as wf:
            wf.write(''.join(val))
        total += len(val)
    print('total: {}'.format(total))
    for type, val in res.items():
        print('{} rate: {}'.format(type, len(val) / total))  
    #os.system('rm -f ana_compare_res/diff_path_others.%s.%s' %(type, vp, date))

    
def check_diff_path_in_other_bgp_part3(prefix_paths, vp, date):
    os.chdir('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/' %vp)
    tracevp_as_info = {'ams-nl': '1103', 'jfk-us': '6939', 'sjc2-us': '6939', 'syd-au': '7575', 'zrh2-ch': '34288', 'nrt-jp': '7660'}
    lines = None
    concerned_ips = {}
    #trace_lines_info = defaultdict(list)
    with open('ana_compare_res/diff_path_others1.%s.%s' %(vp, date), 'r') as rf:
        lines = rf.readlines()
        for i in range(0, len(lines), 3):
            (dst_ip, trace) = lines[i][1:].strip('\n').split(']')
            #concerned_ips[dst_ip] = socket.ntohl(struct.unpack("I",socket.inet_aton(dst_ip))[0])
            concerned_ips[dst_ip] = get_all_prefs(dst_ip)
            # ip_list = lines[i + 2].strip('\n').split(']')[-1].split(' ')
            # (trace_list, trace_to_ip_info, _) = CompressTrace(trace.split(' '), ip_list, 'n')
            # trace_lines_info[' '.join(trace_list)].append(i)
    dst2path = defaultdict(set)
    get_concerned_data_from_all_vps_v2(prefix_paths, concerned_ips, dst2path)
    print('get_concerned_data_from_all_vps() done')

    arrival_segs = set()
    asn_paths = defaultdict(set)
    get_all_arrival_paths_from_vp(vp, date, arrival_segs, asn_paths)
    print('get_all_arrival_paths_from_vp() done')

    res = {'conform_mm_seg_all_vp': [], 'others2': []}
    j = 0
    debug = False
    mal_pos_num = 0
    total_num = 0
    record_num = 0
    for i in range(0, len(lines), 3):
        total_num += 1
        (dst_ip, trace) = lines[i][1:].strip('\n').split(']')
        if dst_ip == '175.156.252.1':
            print('debug')
            debug = True
        else: debug = False
        ori_bgp = lines[i + 1].strip('\n').strip('\t')
        ip_list = lines[i + 2].strip('\n').split(']')[-1].split(' ')
        (trace_list, trace_to_ip_info, _) = CompressTrace(trace.split(' '), ip_list, 'n')
        # print('trace_list: {}'.format(trace_list))
        # print('ori_bgp_list: {}'.format(ori_bgp_list))
        j += 1
        conform = False
        comp_trace_list = drop_stars_in_trace(trace_list) 
        #hop = find_longest_head_mm_hop(comp_trace_list, arrival_segs)
        hop = find_longest_head_mm_hop_v2(comp_trace_list, ori_bgp)
        if hop == comp_trace_list[-1] or hop == None: #error. 这种情况应该在conform_dst_asn_single_vp中出现
            print(j)
            print(lines[i])
            print(ori_bgp)
            print(comp_trace_list)
            print('error')
            continue
        remain_list = trace_list[trace_list.index(hop):] 
        for bgp_path in dst2path[dst_ip]:
            if debug:
                print('bgp_path: {}'.format(bgp_path))
            bgp_list = bgp_path.split(' ')            
            if hop in bgp_list:
                remain_bgp_list = bgp_list[bgp_list.index(hop):]
                if all(elem in remain_bgp_list for elem in remain_list):
                    (ab_count, mal_pos_flag) = CheckAbHopCountAndMalPos(' '.join(remain_bgp_list), remain_list, trace_to_ip_info)
                    if mal_pos_flag:
                        mal_pos_num += 1
                        continue
                    #if CompareCD_PerTrace(' '.join(bgp_list[bgp_list.index(hop):]), remain_list, ip_list, trace_to_ip_info, ip_accur_info, mm_ips, pm_ips, ip_remap):
                    (mm_seg_num, _) = cal_mm_segs(' '.join(remain_bgp_list), remain_list)
                    if mm_seg_num == 0:
                        conform = True
                        res['conform_mm_seg_all_vp'] += lines[i:i+2]
                        res['conform_mm_seg_all_vp'].append('matched bgp: ' + bgp_path + '\n')
                        res['conform_mm_seg_all_vp'].append(lines[i + 2])
                        # print('2 bgp_path: {}'.format(bgp_path))
                        #print('2 conform')
                        record_num += 1
                        break
        if not conform:
            record_num += 1
            res['others2'] += lines[i:i+3]

    print('mal_pos_num:{}'.format(mal_pos_num))
    print('record_num:{}'.format(record_num))
    print('total_num:{}'.format(total_num))
    total = 0
    for type, val in res.items():
        with open('ana_compare_res/diff_path_%s.%s.%s' %(type, vp, date), 'w') as wf:
            wf.write(''.join(val))
        total += len(val)
    print('total: {}'.format(total))
    for type, val in res.items():
        print('{} rate: {}'.format(type, len(val) / total))  
    #os.system('rm -f ana_compare_res/diff_path_others.%s.%s' %(type, vp, date))

def join_all_rib_paths(date):
    GetIxpAsSet(date)
    os.chdir('/mountdisk1/ana_c_d_incongruity/rib_data/all_collectors_one_date_bgpdata/%s/' %date)
    data1 = {}
    data2 = {}
    print('begin')
    with open('prefix_path1.json', 'r') as rf:
        data1 = json.load(rf)
    print('read1 done')
    with open('prefix_path2.json', 'r') as rf:
        data2 = json.load(rf)
    print('read2 done')
    for pref, paths in data2.items():
        if pref not in data1.keys():
            data1[pref] = set(paths)
        else:
            data1[pref] = set(data1[pref]) | set(paths)
    data2.clear()
    # with open('prefix_path.json', 'r') as rf:
    #     data1 = json.load(rf)
    print('to compress')
    for pref, paths in data1.items():
        t = set()
        for path in paths:
            t.add(CompressBGPPath(path))
        data1[pref] = list(t)
    with open('prefix_path_compress.json', 'w') as wf:
        json.dump(data1, wf, indent=1)
    data1.clear()

#简易版的CompareCD_PerTrace
def cal_mm_segs(bgp, trace_list):
    bgp_list = bgp.split(' ')
    if len(bgp_list) == 0 or len(trace_list) == 0:
        return (0, None)
    bgp_list.append('$')
    trace_list.append('$')
    segs = []
    pre_bgp_index = pre_trace_index = 0
    for trace_index in range(1, len(trace_list)): #默认BGP和traceroute第一跳相同
        trace_hop = trace_list[trace_index]
        if trace_hop == '*' or trace_hop == '?' or trace_hop.startswith('<'): #兼容后面修正IXP, IXP hop忽略
            continue
        (find, bgp_index) = FindTraceHopInBGP(bgp_list, trace_hop, pre_bgp_index)
        if find:
            if bgp_index == 255: # 不应该出现这个情况，函数外已过滤
                # print('Mal_pos should already be filtered')
                # print(bgp)
                # print(trace_list)
                while trace_list[-1] == '$': trace_list.pop(-1)
                while bgp_list[-1] == '$': bgp_list.pop(-1)
                return (-1, None)
            segs.append([bgp_list[pre_bgp_index:bgp_index + 1], trace_list[pre_trace_index:trace_index + 1]])
            pre_bgp_index = bgp_index
            pre_trace_index = trace_index

    mm_seg_num = 0
    mm_segs = []
    for seg in segs:
        [bgp_seg, trace_seg] = seg
        if bgp_seg[-1] == '$':
            bgp_seg = bgp_seg[:-1]
            trace_seg = trace_seg[:-1]
        if any(elem != '*' and elem != '?' and not elem.startswith('<') for elem in trace_seg[1:-1]):
            mm_seg_num += 1
            mm_segs.append([bgp_seg, trace_seg])
        elif len(bgp_seg) > len(trace_seg): 
            mm_seg_num += 1
            mm_segs.append([bgp_seg, trace_seg])
    while trace_list[-1] == '$': trace_list.pop(-1)
    while bgp_list[-1] == '$': bgp_list.pop(-1)
    return (mm_seg_num, mm_segs)

# def classify_discrete_continuous_mm_hops(filename, vp, date):
#     tracevp_as_info = {'ams-nl': '1103', 'jfk-us': '6939', 'sjc2-us': '6939', 'syd-au': '7575', 'zrh2-ch': '34288', 'nrt-jp': '7660'}
#     GetIxpPfxDict_2(int(date[:4]), int(date[4:6]))
#     checksiblings = CheckSiblings(date)
#     bgp_path_info = {}
#     InitPref2ASInfo('/mountdisk1/ana_c_d_incongruity/out_ip2as_data/%s.ip2as.prefixes' %date, bgp_path_info)
#     ConnectToBdrMapItDb('/mountdisk1/ana_c_d_incongruity/out_bdrmapit/sxt_bdr/bdrmapit_%s_%s.db' %(vp, date))
#     ConstrBdrCache()
#     out_ip2as = {}
#     InitPref2ASInfo('/mountdisk1/ana_c_d_incongruity/out_ip2as_data/%s.ip2as.prefixes' %date, out_ip2as)

#     data = {'continuous_mm': [], 'discrete_mm': defaultdict(list)}
#     discrete_data = {'ixp': 0, 'sibling': 0, 'neigh': 0, 'others': 0}
#     total = 0
#     discrete_mm_ips = {}
#     t = 0
#     not_find = 0
#     with open(filename, 'r') as rf:
#         lines = [rf.readline() for _ in range(3)]
#         while lines[0]:
#             #print(lines[0])
#             total += 1
#             (dst_ip, trace) = lines[0][1:].strip('\n').split(']')
#             if dst_ip == '64.79.224.1':
#                 print('')
#             ori_trace_list = trace.split(' ')
#             bgp_path = lines[1].strip('\n').strip('\t')
#             # if bgp_path.__contains__('34288 1299 12956 22927 265784'):
#             #     print('')
#             bgp_list = bgp_path.split(' ')
#             (mm_ips, pm_ips, ip_path) = lines[2].strip('\n').split(']')
#             mm_ips = mm_ips[1:].split(',') if mm_ips[1:] else []
#             pm_ips = pm_ips[1:].split(',') if pm_ips[1:] else []
#             ip_list = ip_path.split(' ')
#             # if ip_list[-1] != '*' and ip_list[-1] != dst_ip:
#             #     t += 1
#             #     dst_asns = GetBGPPath_Or_OriASN(bgp_path_info, dst_ip, 'get_all_2')[1]
#             #     dst_asns = dst_asns.split('_')
#             #     last_trace_asn = GetIp2ASFromBdrMapItDb(ip_list[-1])
#             #     if last_trace_asn in dst_asns:
#             #         ip_list.append(dst_ip)
#             #         ori_trace_list.append(last_trace_asn)
#             #         if ip_list[-2] in mm_ips:
#             #             mm_ips.append(dst_ip)
#             (trace_list, trace_to_ip_info, _) = CompressTrace(ori_trace_list, ip_list, tracevp_as_info[vp])
#             comp_trace_list = drop_stars_in_trace(trace_list)
#             continuous_mm = False
#             #处理连续同样的mm_ip    
#             ori_mm_ips= []
#             if mm_ips:
#                 for mm_ip in mm_ips:
#                     if IsIxpIp(mm_ip):
#                         continue
#                     for i in range(0, ip_list.count(mm_ip)):
#                         ori_mm_ips.append(mm_ip)
#                 if ori_mm_ips:
#                     prev_index = ip_list.index(ori_mm_ips[0])
#                     for i in range(1, len(ori_mm_ips)):
#                         if ori_mm_ips[i] not in ip_list[prev_index + 1:]:
#                             print(vp)
#                             print(date)
#                             print('ori_trace_list: {}'.format(ori_trace_list))
#                             print('ip_list: {}'.format(ip_list))
#                             print('trace_list: {}'.format(trace_list))
#                             print('bgp_list: {}'.format(bgp_list))
#                         cur_index = ip_list.index(ori_mm_ips[i], prev_index + 1)
#                         if ori_mm_ips[i] != ori_mm_ips[i - 1] and (cur_index - prev_index == 1 or \
#                             all(hop == '*' or hop == '?' or hop.startswith('<') for hop in ori_trace_list[prev_index + 1:cur_index])):
#                             (pref1, _) = GetBGPPath_Or_OriASN(bgp_path_info, ori_mm_ips[i], 'get_all_2')
#                             (pref2, _) = GetBGPPath_Or_OriASN(bgp_path_info, ori_mm_ips[i - 1], 'get_all_2')
#                             if pref1 != pref2:
#                                 continuous_mm = True
#                                 break
#                         prev_index = cur_index
#             # mm_bgp_hops = [x for x in bgp_list if x not in ori_trace_list]
#             # if not continuous_mm:
#             #     for i in range(1, len(mm_bgp_hops)):
#             #         if bgp_list.index(mm_bgp_hops[i]) - bgp_list.index(mm_bgp_hops[i - 1]) == 1:
#             #             continuous_mm = True
#             #             break
#             # print('ori_trace_list: {}'.format(ori_trace_list))
#             # print('ip_list: {}'.format(ip_list))
#             # print('trace_list: {}'.format(trace_list))
#             # print('bgp_list: {}'.format(bgp_list))
#             #1. check diff path
#             if continuous_mm:
#                 data['continuous_mm'] += lines
#             #2. discrete mm hop, might-be mapping errors
#             else:
#                 mm_seg_num = cal_mm_segs(bgp_path, trace_list)
#                 data['discrete_mm']['%d_mm' %mm_seg_num] += lines
#                 #discrete_mm_ips = defaultdict([int, set])
#                 for ip in mm_ips:
#                     if ip not in discrete_mm_ips.keys():
#                         discrete_mm_ips[ip] = [0, None, set(), None] #[所在trace的个数, mapping AS, 可能替代的mapping]
#                     discrete_mm_ips[ip][0] += 1
#                     index = ip_list.index(ip)
#                     mm_trace_hop = ori_trace_list[index]
#                     discrete_mm_ips[ip][1] = mm_trace_hop
#                     if mm_trace_hop.startswith('<'): #ixp hop, omit
#                         continue
#                     comp_index = comp_trace_list.index(mm_trace_hop)
#                     left_trace_hop = None
#                     t_index = comp_index - 1
#                     while t_index > 0:
#                         if comp_trace_list[t_index] in bgp_list:
#                             left_trace_hop = comp_trace_list[t_index]
#                             break
#                         t_index -= 1
#                     right_trace_hop = None
#                     t_index = comp_index + 1
#                     while t_index < len(comp_trace_list) - 1:
#                         if comp_trace_list[t_index] in bgp_list:
#                             right_trace_hop = comp_trace_list[t_index]
#                             break
#                         t_index += 1
#                     if left_trace_hop: discrete_mm_ips[ip][2].add(left_trace_hop)                        
#                     if right_trace_hop: discrete_mm_ips[ip][2].add(right_trace_hop)
#                     left_bgp_index = bgp_list.index(left_trace_hop) if left_trace_hop else None
#                     right_bgp_index = bgp_list.index(right_trace_hop) if right_trace_hop else None
#                     if left_bgp_index and right_bgp_index:
#                         discrete_mm_ips[ip][2] = discrete_mm_ips[ip][2] | set(bgp_list[left_bgp_index + 1:right_bgp_index])
#                     elif left_bgp_index:
#                         discrete_mm_ips[ip][2] = discrete_mm_ips[ip][2] | set(bgp_list[left_bgp_index + 1:])
#                     elif right_bgp_index:
#                         discrete_mm_ips[ip][2] = discrete_mm_ips[ip][2] | set(bgp_list[:right_bgp_index])
#                     discrete_mm_ips[ip][2].discard(None)
#                 for ip in pm_ips:
#                     if ip not in discrete_mm_ips.keys():
#                         discrete_mm_ips[ip] = [0, None, set(), None] #[所在trace的个数, 可能替代的mapping]
#                     discrete_mm_ips[ip][0] += 1
#                     index = ip_list.index(ip)
#                     mm_trace_hop = ori_trace_list[index]
#                     discrete_mm_ips[ip][1] = mm_trace_hop
#                     if mm_trace_hop.startswith('<'): #ixp hop, omit
#                         continue
#                     comp_index = comp_trace_list.index(mm_trace_hop)
#                     left_trace_hop = None
#                     for i in range(comp_index - 1, -1, -1):
#                         if comp_trace_list[i] in bgp_list:
#                             left_trace_hop = comp_trace_list[i]
#                             break
#                     right_trace_hop = None
#                     for i in range(comp_index + 1, len(comp_trace_list)):
#                         if comp_trace_list[i] in bgp_list:
#                             right_trace_hop = comp_trace_list[i]
#                             break
#                     left_bgp_index = bgp_list.index(left_trace_hop) if left_trace_hop else None
#                     bgp_index = bgp_list.index(mm_trace_hop)
#                     right_bgp_index = bgp_list.index(right_trace_hop) if right_trace_hop else None
#                     if not (index > 0 and ori_trace_list[index - 1] == mm_trace_hop):
#                         if left_bgp_index:
#                             discrete_mm_ips[ip][2] = discrete_mm_ips[ip][2] | set(bgp_list[left_bgp_index + 1:bgp_index])
#                         elif bgp_index > 0:
#                             discrete_mm_ips[ip][2] = discrete_mm_ips[ip][2] | set(bgp_list[:bgp_index])
#                     if not (index < len(ip_list) - 1 and ori_trace_list[index + 1] == mm_trace_hop):
#                         if right_bgp_index:
#                             discrete_mm_ips[ip][2] = discrete_mm_ips[ip][2] | set(bgp_list[bgp_index + 1:right_bgp_index])
#                         elif bgp_index < len(bgp_list) - 1:
#                             discrete_mm_ips[ip][2] = discrete_mm_ips[ip][2] | set(bgp_list[bgp_index + 1])
#             lines = [rf.readline() for _ in range(3)]

#     CloseBdrMapItDb()
#     InitBdrCache()
#     for ip, val in discrete_mm_ips.items():
#         if val[1].startswith('<'):
#             val[3] = 'ixp'
#         else:
#             normal_asn2 = {asn2 for asn2 in val[2] if int(asn2) > 0 and int(asn2) < 0xFFFFFFF}
#             if int(val[1]) < 0 or not normal_asn2:
#                 val[3] = 'others'
#             elif any(checksiblings.check_sibling(neigh, val[1]) for neigh in normal_asn2):
#                 val[3] = 'sibling'
#             elif any(checksiblings.bgp.rel(int(neigh), int(val[1])) for neigh in normal_asn2):
#                 val[3] = 'neigh'
#             else:
#                 val[3] = 'others'
#         discrete_mm_ips[ip][2] = list(val[2])
#         discrete_data[val[3]] += 1
    
#     if not os.path.exists('ana_compare_res'): os.mkdir('ana_compare_res')
#     count = {}
#     #data = {'continuous_mm': [], 'discrete_mm': {'single_mm': [], 'others': []}}
#     for _type, val in data.items():
#         with open('ana_compare_res/%s.%s.%s' %(_type, vp, date), 'w') as wf:
#             if isinstance(val, list):
#                 wf.write(''.join(val))
#                 count[_type] = [len(val) / 3, len(val) / 3 / total]
#             elif isinstance(val, dict):
#                 json.dump(val, wf, indent=1)
#                 count[_type] = [0, 0, {}]
#                 for subtype, subval in val.items():
#                     if isinstance(subval, list):
#                         count[_type][2][subtype] = [len(subval) / 3, len(subval) / 3 / total]
#                         count[_type][0] += len(subval) / 3
#                 count[_type][1] = count[_type][0] / total
#     with open('ana_compare_res/classify.%s.%s.json' %(vp, date), 'w') as wf:
#         json.dump(data, wf, indent=1)
#     if os.path.exists('ana_compare_res/stat_%s' %filename): os.system('rm -f ana_compare_res/stat_%s' %filename)
#     with open('ana_compare_res/stat_classify.%s.%s' %(vp, date), 'w') as wf:
#         json.dump(count, wf, indent=1)
#     os.system('cat ana_compare_res/stat_classify.%s.%s' %(vp, date))
#     with open('ana_compare_res/discrete_mm_ips.%s.%s.json' %(vp, date), 'w') as wf:
#         json.dump(discrete_mm_ips, wf, indent=1)
#     with open('ana_compare_res/discrete_mm_ips_stat.%s.%s' %(vp, date), 'w') as wf:
#         wf.write('discrete_mm_ips num: {}\n'.format(len(discrete_mm_ips)))
#         for type, val in discrete_data.items():
#             wf.write('{} rate: {}\n'.format(type, val / len(discrete_mm_ips)))

#true: valley-free
# provider = 1
# customer = 2
# peer = 3
# none = 4
def check_str2int(s):
    if s.isdigit():
        if len(s) < 8:
            return True
    return False

def check_valley_free(checksiblings, asn1, asn2, asn3):
    if not (check_str2int(asn1) and check_str2int(asn2) and check_str2int(asn3)):
        return False
    rel1 = checksiblings.bgp.reltype(int(asn1), int(asn2))
    rel2 = checksiblings.bgp.reltype(int(asn2), int(asn3))
    if checksiblings.check_sibling(asn1, asn2):
        return rel2 != 4
    if checksiblings.check_sibling(asn2, asn3):
        return rel1 != 4
    if rel1 == 4 or rel2 == 4:
        return False
    if rel1 == 2 or rel2 == 1:
        return True
    return False

def check_path_valley_free(checksiblings, path_list):
    try:
        if len(path_list) == 1: return True
        if len(path_list) == 2:
            if not (check_str2int(path_list[0]) and check_str2int(path_list[1])):
                return False
            else:
                return (checksiblings.bgp.reltype(int(path_list[0]), int(path_list[1])) != 4)
        for j in range(len(path_list)-2):
            #if check_valley_free(checksiblings, comp_mm_trace_seg[j], comp_mm_trace_seg[j+1], comp_mm_trace_seg[j+2]):
            if not check_valley_free(checksiblings, path_list[j], path_list[j+1], path_list[j+2]):
                return False
        return True
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(path_list)
        return None
    
def GetRel_v2(checksiblings, asn1, asn2):
    if not asn1.isdigit() or not asn2.isdigit() or asn1 == '-1' or asn2 == '-1' or int(asn1) > 0xFFFFFF or int(asn2) > 0xFFFFFF:
        return 4
    asn1 = int(asn1)
    asn2 = int(asn2)
    if asn1 == asn2:
        return 5
    else:
        tmp_type = checksiblings.bgp.reltype(asn1, asn2)
        if tmp_type == 1: return -1
        if tmp_type == 2: return 1
        if tmp_type == 3: return 0
        return 4
    
def check_valley_free_v2(checksiblings, asn1, asn2, asn3):
    valleystate = {'11':'normal','12':'normal','13':'normal','14':'semi','15':'normal','21':'abnormal','22':'normal','23':'abnormal','24':'semi','25':'normal','31':'abnormal','32':'normal','33':'semi','34':'semi','35':'normal','41':'semi','42':'semi','43':'semi','44':'abnormal','45':'semi','51':'normal','52':'normal','53':'normal','54':'semi','55':'normal','1':'normal','2':'normal','3':'normal','4':'semi','5':'normal','':'normal'} #这里改成三态的，配合模型参数
    rel1 = str(GetRel_v2(int(asn1), int(asn2))) if asn1.isdigit() and asn2.isdigit() else '4'
    rel2 = str(GetRel_v2(int(asn2), int(asn3))) if asn2.isdigit() and asn3.isdigit() else '4'
    return valleystate[rel1+rel2]

def check_rel_list_rational(rel_list):
    for i in range(len(rel_list)-1):
        if rel_list[i] != 1: #开始判断，后面只能紧跟着一个0，或者后面全是-1
            if rel_list[i+1] == 1: #下一跳只能是0或-1
                return False
            for j in range(i+2, len(rel_list)):
                if rel_list[j] != -1: #后面跳只能是-1
                    return False
    return True

def check_path_valley_free_v2(checksiblings, path_list):
    rel_list = []
    c = 0
    for i in range(1, len(path_list)):
        rel = GetRel_v2(checksiblings, path_list[i-1], path_list[i])
        if rel == 4:
            c += 1
            if c >= 2:
                return False
            rel_list.append(0)
        elif rel != 5:
            # if any(p < rel for p in rel_list):
            #     return False
            rel_list.append(rel)
    return check_rel_list_rational(rel_list)

def jsac_classify_discrete_continuous_mm_hops(filename, vp, date, ab_thresh):
    tracevp_as_info = {'ams-nl': '1103', 'jfk-us': '6939', 'sjc2-us': '6939', 'syd-au': '7575', 'zrh2-ch': '34288', 'nrt-jp': '7660', 'sao-br': '22548'}
    data = {'continuous_mm': [], 'discrete_mm': [], 'truncate_mm': []}
    discrete_data = {'ixp': 0, 'sibling': 0, 'neigh': 0, 'others': 0}
    total = 0
    discrete_mm_ips = {}
    with open(filename, 'r') as rf:
        lines = [rf.readline() for _ in range(3)]
        while lines[0]:
            #print(lines[0])
            total += 1
            (dst_ip, trace) = lines[0][1:].strip('\n').split(']')
            ori_trace_list = trace.split(' ')
            bgp_path = lines[1].strip('\n').strip('\t')
            bgp_list = bgp_path.split(' ')
            (mm_ips, pm_ips, ip_path) = lines[2].strip('\n').split(']')
            mm_ips = mm_ips[1:].split(',') if mm_ips[1:] else []
            pm_ips = pm_ips[1:].split(',') if pm_ips[1:] else []
            ip_list = ip_path.split(' ')
            (trace_list, trace_to_ip_info, _) = CompressTrace(ori_trace_list, ip_list, tracevp_as_info[vp])
            (mm_seg_num, mm_segs) = cal_mm_segs(bgp_path, trace_list)
            for seg in mm_segs:
                [mm_bgp_seg, mm_trace_seg] = seg
                comp_mm_trace_seg = drop_stars_in_trace(mm_trace_seg)
                if len(comp_mm_trace_seg) - 2 > ab_thresh:
                    data['continuous_mm'] += lines
                else:
                    data['discrete_mm'] += lines
            lines = [rf.readline() for _ in range(3)]
    with open('ana_compare_res/jsac_%d_classify.%s.%s.json' %(ab_thresh, vp, date), 'w') as wf:
        json.dump(data, wf, indent=1)

def neighAs_classify_discrete_continuous_mm_hops(filename, vp, date):
    tracevp_as_info = {'ams-nl': '1103', 'jfk-us': '6939', 'sjc2-us': '6939', 'syd-au': '7575', 'zrh2-ch': '34288', 'nrt-jp': '7660', 'sao-br': '22548'}
    data = {'continuous_mm': [], 'discrete_mm': [], 'truncate_mm': []}
    discrete_data = {'ixp': 0, 'sibling': 0, 'neigh': 0, 'others': 0}
    total = 0
    discrete_mm_ips = {}
    checksiblings = CheckSiblings(date)
    with open(filename, 'r') as rf:
        lines = [rf.readline() for _ in range(3)]
        while lines[0]:
            #print(lines[0])
            total += 1
            (dst_ip, trace) = lines[0][1:].strip('\n').split(']')
            ori_trace_list = trace.split(' ')
            bgp_path = lines[1].strip('\n').strip('\t')
            bgp_list = bgp_path.split(' ')
            (mm_ips, pm_ips, ip_path) = lines[2].strip('\n').split(']')
            mm_ips = mm_ips[1:].split(',') if mm_ips[1:] else []
            pm_ips = pm_ips[1:].split(',') if pm_ips[1:] else []
            ip_list = ip_path.split(' ')
            (trace_list, trace_to_ip_info, _) = CompressTrace(ori_trace_list, ip_list, tracevp_as_info[vp])
            (mm_seg_num, mm_segs) = cal_mm_segs(bgp_path, trace_list)
            for seg in mm_segs:
                [mm_bgp_seg, mm_trace_seg] = seg
                comp_mm_trace_seg = drop_stars_in_trace(mm_trace_seg)
                if all(any(checksiblings.bgp.rel(int(hop), int(bgp_hop)) for bgp_hop in mm_bgp_seg) \
                        for hop in comp_mm_trace_seg[1:-1]):
                    data['discrete_mm'] += lines
                else:
                    data['continuous_mm'] += lines
            lines = [rf.readline() for _ in range(3)]
    with open('ana_compare_res/neighAs_classify.%s.%s.json' %(vp, date), 'w') as wf:
        json.dump(data, wf, indent=1)

map_method = 'ml_map'

def classify_discrete_continuous_mm_hops(filename, vp, date, first_asn, atlas=False, checksiblings = None, bgp_path_info = None, pfx2as_info = None):
    # if os.path.exists('ana_compare_res/classify.%s.%s.json' %(vp, date)):
    #     with open('ana_compare_res/classify.%s.%s.json' %(vp, date), 'r') as rf:
    #         temp = json.load(rf)
    #         if 'truncate_mm' in temp.keys():
    #             return
    debug_info = []
    #try:
    if True:
        if not atlas:
            dt = datetime.datetime.strptime(date[4:6] + '/' + date[6:8] + '/' + date[2:4] + ' 00:00:00', '%m/%d/%y %H:%M:%S')
            next_date = (dt + datetime.timedelta(days=30)).strftime('%Y%m%d')
            GetIxpPfxDict_2(int(date[:4]), int(date[4:6]))
            checksiblings = CheckSiblings(date)
            checksiblings2 = CheckSiblings(next_date[:6]+'15')
            bgp_path_info = {}
            InitPref2ASInfo('/mountdisk1/ana_c_d_incongruity/out_ip2as_data/%s.ip2as.prefixes' %date, bgp_path_info)
            ConnectToBdrMapItDb('/mountdisk1/ana_c_d_incongruity/out_bdrmapit/sxt_bdr/bdrmapit_%s_%s.db' %(vp, date))
            ConstrBdrCache()
            pfx2as_info = {}
            InitPref2ASInfo_2('/mountdisk1/ana_c_d_incongruity/rib_data/rv_prefix2as/' + date + '.pfx2as', pfx2as_info)
        # out_ip2as = {}
        # InitPref2ASInfo('/mountdisk1/ana_c_d_incongruity/out_ip2as_data/%s.ip2as.prefixes' %date, out_ip2as)
        # ip_accur_info = {} #ip_accur_info[_ip] = [[0, 0.0, set()], [0, 0.0, set()], [0, 0.0, set()], [0, 0.0, set()], [0, 0.0, set()]]
        # with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/ipaccur_nodstip_%s.%s.json' %(vp, vp, date), 'r') as rf:
        #     ip_accur_info = json.load(rf)
        
        data = {'continuous_mm': [], 'discrete_mm': defaultdict(list), 'truncate_mm': []}
        discrete_data = {'ixp': 0, 'sibling': 0, 'neigh': 0, 'others': 0}
        total = 0
        discrete_mm_ips = {}
        loop_num = 0
        increase_loop = 0
        with open(filename, 'r') as rf:
            lines = [rf.readline() for _ in range(3)]
            while lines[0]:
                #print(lines[0])
                total += 1
                (dst_ip, trace) = lines[0][1:].strip('\n').split(']')
                ori_trace_list = trace.split(' ')
                bgp_path = lines[1].strip('\n').strip('\t')
                # if bgp_path.__contains__('34288 1299 12956 22927 265784'):
                #     print('')
                bgp_list = bgp_path.split(' ')
                (mm_ips, pm_ips, ip_path) = lines[2].strip('\n').split(']')
                mm_ips = mm_ips[1:].split(',') if mm_ips[1:] else []
                pm_ips = pm_ips[1:].split(',') if pm_ips[1:] else []
                ip_list = ip_path.split(' ')
                loop_pos = TestLoopInTrace(ip_list)
                if loop_pos > 0:
                    loop_num += 1
                    lines = [rf.readline() for _ in range(3)]
                    continue
                if dst_ip == '45.6.182.1':
                    a = 1
                #MOAS检查
                filter_mm_ips = []
                backup_ori_trace_list = trace.split(' ')
                i = len(ori_trace_list) - 1
                while ori_trace_list[i] == '*' or ori_trace_list[i] == '?' or ori_trace_list[i][0] == '<':
                    i -= 1
                last_trace_asn = ori_trace_list[i]
                for mm_ip in mm_ips:
                    if ori_trace_list[ip_list.index(mm_ip)] == last_trace_asn: #最后一跳涉及route aggregation，不检查
                        continue
                    rib_orias = GetBGPPath_Or_OriASN(pfx2as_info, mm_ip, 'get_orias_2')
                    rib_found = [asn for asn in rib_orias.split('_') if asn in bgp_list]
                    if rib_found:
                        #修改map结果！
                        ori_trace_list[ip_list.index(mm_ip)] = rib_found[0]
                        filter_mm_ips.append(mm_ip)
                    # if rib_orias.__contains__('_'):
                    #     rib_orias_list = rib_orias.split('_')
                    #     tmp_trace = ori_trace_list[ip_list.index(mm_ip)]
                    #     if tmp_trace in rib_orias_list:
                    #         for elem in rib_orias_list:
                    #             if elem != tmp_trace:
                    #                 if elem in bgp_list:
                    #                     #修改map结果！
                    #                     ori_trace_list[ip_list.index(mm_ip)] = elem
                    #                     filter_mm_ips.append(mm_ip)
                for elem in filter_mm_ips:
                    mm_ips.remove(elem)
                if not mm_ips:
                    lines = [rf.readline() for _ in range(3)]
                    continue
                (trace_list, trace_to_ip_info, _) = CompressTrace(ori_trace_list, ip_list, first_asn)
                if not trace_list: #MOAS修改ori_trace_list后出现了loop
                    # print(ori_trace_list)
                    # print(backup_ori_trace_list)
                    # print('')
                    increase_loop += 1
                    lines = [rf.readline() for _ in range(3)]
                    continue
                debug_info = [dst_ip, bgp_path, trace_list]
                # if dst_ip == '24.50.238.1' or dst_ip == '103.252.169.1' or dst_ip == '':
                #     lines = [rf.readline() for _ in range(3)]
                #     continue
                (mm_seg_num, mm_segs) = cal_mm_segs(bgp_path, trace_list)
                if not mm_segs:
                    lines = [rf.readline() for _ in range(3)]
                    continue
                comp_trace_list = drop_stars_in_trace(trace_list)
                continuous_mm = False
                truncate_mm = False
                #处理连续同样的mm_ip    
                ori_mm_ips= []
                if mm_ips:
                    for mm_ip in mm_ips:
                        # if IsIxpIp(mm_ip):
                        #     continue
                        for i in range(0, ip_list.count(mm_ip)):
                            ori_mm_ips.append(mm_ip)
                    if ori_mm_ips:
                        prev_index = ip_list.index(ori_mm_ips[0])
                        for i in range(1, len(ori_mm_ips)):
                            if ori_mm_ips[i] not in ip_list[prev_index + 1:]:
                                print(vp)
                                print(date)
                                print('ori_trace_list: {}'.format(ori_trace_list))
                                print('ip_list: {}'.format(ip_list))
                                print('trace_list: {}'.format(trace_list))
                                print('bgp_list: {}'.format(bgp_list))
                            cur_index = ip_list.index(ori_mm_ips[i], prev_index + 1)
                            if ori_mm_ips[i] != ori_mm_ips[i - 1] and (cur_index - prev_index == 1 or \
                                all(hop == '*' or hop == '?' or hop.startswith('<') for hop in ori_trace_list[prev_index + 1:cur_index])):
                                #if ori_trace_list[prev_index] == ori_trace_list[cur_index]:
                                if True:
                                    (pref1, _) = GetBGPPath_Or_OriASN(bgp_path_info, ori_mm_ips[i], 'get_all_2')
                                    (pref2, _) = GetBGPPath_Or_OriASN(bgp_path_info, ori_mm_ips[i - 1], 'get_all_2')
                                    if pref1 != pref2:
                                        continuous_mm = True
                                        break
                            prev_index = cur_index
                if not continuous_mm: #处理尾端mismatch
                    if ip_list[-1] in mm_ips and ip_list[-1] != dst_ip:
                        dst_asns = GetBGPPath_Or_OriASN(bgp_path_info, dst_ip, 'get_all_2')[1]
                        dst_asns = dst_asns.split('_')
                        last_trace_asn = GetIp2ASFromBdrMapItDb(ip_list[-1])
                        if last_trace_asn in dst_asns:
                            continuous_mm = True #MOAS/route aggregation导致的
                        else:
                            truncate_mm = True
                if not continuous_mm: #根据路径合理性选择
                    for seg in mm_segs:
                        [mm_bgp_seg, mm_trace_seg] = seg
                        comp_mm_trace_seg = drop_stars_in_trace(mm_trace_seg)
                        mm_trace_hop_num = len([hop for hop in comp_mm_trace_seg if hop not in mm_bgp_seg])
                        if check_path_valley_free(checksiblings, comp_mm_trace_seg) == None or check_path_valley_free(checksiblings, mm_bgp_seg) == None:
                            print('err!!!!!!!!!!!!!{}, {}'.format(vp, date))
                            print(lines)
                        if mm_trace_hop_num == 0: #missing hop，默认mapping error
                            continue
                        if mm_trace_hop_num == 1: #只有一个hop mismatch，倾向于mapping error。此时要求trace合理且bgp不合理，才认为是rm
                            if (check_path_valley_free(checksiblings, comp_mm_trace_seg) and not check_path_valley_free(checksiblings, mm_bgp_seg)) and \
                                (check_path_valley_free(checksiblings2, comp_mm_trace_seg) and not check_path_valley_free(checksiblings2, mm_bgp_seg)):
                                continuous_mm = True
                        else: #有多于一个hop mismatch, 倾向于rm，只要trace合理就认为rm
                            if check_path_valley_free(checksiblings, comp_mm_trace_seg) or \
                                check_path_valley_free(checksiblings2, comp_mm_trace_seg):
                                continuous_mm = True
                        if continuous_mm:
                            break
                                    # if len(mm_bgp_seg) == 2 and not checksiblings.bgp.rel(int(mm_bgp_seg[0]), int(mm_bgp_seg[1])):
                                    #     continuous_mm = True
                                    # elif len(mm_bgp_seg) ==3 and not check_valley_free(checksiblings, mm_bgp_seg[0], mm_bgp_seg[1], mm_bgp_seg[2]):
                                    #     continuous_mm = True
                                    # elif len(mm_bgp_seg) > 3:
                                    #     continuous_mm = True

                # mm_bgp_hops = [x for x in bgp_list if x not in ori_trace_list]
                # if not continuous_mm:
                #     for i in range(1, len(mm_bgp_hops)):
                #         if bgp_list.index(mm_bgp_hops[i]) - bgp_list.index(mm_bgp_hops[i - 1]) == 1:
                #             continuous_mm = True
                #             break
                # print('ori_trace_list: {}'.format(ori_trace_list))
                # print('ip_list: {}'.format(ip_list))
                # print('trace_list: {}'.format(trace_list))
                # print('bgp_list: {}'.format(bgp_list))
                #1. continuous mm hop, might be diff path
                if continuous_mm:
                    data['continuous_mm'] += lines
                #2. truncate_mm:
                elif truncate_mm:
                    data['truncate_mm'] += lines
                #2. discrete mm hop, might-be mapping errors
                else:                
                    data['discrete_mm']['%d_mm' %mm_seg_num] += lines
                    for ip in mm_ips:
                        if ip not in discrete_mm_ips.keys():
                            discrete_mm_ips[ip] = [0, None, set(), None] #[所在trace的个数, mapping AS, 可能替代的mapping]
                        discrete_mm_ips[ip][0] += 1
                        index = ip_list.index(ip)
                        mm_trace_hop = ori_trace_list[index]
                        discrete_mm_ips[ip][1] = mm_trace_hop
                        if mm_trace_hop.startswith('<'): #ixp hop, omit
                            continue
                        comp_index = comp_trace_list.index(mm_trace_hop)
                        left_trace_hop = None
                        t_index = comp_index - 1
                        while t_index > 0:
                            if comp_trace_list[t_index] in bgp_list:
                                left_trace_hop = comp_trace_list[t_index]
                                break
                            t_index -= 1
                        right_trace_hop = None
                        t_index = comp_index + 1
                        while t_index < len(comp_trace_list) - 1:
                            if comp_trace_list[t_index] in bgp_list:
                                right_trace_hop = comp_trace_list[t_index]
                                break
                            t_index += 1
                        if left_trace_hop: discrete_mm_ips[ip][2].add(left_trace_hop)                        
                        if right_trace_hop: discrete_mm_ips[ip][2].add(right_trace_hop)
                        left_bgp_index = bgp_list.index(left_trace_hop) if left_trace_hop else None
                        right_bgp_index = bgp_list.index(right_trace_hop) if right_trace_hop else None
                        if left_bgp_index and right_bgp_index:
                            discrete_mm_ips[ip][2] = discrete_mm_ips[ip][2] | set(bgp_list[left_bgp_index + 1:right_bgp_index])
                        elif left_bgp_index:
                            discrete_mm_ips[ip][2] = discrete_mm_ips[ip][2] | set(bgp_list[left_bgp_index + 1:])
                        elif right_bgp_index:
                            discrete_mm_ips[ip][2] = discrete_mm_ips[ip][2] | set(bgp_list[:right_bgp_index])
                        discrete_mm_ips[ip][2].discard(None)
                    for ip in pm_ips:
                        if ip not in discrete_mm_ips.keys():
                            discrete_mm_ips[ip] = [0, None, set(), None] #[所在trace的个数, 可能替代的mapping]
                        discrete_mm_ips[ip][0] += 1
                        index = ip_list.index(ip)
                        mm_trace_hop = ori_trace_list[index]
                        discrete_mm_ips[ip][1] = mm_trace_hop
                        if mm_trace_hop.startswith('<'): #ixp hop, omit
                            continue
                        comp_index = comp_trace_list.index(mm_trace_hop)
                        left_trace_hop = None
                        for i in range(comp_index - 1, -1, -1):
                            if comp_trace_list[i] in bgp_list:
                                left_trace_hop = comp_trace_list[i]
                                break
                        right_trace_hop = None
                        for i in range(comp_index + 1, len(comp_trace_list)):
                            if comp_trace_list[i] in bgp_list:
                                right_trace_hop = comp_trace_list[i]
                                break
                        left_bgp_index = bgp_list.index(left_trace_hop) if left_trace_hop else None
                        bgp_index = bgp_list.index(mm_trace_hop)
                        right_bgp_index = bgp_list.index(right_trace_hop) if right_trace_hop else None
                        if not (index > 0 and ori_trace_list[index - 1] == mm_trace_hop):
                            if left_bgp_index:
                                discrete_mm_ips[ip][2] = discrete_mm_ips[ip][2] | set(bgp_list[left_bgp_index + 1:bgp_index])
                            elif bgp_index > 0:
                                discrete_mm_ips[ip][2] = discrete_mm_ips[ip][2] | set(bgp_list[:bgp_index])
                        if not (index < len(ip_list) - 1 and ori_trace_list[index + 1] == mm_trace_hop):
                            if right_bgp_index:
                                discrete_mm_ips[ip][2] = discrete_mm_ips[ip][2] | set(bgp_list[bgp_index + 1:right_bgp_index])
                            elif bgp_index < len(bgp_list) - 1:
                                discrete_mm_ips[ip][2] = discrete_mm_ips[ip][2] | set(bgp_list[bgp_index + 1])
                lines = [rf.readline() for _ in range(3)]
        #print('loop num: {}'.format(loop_num))

        if not atlas:
            CloseBdrMapItDb()
            InitBdrCache()
        for ip, val in discrete_mm_ips.items():
            if IsIxpIp(ip):
                val[3] = 'ixp'
            else:
                normal_asn2 = {asn2 for asn2 in val[2] if int(asn2) > 0 and int(asn2) < 0xFFFFFFF}
                if int(val[1]) < 0 or not normal_asn2:
                    val[3] = 'others'
                elif any(checksiblings.check_sibling(neigh, val[1]) for neigh in normal_asn2):
                    val[3] = 'sibling'
                elif any(checksiblings.bgp.rel(int(neigh), int(val[1])) for neigh in normal_asn2):
                    val[3] = 'neigh'
                else:
                    val[3] = 'others'
            discrete_mm_ips[ip][2] = list(val[2])
            discrete_data[val[3]] += 1
        
        if not os.path.exists('ana_compare_res'): os.mkdir('ana_compare_res')
        count = {}
        #data = {'continuous_mm': [], 'discrete_mm': {'single_mm': [], 'others': []}}
        for _type, val in data.items():
            with open('ana_compare_res/%s.%s.%s' %(_type, vp, date), 'w') as wf:
                if isinstance(val, list):
                    wf.write(''.join(val))
                    count[_type] = [len(val) / 3, len(val) / 3 / total]
                elif isinstance(val, dict):
                    json.dump(val, wf, indent=1)
                    count[_type] = [0, 0, {}]
                    for subtype, subval in val.items():
                        if isinstance(subval, list):
                            count[_type][2][subtype] = [len(subval) / 3, len(subval) / 3 / total]
                            count[_type][0] += len(subval) / 3
                    count[_type][1] = count[_type][0] / total
        with open('ana_compare_res/classify.%s.%s.json' %(vp, date), 'w') as wf:
            json.dump(data, wf, indent=1)
        if os.path.exists('ana_compare_res/stat_%s' %filename): os.system('rm -f ana_compare_res/stat_%s' %filename)
        with open('ana_compare_res/stat_classify.%s.%s' %(vp, date), 'w') as wf:
            json.dump(count, wf, indent=1)
        #os.system('cat ana_compare_res/stat_classify.%s.%s' %(vp, date))
        with open('ana_compare_res/discrete_mm_ips.%s.%s.json' %(vp, date), 'w') as wf:
            json.dump(discrete_mm_ips, wf, indent=1)
        with open('ana_compare_res/discrete_mm_ips_stat.%s.%s' %(vp, date), 'w') as wf:
            wf.write('discrete_mm_ips num: {}\n'.format(len(discrete_mm_ips)))
            for type, val in discrete_data.items():
                if len(discrete_mm_ips) == 0:
                    wf.write('{} rate: 0\n'.format(type))
                else:
                    wf.write('{} rate: {}\n'.format(type, val / len(discrete_mm_ips)))
    # except Exception as e:
    #     print(debug_info)
    #     import traceback
    #     traceback.print_exc()
    print(increase_loop)
    print('{}{} done'.format(vp, date))
            
def PerTask_Atlas(date, no_use):
    print('{}'.format(date))
    os.chdir('/mountdisk2/common_vps/%s/cmp_res/sxt_bdr/' %date)
    
    common_vps = {} #common_vps[asn][atlas_vp][bgp_vp] = dist
    with open('/mountdisk2/common_vps/%s/common_vp_%s.json' %(date, date), 'r') as rf:
        common_vps = json.load(rf)
    GetIxpPfxDict_2(int(date[:4]), int(date[4:6]))
    checksiblings = CheckSiblings(date)
    bgp_path_info = {}
    InitPref2ASInfo('/mountdisk1/ana_c_d_incongruity/out_ip2as_data/%s.ip2as.prefixes' %date, bgp_path_info)
    ConnectToBdrMapItDb('/mountdisk2/common_vps/%s/atlas/bdrmapit/sxt_bdr.db' %date)
    ConstrBdrCache()
    pfx2as_info = {}
    InitPref2ASInfo_2('/mountdisk1/ana_c_d_incongruity/rib_data/rv_prefix2as/' + date + '.pfx2as', pfx2as_info)
    for asn, val in common_vps.items():
        for atlas_vp in val.keys():
            if not os.path.exists('mm_%s' %atlas_vp) or os.path.getsize('mm_%s' %atlas_vp) == 0:
                continue
            # if atlas_vp != '46.101.130.201':
            #     continue
            classify_discrete_continuous_mm_hops('mm_%s' %atlas_vp, atlas_vp, date, asn, True, checksiblings, bgp_path_info, pfx2as_info)
    CloseBdrMapItDb()
    InitBdrCache()

def PerTask(vp, date):
    os.chdir('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/%s_filtersndmm/' %(vp, map_method))
    # if os.path.exists('ana_compare_res/neighAs_classify.%s.%s.json' %(vp, date)) and os.path.getsize('ana_compare_res/neighAs_classify.%s.%s.json' %(vp, date)) > 0:
    #     return
    print('{}.{}'.format(vp, date))
    step = 0
    total = 0
    # #1.过滤掉IXP addr
    # total = filter_ixpips_in_mm_trace(vp, date, step, total)   
    # #2.succ_ip failed traces
    # step += 1
    # total = get_mm_trace_by_succ_ip_v2(vp, date, step, total) 
    # #3.dst_ip因为vp-rib和coa-rib视角不一致导致的moas
    # step += 1
    # total = get_extra_moas_trail_mm_trace(vp, date, 2, total)
    
    # #another classify mm traces
    tracevp_as_info = {'ams-nl': '1103', 'jfk-us': '6939', 'sjc2-us': '6939', 'syd-au': '7575', 'nrt-jp': '7660', 'sao-br': '22548'}
    classify_discrete_continuous_mm_hops('mm_%s.%s' %(vp, date), vp, date, tracevp_as_info[vp])
    # jsac_classify_discrete_continuous_mm_hops('mm_%s.%s' %(vp, date), vp, date, 1)
    # jsac_classify_discrete_continuous_mm_hops('mm_%s.%s' %(vp, date), vp, date, 2)
    # neighAs_classify_discrete_continuous_mm_hops('mm_%s.%s' %(vp, date), vp, date)

def PerTask_EvalDiscri(vp, date, pre):
    #print('{}.{}'.format(vp, date))
    os.chdir('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ori_bdr_filtersndmm/' %(vp))
    wf = open('cmp_%s' %pre, 'a') 

    #data = {'continuous_mm': [], 'discrete_mm': defaultdict(list)}
    classi = {'continuous_mm': set(), 'discrete_mm': set(), 'truncate_mm': set()}
    #with open('ana_compare_res/classify.%s.%s.json' %(vp, date), 'r') as rf:
    with open('ana_compare_res/%s.%s.%s.json' %(pre, vp, date), 'r') as rf:
        data = json.load(rf)
        for i in range(0, len(data['continuous_mm']), 3):
            classi['continuous_mm'].add(data['continuous_mm'][i].split(']')[0][1:])
        for i in range(0, len(data['truncate_mm']), 3):
            classi['truncate_mm'].add(data['truncate_mm'][i].split(']')[0][1:])
        if pre == 'classify':
            for key, val in data['discrete_mm'].items():
                for i in range(0, len(val), 3):
                    classi['discrete_mm'].add(val[i].split(']')[0][1:])
        else:
            for i in range(0, len(data['discrete_mm']), 3):
                classi['discrete_mm'].add(data['discrete_mm'][i].split(']')[0][1:])
    
    ixp_t = 0
    ixp_s = 0
    with open('1_ixpip_failed.%s.%s' %(vp, date), 'r') as rf:
        lines = [rf.readline() for _ in range(3)]
        while lines[0]:
            dst_ip = lines[0].split(']')[0][1:]
            if dst_ip in classi['discrete_mm']:
                ixp_s += 1
            ixp_t += 1
            lines = [rf.readline() for _ in range(3)]
    wf.write('ixp.{}.{}: {}\n'.format(vp, date, ixp_s / ixp_t))

    succeed_t = 0
    succeed_s = 0
    all_t = 0
    #stat_info['succ'][_ip] = [tmp_total, pos_avg]
    stat_info = {}
    with open('ipclass_nodstip_%s.%s.json' %(vp, date), 'r') as rf:
        stat_info = json.load(rf)
    with open('2_succip_failed.%s.%s' %(vp, date), 'r') as rf:
        lines = [rf.readline() for _ in range(3)]
        while lines[0]:
            test = False
            (mm_ips, pm_ips, _) = lines[2].split(']')
            if any(_ip in stat_info['succ'].keys() and stat_info['succ'][_ip][0] > 1 for _ip in mm_ips[1:].split(',')):
                dst_ip = lines[0].split(']')[0][1:]
                if dst_ip in classi['continuous_mm']:# or dst_ip in classi['truncate_mm']:
                    succeed_s += 1
                else:
                    a = 1
                succeed_t += 1
            all_t += 1
            lines = [rf.readline() for _ in range(3)]
    wf.write('succeed.{}.{}: {}\n'.format(vp, date, succeed_s / succeed_t))
    
    extra_t = 0
    extra_s = 0
    with open('3_extra_moas_trail.%s.%s' %(vp, date), 'r') as rf:
        lines = [rf.readline() for _ in range(3)]
        while lines[0]:
            dst_ip = lines[0].split(']')[0][1:]
            if dst_ip in classi['continuous_mm']:# or dst_ip in classi['truncate_mm']:
                extra_s += 1
            else:
                extra_t = extra_t
            extra_t += 1
            lines = [rf.readline() for _ in range(3)]
    wf.write('extra.{}.{}: {}\n'.format(vp, date, extra_s / extra_t))
    wf.close()

def union_ip_accur(date_pre, no_use):
    cal_flag = True
    print(date_pre)
    #for map_method in ['coa_rib_based', 'midar', 'ori_bdr', 'hoiho_l_bdr', 'snmp_bdr', 'sxt_bdr']:
    for map_method in ['hoiho_s_bdr']:
        if os.path.exists('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/cmp_method/union_ip_accur_info.%s.%s' %(map_method, date_pre)):
            continue
        print(date_pre + map_method)
        union_ip_accur_info = defaultdict(defaultdict)
        for vp in ['ams-nl', 'jfk-us', 'sjc2-us', 'syd-au', 'zrh2-ch', 'nrt-jp']:
            os.chdir('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/%s/' %(vp, map_method))
            for filename in glob.glob('ipaccur_nodstip_*'):
                if filename.split('.')[-2][:6] != date_pre:
                    continue
                with open(filename, 'r') as rf:
                    #ip_accur_info[_ip] = [[0, 0.0, set()], [0, 0.0, set()], [0, 0.0, set()], [0, 0.0, set()], [0, 0.0, set()]]
                    ip_accur_info = json.load(rf)
                    for _ip, val in ip_accur_info.items():
                        if _ip not in union_ip_accur_info.keys():
                            union_ip_accur_info[_ip] = [[0, 0.0, set()], [0, 0.0, set()], [0, 0.0, set()], [0, 0.0, set()], [0, 0.0, set()]]
                        for i in range(0, 5):
                            union_ip_accur_info[_ip][i][0] += val[i][0]
                            union_ip_accur_info[_ip][i][1] += val[i][1]    
                            for elem in val[i][2]:
                                (ip1, ip2) = elem
                                union_ip_accur_info[_ip][i][2].add((ip1, ip2))
        print(date_pre + map_method + ' done')
        # if cal_flag:
        #     multi = 0
        #     for _ip, val in union_ip_accur_info.items():
        #         union_sets = val[0][2] | val[1][2]
        #         union_sets = union_sets | val[3][2]
        #         union_sets = union_sets | val[4][2]
        #         if len(union_sets) > 1:
        #             multi += 1
        #     print('multi rate: {}'.format(multi / len(val)))
        #     print('total: {}'.format(len(val)))
        #     cal_flag = False
        for _ip, val in union_ip_accur_info.items():
            for elem in val:
                elem[2] = list(elem[2])
        with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/cmp_method/union_ip_accur_info.%s.%s' %(map_method, date_pre), 'w') as wf:
            json.dump(union_ip_accur_info, wf, indent=1)
    print(date_pre + 'done')

    for filename in glob.glob(r'/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/cmp_method/union_ip_accur_info*'):
        (_, map_method, t_date_pre) = filename.split('.')
        if t_date_pre != date_pre:
            continue
        if map_method != 'hoiho_s_bdr':
            continue
        union_ip_accur_info = None
        with open(filename, 'r') as rf:
            union_ip_accur_info = json.load(rf)
        stat_info = {'succ': {}, 'fail': {}, 'other': {}, 'unmap': {}, 'ixp_as': {}}
        span = {'succ': 0, 'fail': 0, 'other': 0, 'unmap': 0, 'ixp_as': 0}
        mismatch_count = 0
        for (_ip, val) in union_ip_accur_info.items():
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
        with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/cmp_method/ip_stat.%s.%s' %(map_method, date_pre), 'w') as wf:
            for (_type, val) in count.items():
                if val > 0:
                    wf.write('%s: %.2f(%d), average span: %d\n' %(_type, val / total_count, val, span[_type] / val))
                else:
                    wf.write('%s: 0.0, average span: #\n' %_type)
            wf.write('total_count: %d\n' %total_count)
        os.system('cat /mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/cmp_method/ip_stat.%s.%s' %(map_method, date_pre))
        for _type in stat_info.keys():
            stat_info[_type] = dict(sorted(stat_info[_type].items(), key=lambda d:d[1][0], reverse=True))
        with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/cmp_method/ipclass.%s.%s.json' %(map_method, date_pre), 'w') as wf:
            json.dump(stat_info, wf, indent=1)  # 写为多行

def stat_atlas_continous_mm():
    res = defaultdict(defaultdict)
    cur_date = '20221001'
    for year in range(2019, 2023):
        for month in range(1, 13):
            date = str(year) + str(month).zfill(2) + '15'
            if date > cur_date:
                break
            common_vps = {} #common_vps[asn][atlas_vp][bgp_vp] = dist
            with open('/mountdisk2/common_vps/%s/common_vp_%s.json' %(date, date), 'r') as rf:
                common_vps = json.load(rf)
            for asn, val in common_vps.items():
                for atlas_vp in val.keys():
                    cont_mm_filename = '/mountdisk2/common_vps/%s/cmp_res/sxt_bdr/ana_compare_res/stat_classify.%s.%s' %(date, atlas_vp, date)
                    if not os.path.exists(cont_mm_filename) or os.path.getsize(cont_mm_filename) == 0:
                        continue
                    try:
                        with open(cont_mm_filename, 'r') as rf:
                            data = json.load(rf)
                            cont_num = data['continuous_mm'][0]
                            trace_filename = '/mountdisk2/common_vps/%s/cmp_res/sxt_bdr/trace_stat_%s' %(date, atlas_vp)
                            with open(trace_filename, 'r') as rf2:
                                data2 = json.load(rf2)
                                total = data2['total']
                                if total > 80:
                                    res[asn + '|' + atlas_vp][date] = cont_num / total
                    except Exception as e:
                        print('err!! {}'.format(cont_mm_filename))
                        print(e)
    with open('/mountdisk2/common_vps/real_mm_rates.json', 'w') as wf:
        json.dump(res, wf, indent=1)
    os.system('cat /mountdisk2/common_vps/real_mm_rates.json')
    print(len(res))
    
def check_loop(elems):
    prevs = {elems[0], elems[1]}
    for j in range(2, len(elems)):
        if elems[j] == '*':
            continue
        if elems[j] in prevs:
            return True
        prevs.add(elems[j])
    return False    
    
def stat_atlas_continous_mm_2():
    res = defaultdict(defaultdict)
    cur_date = '20221001'
    for year in range(2019, 2023):
        for month in range(1, 13):
            date = str(year) + str(month).zfill(2) + '15'
            if date > cur_date:
                break
            common_vps = {} #common_vps[asn][atlas_vp][bgp_vp] = dist
            with open('/mountdisk2/common_vps/%s/common_vp_%s.json' %(date, date), 'r') as rf:
                common_vps = json.load(rf)
            for asn, val in common_vps.items():
                for atlas_vp in val.keys():
                    mm_filename = '/mountdisk2/common_vps/%s/cmp_res/sxt_bdr/ana_compare_res/classify.%s.%s.json' %(date, atlas_vp, date)
                    if not os.path.exists(mm_filename) or os.path.getsize(mm_filename) == 0:
                        continue
                    try:
                        with open(mm_filename, 'r') as rf:
                            data = json.load(rf)
                            rm_num = 0
                            me_num = 0
                            tmp = data['continuous_mm']
                            for i in range(2, len(tmp), 3):
                                elems = tmp[i].strip('\n').split(']')[-1].split(' ')
                                if not check_loop(elems): rm_num += 1
                            tmp = data['discrete_mm']
                            tmp1 = [line for val in tmp.values() for line in val]
                            for i in range(2, len(tmp1), 3):
                                elems = tmp1[i].strip('\n').split(']')[-1].split(' ')
                                if not check_loop(elems): me_num += 1
                            trace_filename = '/mountdisk2/common_vps/%s/cmp_res/sxt_bdr/trace_stat_%s' %(date, atlas_vp)
                            with open(trace_filename, 'r') as rf2:
                                data2 = json.load(rf2)
                                total = data2['total']
                                mm = data2['can_compare'] * (1 - data2['loop'] - data2['malpos'] - data2['match'])
                                if total > 80:
                                    res[asn + '|' + atlas_vp][date] = [rm_num / total, rm_num / mm]
                    except Exception as e:
                        print('err!! {}'.format(mm_filename))
                        print(e)
    with open('/mountdisk2/common_vps/real_mm_rates_2.json', 'w') as wf:
        json.dump(res, wf, indent=1)
    os.system('cat /mountdisk2/common_vps/real_mm_rates_2.json')
    print(len(res))
    
def stat_atlas_continous_mm_relative_rate():
    res = defaultdict(defaultdict)
    cur_date = '20221001'
    for year in range(2019, 2023):
        for month in range(1, 13):
            date = str(year) + str(month).zfill(2) + '15'
            if date > cur_date:
                break
            common_vps = {} #common_vps[asn][atlas_vp][bgp_vp] = dist
            with open('/mountdisk2/common_vps/%s/common_vp_%s.json' %(date, date), 'r') as rf:
                common_vps = json.load(rf)
            for asn, val in common_vps.items():
                for atlas_vp in val.keys():
                    cont_mm_filename = '/mountdisk2/common_vps/%s/cmp_res/sxt_bdr/ana_compare_res/stat_classify.%s.%s' %(date, atlas_vp, date)
                    if not os.path.exists(cont_mm_filename) or os.path.getsize(cont_mm_filename) == 0:
                        continue
                    try:
                        with open(cont_mm_filename, 'r') as rf:
                            data = json.load(rf)
                            res[asn + '|' + atlas_vp][date] = data['continuous_mm'][1]
                    except Exception as e:
                        print('err!! {}'.format(cont_mm_filename))
                        print(e)
    with open('/mountdisk2/common_vps/real_mm_rates_relative_rates.json', 'w') as wf:
        json.dump(res, wf, indent=1)
    os.system('cat /mountdisk2/common_vps/real_mm_rates_relative_rates.json')
    print(len(res))
    
def stat_ark_continous_mm():
    res = defaultdict(defaultdict)
    cur_date = '20221001'
    for year in range(2018, 2023):
        for month in range(1, 13):
            date = str(year) + str(month).zfill(2) + '15'
            if date > cur_date:
                break
            for vp in ['ams-nl', 'sjc2-us', 'syd-au', 'nrt-jp', 'sao-br']:
                fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/ana_compare_res/stat_classify.%s.%s*' %(vp, vp, date[:6]))
                if not fns:
                    continue
                cont_mm_filename = fns[0]
                modi_date = cont_mm_filename.split('.')[-1]
                try:
                    with open(cont_mm_filename, 'r') as rf:
                        data = json.load(rf)
                        cont_num = data['continuous_mm'][0]
                        trace_filename = '/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/trace_stat_%s.%s' %(vp, vp, modi_date)
                        with open(trace_filename, 'r') as rf2:
                            for line in rf2:
                                if line.startswith('can'):
                                    total = int(line.split(',')[0].split(':')[-1][1:])
                                    res[vp][date] = cont_num / total
                except Exception as e:
                    print('err!! {}'.format(cont_mm_filename))
                    print(e)
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/real_mm_rates.json', 'w') as wf:
        json.dump(res, wf, indent=1)
    os.system('cat /mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/real_mm_rates.json')
    print(len(res))

def stat_2nd_hop_mm():
    res = {}
    for vp in ['ams-nl', 'sjc2-us', 'syd-au', 'nrt-jp', 'sao-br']:
        res[vp] = {}
        fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/ana_compare_res/continuous_mm.*' %vp)
        for fn in fns:
            date = fn.split('.')[-1]
            with open(fn, 'r') as rf:
                total = 0
                c = 0
                lines = [rf.readline() for _ in range(3)]
                while lines[0]:
                    total += 1
                    trace_list = lines[0].strip('\n').split(']')[-1].split(' ')
                    bgp_list = lines[1].strip('\n').strip('\t').split(' ')
                    if len(bgp_list) == 1:
                        lines = [rf.readline() for _ in range(3)]
                        continue
                    for elem in trace_list:
                        if elem != '*' and elem != '?' and elem[0] != '<' and elem != bgp_list[0]:
                            if elem != bgp_list[1]: c += 1
                            break
                    lines = [rf.readline() for _ in range(3)]
                res[vp][date[:6]] = c / total
    print(res['ams-nl'])
    #t = {vp: sum(val.values()) / len(val) for vp, val in res.items()}
    t = {vp: val.values() for vp, val in res.items()}
    print(t)
                
def stat_2nd_hop_mm_atlas():
    fns = glob.glob('/mountdisk2/common_vps/*/cmp_res/sxt_bdr/ana_compare_res/continuous_mm.*')
    res = defaultdict(defaultdict)
    for fn in fns:
        tmp = fn[fn.index('.')+1:]
        date = tmp[tmp.rindex('.')+1:]
        vp = tmp[:tmp.rindex('.')]
        with open(fn, 'r') as rf:
            total = 0
            c = 0
            lines = [rf.readline() for _ in range(3)]
            while lines[0]:
                total += 1
                trace_list = lines[0].strip('\n').split(']')[-1].split(' ')
                bgp_list = lines[1].strip('\n').strip('\t').split(' ')
                if len(bgp_list) == 1:
                    lines = [rf.readline() for _ in range(3)]
                    continue
                for elem in trace_list:
                    if elem != '*' and elem != '?' and elem[0] != '<' and elem != bgp_list[0]:
                        if elem != bgp_list[1]: c += 1
                        break
                lines = [rf.readline() for _ in range(3)]
            if total > 0:
                res[vp][date] = c / total
    rates = []
    with open('/mountdisk2/common_vps/real_mm_rates_2_filter.json', 'r') as rf:
        data = json.load(rf)
        for key, val in data.items():
            _, vp = key.split('|')
            for date in val.keys():
                if vp not in res.keys() or date not in res[vp].keys():
                    continue
                rates.append(res[vp][date])
    print(np.mean(rates))

def stat_1():
    res = defaultdict(set)
    with open('/mountdisk2/common_vps/real_mm_rates_filter.json', 'r') as rf:
        data = json.load(rf)
        for key, val in data.items():
            for date in val.keys():
                res[date].add(key)
    t = {len(val) for val in res.values()}
    print(t)
    
def stat_snmp():
    snmp_ips = set()
    with open('/mountdisk1/ana_c_d_incongruity/snmpv3/2021-04-alias-sets.csv', 'r') as rf:
        for line in rf:
            if line.startswith('Node'): continue
            ips = line.strip('\n').split('|')[-1].split(',')
            snmp_ips = snmp_ips | set(ips)
    trace_dir = '/mountdisk1/ana_c_d_incongruity/traceroute_data/result/'
    tracevp_as_info = {'ams-nl': '1103', 'jfk-us': '6939', 'sjc2-us': '6939', 'syd-au': '7575', 'nrt-jp': '7660', 'sao-br': '22548'}
    trace_ips = set()
    for vp in tracevp_as_info.keys():
        trace_filenames = glob.glob(r'%strace_%s*' %(trace_dir, vp))
        for trace_filename in trace_filenames:
            with open(trace_filename, 'r') as rf:
                for line in rf:
                    ips = line.strip('\n').split(':')[-1].split(',')
                    trace_ips = trace_ips | set(ips)
    hit_ips = snmp_ips & trace_ips
    print(len(snmp_ips))
    print(len(hit_ips))
    print(len(hit_ips) / len(trace_ips))
    
def AnaSnmp():
    fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/*/ml_map_filtersndmm/ipattr_trip_AS_*_202104*.json')
    ark_ips = set()
    for fn in fns:
        with open(fn, 'r') as rf:
            data = json.load(rf)
            ark_ips = ark_ips | set(data.keys())
            #print(len(data))
    t = 0
    a = 0
    with open('/mountdisk1/ana_c_d_incongruity/snmpv3/2021-04-alias-sets.csv', 'r') as rf:
        for line in rf:
            _, __, _ips = line.strip('\n').split('|')
            ips = _ips.split(',')
            if len(ips) > 1:
                new_ips = [ip for ip in ips if not ip.__contains__(':')]
                if len(new_ips) > 1:
                    t += len(new_ips)
                    tmp = [ip for ip in new_ips if ip in ark_ips]
                    if len(tmp) > 1:
                        a += len(tmp)
    
    #print(len(ark_ips))
    print(t)
    print(a)

def FindCorrectedMappings():
    c_asn = '4809'
    rec = {}
    all_data = []
    fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/*/ml_map_filtersndmm/model_modified_ip_mappings_*_20220215.json')
    for fn in fns:
        elems = fn.split('/')
        vp = elems[4]
        mappings = {}
        with open(fn, 'r') as rf:
            mappings = json.load(rf)            
        ori_scores = {}
        with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/ipattr_score_%s_20220215.json' %(vp, vp), 'r') as rf:
            ori_scores = json.load(rf)
        recal_scores = {}
        with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/ipattr_score_recal_%s_20220215.json' %(vp, vp), 'r') as rf:
            recal_scores = json.load(rf)
        for ip, mapping in mappings.items():
            if mapping == c_asn:
                c = False
                for cur in all_data:
                    if ip in cur and mapping != cur[ip]:
                        c = True
                        break
                if c:
                    continue
                if ip in ori_scores.keys() and ori_scores[ip] < 0.5 and ip in recal_scores and recal_scores[ip] >= 0.5:
                    rec[ip] = mapping
        all_data.append(mappings)
    print(len(rec))
    with open('modified_ips_%s' %c_asn, 'w') as wf:
        json.dump(rec, wf, indent=1)


def FindCorrectedMappings_v2():
    c_asn = '4809'
    rec = {}
    all_data = []
    fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/*/ml_map_filtersndmm/model_modified_ip_mappings_*_20220215.json')
    for fn in fns:
        elems = fn.split('/')
        vp = elems[4]
        mappings = {}
        with open(fn, 'r') as rf:
            mappings = json.load(rf)                
        ori_scores = {}
        with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/ipattr_score_%s_20220215.json' %(vp, vp), 'r') as rf:
            ori_scores = json.load(rf)
        recal_scores = {}
        with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/ipattr_score_recal_%s_20220215.json' %(vp, vp), 'r') as rf:
            recal_scores = json.load(rf)     
        ConnectToBdrMapItDb('/mountdisk1/ana_c_d_incongruity/out_bdrmapit/ori_bdr/bdrmapit_%s_20220215.db' %vp)   
        ConstrBdrCache()
        for ip, mapping in mappings.items():
            if mapping == c_asn:
                c = False
                for cur in all_data:
                    if ip in cur and mapping != cur[ip]:
                        c = True
                        break
                if c:
                    continue
                ori_mapping = GetIp2ASFromBdrMapItDb(ip)
                if ori_mapping != mapping:
                    rec[ip] = [mapping, ori_scores.get(ip), recal_scores.get(ip)]
                continue
        CloseBdrMapItDb()
        InitBdrCache()
        all_data.append(mappings)
    print(len(rec))
    with open('modified_ips_%s' %c_asn, 'w') as wf:
        json.dump(rec, wf, indent=1)

def Validate_4809():
    gt = {'115.170.13','5.154.154','59.43.105','59.43.130','59.43.130','59.43.130','59.43.132','59.43.137','59.43.138','59.43.138','59.43.16','59.43.18','59.43.180','59.43.180','59.43.180','59.43.181','59.43.182','59.43.182','59.43.182','59.43.183','59.43.183','59.43.183','59.43.183','59.43.183','59.43.184','59.43.184','59.43.184','59.43.184','59.43.184','59.43.184','59.43.185','59.43.186','59.43.186','59.43.186','59.43.187','59.43.187','59.43.187','59.43.187','59.43.187','59.43.187','59.43.189','59.43.189','59.43.246','59.43.246','59.43.246','59.43.246','59.43.247','59.43.247','59.43.247','59.43.247','59.43.247','59.43.247','59.43.247','59.43.247','59.43.247','59.43.247','59.43.247','59.43.248','59.43.248','59.43.248','59.43.248','59.43.248','59.43.248','59.43.248','59.43.248','59.43.248','59.43.248','59.43.249','59.43.249','59.43.249','59.43.249','59.43.249','59.43.249','59.43.249','59.43.249','59.43.249','59.43.250','59.43.250','59.43.250','59.43.46','59.43.46','59.43.46','59.43.46','59.43.47','59.43.64','59.43.64','59.43.95','218.30.48','58.221.112','195.22.211','219.148.166','184.104.224','117.103.177','203.131.241','217.163.44','118.84.190','218.185.243','218.30.38'}
    gt_r = {'218.3.104','218.30.33','63.222.64','121.59.105','121.59.105','121.59.105','121.59.105','59.60.2','112.112.0','125.71.139','58.221.112','195.22.211','219.148.166','124.126.254','5.154.154'}
    rem = []
    c = 0
    w = 0
    with open('modified_ips_4809', 'r') as rf:
        data = json.load(rf)
        for ip in data.keys():
            if ip[:ip.rindex('.')] in gt:
                c += 1
                print(ip)
                print(data[ip])
            elif ip[:ip.rindex('.')] in gt_r:
                w += 1
            else:
                rem.append(ip)
    print(c)
    print(w)
    print(len(rem))
    with open('modified_ips_4809_rem', 'w') as wf:
        wf.write('\n'.join(rem))

def stat_rm():
    res = {'rmr': defaultdict(list), 'amr': defaultdict(list)}
    for m in ['ori_bdr', 'ml_map']:
        rm_fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/*/%s_filtersndmm/ana_compare_res/stat_classify.*' %m)
        for rm_fn in rm_fns:
            elems = rm_fn.split('.')
            vp, date = elems[-2], elems[-1]
            with open(rm_fn, 'r') as rf:
                data = json.load(rf)
                r = data["continuous_mm"][0]
                d = data["discrete_mm"][0]
                res['rmr'][m].append(r/d)
                with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/%s_filtersndmm/trace_stat_%s.%s' %(vp, m, vp, date), 'r') as rf1:
                    for line in rf1:
                        if line.startswith('can'):
                            t = int(line.split(',')[0].split(': ')[-1])
                            res['amr'][m].append(r/t)
                            break
    with open('amr_rmr.json', 'w') as wf:
        json.dump(res, wf, indent=1)
    
    
def new_count_rm_process(m, date):
    rates = []
    checksiblings = None
    checksiblings2 = None
    pfx2as_info = {}
    tmp_fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/*/%s_filtersndmm/mm_*.%s' %(m, date))
    #tmp_fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/*/%s/mm_*.%s' %(m, date))
    for fn in tmp_fns:
        vp = fn.split('_')[-1].split('.')[0]
        # if os.path.exists('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/%s_filtersndmm/real_mm_%s.%s' %(vp, m, vp, date)):
        #     continue
        if not checksiblings:
            checksiblings = CheckSiblings(date)
            dt = datetime.datetime.strptime(date[4:6] + '/' + date[6:8] + '/' + date[2:4] + ' 00:00:00', '%m/%d/%y %H:%M:%S')
            next_date = (dt + datetime.timedelta(days=30)).strftime('%Y%m%d')
            checksiblings2 = CheckSiblings(next_date[:6]+'15')
            InitPref2ASInfo_2('/mountdisk1/ana_c_d_incongruity/rib_data/rv_prefix2as/' + date + '.pfx2as', pfx2as_info)
        ConnectToBdrMapItDb('/mountdisk1/ana_c_d_incongruity/out_bdrmapit/ori_bdr/bdrmapit_%s_%s.db' %(vp, date))
        ConstrBdrCache()
        ip_to_as = GetBdrCache()
        if m == 'ml_map':
            with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/model_modified_ip_mappings_%s_%s.json' %(vp, vp, date), 'r') as rf:
                data = json.load(rf)
                ip_to_as.update(data)
        t = 0
        rm = []
        print('{}{}'.format(date, vp))
        with open(fn, 'r') as rf:                
            lines = [rf.readline() for _ in range(3)]
            while lines[0]:
                t += 1
                if t % 10000 == 0: print(t)
                dst_ip, trace = lines[0][1:].strip('\n').split(']')
                bgp_list = lines[1].strip('\n').strip('\t').split(' ')
                ip_list = lines[2].strip('\n').split(']')[-1].split(' ')
                if dst_ip != ip_list[-1]:
                    if ip_to_as.get(dst_ip) == trace.split(' ')[-1]:
                        ip_list.append(dst_ip)
                (trace_list, trace_to_ip_info, _) = CompressTrace(trace.split(' '), ip_list, 'n')
                r = False
                trace_list = [hop for hop in trace_list if hop.isdigit() and hop != '-1']
                susp_hops = set()
                for hop in trace_list:
                    if hop not in bgp_list:
                        cur_ips = trace_to_ip_info[hop]
                        if len(cur_ips) > 1:
                            prefs = set()
                            for cur_ip in cur_ips:
                                cur_pref = GetBGPPath_Or_OriASN(pfx2as_info, cur_ip, 'get_prefix')
                                if not cur_pref:
                                    cur_pref = cur_ip[:cur_ip.rindex('.')] + '.0'
                                prefs.add(cur_pref)
                            if len(prefs) > 1:
                                susp_hops.add(hop)
                                break
                if not r:
                    left, right, j = bgp_list[0], None, None
                    for i in range(len(trace_list)):
                        if trace_list[i] in bgp_list:
                            left = trace_list[i]
                        else:
                            for j in range(i+1, len(trace_list)):
                                if trace_list[j] in bgp_list:
                                    right = trace_list[j]
                                    break
                            if not j:
                                continue
                            if j == len(trace_list) - 1:
                                right = bgp_list[-1]
                            d_free = check_path_valley_free_v2(checksiblings, trace_list[i-1:j+1])
                            if not d_free:
                                d_free = check_path_valley_free_v2(checksiblings2, trace_list[i-1:j+1])
                            if d_free:
                                if j - i > 1: #有多个mismatch
                                    r = True
                                elif trace_list[i] in susp_hops:
                                    r = True
                                else:
                                    c_segs = bgp_list[bgp_list.index(left):bgp_list.index(right)+1]
                                    if not check_path_valley_free_v2(checksiblings, c_segs) and not check_path_valley_free_v2(checksiblings2, c_segs):
                                        r = True
                            if r:
                                break
                            i = j - 1
                if r:
                    d_free = check_path_valley_free_v2(checksiblings, trace_list)
                    if not d_free:
                        d_free = check_path_valley_free_v2(checksiblings2, trace_list)
                    if d_free:
                        rm = rm + lines    
                lines = [rf.readline() for _ in range(3)]
        #print('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/%s_filtersndmm/real_mm_%s.%s' %(vp, m, vp, date))
        with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/%s_filtersndmm/real_mm_%s.%s' %(vp, m, vp, date), 'w') as wf:
        #with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/%s/real_mm_%s.%s' %(vp, m, vp, date), 'w') as wf:
            wf.write(''.join(rm))
        if t > 0:
            rates.append(len(rm) / 3 / t)
        InitBdrCache()
        CloseBdrMapItDb()
    if rates:
        print(rates)
    return rates

def new_count_rm_process_v2(m, date):
    rates = []
    checksiblings = None
    checksiblings2 = None
    pfx2as_info = {}
    tmp_fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/*/%s_filtersndmm/mm_*.%s' %(m, date))
    #tmp_fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/*/%s/mm_*.%s' %(m, date))
    for fn in tmp_fns:
        vp = fn.split('_')[-1].split('.')[0]
        # if os.path.exists('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/%s_filtersndmm/real_mm_%s.%s' %(vp, m, vp, date)):
        #     continue
        # if vp != 'sao-br':
        #     continue
        if not checksiblings:
            checksiblings = CheckSiblings(date)
            dt = datetime.datetime.strptime(date[4:6] + '/' + date[6:8] + '/' + date[2:4] + ' 00:00:00', '%m/%d/%y %H:%M:%S')
            next_date = (dt + datetime.timedelta(days=30)).strftime('%Y%m%d')
            checksiblings2 = CheckSiblings(next_date[:6]+'15')
            InitPref2ASInfo_2('/mountdisk1/ana_c_d_incongruity/rib_data/rv_prefix2as/' + date + '.pfx2as', pfx2as_info)
        ixp_asns = []
        with open('/mountdisk1/ana_c_d_incongruity/as_rel_cc_data/%s01.as-rel3.txt' %date[:6], 'r') as rf:
            for line in rf:
                if line[:len('# IXP ASes:')] == '# IXP ASes:':
                    ixp_asns = line[len('# IXP ASes:'):].strip('\n').strip(' ').split(' ')        
        #print('ixp_asns: {}'.format(ixp_asns))
        ConnectToBdrMapItDb('/mountdisk1/ana_c_d_incongruity/out_bdrmapit/ori_bdr/bdrmapit_%s_%s.db' %(vp, date))
        ConstrBdrCache()
        ip_to_as = GetBdrCache()
        if m == 'ml_map':
            with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/model_modified_ip_mappings_%s_%s.json' %(vp, vp, date), 'r') as rf:
                data = json.load(rf)
                ip_to_as.update(data)
        t = 0
        rm = []
        me = []
        print('{}{}'.format(date, vp))
        with open(fn, 'r') as rf:                
            lines = [rf.readline() for _ in range(3)]
            while lines[0]:
                t += 1
                if t % 10000 == 0: print(t)
                dst_ip, trace = lines[0][1:].strip('\n').split(']')
                bgp_list = lines[1].strip('\n').strip('\t').split(' ')
                ip_list = lines[2].strip('\n').split(']')[-1].split(' ')
                if dst_ip != ip_list[-1]:
                    if ip_to_as.get(dst_ip) == trace.split(' ')[-1]:
                        ip_list.append(dst_ip)
                (trace_list, trace_to_ip_info, _) = CompressTrace(trace.split(' '), ip_list, 'n')
                r = False
                trace_list = [hop for hop in trace_list if hop.isdigit() and hop != '-1' and hop not in ixp_asns]
                bgp_list = [hop for hop in bgp_list if hop not in ixp_asns]
                susp_hops = set()
                for hop in trace_list:
                    if hop not in bgp_list:
                        cur_ips = trace_to_ip_info[hop]
                        if len(cur_ips) > 1:
                            prefs = set()
                            for cur_ip in cur_ips:
                                cur_pref = GetBGPPath_Or_OriASN(pfx2as_info, cur_ip, 'get_prefix')
                                if not cur_pref:
                                    cur_pref = cur_ip[:cur_ip.rindex('.')] + '.0'
                                prefs.add(cur_pref)
                            if len(prefs) > 1:
                                susp_hops.add(hop)
                                break
                if not r:
                    left, right, j = bgp_list[0], None, None
                    for i in range(len(trace_list)):
                        if trace_list[i] in bgp_list:
                            left = trace_list[i]
                        else:
                            for j in range(i+1, len(trace_list)):
                                if trace_list[j] in bgp_list:
                                    break
                            if not j:
                                continue                            
                            if j - i > 1: #有多个mismatch
                                r = True
                            elif trace_list[i] in susp_hops:
                                r = True
                            if r:
                                break
                            i = j - 1
                d_free = check_path_valley_free_v2(checksiblings, trace_list)
                if not d_free:
                    d_free = check_path_valley_free_v2(checksiblings2, trace_list)
                if d_free:
                    if r:
                        rm = rm + lines
                    elif not check_path_valley_free_v2(checksiblings, bgp_list) and not check_path_valley_free_v2(checksiblings2, bgp_list):
                        rm = rm + lines
                elif not r:
                    me = me + lines
                lines = [rf.readline() for _ in range(3)]
        #print('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/%s_filtersndmm/real_mm_%s.%s' %(vp, m, vp, date))
        with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/%s_filtersndmm/real_mm_%s.%s' %(vp, m, vp, date), 'w') as wf:
        #with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/%s/real_mm_%s.%s' %(vp, m, vp, date), 'w') as wf:
            wf.write(''.join(rm))
        with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/%s_filtersndmm/mapping_error_%s.%s' %(vp, m, vp, date), 'w') as wf:
        #with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/%s/real_mm_%s.%s' %(vp, m, vp, date), 'w') as wf:
            wf.write(''.join(me))
        if t > 0:
            rates.append(len(rm) / 3 / t)
        InitBdrCache()
        CloseBdrMapItDb()
    if rates:
        print(rates)
    return rates


    
def new_count_rm(m):
    #rates = []
    pool = Pool(processes=g_parell_num)
    paras = []
    for year in range(2018, 2023):
        for month in range(1, 13):
            fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/*/%s_filtersndmm/mm_*.%s*' %(m, str(year)+str(month).zfill(2)))
            #fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/*/%s/mm_*.%s*' %(m, str(year)+str(month).zfill(2)))
            dates = {fn.split('.')[-1] for fn in fns if fn[-1].isdigit()}
            for date in dates:
               paras.append((m, date)) 
    res = pool.starmap(new_count_rm_process_v2, paras)
    pool.close()
    pool.join()
    
    rec = []
    for rates in res:
        rec = rec + [str(rate) for rate in rates]
    print(rec)
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/rmr_%s' %m, 'w') as wf:
        wf.write('\n'.join(rec))

def new_count_rm_direct():
    for m in ['ml_map']:#'ori_bdr', 
        rrm = []
        arm = []
        rme = []
        ame = []
        fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/*/%s_filtersndmm/real_mm_*' %m)
        for fn in fns:
            #print(fn)
            vp, date = fn.split('/')[-1].split('_')[-1].split('.') 
            rm = 0
            with open(fn, 'r') as rf1:
                rm = len(rf1.readlines()) / 3
            me = 0
            with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/%s_filtersndmm/mapping_error_%s.%s' %(vp, m, vp, date), 'r') as rf2:
                me = len(rf2.readlines()) / 3
            rt = 0
            t_fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/%s_filtersndmm/mm_%s.%s*' %(vp, m, vp, date[:6]))
            if not t_fns:
                continue
            with open(t_fns[0], 'r') as rf:
                rt = len(rf.readlines()) / 3
                # for line in rf:
                #     if line.startswith('c'):
                #         t = int(line.split(':')[1].split(',')[0].strip(' '))
            at = 0
            t_fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/%s_filtersndmm/trace_stat_%s.%s*' %(vp, m, vp, date[:6]))
            if not t_fns:
                continue
            with open(t_fns[0], 'r') as rf:
                for line in rf:
                    if line.startswith('c'):
                        at = int(line.split(':')[1].split(',')[0].strip(' '))
            if rt > 0:
                rrm.append(rm / rt)
                arm.append(rm / at)
                if rm/at > 0.05:
                    print(vp+date+m)
                    print(rm/at)
                rme.append(me / rt)
                ame.append(me / at)
        #rrm = sorted(rrm)
        #print(rates)    
        rrm = [str(rate) for rate in rrm]
        arm = [str(rate) for rate in arm]
        rme = [str(rate) for rate in rme]
        ame = [str(rate) for rate in ame]
        with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/rmr_%s' %m, 'w') as wf:
            wf.write('\n'.join(rrm))
        with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/amr_%s' %m, 'w') as wf:
            wf.write('\n'.join(arm))
        with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/ame_%s' %m, 'w') as wf:
            wf.write('\n'.join(ame))
        with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/rme_%s' %m, 'w') as wf:
            wf.write('\n'.join(rme))

def check_succeed_mapping():
    fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/*/ml_map_filtersndmm/ipclass_nodstip_*.json')
    for fn in fns:
        vp, date, _ = fn.split('_')[-1].split('.')
        succ_ips = set()
        with open(fn, 'r') as rf:
            data = json.load(rf)
            succ_ips = set(data['succ'].keys())
        real_mms = set()
        with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/real_mm_%s.%s' %(vp, vp, date), 'r') as rf:
            lines = [rf.readline() for _ in range(3)]
            while lines[0]:
                dst_ip = lines[0].split(']')[0][1:]
                real_mms.add(dst_ip)
                lines = [rf.readline() for _ in range(3)]
        succ_mappings = set()
        with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/mm_%s.%s' %(vp, vp, date), 'r') as rf:
            lines = [rf.readline() for _ in range(3)]
            while lines[0]:
                dst_ip = lines[0].split(']')[0][1:]
                a = 0
                if dst_ip in real_mms:
                    a = 1
                mm_ips, _, _ = lines[2].split(']')
                mm_ips = mm_ips[1:].split(',')
                if any(ip in succ_ips for ip in mm_ips): #success-mapping
                    succ_mappings.add(dst_ip)
                lines = [rf.readline() for _ in range(3)]

        print(len(real_mms & succ_mappings) / len(succ_mappings))
        #print(len(real_mms & succ_mappings) / len(real_mms))
        
        
def check_succeed_mapping_v2():
    fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/*/ori_bdr_filtersndmm/ipclass_nodstip_*.json')
    succ_mappings_res = []
    for fn in fns:
        vp, date, _ = fn.split('_')[-1].split('.')
        succ_ips = set()
        with open(fn, 'r') as rf:
            data = json.load(rf)
            succ_ips = set(data['succ'].keys())
        # real_mms = set()
        # with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/real_mm_%s.%s' %(vp, vp, date), 'r') as rf:
        #     lines = [rf.readline() for _ in range(3)]
        #     while lines[0]:
        #         dst_ip = lines[0].split(']')[0][1:]
        #         real_mms.add(dst_ip)
        #         lines = [rf.readline() for _ in range(3)]
        succ_mappings = set()
        with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ori_bdr_filtersndmm/mm_%s.%s' %(vp, vp, date), 'r') as rf:
            lines = [rf.readline() for _ in range(3)]
            while lines[0]:
                dst_ip = lines[0].split(']')[0][1:]
                mm_ips, _, _ = lines[2].split(']')
                mm_ips = mm_ips[1:].split(',')
                if any(ip in succ_ips for ip in mm_ips): #success-mapping
                    succ_mappings.add(dst_ip)
                lines = [rf.readline() for _ in range(3)]
        #print(len(real_mms & succ_mappings) / len(succ_mappings))
        #print(len(succ_mappings))
        succ_mappings_res.append(len(succ_mappings))
    print(sum(succ_mappings_res) / len(succ_mappings_res))    
    
    
def check_extra_tail():
    #fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/*/ori_bdr_filtersndmm/ipclass_nodstip_*.json')
    fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/*/ori_bdr_filtersndmm/ipclass_nodstip_*.json')
    dates = [fn.split('.')[-2] for fn in fns]
    extra_tails = []
    for date in dates:
        print(date)
        checksiblings = CheckSiblings(date)
        fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/*/ori_bdr_filtersndmm/mm_*.%s' %date)
        for fn in fns:
            print(fn)
            with open(fn, 'r') as rf:
                extra_tail = 0
                lines = [rf.readline() for _ in range(3)]
                while lines[0]:
                    dst_ip, trace = lines[0][1:].strip('\n').split(']')
                    bgp_list = lines[1].strip('\n').strip('\t').split(' ')
                    ip_list = lines[2].strip('\n').split(']')[-1].split(' ')
                    trace_list = []
                    for hop in trace.split(' '):
                        if hop.isdigit() and hop != '-1' and hop not in trace_list:
                            trace_list.append(hop)
                    if bgp_list[-1] in trace_list:
                        ind = trace_list.index(bgp_list[-1])
                        if ind < len(trace_list) - 1:
                            if all(elem in bgp_list for elem in trace_list[:ind]):
                                if checksiblings.bgp.rel(int(trace_list[ind]), int(trace_list[ind+1])) == 1:
                                    extra_tail += 1
                    lines = [rf.readline() for _ in range(3)]
                extra_tails.append(extra_tail)
    print(sum(extra_tails) / len(extra_tails))    
                    
def main_func():
    #check_extra_tail()
    #check_succeed_mapping_v2()
    #check_succeed_mapping()
    #new_count_rm_process_v2('ori_bdr', '20211015')
    #new_count_rm('ori_bdr')
    #new_count_rm_direct()
    #new_count_rm_direct()
    #FindCorrectedMappings()
    #FindCorrectedMappings_v2()
    #Validate_4809()
    #AnaSnmp()
    #stat_rm()
    #stat_snmp()
    #stat_1()
    #stat_2nd_hop_mm_atlas()
    # #stat_2nd_hop_mm()
    #stat_atlas_continous_mm_2()
    # #stat_atlas_continous_mm_relative_rate()
    # union_ip_accur('201801', True)
    # task_list = []
    # for year in range(2018, 2021):
    #     for month in range(1, 13):
    #         task = Process(target=union_ip_accur, args=(str(year) + str(month).zfill(2), True))
    #         task_list.append(task)
    #         task.start()
    # for task in task_list:
    #     task.join()
    return

    cur_date = '20220301'
    paras = []
    foratlas = False
    for year in range(2018, 2023):
        for month in range(1, 13):
            date = str(year) + str(month).zfill(2) + '15'
            if date > cur_date:
                break
            # if date[:6] != '202202':
            #     continue
            if foratlas:
                paras.append((date, True))
            else:
                for vp in ['ams-nl', 'sjc2-us', 'syd-au', 'nrt-jp', 'sao-br']:
                #for vp in ['syd-au']:
                    #fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/trace_stat_%s.%s*' %(vp, vp, date[:6]))
                    fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/trace_stat_%s.%s*' %(vp, vp, date[:6]))
                    if fns:
                        modi_date = fns[0].split('/')[-1].split('.')[-1]
                        paras.append((vp, modi_date))
    pool = Pool(processes=g_parell_num)
    #pool = Pool(processes=1)
    if foratlas:
        results = pool.starmap(PerTask_Atlas, paras)
    else:
        results = pool.starmap(PerTask, paras)
    pool.close()
    pool.join()
    if foratlas:
        stat_atlas_continous_mm()
    else:
        stat_ark_continous_mm()
    return
    
    pool = Pool(processes=g_parell_num)
    paras = []
    for vp in ['ams-nl', 'sjc2-us', 'syd-au', 'nrt-jp', 'sao-br']:
        dates = {filename.split('.')[-1] for filename in glob.glob(r'/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ori_bdr_filtersndmm/match_*' %vp)}
        for date in dates:
            # if date[:6] != '202202':
            #     continue
            if not os.path.exists('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ori_bdr_filtersndmm/ana_compare_res/continuous_mm.%s.%s' %(vp, vp, date)):
                continue
            for pre in ['neighAs_classify', 'classify', 'jsac_2_classify']:
                paras.append((vp, date, pre))
                #PerTask_EvalDiscri(vp, date, pre)
    pool.starmap(PerTask_EvalDiscri, paras)
    pool.close()
    pool.join()

    # for (vp, date) in [('ams-nl', '20201216'), ('jfk-us', '20201016'), ('nrt-jp', '20201215'), ('sjc2-us', '20201215'), ('syd-au', '20201215'), ('zrh2-ch', '20200714')]:
    #     print('{}.{}'.format(vp, date))
    #     check_diff_path_in_other_bgp_part1(vp, date)
    
    #for (vp, date) in [('ams-nl', '20201216'), ('jfk-us', '20201016'), ('nrt-jp', '20201215'), ('sjc2-us', '20201215'), ('syd-au', '20201215'), ('zrh2-ch', '20200714')]:
    # for (vp, date) in [('syd-au', '20201215'), ('zrh2-ch', '20200714')]:
    # #for (vp, date) in [('ams-nl', '20201216')]:
    #     print(vp + date)
    #     prefix_paths = collect_one_date_all_bgp_paths(date)
    #     print('len of prefix_paths: {}'.format(len(prefix_paths)))
    #     #check_diff_path_in_other_bgp_part2_v2(prefix_paths, vp, date)
    #     check_diff_path_in_other_bgp_part3(prefix_paths, vp, date)

if __name__ == '__main__':
    main_func()
