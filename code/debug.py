
from collections import defaultdict
import json
import glob
import os
import sys
import re
import socket
import struct
from typing import Counter, Dict

#import scipy as sp
#from sklearn.datasets import fetch_species_distributions

from utils_v2 import DropStarsInTraceList, IsSib_2, GetSibRel, GetIxpPfxDict_2, IsIxpIp, GetIxpAsSet, IsIxpAs, ClearIxpPfxDict
from get_ip2as_from_bdrmapit import ConnectToBdrMapItDb, ConstrBdrCache, GetIp2ASFromBdrMapItDb, CloseBdrMapItDb, InitBdrCache, ConstrLocalBdrCache
from compare_cd import InitPref2ASInfo, GetBGPPath_Or_OriASN, InitBGPPathInfo, CompressTrace
from traceutils.ixps import AbstractPeeringDB, create_peeringdb
from rect_bdrmapit import CheckSiblings
from ana_compare_res import drop_stars_in_trace, cal_mm_segs

def CmpUnmap(json_filename, old_filename):
    ip_accur_info = {}
    count_1 = count_2 = 0
    with open(json_filename, 'r') as rf:
        ip_accur_info = json.load(rf)
    ips = set()
    with open(old_filename, 'r') as rf:
        ips = set(rf.read().split('\n'))
    for _ip in ip_accur_info['unmap'].keys():
        if _ip not in ips:
            if _ip.split('.')[-1] != '1':
                count_1 += 1
            else:
                count_2 += 1
                print(_ip)
    print(count_1)
    print(count_2)

def CheckUnmap(filenames):
    ips = set()
    for filename in filenames:
        with open(filename, 'r') as rf:
            curline_trace = rf.readline()
            while curline_trace:
                nouse = rf.readline()
                curline_ip = rf.readline()
                trace_list = curline_trace.split(']')[-1].strip('\n').strip(' ').split(' ')
                ip_list = curline_ip.split(']')[-1].strip('\n').strip(' ').split(' ')
                for i in range(0, len(trace_list)):
                    if trace_list[i] == '?':
                        ips.add(ip_list[i])
                curline_trace = rf.readline()
    print(len(ips))
    with open('debug', 'w') as wf:
        wf.write('\n'.join(list(ips)))

def CountDstInMidarUnmap(filename):
    stat_info = {}
    count_1 = count_2 = 0
    with open(filename, 'r') as rf:
        stat_info = json.load(rf)
    for (_type, info1) in stat_info.items():
        for (_ip, info2) in info1.items():
            if _ip.split('.')[-1] == '1' and info2[1] == 1:
                if info2[0] == 1:
                    count_1 += 1
                else:
                    print(_ip)
            count_2 += 1
    print(count_1)
    print(count_1 / count_2)

def CmpWithAndNoDst(filename):
    stat_info1 = {}
    with open('stat1_' + filename, 'r') as rf:
        stat_info1 = json.load(rf)
    stat_info2 = {}
    with open('stat2_nodstip_' + filename, 'r') as rf:
        stat_info2 = json.load(rf)
    total_count1 = total_count2 = 0
    for (_type, info) in stat_info1.items():
        total_count1 += len(info)
        total_count2 += len(stat_info2[_type])
    print(filename)
    for (_type, info) in stat_info1.items():
        print('%s: %.2f\t%.2f' %(_type, len(info) / total_count1, len(stat_info2[_type]) / total_count2))
    print('total: %d\t%d' %(total_count1, total_count2))

def CmpMethods(filename):
    print(filename)
    stat_info = {}
    total_count = {}
    for map_method in ['rib_based', 'bdrmapit', 'midar']:
        stat_info[map_method] = {}
        with open(map_method + '/stat2_nodstip_' + filename, 'r') as rf:
            stat_info[map_method] = json.load(rf)
            total_count[map_method] = 0
            for _type in stat_info[map_method].keys():
                total_count[map_method] += len(stat_info[map_method][_type])
    print('type\trib_based\tbdrmapit\tmidar')
    for _type in ['succ', 'fail', 'other', 'unmap']:
        print('%s\t' %_type, end='')
        for map_method in ['rib_based', 'bdrmapit', 'midar']:        
            print('%.2f\t' %(len(stat_info[map_method][_type]) / total_count[map_method]), end='')
        print('')
    print('total\t', end='')
    for map_method in ['rib_based', 'bdrmapit', 'midar']:        
        print('%d\t' %total_count[map_method], end='')
    print('')

def CmpLoopRate():
    loop_info = {}
    for map_method in ['rib_based', 'bdrmapit', 'midar']:
        loop_info[map_method] = {}
    for vp in ['ams-nl', 'jfk-us', 'sjc2-us', 'syd-au', 'zrh2-ch', 'nrt-jp']:
        os.chdir('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/' + vp + '/')
        for map_method in ['rib_based', 'bdrmapit', 'midar']:
            if vp not in loop_info[map_method].keys():
                loop_info[map_method][vp] = {}
            filenames = glob.glob(r'%s/stat_%s*' %(map_method, vp))
            for filename in filenames:
                date = filename.split('.')[-1]
                with open(filename, 'r') as rf:
                    data = rf.read()
                    find_res = re.findall(r'loop:(.*)?\n', data)
                    loop_info[map_method][vp][date] = float(find_res[0])
    # for vp in loop_info['rib_based'].keys():
    #     for date in loop_info['rib_based'][vp].keys():
    #         print('%s.%s\t' %(vp, date), end='')        
    #         print('%.2f\t%.2f\t%.2f' %(loop_info['rib_based'][vp][date], loop_info['bdrmapit'][vp][date], loop_info['midar'][vp][date]))
    return loop_info

def CalAvg():
    data_info = {}
    for vp in ['ams-nl', 'jfk-us', 'sjc2-us', 'syd-au', 'zrh2-ch', 'nrt-jp']:
        data_info[vp] = {}
        print('%s&' %vp, end='')
        for _type in ['succ', 'fail', 'other', 'unmap']:
            data_info[vp][_type] = {'rib_based': 0, 'bdrmapit': 0, 'midar': 0}
            for map_method in data_info[vp][_type].keys():
                filenames = glob.glob(r'/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/%s/stat_nodstip_stat1_ipaccur_%s*' %(vp, map_method, vp))
                for filename in filenames:
                    with open(filename, 'r') as rf:
                        data = rf.read()
                        find_res = re.findall(r'%s: .*?span: (.*?)\n' %_type, data)
                        data_info[vp][_type][map_method] += int(find_res[0])
                data_info[vp][_type][map_method] /= len(filenames)
                print('%d&' %data_info[vp][_type][map_method], end='')
        print('')
            
def CalBdrmapItVoters(filename, obj_ip):
    votes = {'pre': set(), 'succ': set(), 'possi_succ': set(), 'dst': set()}
    with open(filename, 'r') as rf:
        line_trace = rf.readline()
        while line_trace:
            line_bgp = rf.readline().strip('\n')
            line_ip = rf.readline().strip('\n')
            #print(line_trace)
            trace_list = line_trace[line_trace.index(']') + 1:-1].split(' ')
            ip_list = line_ip.split(' ')
            dst_as = line_bgp.split(' ')[-1]
            dst_ip = line_trace[1:line_trace.index(']')]
            if obj_ip not in ip_list:
                line_trace = rf.readline()
                while line_trace and not line_trace.__contains__(']'):
                    line_trace = rf.readline()
                continue
            pos = ip_list.index(obj_ip)
            if pos == len(ip_list) - 1: #last ip
                votes['dst'].add(dst_ip + '|' + dst_as)
            else:
                tmp_as = trace_list[pos + 1]
                if tmp_as != '*' and tmp_as != '?':
                    votes['succ'].add(ip_list[pos + 1] + '|' + tmp_as)
                else:
                    for i in range(pos + 1, len(ip_list)):
                        tmp_as = trace_list[i]
                        if tmp_as != '*' and tmp_as != '?':
                            votes['possi_succ'].add(ip_list[i] + '|' + tmp_as)
                            break
                        if i == len(ip_list) - 1: #last ip
                            votes['dst'].add(dst_ip + '|' + dst_as)
                for i in range(pos - 1, 0, -1):
                    tmp_as = trace_list[i]
                    if tmp_as != '*' and tmp_as != '?':
                        votes['pre'].add(ip_list[i] + '|' + tmp_as)
                        break
            line_trace = rf.readline()
            while line_trace and not line_trace.__contains__(']'):
                line_trace = rf.readline()
    stat_votes = {}
    for (type, val) in votes.items():
        stat_votes[type] = {}
        for sub_val in val:
            [_ip, asn] = sub_val.split('|')
            if asn not in stat_votes[type].keys():
                stat_votes[type][asn] = 0
            stat_votes[type][asn] += 1
    for (type, val) in stat_votes.items():
        sorted_list = sorted(val.items(), key=lambda d:d[1], reverse=True)
        print(type + ':')
        print(sorted_list)

def SelMPLSIps(filename): #/mountdisk3/traceroute_download_all/back/test
    ip_form = re.compile('\d+\.\d+\.\d+\.\d+')
    spec_ips = {'87.245.230.95': False, '129.250.196.202': False, '200.160.199.110': False, '84.235.111.163': False, '84.16.6.161': False, '110.9.3.95': False, '5.175.255.131': False, '195.219.87.50': False, '84.235.111.183': False, '176.52.252.239': False}
    #spec_ips = {'80.156.162.22': False, '188.111.153.134': False, '80.179.165.57': False, '64.124.98.202': False, '111.87.3.218': False, '59.128.3.46': False, '154.54.85.234': False, '154.54.31.158': False, '111.87.3.218': False, '77.67.54.163': False, '59.128.3.46': False, '80.179.165.213': False, '198.7.255.114': False, '80.179.165.209': False, '189.125.24.238': False, '195.22.214.235': False, '193.251.154.102': False, '180.87.12.26': False, '221.5.247.182': False, '59.153.9.241': False, '154.24.43.30': False, '154.54.47.30': False, '23.30.206.242': False, '190.106.192.243': False, '159.226.254.158': False, '80.179.165.17': False}
    #with open(filename + '_mpls', 'w') as wf:
    with open(filename, 'r') as rf:
        lines = rf.readlines(100000)
        while lines:
            for line in lines:
                #if line.__contains__('frpla') or line.__contains__('rtla') or \
                #    line.__contains__('uturn'):
                if line.__contains__('Labels'):
                    find_res = re.findall(ip_form, line)
                    if find_res:
                        if find_res[0] in spec_ips.keys():
                            print(line, end='')
                            spec_ips[find_res[0]] = True
                    #wf.write(line)
            lines = rf.readlines(100000)
    for (_ip, flag) in spec_ips.items():
        if not flag:
            print(_ip)

def FindDupIPs(filename):
    fun_ips = {}
    with open(filename, 'r') as rf:
        lines = rf.readlines(100000)
        while lines:
            for line in lines:
                ip_list = line[line.index(':') + 1:-1].split(',')
                for i in range(1, len(ip_list)):
                    if ip_list[i] != '*' and ip_list[i] == ip_list[i - 1]:
                        fun_ip = ip_list[i - 2]
                        if fun_ip != '*':
                            if fun_ip not in fun_ips.keys():
                                fun_ips[fun_ip] = set()
                            fun_ips[fun_ip].add(ip_list[i])
            lines = rf.readlines(100000)
    # for (fun_ip, succ_ips) in fun_ips.items():
    #     print(fun_ip + ':', end='')
    #     print(succ_ips)
    spec_ips = {'87.245.230.95': False, '129.250.196.202': False, '200.160.199.110': False, '84.235.111.163': False, '84.16.6.161': False, '110.9.3.95': False, '5.175.255.131': False, '195.219.87.50': False, '84.235.111.183': False, '176.52.252.239': False}
    for _ip in spec_ips:
        if _ip in fun_ips.keys():
            print(_ip + ':', end='')
            print(fun_ips[_ip])

def CmpPfx2AS(ip2as_filename, bgp_filename):
    pfx2as = {}
    with open(ip2as_filename, 'r') as rf:
        curlines = rf.readlines()
        while curlines:
            for curline in curlines:
                (pfx, asn) = curline.strip('\n').split(' ')
                pfx2as[pfx] = asn
            curlines = rf.readlines()
    pfx2as_bgp = {}
    with open(bgp_filename, 'r') as rf:
        curlines = rf.readlines()
        while curlines:
            for curline in curlines:
                (pfx, path) = curline.strip('\n').split('|')
                if pfx not in pfx2as_bgp.keys():
                    pfx2as_bgp[pfx] = path.split(' ')[-1]
            curlines = rf.readlines()
    print(len(set(pfx2as.keys()).difference(set(pfx2as_bgp.keys()))))
    print(len(set(pfx2as_bgp.keys()).difference(set(pfx2as.keys()))))
    print('total1: %d' %len(pfx2as))
    print('total1: %d' %len(pfx2as_bgp))
    with open('difference', 'w') as wf:
        for pfx in set(pfx2as.keys()) & set(pfx2as_bgp.keys()):
            if pfx2as[pfx] != pfx2as_bgp[pfx]:
                wf.write('%s: (%s)(%s)\n' %(pfx, pfx2as[pfx], pfx2as_bgp[pfx]))

def ReadPeerDB(filename):
    pb = {}
    with open(filename, 'r') as rf:
        pb = json.load(rf)
    
    _ip = '123.255.90.158'
    for elem in pb['netixlan']['data']:
        if elem['ipaddr4'] == _ip:
            print(elem)
            return

def CalMulPreHop(filename):
    GetIxpPfxDict_2(2018, 8)
    pre_info = {}
    with open(filename, 'r') as rf:
        curlines = rf.readlines(100000)
        while curlines:
            for curline in curlines:
                (dst, ips) = curline[:-1].split(':')
                ip_list = ips.split(',')
                ip_list_len = len(ip_list)
                if ip_list_len < 2:
                    continue
                for i in range(1, ip_list_len - 1):
                    if ip_list[i] not in pre_info.keys():
                        pre_info[ip_list[i]] = [set(), 0, 0]
                    pre_info[ip_list[i]][0].add(ip_list[i - 1])
                    pre_info[ip_list[i]][1] += 1
                    pre_info[ip_list[i]][2] += ((i + 1) / ip_list_len)
                if ip_list[-1] != dst:
                    if ip_list[-1] not in pre_info.keys():
                        pre_info[ip_list[-1]] = [set(), 0, 0]
                    pre_info[ip_list[-1]][0].add(ip_list[-2])
                    pre_info[ip_list[-1]][1] += 1
                    pre_info[ip_list[-1]][2] += 1
                curlines = rf.readlines(100000)
    count = 0
    pos_list = []
    pre_info2 = {}
    for (_ip, val) in pre_info.items():
        val[0].discard('*')
        if len(val[0]) > 1:                        
            if not IsIxpIp(_ip):
                print(_ip + ':' + ','.join(list(val[0])))
                count += 1
                pos_list.append(val[2] / val[1])
    #print(count)
    #print(pos_list)

def PrintJsonIndent(filename):
    with open(filename, 'r') as rf:
        data = json.load(rf)
        with open('test.json', 'w') as wf:
            json.dump(data, wf, indent=1)

def TestLoadPeeringdbFiles():
    os.chdir('/mountdisk1/ana_c_d_incongruity/peeringdb_data/')
    for filename in os.listdir():
        if filename.endswith('json'):
            print(filename)
            with open(filename, 'r') as rf:
                data = json.load(rf)

#def CalUnmapBdr(filename):

def CmpAbTraces():
    filename_pre = '/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/nrt-jp/'
    filename_suffix = '/ab_nrt-jp.20180815'
    data = {'ori_bdrmapit': {}, 'bdrmapit': {}}
    for method in data.keys():
        with open(filename_pre + method + filename_suffix, 'r') as rf:
            curline_trace = rf.readline()
            while curline_trace:
                curline_bgp = rf.readline()
                curline_ip = rf.readline()
                dst_ip = curline_trace.split(']')[0][1:]
                data[method][dst_ip] = [curline_trace, curline_bgp, curline_ip]
                curline_trace = rf.readline()
    ori_keys = set(data['ori_bdrmapit'].keys())
    new_keys = set(data['bdrmapit'].keys())
    with open('ori_ab', 'w') as wf:
        for dst_ip in ori_keys.difference(new_keys):
            wf.write(''.join(data['ori_bdrmapit'][dst_ip]))
    with open('new_ab', 'w') as wf:
        for dst_ip in new_keys.difference(ori_keys):
            wf.write(''.join(data['bdrmapit'][dst_ip]))

def GetProviders(filename, asn):
    with open(filename, 'r') as rf:
        for line in rf.readlines():
            if line.__contains__(asn):
                print(line.split(' ')[0])

def CheckLastHopAnno():
    dst_bgp_info = {}
    for filename in ['/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/nrt-jp/bdrmapit/match_nrt-jp.20180815', \
                    '/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/nrt-jp/bdrmapit/ab_nrt-jp.20180815']:
        with open(filename, 'r') as rf:
            curline_trace = rf.readline()
            while curline_trace:
                curline_bgp = rf.readline()
                curline_ip = rf.readline()
                dst_ip = curline_trace.split(']')[0][1:]
                bgp_list = curline_bgp.strip('\n').strip('\t').split(' ')
                dst_bgp_info[dst_ip] = bgp_list
                curline_trace = rf.readline()
            
    ConnectToBdrMapItDb('/mountdisk1/ana_c_d_incongruity/out_bdrmapit/bdrmapit_nrt-jp_20180815.db')
    ConstrBdrCache()
    bgp_path_info = {}
    InitPref2ASInfo('/mountdisk1/ana_c_d_incongruity/out_ip2as_data/20180815.ip2as.prefixes', bgp_path_info)
    bgp_path_info_1 = {}
    InitBGPPathInfo('/mountdisk1/ana_c_d_incongruity/rib_data/bgpdata/bgp_203.181.248.168_20180815', bgp_path_info_1)
    count = {'high': [0, 0], 'mid': [0, 0]}
    count1 = [0, 0]
    with open('lasthops_res', 'r') as rf:
        for data in rf.readlines():
            [dst_ip, hop] = data.strip('\n').split(':')
            [last_hop_addr, last_hop_asn] = hop.split('|')
            # if last_hop_addr == dst_ip:
            #     continue            
            if dst_ip in dst_bgp_info.keys():
                bdr_asn = GetIp2ASFromBdrMapItDb(last_hop_addr)
                if bdr_asn in dst_bgp_info[dst_ip]:
                    count1[0] += 1
                count1[1] += 1
                if last_hop_asn.startswith('-'):
                    last_hop_asn = last_hop_asn[1:]
                    if last_hop_asn in dst_bgp_info[dst_ip]:
                        count['mid'][0] += 1
                    else:
                        rib_asn = GetBGPPath_Or_OriASN(bgp_path_info, last_hop_addr, 'get_orias_2')
                        print(data.strip('\n') + '|' + bdr_asn + '|' + rib_asn + '|' + GetBGPPath_Or_OriASN(bgp_path_info_1, last_hop_addr, 'get_orias'))
                        pass
                    count['mid'][1] += 1
                else:
                    if last_hop_asn in dst_bgp_info[dst_ip]:
                        count['high'][0] += 1
                    elif bdr_asn in dst_bgp_info[dst_ip]:
                        rib_asn = GetBGPPath_Or_OriASN(bgp_path_info, last_hop_addr, 'get_orias_2')
                        # print(data.strip('\n') + '|' + bdr_asn + '|' + rib_asn + '|' + GetBGPPath_Or_OriASN(bgp_path_info_1, last_hop_addr, 'get_orias'))
                        # os.system('grep -w %s /mountdisk1/ana_c_d_incongruity/as_rel_cc_data/20180801.as-rel3.txt | grep %s' %(bdr_asn, last_hop_asn))
                        # os.system('grep -w %s /mountdisk1/ana_c_d_incongruity/as_rel_cc_data/20180801.as-rel3.txt | grep %s' %(last_hop_asn, rib_asn))
                        # os.system('grep %s /mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/nrt-jp/bdrmapit/*20180815 -B 2' %last_hop_addr)
                        # print('\n')
                        pass
                    count['high'][1] += 1                
    for _type in count.keys():
        print('{}: {}({})'.format(_type, count[_type][0] / count[_type][1], count[_type][1]))
    print('bdr: {}({})'.format(count1[0] / count1[1], count1[1]))
                    
def CheckPrefixAnnounce():
    dst_ips = []
    with open('/home/slt/code/ana_c_d_incongruity/dstips.dat', 'r') as rf:
        dst_ips = rf.read().split('\n')
    sel_vp = '7660'
    bgp_all = defaultdict(set)
    bgp_vp = defaultdict(set)
    with open('test', 'r') as rf:
        curlines = rf.readlines(100000)
        while curlines:
            for curline in curlines:
                elems = curline.split('|')
                [vp, pref, path] = elems[4:7]
                ori_as = path.split(' ')[-1]
                bgp_all[pref].add(ori_as)
                if vp == sel_vp:
                    bgp_vp[pref].add(ori_as)
            curlines = rf.readlines(100000)
    with open('7660_susp_prefixes', 'w') as wf:
        wf.write('ip:specific-prefix(ori-asn)|7660-prefix(ori-asn)\n')
        for _ip in dst_ips:
            if _ip == '':
                continue
            (pref1, ori_asn1) = GetBGPPath_Or_OriASN(bgp_all, _ip, 'get_all_2')
            (pref2, ori_asn2) = GetBGPPath_Or_OriASN(bgp_vp, _ip, 'get_all_2')
            if pref1 != pref2 or ori_asn1 != ori_asn2:
                wf.write('{}: {}({})|{}({})\n'.format(_ip, pref1, ori_asn1, pref2, ori_asn2))

def ModifyCC(filename):
    info = {}
    with open(filename, 'r') as rf:
        curlines = rf.readlines(10000)
        while curlines:
            for curline in curlines:
                if curline.startswith('#'):
                    continue
                elems = curline.strip('\n').split(' ')
                subs = elems[1:]
                if elems[0] in subs:
                    subs.remove(elems[0])
                else:
                    print(elems)
                    pass
                info[elems[0]] = set(subs)
            curlines = rf.readlines(10000)
    
    loop_num = 0
    while True:
        loop_num += 1
        print('loop {}'.format(loop_num))
        add_info = {}
        for (asn, subs) in info.items():
            add_subs = set()
            for sub_asn in subs:
                if sub_asn in info.keys():
                    for sub_sub_asn in info[sub_asn]:
                        if sub_sub_asn not in subs:
                            add_subs.add(sub_sub_asn)
            if add_subs:
                add_info[asn] = add_subs
        if not add_info:
            break
        for (asn, add_subs) in add_info.items():
            info[asn] = info[asn] | add_subs
    
    with open(filename + '_2', 'w') as wf:
        for (asn, subs) in info.items():
            wl = [asn] + list(subs) + [asn]
            wf.write(' '.join(wl))
            wf.write('\n')

def CheckFailedIPs():
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/nrt-jp/bdrmapit/stat2_nodstip_ipaccur_nrt-jp.20180815.json', 'r') as rf:
        mydata = json.load(rf)
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/nrt-jp/ori_bdrmapit/stat2_nodstip_ipaccur_nrt-jp.20180815.json', 'r') as rf:
        oridata = json.load(rf)
    ori_fail = oridata['fail'].keys()
    num = 0
    for (ip, info) in mydata['fail'].items():
        if ip not in ori_fail:
            if int(info[0]) > 100:
                print('{}:{}'.format(ip, info))
            num += 1
    print('num: {}'.format(num))

def check_common_prefix():
    ips1 = {'203.99.100', '202.158.138', '203.81.250', '119.110.112', '202.43.72', '117.104.211', '103.104.34', '202.129.227', '119.110.127', '203.99.96', '103.43.0', '203.153.101', '222.165.233', '23.13.0', '118.151.212', '202.123.230', '43.243.155', '103.53.184', '103.143.52', '202.123.234', '222.165.230', '117.104.197', '103.29.151', '116.58.195', '103.140.66', '103.143.114', '202.46.95', '223.255.230', '119.110.121', '103.140.88', '103.85.117', '116.58.194', '122.129.116', '103.243.246', '103.137.146', '203.99.97', '203.99.103', '117.104.220', '117.104.202', '203.99.110', '103.143.190', '103.143.110', '119.110.122', '202.153.30', '103.247.101', '117.104.199', '118.151.213', '118.151.214', '202.123.227', '202.95.132', '114.142.172', '103.229.77', '117.104.218', '119.110.120', '119.110.114', '116.58.193', '103.53.185', '103.43.1', '117.104.195', '43.243.153'}
    ips2 = {'212.117.56', '202.127.111', '103.135.50', '202.46.73', '103.135.25', '202.127.101', '101.0.6', '202.68.180', '122.129.118', '202.125.100', '192.23.186', '103.48.13', '103.255.52', '103.151.194', '103.150.12', '119.11.192', '202.95.150', '45.126.152', '202.127.102', '103.30.220', '203.77.248', '203.123.247', '202.182.162', '146.196.106', '117.103.35', '183.81.153', '103.152.70', '202.95.158', '103.135.0', '202.182.168', '103.53.189', '103.54.93', '103.136.6', '203.190.242', '203.123.243', '45.116.156', '103.211.233', '203.160.60', '202.6.220', '43.245.189', '103.89.139', '202.95.154', '103.105.130', '202.123.237', '203.123.245', '103.223.0', '103.104.133', '202.95.157', '203.123.248', '103.121.23', '103.84.5', '103.52.45', '103.93.227', '202.171.0', '103.52.47', '103.84.233', '202.127.108', '202.46.78', '103.84.234', '103.133.36', '103.135.6', '75.96.161', '202.46.89', '203.80.9', '203.123.241', '103.211.232', '103.50.217', '66.96.237', '103.94.98', '103.93.230', '202.46.72', '103.69.178', '203.123.233', '202.46.93', '202.46.85', '202.46.92', '117.103.32', '203.123.251', '203.123.238', '103.130.136', '103.119.65', '202.68.184', '122.129.107', '103.84.4', '103.151.100', '103.223.3', '43.247.15', '202.46.86', '202.46.71', '103.135.4', '103.150.16', '103.84.232', '210.79.209', '202.4.189', '210.79.208', '202.127.98', '203.160.56', '203.77.229', '202.93.135', '122.129.109', '103.29.149', '202.46.91', '202.127.105', '45.116.157', '122.129.106', '103.117.206', '203.123.235', '203.123.237', '121.58.191', '103.93.226', '103.85.222', '103.223.1', '203.123.228', '103.149.225', '122.129.110', '202.46.84', '203.123.253', '121.58.188', '202.93.136', '203.160.58', '103.53.188', '180.150.245', '203.123.234', '202.127.99', '203.190.244', '203.80.11', '203.77.230', '103.54.92', '124.158.129', '202.46.64', '139.5.155', '103.135.5', '203.123.240', '103.104.132', '103.237.134', '203.210.83', '103.147.246', '103.153.190', '122.144.6', '103.255.12', '202.127.107', '121.58.189', '203.77.225', '103.94.97', '103.152.90', '202.68.178', '103.135.3', '43.247.36', '183.81.158', '203.123.229', '146.196.107', '203.77.254', '103.89.248', '103.107.151', '124.158.128', '103.80.93', '103.151.18', '103.152.110', '202.95.144', '202.127.97'}

    pref16_1 = {pref[:pref.rindex('.')] for pref in ips1}
    pref16_2 = {pref[:pref.rindex('.')] for pref in ips2}
    if pref16_1 & pref16_2:
        print(pref16_1 & pref16_2)

def rewrite_peering():
    os.chdir('/mountdisk1/ana_c_d_incongruity/peeringdb_data/')
    with open('peeringdb_2020_12_15.json', 'r') as rf:
        data = json.load(rf)
        for (_key, _val) in data.items():
            with open('test_' + _key + '.json', 'w') as wf:
                json.dump(_val, wf, indent=1)

def get_rels(date, asns_list):
    print(asns_list)
    checksiblings = CheckSiblings(date)
    #asns_list = asns.split(' ')
    for i in range(len(asns_list)):
        for j in range(i + 1, len(asns_list)):
            if checksiblings.check_sibling(asns_list[i], asns_list[j]):
                print('{}, {}: {}, sibling'.format(asns_list[i], asns_list[j], checksiblings.bgp.reltype(int(asns_list[i]), int(asns_list[j]))))
            else:
                print('{}, {}: {}'.format(asns_list[i], asns_list[j], checksiblings.bgp.reltype(int(asns_list[i]), int(asns_list[j]))))

def cal_reply_rate(filename):
    total = 0
    reach = 0
    with open(filename, 'r') as rf:
        for line in rf:
            if line.startswith('#'):
                continue
            total += 1
            if line.split('\t')[6] == 'R':
                reach += 1
    print(total)
    print(reach)

# debug_prefix = '113.29.97.'


# def tag_nets(pref, id, record_neighs, cache, tag, group):
#     tag[pref] = id
#     group[id].add(pref)        
#     for neigh in record_neighs[pref].keys():
#         if neigh not in tag.keys():
#             tag_nets(neigh, id, record_neighs, cache, tag, group)
#         elif tag[neigh] != id:
#             print('tag[neigh]: {}, id: {}'.format(tag[neigh], id))

# def check_net_diff_asn():
#     record_neighs = {}
#     with open('test_neighs_asn.json', 'r') as rf:
#         record_neighs = json.load(rf)
#     record = {}
#     for key, val in record_neighs.items():
#         if len(val) > 1 or list(val.keys())[0] != key.split('|')[1]:
#             record[key] = val
#     with open('test_neighs_asn_diff.json', 'w') as wf:
#         json.dump(record, wf, indent=1)
#     print(len(record_neighs))
#     print(len(record))

def check_exits():
    os.chdir('/mountdisk1/ana_c_d_incongruity/traceroute_data/result/')
    filenames = glob.glob(r'trace_ams-nl.2020*')
    info = defaultdict(Counter)
    for filename in filenames:
    #for filename in ['trace_ams-nl.20191116']:
        if filename == 'trace_ams-nl.20191116':
            continue
        date = filename.split('.')[-1]
        with open(filename, 'r') as rf:
            for line in rf:
                #if line.count(',') >= 2:
                    #info[date][line.split(':')[-1].split(',')[2]] += 1
                if line.count(',') >= 3:
                    info[date][line.split(':')[-1].split(',')[3]] += 1
    for date, val in info.items():
        sort = sorted(val.items(), key=lambda x:x[1], reverse=True)
        #print('{}: {}'.format(date, sort[0]))
        print('{}: {}'.format(date, sort))

def strip_mm_in_ams_nl():
    os.chdir('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/ams-nl/sxt_bdr/')
    first_dates_set = []
    for month in [11, 12]:
        first_dates_set.append('2018' + str(month).zfill(2))
    for month in [1, 4, 5, 6]:
        first_dates_set.append('2019' + str(month).zfill(2))
    for date in first_dates_set:
        filename = glob.glob(r'mm_ams-nl.%s*' %date)[0]
        #os.system('cp %s ori_%s' %(filename, filename))
        with open(filename, 'w') as wf:
            with open('ori_' + filename, 'r') as rf:
                lines = [rf.readline() for _ in range(3)]
                while lines[0]:
                    if lines[0].__contains__('224 224'):
                        pass
                    else:
                        wf.write(''.join(lines))
                    lines = [rf.readline() for _ in range(3)]
    return
    second_dates_set = []
    for month in range(7, 13):
        if month == 11:
            continue
        second_dates_set.append('2019' + str(month).zfill(2))
    for month in range(1, 13):
        second_dates_set.append('2020' + str(month).zfill(2))
    for date in second_dates_set:
        filename = glob.glob(r'mm_ams-nl.%s*' %date)[0]
        # os.system('cp %s ori_%s' %(filename, filename))
        with open(filename, 'w') as wf:
            with open('ori_' + filename, 'r') as rf:
                lines = [rf.readline() for _ in range(3)]
                while lines[0]:
                    (mm_ips, pm_ips, _) = lines[2].split(']')
                    ips_str = mm_ips[1:] + ',' + pm_ips[1:]
                    if ips_str.__contains__('109.105.97.') or ips_str.__contains__('134.222.155.'):
                        pass
                    else:
                        wf.write(''.join(lines))
                    lines = [rf.readline() for _ in range(3)]

def restore_mm_in_ams_nl():
    os.chdir('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/ams-nl/sxt_bdr/')
    first_dates_set = []
    for month in [11, 12]:
        first_dates_set.append('2018' + str(month).zfill(2))
    for month in [1, 4, 5, 6]:
        first_dates_set.append('2019' + str(month).zfill(2))
    for date in first_dates_set:
        filename = glob.glob(r'mm_ams-nl.%s*' %date)[0]
        os.system('cp ori_%s %s' %(filename, filename))
    second_dates_set = []
    for month in range(7, 13):
        if month == 11:
            continue
        second_dates_set.append('2019' + str(month).zfill(2))
    for month in range(1, 13):
        second_dates_set.append('2020' + str(month).zfill(2))
    for date in second_dates_set:
        filename = glob.glob(r'mm_ams-nl.%s*' %date)[0]
        os.system('cp ori_%s %s' %(filename, filename))

def cal_detour_in_jfk_us():
    os.chdir('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/jfk-us/sxt_bdr/')
    total = 0
    detour = 0
    ixp = 0
    for filename in glob.glob(r'mm_jfk-us*'):
        date = filename.split('.')[-1]
        GetIxpPfxDict_2(int(date[:4]), int(date[4:6]))
        with open(filename, 'r') as rf:            
            lines = [rf.readline() for _ in range(3)]
            while lines[0]:
                total += 1
                bgp_path = lines[1].strip('\n').strip('\t')
                bgp_list = bgp_path.split(' ')
                #if len(bgp_list) == 2:
                if len(bgp_list) > 1:
                    (dst_ip, trace) = lines[0][1:].strip('\n').split(']')
                    if dst_ip == '190.131.247.1':
                        print('')
                    ori_trace_list = trace.split(' ')
                    (_, _, ip_path) = lines[2].strip('\n').split(']')
                    ip_list = ip_path.split(' ')              
                    (trace_list, trace_to_ip_info, _) = CompressTrace(ori_trace_list, ip_list, '6939')
                    comp_trace_list = drop_stars_in_trace(trace_list)
                    if bgp_list[1] in comp_trace_list and comp_trace_list.index(bgp_list[1]) > 1:
                        detour += 1
                        _ip = trace_to_ip_info[comp_trace_list[1]][0]
                        if IsIxpIp(_ip):
                            ixp += 1
                lines = [rf.readline() for _ in range(3)]
            print('{}, {}'.format(detour / total, ixp / detour))
        ClearIxpPfxDict()

def cal_branches_in_jfk_us():
    os.chdir('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/jfk-us/sxt_bdr/')
    total = 0
    branches = 0
    ixp = 0
    for filename in glob.glob(r'mm_jfk-us*'):
        # date = filename.split('.')[-1]
        # GetIxpPfxDict_2(int(date[:4]), int(date[4:6]))
        with open(filename, 'r') as rf:            
            lines = [rf.readline() for _ in range(3)]
            while lines[0]:
                total += 1
                bgp_path = lines[1].strip('\n').strip('\t')
                bgp_list = bgp_path.split(' ')
                #if len(bgp_list) == 2:
                if len(bgp_list) > 2:
                    (dst_ip, trace) = lines[0][1:].strip('\n').split(']')
                    ori_trace_list = trace.split(' ')
                    (_, _, ip_path) = lines[2].strip('\n').split(']')
                    ip_list = ip_path.split(' ')              
                    (trace_list, trace_to_ip_info, _) = CompressTrace(ori_trace_list, ip_list, '6939')
                    comp_trace_list = drop_stars_in_trace(trace_list)
                    end_bgp_index = 0
                    end_trace_index = 0
                    for i in range(1, len(bgp_list)):
                        if bgp_list[i] in comp_trace_list:
                            end_bgp_index = i
                            end_trace_index = comp_trace_list.index(bgp_list[i])
                            break
                    if end_bgp_index > 1 and end_trace_index > 1:
                        if not (set(comp_trace_list[1:end_trace_index]) & set(bgp_list[1:end_bgp_index])):
                            branches += 1
                            #print(comp_trace_list)
                            #print(bgp_list)
                lines = [rf.readline() for _ in range(3)]
            print('{}'.format(branches / total))
        #ClearIxpPfxDict()

def b1():
    a = {'20190815': ['109.105.97.49'], '20191216': ['109.105.97.143'], '20200115': ['109.105.97.145'], '20200216': ['109.105.97.143'], '20200516': ['109.105.97.241'], '20200816': ['109.105.97.143', '109.105.97.120'], '20200915': ['109.105.97.241', '109.105.97.120'], '20201015': ['109.105.97.143', '109.105.97.120', '109.105.97.241'], '20201116': ['109.105.97.143', '109.105.97.241', '109.105.97.120'], '20201216': ['109.105.97.120', '109.105.97.145'], '20200315': ['109.105.97.120'], '20200416': ['109.105.97.120'], '20200715': ['109.105.97.143', '109.105.97.120']}
    for date, ips in a.items():
        print(date)
        ConnectToBdrMapItDb('/mountdisk1/ana_c_d_incongruity/out_bdrmapit/sxt_bdr/bdrmapit_ams-nl_%s.db' %date)
        ConstrBdrCache()
        for _ip in ips:
            print(GetIp2ASFromBdrMapItDb(_ip))
        CloseBdrMapItDb()
        InitBdrCache()

def stat_sibling_bgp_links():    
    for year in range(2018, 2021):
        for month in range(1, 13):
            date = str(year) + str(month).zfill(2)
            checksiblings = CheckSiblings(date)
            for filename in glob.glob(r'/mountdisk1/ana_c_d_incongruity/rib_data/bgpdata/bgp_*%s15' %date):
                with open(filename, 'r') as rf:
                    links = set()
                    for line in rf:
                        (pref, path) = line.strip('\n').split('|')
                        path_list = path.split(' ')
                        prev_elem = path_list[0]
                        for elem in path_list[1:]:
                            if not prev_elem.__contains__('{') and not elem.__contains__('{'):
                                if int(prev_elem) < 0xFFFFFF and int(elem) < 0xFFFFFF:
                                    links.add((prev_elem, elem))
                            prev_elem = elem
                    total = len(links)
                    sibling = 0
                    for link in links:
                        #print(link)
                        if checksiblings.check_sibling(link[0], link[1]):
                            sibling += 1
                    print('{}'.format(sibling / total))

def stat_sibling_traceroute_links():    
    for year in range(2018, 2021):
        for month in range(1, 13):
            date = str(year) + str(month).zfill(2)
            checksiblings = CheckSiblings(date)
            links = set()
            filenames = []#glob.glob(r'/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/ams-nl/sxt_bdr/match_ams-nl.%s15' %date)
            filenames = filenames + glob.glob(r'/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/nrt-jp/sxt_bdr/mm_*.%s15' %date)
            for filename in filenames:
                with open(filename, 'r') as rf:
                    lines = [rf.readline() for _ in range(3)]                    
                    while lines[0]:
                        path = lines[0].strip('\n').split(']')[-1]
                        (path_list, _, __) = CompressTrace(path.split(' '), lines[2].strip('\n').split(']')[-1].split(' '), 'n')
                        prev_elem = path_list[0]
                        for elem in path_list[1:]:
                            if prev_elem == '*' or prev_elem == '?' or prev_elem.startswith('<') or\
                                elem == '*' or elem == '?' or elem.startswith('<'):
                                pass
                            else:
                                if int(prev_elem) < 0xFFFFFF and int(elem) < 0xFFFFFF:
                                    links.add((prev_elem, elem))
                            prev_elem = elem
                        lines = [rf.readline() for _ in range(3)]
            if links:
                total = len(links)
                sibling = 0
                for link in links:
                    #print(link)
                    if checksiblings.check_sibling(link[0], link[1]):
                        sibling += 1
                print('{}'.format(sibling / total))

def cmp_rect_bdr_and_before_rect(vp, date):
    before_data = {}
    if not os.path.exists('/mountdisk1/ana_c_d_incongruity/out_bdrmapit/sxt_bdr/before_rect_bdrmapit_%s_%s.db' %(vp, date)):
        return
    ConnectToBdrMapItDb('/mountdisk1/ana_c_d_incongruity/out_bdrmapit/sxt_bdr/before_rect_bdrmapit_%s_%s.db' %(vp, date))
    ConstrLocalBdrCache(before_data)
    CloseBdrMapItDb()
    after_data = {}
    ConnectToBdrMapItDb('/mountdisk1/ana_c_d_incongruity/out_bdrmapit/sxt_bdr/bdrmapit_%s_%s.db' %(vp, date))
    ConstrLocalBdrCache(after_data)
    CloseBdrMapItDb()
    ipclass = {}
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/ipclass_nodstip_%s.%s.json' %(vp, vp, date), 'r') as rf:
        ipclass = json.load(rf)
    #diff_data = {}
    total = 0
    succ = 0
    for _ip, asn in after_data.items():
        if _ip in before_data.keys() and before_data[_ip] != asn:
            #diff_data[_ip] = asn
            total += 1
            if _ip in ipclass['succ'].keys():
                succ += 1
    print('{}({})'.format(succ / total, total))

def recal_ams_nl_trace_mm_rate():
    os.chdir('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/ams-nl/sxt_bdr/')
    for year in range(2018, 2021):
        for month in range(1, 13):
            date = str(year) + str(month).zfill(2)
            match_len = 0
            mm_len = 0
            ori_mm_len = 0
            for filename in glob.glob(r'match_ams-nl.%s*' %date):
                with open(filename, 'r') as rf:
                    match_len = len(rf.readlines())
            for filename in glob.glob(r'mm_ams-nl.%s*' %date):
                with open(filename, 'r') as rf:
                    mm_len = len(rf.readlines())
            # for filename in glob.glob(r'ori_mm_ams-nl.%s*' %date):
            #     with open(filename, 'r') as rf:
            #         ori_mm_len = len(rf.readlines())
            if ori_mm_len == 0: ori_mm_len = mm_len
            print('{}:{}'.format(date, mm_len / (ori_mm_len + match_len)))

def check_snmp_common_ips():
    os.chdir('/mountdisk1/ana_c_d_incongruity/out_bdrmapit/sxt_bdr/')
    dbnames = ['bdrmapit_ams-nl_20201216.db', 'bdrmapit_nrt-jp_20201215.db', 'bdrmapit_sjc2-us_20201215.db', 'bdrmapit_syd-au_20201215.db']
    dbs_ips = {}
    for dbname in dbnames:
        vp_date = dbname[len('bdrmapit_'):-1*len('.db')]
        ConnectToBdrMapItDb(dbname)
        tmp = {}
        ConstrLocalBdrCache(tmp)
        dbs_ips[vp_date] = set(tmp.keys()).copy()
    common_counts = Counter()
    with open('/mountdisk1/ana_c_d_incongruity/snmpv3/2021-04-alias-sets.csv', 'r') as rf:
        for line in rf:
            (id, _, ips) = line.split('|')
            ip_list = ips.split(',')
            if len(ip_list) == 1:
                continue
            for _ip in ip_list:
                if _ip.__contains__(':'):
                    continue
                for vp_date, ips in dbs_ips.items():
                    if _ip in ips:
                        common_counts[vp_date] += 1
    for vp_date, count in common_counts.items():
        print(vp_date + ':', end='')
        print('{}({})'.format(count / len(dbs_ips[vp_date]), count))

def check_diff_bgp(asn):
    prev_pref2as = {}
    InitPref2ASInfo('/mountdisk1/ana_c_d_incongruity/out_ip2as_data/20180115.ip2as.prefixes', prev_pref2as)
    prev_asn_prefs = set()
    for pref, asns in prev_pref2as.items():
        if asn in asns:
            prev_asn_prefs.add(pref)
    for year in range(2018, 2021):
        for month in range(1, 13):
            date = str(year) + str(month).zfill(2)
            if date == '201801':
                continue
            cur_pref2as = {}
            filename = glob.glob('/mountdisk1/ana_c_d_incongruity/out_ip2as_data/%s*.ip2as.prefixes' %date)[0]
            print(date)
            InitPref2ASInfo(filename, cur_pref2as)
            cur_asn_prefs = set()
            for pref, asns in cur_pref2as.items():
                if asn in asns:
                    cur_asn_prefs.add(pref)
            add_prefs = cur_asn_prefs.difference(prev_asn_prefs)
            sub_prefs = prev_asn_prefs.difference(cur_asn_prefs)
            print('add prefs: ', end='')
            for pref in add_prefs:
                if pref in prev_pref2as.keys():
                    print('{}({}),'.format(pref, prev_pref2as[pref]), end='')
                else:
                    print('{}(),'.format(pref), end='')
            print('\nsub prefs: {}\n\n'.format(sub_prefs))
            prev_asn_prefs = cur_asn_prefs
            prev_pref2as = cur_pref2as

def check_match_prefix_for_spec_mid_ips():
    bgp_path_info = {}
    multi_path_ips = defaultdict(lambda:defaultdict(list))
    for year in range(2019, 2021):
        for month in range(1, 13):
            date_pre = str(year) + str(month).zfill(2)
            if date_pre < '201907':
                continue
            filename = glob.glob('/mountdisk1/ana_c_d_incongruity/rib_data/bgpdata/bgp_80.249.208.34_%s*' %date_pre)[0]
            date = filename.split('_')[-1]
            print(date + ':')
            InitBGPPathInfo(filename, bgp_path_info)
            total = 0
            multi_path = 0
            #with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/ams-nl/sxt_bdr/match_ams-nl.%s' %date, 'r') as rf:
            with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/ams-nl/sxt_bdr/mm_ams-nl.%s' %date, 'r') as rf:
                lines = [rf.readline() for _ in range(3)]
                while lines[0]:
                    #if lines[2].__contains__('134.222.155.83 134.222.155.82'):
                    if lines[2].__contains__('77.67.76.34 77.67.76.33'):
                        total += 1
                        (dst_ip, _) = lines[0].split(']')                        
                        dst_ip = dst_ip[1:]
                        # if dst_ip != '176.234.72.1':
                        #     lines = [rf.readline() for _ in range(3)]
                        #     continue
                        paths_prefs = GetBGPPath_Or_OriASN(bgp_path_info, dst_ip, 'get_paths_prefs')
                        paths = set()
                        for (cur_prefix, cur_paths) in paths_prefs:
                            for cur_path in cur_paths:
                                paths.add(cur_path)
                        if True:#len(paths) > 1:
                            #multi_path += 1
                            multi_path_ips[dst_ip][date] = paths_prefs
                    lines = [rf.readline() for _ in range(3)]
            #print('')
            bgp_path_info.clear()
            print('{}(total:{})'.format(multi_path / total, total))
            print(total - multi_path)
    s = sorted(multi_path_ips.items(), key=lambda x:len(x[1]), reverse=True)
    with open('test1', 'w') as wf:
        for (dst_ip, val) in s:
            if len(val) > 1:
                print('{}:{}'.format(dst_ip, val.keys()))
            wf.write('{}:{}\n'.format(dst_ip, len(val)))
            for date, paths_prefs in val.items():
                wf.write('\t{}:{}\n'.format(date, paths_prefs))

def find_common_trace():
    traces = set()
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/ams-nl/sxt_bdr/ana_compare_res/diff_path_others.ams-nl.20201216', 'r') as rf:
        lines = rf.readlines()
        for i in range(0, len(lines), 3):
            (dst_ip, trace) = lines[i][1:].strip('\n').split(']')
            #concerned_ips[dst_ip] = socket.ntohl(struct.unpack("I",socket.inet_aton(dst_ip))[0])
            ip_list = lines[i + 2].strip('\n').split(']')[-1].split(' ')
            (trace_list, trace_to_ip_info, _) = CompressTrace(trace.split(' '), ip_list, 'n')
            traces.add(' '.join(trace_list))
    print(len(traces))

def cal_double_hops(filename):
    total = 0
    double = 0
    with open(filename, 'r') as rf:
        for line in rf:
            (dst, trace) = line.split(':')
            trace_list = trace.strip('\n').split(',')
            for i in range(1, len(trace_list)):
                if trace_list[i] == trace_list[i - 1]:
                    double += 1
                    break
            total += 1
    print(double / total)

def check_moas(filename, vp, date):
    total = 0
    moas = 0
    rel = Counter()
    dbname = glob.glob('/mountdisk1/ana_c_d_incongruity/out_bdrmapit/sxt_bdr/bdrmapit_%s_%s*' %(vp, date))[0]
    ConnectToBdrMapItDb(dbname)
    ConstrBdrCache()
    checksiblings = CheckSiblings(date)
    with open(filename, 'r') as rf:
        lines = [rf.readline() for _ in range(3)]
        while lines[0]:
            total += 1
            (dst_ip, trace) = lines[0].split(']')
            bdr_asn = GetIp2ASFromBdrMapItDb(dst_ip[1:])
            vp_asn = lines[1].strip('\n').strip('\t').split(' ')[-1]
            if bdr_asn != vp_asn and bdr_asn != '':
                moas += 1
                rel[checksiblings.bgp.reltype(int(vp_asn), int(bdr_asn))] += 1
            lines = [rf.readline() for _ in range(3)]
    CloseBdrMapItDb()
    InitBdrCache()
    print(moas / total)
    print(rel)

def stat_missing_segs(filename):
    total = 0
    t = 0
    with open(filename, 'r') as rf:
        lines = [rf.readline() for _ in range(3)]
        while lines[0]:
            (dst_ip, trace) = lines[0].strip('\n').split(']')
            bgp_path = lines[1].strip('\t').strip('\n')
            ip_list = lines[2].strip('\n').split(']')[-1].split(' ')
            (trace_list, trace_to_ip_info, _) = CompressTrace(trace.split(' '), ip_list, 'n')
            conform = False
            (mm_seg_num, mm_segs) = cal_mm_segs(bgp_path, trace_list)
            for seg in mm_segs:
                [mm_bgp_seg, mm_trace_seg] = seg
                comp_mm_trace_seg = drop_stars_in_trace(mm_trace_seg)
                if len(comp_mm_trace_seg) < len(mm_bgp_seg):
                    total += 1
                    if len(comp_mm_trace_seg) + 1 == len(mm_bgp_seg):
                        t += 1
            lines = [rf.readline() for _ in range(3)]
    print(t / total)

def ana_vp2dstAS(s_filename):
    vp = 'ams-nl'
    date = '20201216'
    os.chdir('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr' %vp)
    path_info = defaultdict(lambda: defaultdict(list))
    for filename in ['mm_%s.%s' %(vp, date), 'match_%s.%s' %(vp, date)]:
        with open(filename, 'r') as rf:
            lines = [rf.readline() for _ in range(3)]
            while lines[0]:
                (dst_ip, trace) = lines[0][1:].strip('\n').split(']')
                ori_bgp = lines[1].strip('\t').strip('\n')
                ip_list = lines[2].strip('\n').split(']')[-1].split(' ')
                (trace_list, trace_to_ip_info, _) = CompressTrace(trace.split(' '), ip_list, 'n')
                comp_trace_list = drop_stars_in_trace(trace_list)
                comp_trace = ' '.join(comp_trace_list)
                if filename.startswith('mm'):
                    path_info[comp_trace][ori_bgp].append(dst_ip)
                else:
                    path_info[comp_trace]['match'].append(dst_ip)
                lines = [rf.readline() for _ in range(3)]
    total = 0
    info = defaultdict(defaultdict)
    equal_len = 0
    diff_len = 0
    multi_path = 0
    single_path = 0
    has_succeed = 0
    with open(s_filename, 'r') as rf:
        lines = [rf.readline() for _ in range(4)]
        while lines[0]:
            total += 1
            #print(lines[0])
            (dst_ip, trace) = lines[0][1:].strip('\n').split(']')
            bgp = lines[1].strip('\n').strip('\t')
            trace = lines[2].strip('\n').split(': ')[-1]
            if len(bgp.split(' ')) == len(trace.split(' ')):
                equal_len += 1
            else:
                diff_len += 1
            if trace in path_info.keys():
                if len(path_info[trace]) > 1:
                    multi_path += 1
                    if 'match' in path_info[trace].keys():
                        has_succeed += 1
                else:
                    single_path += 1
                for (bgp, dst_ips) in path_info[trace].items():
                    info[trace][bgp] = len(dst_ips)
            lines = [rf.readline() for _ in range(4)]
    print('equal_len: {}'.format(equal_len))
    print('diff_len: {}'.format(diff_len))
    print('single_path: {}'.format(single_path))
    print('multi_path: {}'.format(multi_path))
    print('has_succeed: {}'.format(has_succeed))
    with open('test.json', 'w') as wf:
        json.dump(info, wf, indent=2)

def ana_vp2dstAS_v2(vp, date):
    tracevp_bgpvp_info = {'ams-nl': '80.249.208.34', 'jfk-us': '198.32.160.61', 'sjc2-us': '64.71.137.241', 'syd-au': '198.32.176.177', 'per-au': '198.32.176.177', 'zrh2-ch': '109.233.180.32', 'nrt-jp': '203.181.248.168'}
    info = defaultdict(list)
    with open('/home/slt/code/ana_c_d_incongruity/test.json', 'r') as rf:
        data = json.load(rf)
        for dst_asn, val in data.items():
            for path_pair, dst_ips in val.items():
                info[path_pair] = info[path_pair] + dst_ips
    s = sorted(info.items(), key=lambda x:len(x[1]), reverse=True)
    # with open('path_pair', 'w') as wf:
    #     for path_pair in info.keys():
    #         wf.write(path_pair + '\n')
    # return
    #print(s[0][0])
    #print(len(s[0][1]))
    (ori_trace, ori_bgp) = s[0][0].split('|')
    # rev = ori_bgp + '|' + ori_trace
    # rev_ips = None
    # if rev in info.keys():
    #     rev_ips = info[rev]
    # print(rev)
    # if not rev_ips:
    #     return
    # print(len(rev_ips))
    bgp_path_info = {}
    InitBGPPathInfo('/mountdisk1/ana_c_d_incongruity/rib_data/bgpdata/bgp_%s_%s' %(tracevp_bgpvp_info[vp], date), bgp_path_info)
    os.chdir('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr' %vp)
    mm = defaultdict()
    match = defaultdict()
    match_res = set()
    for filename in ['mm_%s.%s' %(vp, date), 'match_%s.%s' %(vp, date)]:
        with open(filename, 'r') as rf:
            lines = [rf.readline() for _ in range(3)]
            while lines[0]:
                (dst_ip, trace) = lines[0][1:].strip('\n').split(']')
                ip_list = lines[2].strip('\n').split(']')[-1].split(' ')
                (trace_list, trace_to_ip_info, _) = CompressTrace(trace.split(' '), ip_list, 'n')
                comp_trace_list = drop_stars_in_trace(trace_list)
                comp_trace = ' '.join(comp_trace_list)
                if comp_trace == ori_trace:
                    match_res.add(dst_ip)
                if filename.startswith('mm'):
                    mm[dst_ip] = comp_trace
                else:
                    match[dst_ip] = comp_trace
                lines = [rf.readline() for _ in range(3)]
    print(len(match_res))
    res = defaultdict(list)
    done = set()
    similar = 0
    for path_pair, dst_ips in info.items():
        mm_total = 0
        match_total = 0
        for dst_ip in dst_ips:
        #for dst_ip in rev_ips:
            if dst_ip in done:
                continue
            prefix = GetBGPPath_Or_OriASN(bgp_path_info, dst_ip, 'get_prefix')
            (start_ip, pref_len) = prefix.split('/')
            ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton(start_ip))[0])
            for i in range(0, 1 << (24 - int(pref_len))):
                cur_ip_int = ip_int + (i << 8) + 1
                cur_ip = str(socket.inet_ntoa(struct.pack('I',socket.htonl(cur_ip_int))))
                if cur_ip in mm.keys():
                    res[prefix].append('{}(mm):{}'.format(cur_ip, mm[cur_ip]))
                    mm_total += 1
                elif cur_ip in match.keys():
                    res[prefix].append('{}(match):{}'.format(cur_ip, match[cur_ip]))
                    match_total += 1
                done.add(cur_ip)
        if mm_total > 0 and match_total > 0 and \
            (min(mm_total, match_total) / max(mm_total, match_total)) > 0.5:
            similar += 1
        print('{}, {}'.format(mm_total, match_total))
    # with open('/home/slt/code/ana_c_d_incongruity/test1.json', 'w') as wf:
    #     json.dump(res, wf, indent=1)
    # print(mm_total)
    print(len(info))
    print(similar)

def check_seg_conform():    
    for (vp, date) in [('ams-nl', '20201216'), ('nrt-jp', '20201215'), ('sjc2-us', '20201215'), ('syd-au', '20201215'), ('zrh2-ch', '20200714')]:
        total = 0
        m = 0
        with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/ana_compare_res/diff_path_conform_mm_seg_all_vp.%s.%s' %(vp, vp, date), 'r') as rf:
            lines = [rf.readline() for _ in range(4)]
            while lines[0]:
                total += 1
                (dst_ip, trace) = lines[0][1:].strip('\n').split(']')
                bgp = lines[1].strip('\n').strip('\t')
                ip_list = lines[3].strip('\n').split(']')[-1].split(' ')
                (trace_list, trace_to_ip_info, _) = CompressTrace(trace.split(' '), ip_list, 'n')
                comp_trace_list = drop_stars_in_trace(trace_list)
                if ' '.join(comp_trace_list).__contains__(bgp):
                    m += 1
                lines = [rf.readline() for _ in range(4)]
        print(vp + date)
        print(m / total)

def check_lb(s_filename):
    vp = 'ams-nl'
    date = '20201216'
    concerned_asns = set()
    with open(s_filename, 'r') as rf:
        lines = [rf.readline() for _ in range(4)]
        while lines[0]:
            bgp = lines[1].strip('\n').strip('\t')
            concerned_asns.add(bgp.split(' ')[-1])
            lines = [rf.readline() for _ in range(4)]
    os.chdir('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr' %vp)
    dstAS_info = defaultdict(lambda: defaultdict(Counter))
    for filename in ['mm_%s.%s' %(vp, date), 'match_%s.%s' %(vp, date)]:
        match_flag = 'mm' if filename.startswith('mm') else 'match'
        with open(filename, 'r') as rf:
            lines = [rf.readline() for _ in range(3)]
            while lines[0]:
                (dst_ip, trace) = lines[0][1:].strip('\n').split(']')
                ori_bgp = lines[1].strip('\t').strip('\n')
                orias = ori_bgp.split(' ')[-1]
                if orias not in concerned_asns:
                    lines = [rf.readline() for _ in range(3)]
                    continue
                ip_list = lines[2].strip('\n').split(']')[-1].split(' ')
                (trace_list, trace_to_ip_info, _) = CompressTrace(trace.split(' '), ip_list, 'n')
                comp_trace_list = drop_stars_in_trace(trace_list)
                if comp_trace_list[-1] != orias:
                    lines = [rf.readline() for _ in range(3)]
                    continue
                comp_trace = ' '.join(comp_trace_list)                
                dstAS_info[orias][comp_trace][match_flag] += 1
                lines = [rf.readline() for _ in range(3)]
    with open('/home/slt/code/ana_c_d_incongruity/test3.json', 'w') as wf:
        json.dump(dstAS_info, wf, indent=2)

def link_correlated_prefixes():    
    os.chdir('/mountdisk1/ana_c_d_incongruity/rib_data/bgpdata/')
    done_vp = set()
    for filename in os.listdir():
        if not filename.startswith('bgp_') or not filename.__contains__('202012'): continue
        # vp = filename.split('_')[1]
        # if vp in done_vp: continue
        # done_vp.add(vp)
        link_info = defaultdict(set)
        trip_info = defaultdict(set)
        pref_info = defaultdict(list)
        with open(filename, 'r') as rf:
            for curline in rf:
                pref, ori_path = curline.strip('\n').split('|')
                ori_path_list = ori_path.split(' ')
                prev_asn = None
                prev_link = None
                i = 0
                for asn in ori_path_list:
                    if asn != prev_asn:
                        if prev_asn:
                            if prev_asn.startswith('{') or asn.startswith('{'):
                                prev_link = None
                            else:
                                link = prev_asn + ' ' + asn
                                link_info[link].add(pref)
                                if i < 5:
                                    pref_info[pref].append(link)
                                    i += 1
                                if prev_link:
                                    trip_info[prev_link + ' ' + asn].add(pref)
                                prev_link = link
                        prev_asn = asn
            pop_links = set()
            for link, prefs in link_info.items():
                if len(prefs) > 1500:
                    pop_links.add(link)
            least_pref = set()
            all_pref = set()
            for pref, links in pref_info.items():
                if any(link in pop_links for link in links):
                    least_pref.add(pref)
                if all(link in pop_links for link in links):
                    all_pref.add(pref)
            print('pop links: {}({})'.format(len(pop_links), len(link_info)))
            print('least prefs: {}({})'.format(len(least_pref), len(least_pref) / len(pref_info)))
            print('all prefs: {}({})'.format(len(all_pref), len(all_pref) / len(pref_info)))

if __name__ == '__main__':  
    link_correlated_prefixes()
    #check_seg_conform()
    #ana_vp2dstAS('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/ams-nl/sxt_bdr/back/diff_path_conform_dst_pref_all_vp.ams-nl.20201216')
    #check_lb('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/ams-nl/sxt_bdr/back/diff_path_conform_dst_pref_all_vp.ams-nl.20201216')
    #ana_vp2dstAS_v2('ams-nl', '20201216')
    # for vp in ['ams-nl', 'jfk-us', 'sjc2-us', 'syd-au', 'zrh2-ch', 'nrt-jp']:
    #     print(vp)
    #     for year in range(2018, 2021):
    #         for month in range(1, 13):
    #             date = str(year) + str(month).zfill(2)
    #             if date != '201811':
    #                 continue
    #             #for filename in glob.glob('/mountdisk1/ana_c_d_incongruity/traceroute_data/result/trace_%s.%s*' %(vp, date)):
    #             for filename in glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/ana_compare_res/continuous_mm.%s.%s*' %(vp, vp, date)):
    #                 #cal_double_hops(filename)
    #                 print(date)
    #                 #check_moas(filename, vp, date)
    #                 stat_missing_segs(filename)
    #find_common_trace()
    #check_match_prefix_for_spec_mid_ips()
    #check_diff_bgp('286')
    #check_snmp_common_ips()
    #recal_ams_nl_trace_mm_rate()
    # for vp in ['ams-nl', 'jfk-us', 'sjc2-us', 'syd-au', 'zrh2-ch', 'nrt-jp']:
    #     print(vp)
    #     for year in range(2018, 2021):
    #         for month in range(1, 13):
    #             date = str(year) + str(month).zfill(2) + '15'
    #             cmp_rect_bdr_and_before_rect('ams-nl', date)
    #b1()
    #stat_sibling_traceroute_links()
    #cal_branches_in_jfk_us()
    #strip_mm_in_ams_nl()
    #restore_mm_in_ams_nl()
    #check_exits()
    #date = '20201016'
    #links = [('129.250.3.18', '213.198.82.174'), ('129.250.3.72', '213.198.82.206'), ('129.250.3.18', '213.198.82.206'), ('129.250.3.18', '213.198.82.202'), ('129.250.3.72', '213.198.82.174')]
    #non_rel_links_num(links)
    
    #get_rels(date, sys.argv[1:])
    #cal_reply_rate('/mountdisk3/traceroute_download_all/result/sao-br.20201215')
    #ixp = create_peeringdb('/mountdisk1/ana_c_d_incongruity/peeringdb_data/peeringdb_2020_12_15.json')
    #rewrite_peering()
    #check_common_prefix()
    #CheckFailedIPs()
    #ModifyCC('/mountdisk1/ana_c_d_incongruity/as_rel_cc_data/20180801.ppdc-ases.txt')
    #CheckLastHopAnno()
    #GetProviders('/mountdisk1/ana_c_d_incongruity/as_rel_cc_data/20180801.ppdc-ases.txt', '23947')
    #CmpAbTraces()
    #TestLoadPeeringdbFiles()
    #PrintJsonIndent('/mountdisk1/ana_c_d_incongruity/peeringdb_data/peeringdb_2018_08_15.json')

    #CalMulPreHop('/mountdisk1/ana_c_d_incongruity/traceroute_data/result/trace_ams-nl.20180815')

    #ReadPeerDB('/mountdisk1/ana_c_d_incongruity/peeringdb_data/peeringdb_2018_08_15.json')

    #CmpPfx2AS('/mountdisk1/ana_c_d_incongruity/out_ip2as_data/20180815.ip2as.prefixes', '/mountdisk1/ana_c_d_incongruity/rib_data/compress_files/dir_ribs/80.249.208.34_20180815')

    # num = 0
    # with open('/home/slt/code/ana_c_d_incongruity/test4', 'r') as rf:
    #     begin = False
    #     for data in rf.readlines():
    #         _ip = data[:data.index(' (')]
    #         print(data, end='')
    #         if _ip == '176.52.252.239':
    #             begin = True
    #         if begin:
    #             num += 1
    #             os.system('grep \"%s\" /mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/nrt-jp/bdrmapit/ab_nrt-jp.20180815 -B 2 > /mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/nrt-jp/bdrmapit/test' %_ip)
    #             CalBdrmapItVoters('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/nrt-jp/bdrmapit/test', _ip)
    # print(num)
    
    # _ip = '176.52.252.239'
    # os.system('grep \"%s\" /mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/nrt-jp/bdrmapit/ab_nrt-jp.20180815 -B 2 > /mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/nrt-jp/bdrmapit/test' %_ip)
    # os.system('grep \"%s\" /mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/nrt-jp/bdrmapit/match_nrt-jp.20180815 -B 2 >> /mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/nrt-jp/bdrmapit/test' %_ip)
    # CalBdrmapItVoters('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/nrt-jp/bdrmapit/test', _ip)
    #CalAvg()
    #CheckUnmap(['/mountdisk1/ana_c_d_incongruity/out_my_anatrace/nrt-jp_20200115/midar/final_unmap', '/mountdisk1/ana_c_d_incongruity/out_my_anatrace/nrt-jp_20200115/midar/final_ab'])
    #CmpUnmap('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/nrt-jp/midar/stat1_ipaccur_nrt-jp.20200115.json', 'debug')
    #CountDstInMidarUnmap('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/nrt-jp/midar/stat1_ipaccur_nrt-jp.20200115.json')
    
    # for map_method in ['bdrmapit']:
    #     for vp in ['ams-nl', 'jfk-us', 'sjc2-us', 'syd-au', 'zrh2-ch', 'nrt-jp']:
    #         os.chdir('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/' + vp + '/' + map_method + '/')
    #         filenames = glob.glob(r'ipaccur_*')
    #         for filename in filenames:
    #             CmpWithAndNoDst(filename + '.json')

    # for vp in ['ams-nl', 'jfk-us', 'sjc2-us', 'syd-au', 'zrh2-ch', 'nrt-jp']:
    #     os.chdir('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/' + vp + '/')
    #     filenames = glob.glob(r'rib_based/ipaccur_*')
    #     for filename in filenames:
    #         CmpMethods(filename.split('/')[-1] + '.json')

    #CmpLoopRate()

    #SelMPLSIps('/mountdisk3/traceroute_download_all/back/test')
    #FindDupIPs('/mountdisk1/ana_c_d_incongruity/traceroute_data/result/trace_nrt-jp.20180815')
    