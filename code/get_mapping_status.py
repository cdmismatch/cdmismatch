
import os
import glob
import json
from utils_v2 import GetIxpAsSet, IsIxpAs, GetIxpPfxDict_2, IsIxpIp, ClearIxpPfxDict, ClearIxpAsSet
from collections import Counter, defaultdict
from get_ip2as_from_bdrmapit import ConnectToBdrMapItDb, GetBdrCache, CloseBdrMapItDb, InitBdrCache, \
                                    ConstrBdrCache
from traceutils.bgp.bgp import BGP
from rect_bdrmapit import CheckSiblings
import matplotlib.pyplot as plt
import numpy as np
from compare_cd import InitPref2ASInfo, GetBGPPath_Or_OriASN

vps = ['ams-nl', 'nrt-jp', 'sao-br']#, 'jfk-us', 'sjc2-us', 'syd-au']

def ModifyIPMappingStatus():
    global vps
    GetIxpAsSet('20220215')
    filter_dsts = None
    # with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/filter_dsts', 'r') as rf:
    #     filter_dsts = json.load(rf)     
    for vp in vps:
    #for vp in ['ams-nl']:
        if os.path.exists('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/modify_ip_mapping_status_%s.json' %(vp, vp)):
            continue
        print(vp)
        ip_info = {}
        #fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/match_*.202202*' %vp)
        fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ori_bdr_filtersndmm/match_*.202202*' %vp)
        for fn in fns:
            with open(fn, 'r') as rf:
                lines = [rf.readline() for _ in range(3)]
                while lines[0]:
                    trace_hops = lines[0].strip('\n').split(']')[-1].split(' ')
                    bgp_hops = lines[1].strip('\n').strip('\t').split(' ')
                    ips = lines[2].strip('\n').split(' ')
                    for i in range(len(ips)):
                        ip = ips[i]
                        if ip == '*':
                            continue
                        if ip not in ip_info.keys():
                            ip_info[ip] = [0, 0, 0, 0, trace_hops[i]]
                        if trace_hops[i].__contains__('<'):
                            trace_hop = trace_hops[i][1:-1]
                            if IsIxpAs(trace_hop):
                                pass
                            elif trace_hop in bgp_hops:
                                ip_info[ip][0] += 1
                            else:
                                ip_info[ip][1] += 1
                        elif trace_hops[i] == '?':
                            ip_info[ip][3] = 1
                        else:
                            ip_info[ip][0] += 1
                    lines = [rf.readline() for _ in range(3)]
        #fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/ana_compare_res/discrete_mm.*202202*' %vp)
        fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ori_bdr_filtersndmm/ana_compare_res/discrete_mm.*202202*' %vp)
        for fn in fns:
            print(fn)
            date = fn.split('.')[-1]
            #cur_filter_dsts = filter_dsts[date][vp] if date in filter_dsts and vp in filter_dsts[date] else set()
            with open(fn, 'r') as rf:
                data = json.load(rf)
                for val in data.values():
                    for i in range(0, len(val), 3):
                        dst_ip, tmp = val[i].strip('\n')[1:].split(']')
                        # if dst_ip in cur_filter_dsts:
                        #     continue
                        trace_hops = tmp.split(' ')
                        bgp_hops = val[i+1].strip('\n').strip('\t').split(' ')
                        cur_splits = val[i+2].strip('\n').split(']')
                        mm_ips = cur_splits[0][1:].split(',')
                        pm_ips = cur_splits[1][1:].split(',')
                        ips = cur_splits[2].split(' ')
                        for j in range(len(ips)):
                            ip = ips[j]
                            if ip == '*':
                                continue
                            # if ip == '221.120.223.50':
                            #     a = 1
                            if ip not in ip_info.keys():
                                ip_info[ip] = [0, 0, 0, 0, trace_hops[j]]
                            if trace_hops[j].__contains__('<'):
                                trace_hop = trace_hops[j][1:-1]
                                if IsIxpAs(trace_hop):
                                    pass
                                elif trace_hop in bgp_hops:
                                    ip_info[ip][0] += 1
                                else:
                                    ip_info[ip][1] += 1
                            elif trace_hops[j] == '?':
                                ip_info[ip][3] = 1
                            else:
                                if ip in mm_ips:
                                    ip_info[ip][1] += 1
                                elif ip in pm_ips:
                                    ip_info[ip][2] += 1
                                else:
                                    ip_info[ip][0] += 1
        ip_stat = {}
        for ip, val in ip_info.items():
            if ip == "221.120.223.50":
                a = 1
            total = sum(val[:-1])
            if total == 0:
                continue
            if (val[0] / total) > ((val[1] / total) * 10):
                ip_stat[ip] = ['succ', val[-1]]
            elif ((val[1] / total) > ((val[0] / total) * 10)) or ((val[2] / total) > ((val[0] / total) * 10)):
                ip_stat[ip] = ['fail', val[-1]]
            elif (val[3] > val[0]) or (val[3] > val[1]):
                ip_stat[ip] = ['unmap', val[-1]]
            else:
                ip_stat[ip] = ['other', val[-1]]
        if not os.path.exists('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/' %vp):
            os.mkdir('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/' %vp)
        with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/modify_ip_mapping_status_%s.json' %(vp, vp), 'w') as wf:
            json.dump(ip_stat, wf, indent=1)
        print(len(ip_stat))
        print(len([ip for ip, val in ip_stat.items() if val[0] == 'succ']))
        print(len([ip for ip, val in ip_stat.items() if val[0] == 'fail']))
        print(len([ip for ip, val in ip_stat.items() if val[0] == 'unmap']))
        print(len([ip for ip, val in ip_stat.items() if val[0] == 'other']))
            
#deprecated
#'match': 0, 'mismatch': 1, 'partial_match': 2, 'unmap': 3
def DebugTestPre():
    wfn = 'mapping_status.json'
    res = {}
    if not os.path.exists(wfn):
        fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/*/ori_bdr/ipaccur_*.202104*.json')
        ip_info = {}
        for fn in fns:
            with open(fn, 'r') as rf:
                data = json.load(rf)
                for _ip, val in data.items():
                    if _ip[-2:] == '.1':
                        continue
                    if _ip not in ip_info.keys():
                        ip_info[_ip] = Counter()
                    for i in [0, 1, 2, 3]:
                        ip_info[_ip][i] += val[i][0]
        with open('mapping_info.json', 'w') as wf:
            json.dump(ip_info, wf, indent=1)
        for _ip, val in ip_info.items():
            (match, mismatch, partial_match) = val[0], val[1], val[2]
            tmp_total = match + mismatch + partial_match
            if tmp_total == 0:
                continue
            if (match / tmp_total) > ((mismatch / tmp_total) * 10):
                res[_ip] = 'succ'
            elif (mismatch / tmp_total) > ((match / tmp_total) * 10):
                res[_ip] = 'fail'
            elif (partial_match / tmp_total) > ((match / tmp_total) * 10):
                res[_ip] = 'fail'
            else:
                res[_ip] = 'other'
        with open('mapping_status.json', 'w') as wf:
            json.dump(res, wf, indent=1)
    else:
        with open('mapping_status.json', 'r') as rf:
            res = json.load(rf)
    return res
    
def DebugTest():
    #key_asns = {'4809', '23764', '4812', '23724', '4811', '4816', '23650', '138950', '139721', '134756', '4835', '140061', '36678', '139018', '135386', '140330', '24545', '140903', '140292', '139019', '131285', '141771', '137702', '140485', '140647', '140553', '140265', '141739', '141998', '140345', '141679', '140527', '139203', '139201', '140636', '139220', '132147', '149178', '138982', '132225', '138169', '140638', '140083', '146966', '150145', '149837', '139462', '139887', '132153', '148969', '135089', '140278', '142608', '140276', '132833', '142404', '138991', '134425', '136167', '141157', '138949', '58518', '63527', '149979', '58517', '147038', '148981', '139767', '44218'}
    #key_asns = {'24348', '24369', '24364', '24355', '24357', '24361', '24349', '24363', '24362', '24358', '24350', '24353', '24370', '24367', '24352', '24371', '24356', '24372', '23910', '45576'}
    key_asns = {'4538', '24348', '138371', '24369', '24364', '133111', '24355', '45576', '23910', '38272', '24357', '24361', '24349', '24363', '24302', '24362', '38255', '139774', '24358', '138378', '24350', '24353', '24370', '24367', '138393', '24352', '138369', '24371', '138373', '24356', '24372'}
    ip_asns = CheckBdrConformity()
    key_ips = {ip for ip, val in ip_asns.items() if set(val.keys()) & key_asns}
    ip_stat = {}
    fns = glob.glob('modify_ip_mapping_status_*.json')
    concern_ips = defaultdict(lambda:defaultdict(set))
    for fn in fns:
        with open(fn, 'r') as rf:
            ip_stat = json.load(rf)
            for ip, val in ip_stat.items():
                if ip in key_ips:
                #if val[1] in key_asns:
                    if val[0] != 'others': #other去掉
                        concern_ips[ip][val[0]].add(val[1])
    # for ip, val in concern_ips.items():
    #     print('{}:[{}, {}]'.format(ip, val, ip_asns[ip]), end=', ')
    neat_form = {}
    for ip, val in concern_ips.items():
        if 'fail' in val.keys() or (len(val['succ']) == 1 and len(ip_asns[ip][list(val['succ'])[0]]) > 1):
            neat_form[ip] = val
            continue
        #print(ip_asns[ip])
        # s_succ_asns = sorted(val['succ'], key=lambda x:len(ip_asns[ip][x]), reverse=True)
        # if len(ip_asns[ip][s_succ_asns[0]]) > len(ip_asns[ip][s_succ_asns[1]]):
        #     neat_form[ip]['succ'] = {s_succ_asns[0]}
        # else:
        #     neat_form[ip]['succ'] = val['succ']
        # if 'fail' in val.keys():
        #     neat_form[ip]['fail'] = val['fail']
    for ip, val in neat_form.items():
        print('{}:{}, {}'.format(ip, val, ip_asns[ip]))

def CheckBdrConformity():
    wfn = 'bdrmapit_res_all.json'
    if not os.path.exists(wfn):
        fns = glob.glob('/mountdisk1/ana_c_d_incongruity/out_bdrmapit/sxt_bdr/bdrmapit_*_202202*.db')
        ip_info = defaultdict(lambda:defaultdict(list))
        for fn in fns:
            if os.path.getsize(fn) == 0:
                continue
            # if fn.__contains__('sao'):
            #     continue
            print(fn)
            fn_key = fn.split('/')[-1].split('_')[1]
            ConnectToBdrMapItDb(fn)
            ConstrBdrCache()
            cache = GetBdrCache()
            for ip, asn in cache.items():
                if fn_key not in ip_info[ip][asn]:
                    ip_info[ip][asn].append(fn_key)
            CloseBdrMapItDb()
            InitBdrCache()
        # for ip, val in ip_info.items():
        #     if len(val) == 1:
        #         continue
        #     print('{}:{}'.format(ip, val))
        # print(len(ip_info))
        # print(len({ip for ip, val in ip_info.items() if len(val) == 1}))
        with open(wfn, 'w') as wf:
            json.dump(ip_info, wf, indent = 1)
        return ip_info
    else:
        with open(wfn, 'r') as rf:
            ip_info = json.load(rf)
        return ip_info

def GetMatchAndMapErrorLines(vp, date):
    lines = []
    fn1 = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ori_bdr_filtersndmm/match_%s.%s*' %(vp, vp, date[:6]))
    if fn1:
        with open(fn1[0], 'r') as rf:
            lines = rf.readlines()
    fn2 = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ori_bdr_filtersndmm/ana_compare_res/discrete_mm.%s.%s*' %(vp, vp, date[:6]))
    if fn2:
        with open(fn2[0], 'r') as rf:
            data = json.load(rf)
            for val in data.values():
                lines = lines + val
    return lines

def GetMatchAndMmLines(vp, date):
    lines = []
    fn1 = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ori_bdr_filtersndmm/match_%s.%s*' %(vp, vp, date[:6]))
    if fn1:
        with open(fn1[0], 'r') as rf:
            lines = rf.readlines()
    fn2 = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ori_bdr_filtersndmm/mm_%s.%s*' %(vp, vp, date[:6]))
    if fn2:
        with open(fn2[0], 'r') as rf:
            lines = lines + rf.readlines()
    return lines

def GetMatchAndMmLines_forsxtbdr(vp, date):
    lines = []
    filtered = {}
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/filter_dsts', 'r') as rf:
        filtered = json.load(rf)
    cur_filtered = set(filtered[date][vp])
    fn1 = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/match_%s.%s*' %(vp, vp, date[:6]))
    with open(fn1[0], 'r') as rf:
        lines = rf.readlines()
    fn2 = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/mm_%s.%s*' %(vp, vp, date[:6]))
    with open(fn2[0], 'r') as rf:
        mm_lines = rf.readlines()
    tmp_lines = [None] * len(mm_lines)
    j = 0
    for i in range(0, len(mm_lines), 3):
        if mm_lines[i].split(']')[0][1:] not in cur_filtered:
            tmp_lines[j] = mm_lines[i]
            tmp_lines[j+1] = mm_lines[i+1]
            tmp_lines[j+2] = mm_lines[i+2]
            j = j + 3
    lines = lines + tmp_lines[:j]
    return lines

def IsLegalASN(asn):
    return asn.isdigit() and int(asn) < 0xFFFFFF and int(asn) > 0

def GetRel(checksiblings, asn1, asn2):
    asn1 = int(asn1)
    asn2 = int(asn2)
    if asn1 == asn2:
        return 5
    else:
        return checksiblings.bgp.reltype(asn1, asn2)

def StripHops(ori_ips, ori_hops):
    ips = []
    hops = []
    for i in range(0, len(ori_hops)):
        if (i == 0 or ori_ips[i] != ori_ips[i-1]) and ori_ips[i] != '*':
            ips.append(ori_ips[i])
            hops.append(ori_hops[i])
    return ips, hops

version = 'new'

def CompressTrace(ori_hop_list, dst_asn):
    global version
    if version == 'old':
        list_len = len(ori_hop_list)
        modify = {}
        for i in range(list_len):
            elem = ori_hop_list[i]
            if not IsLegalASN(elem):
                prev_last, next_fst = ori_hop_list[0], dst_asn
                for j in range(i-1, -1, -1):
                    if IsLegalASN(ori_hop_list[j]):
                        prev_last = ori_hop_list[j]
                        break
                for j in range(i+1, list_len):
                    if IsLegalASN(ori_hop_list[j]):
                        next_fst = ori_hop_list[j]
                        break
                if prev_last == next_fst:
                    modify[i] = prev_last
                    ori_hop_list[i] = prev_last
        hop_list = []
        prev_elem = ''
        loop = False
        for elem in ori_hop_list:
            if elem != prev_elem:
                if IsLegalASN(elem) and elem in hop_list:
                    #print('loop: {}'.format(ori_hop_list))
                    loop = True
                hop_list.append(elem)
            prev_elem = elem
        return (hop_list, modify, loop)
    else:
        list_len = len(ori_hop_list)
        hop_list = []
        prev_elem = ''
        loop = False
        for elem in ori_hop_list:
            if IsLegalASN(elem):
                if elem != prev_elem:
                    if elem in hop_list:
                        #print('loop: {}'.format(ori_hop_list))
                        loop = True
                    hop_list.append(elem)
                    prev_elem = elem
        return (hop_list, None, loop)


#bgp.reltype: 1: later is customer; 2: later is provider; 3: peer; 4: unknown     
def GetIPAttr(date, vp):
    global version
    GetIxpPfxDict_2(int(date[:4]), int(date[4:6]))
    GetIxpAsSet(date)
    #for vp in ['sjc2-us']:#'ams-nl',"nrt-jp","sao-br"]:
    if True:
        lines = []
        if vp:
            print(vp + ' begin GetIPAttr')
            #lines = GetMatchAndMapErrorLines(vp, date)
            #lines = GetMatchAndMmLines_forsxtbdr(vp, date)
            lines = GetMatchAndMmLines(vp, date)
        else:
            print(date + ' begin GetIPAttr')
            # with open('/mountdisk2/common_vps/%s/atlas/mapped_%s' %(date, date), 'r') as rf:
            #     lines = rf.readlines()
            filtered = set()
            with open('/mountdisk2/common_vps/%s/atlas/filter_dsts_atlas_%s' %(date, date), 'r') as rf:
                data = json.load(rf)
                for val in data.values():
                    filtered = filtered | set(val)
            fns = glob.glob('/mountdisk2/common_vps/%s/cmp_res/sxt_bdr/match_*' %date)
            for fn in fns:
                with open(fn, 'r') as rf:
                    lines = lines + rf.readlines()
            fns = glob.glob('/mountdisk2/common_vps/%s/cmp_res/sxt_bdr/mm_*' %date)
            mm_lines = []
            for fn in fns:
                if fn.split('/')[-1][3] == 'i':
                    continue
                with open(fn, 'r') as rf:
                    mm_lines = mm_lines + rf.readlines()
            tmp_lines = [None] * len(mm_lines)
            j = 0
            for i in range(0, len(mm_lines), 3):
                if mm_lines[i].split(']')[0][1:] not in filtered:
                    tmp_lines[j] = mm_lines[i]
                    tmp_lines[j+1] = mm_lines[i+1]
                    tmp_lines[j+2] = mm_lines[i+2]
                    j = j + 3
            lines = lines + tmp_lines[:j]
        print(len(lines))
        is_ixp = {}
        prev_ips = defaultdict(defaultdict)
        succ_ips = defaultdict(defaultdict)
        #trip = defaultdict(Counter)
        trip_AS = defaultdict(Counter)
        modify = defaultdict(set)
        #ip_trace_idxs = defaultdict(set)
        traces = []
        if version == 'old':
            if lines:
                for i in range(0, len(lines), 3):
                    dst_ip, trace = lines[i].strip('\n').split(']')
                    dst_ip = dst_ip[1:]
                    bgp_hops = lines[i+1].strip('\n').strip('\t').split(' ')
                    ori_hops = [hop.strip('<').strip('>') for hop in trace.split(' ')]
                    ori_ips = lines[i + 2].strip('\n').split(']')[-1].split(' ')
                    ips, hops = StripHops(ori_ips, ori_hops)
                    if len(ips) != len(hops):
                        a = 1
                    cpr_hops, tmp_modify, loop = CompressTrace(hops, bgp_hops[-1])
                    for j, val in tmp_modify.items():
                        modify[ips[j]].add(val)
                    traces.append(' '.join(ori_ips) + '|%s,%s'%(dst_ip, bgp_hops[-1]))
                    trace_idx = i/3
                    for j in range(len(ips)):
                        #ip_trace_idxs[ips[j]].add(trace_idx)
                        cur_hop = hops[j]
                        is_ixp[ips[j]] = (IsIxpIp(ips[j]) or IsIxpAs(cur_hop))                    
                        if j > 0:
                            prev_ips[ips[j]][trace_idx] = ips[j-1] + '|' + hops[j-1]
                        if ips[j] != dst_ip:  #最后一跳是dst_ip的，丢掉不管
                            succ_ip = ips[j+1] if j < len(ips) - 1 else dst_ip
                            succ_hop = hops[j+1] if j < len(ips) - 1 else bgp_hops[-1]+'$'
                            succ_ips[ips[j]][trace_idx] = succ_ip + '|' + succ_hop
                            if j > 0:
                                tmp_idx = cpr_hops.index(cur_hop)
                                prev_cpr_hop = cpr_hops[tmp_idx-1] if tmp_idx > 0 else cpr_hops[0]
                                succ_cpr_hop = cpr_hops[tmp_idx+1] if tmp_idx < len(cpr_hops) - 1 else bgp_hops[-1]+'$'
                                #trip_AS[ips[j]][trace_idx] = ips[j-1] + ',' + succ_ip + '|' + prev_cpr_hop + ',' + succ_cpr_hop
                                last_prev_ip = ips[0]
                                if tmp_idx > 0:
                                    last_prev_idx = hops.index(prev_cpr_hop)
                                    while hops[last_prev_idx + 1] == prev_cpr_hop: last_prev_idx += 1
                                    last_prev_ip = ips[last_prev_idx]
                                fst_succ_ip = dst_ip
                                if tmp_idx < len(cpr_hops) - 1:                              
                                    fst_succ_idx = hops.index(succ_cpr_hop)
                                    fst_succ_ip = ips[fst_succ_idx]
                                trip_AS[ips[j]][trace_idx] = last_prev_ip + ',' + fst_succ_ip + '|' + prev_cpr_hop + ',' + succ_cpr_hop
                with open('ipattr_is_ixp_%s.json' %vp, 'w') as wf:
                    json.dump(is_ixp, wf, indent=1)
                with open('ipattr_prev_ips_%s.json' %vp, 'w') as wf:
                    json.dump(prev_ips, wf, indent=1)
                with open('ipattr_succ_ips_%s.json' %vp, 'w') as wf:
                    json.dump(succ_ips, wf, indent=1)
                # with open('ipattr_trip_%s.json' %vp, 'w') as wf:
                #     json.dump(trip, wf, indent=1)           
                with open('ipattr_trip_AS_%s.json' %vp, 'w') as wf:
                    json.dump(trip_AS, wf, indent=1)  
                with open('ipattr_traces_%s' %vp, 'w') as wf:
                    for line in traces:
                        wf.write(line+'\n')
                # rec_ip_trace_idx = {ip:list(val) for ip, val in ip_trace_idxs.items()}
                # with open('ipattr_ip_trace_idxs.json', 'w') as wf:
                #     json.dump(rec_ip_trace_idx, wf, indent=1)
                rec_modify = {key:list(val)[0] for key, val in modify.items() if len(val) == 1}
                contradict = {key:list(val) for key, val in modify.items() if len(val) > 1}
                if contradict:
                    print('contradict unmap modify')
                    print('{}'.format(contradict))
                with open('ipattr_modify_unmap_%s.json' %vp, 'w') as wf:
                    json.dump(rec_modify, wf, indent=1)
        else:
            if lines:
                trace_idx = 0
                for i in range(0, len(lines), 3):
                    bgp_hops = lines[i+1].strip('\n').strip('\t').split(' ')
                    if bgp_hops[-1] == '?':
                        continue                 
                    dst_ip, trace = lines[i].strip('\n').split(']')
                    dst_ip = dst_ip[1:]
                    ori_hops = [hop.strip('<').strip('>') for hop in trace.split(' ')] #只把<去掉
                    ori_ips = lines[i + 2].strip('\n').split(']')[-1].split(' ') #ori_ips
                    # if '*' in ori_hops and ('?' in ori_hops or '-1' in ori_hops):
                    #     print(ori_hops)
                    #     print(ori_ips)
                    ips, hops = StripHops(ori_ips, ori_hops) #把*去掉
                    if len(ips) != len(hops):
                        print('ips and hops not consistent!')
                    if bgp_hops[-1].__contains__('_') or bgp_hops[-1].__contains__(','):
                    #     tmp_ori_asns = bgp_hops[-1].split('_')
                    #     for tmp_ori_asn in tmp_ori_asns:
                    #         if tmp_ori_asn in hops:
                    #             bgp_hops[-1] = tmp_ori_asn
                    #             break
                    # if bgp_hops[-1].__contains__('_'):
                        continue
                    cpr_hops, tmp_modify, loop = CompressTrace(hops, bgp_hops[-1]) #把重复的，？和-1都去掉
                    # for j, val in tmp_modify.items():
                    #     modify[ips[j]].add(val)
                    for j in range(len(ips)):
                        #ip_trace_idxs[ips[j]].add(trace_idx)
                        cur_ip = ips[j]
                        cur_hop = hops[j]
                        ori_ip_idx = ori_ips.index(cur_ip)
                        is_ixp[cur_ip] = (IsIxpIp(cur_ip) or IsIxpAs(cur_hop))                    
                        if j > 0:
                            prev_ip = ips[j-1]
                            ori_prev_ip_idx = ori_ips.index(prev_ip)
                            stars_flag = '*' if (ori_ip_idx - ori_prev_ip_idx > 1) else ''
                            prev_ips[cur_ip][trace_idx] = prev_ip + stars_flag + '|' + hops[j-1]
                        if ips[j] != dst_ip:  #最后一跳是dst_ip的，丢掉不管
                            succ_ip = ips[j+1] if j < len(ips) - 1 else dst_ip
                            stars_flag = '*'
                            if j < len(ips) - 1:
                                ori_succ_ip_idx = ori_ips.index(ips[j + 1])
                                if ori_succ_ip_idx - ori_ip_idx == 1:
                                    stars_flag = ''
                            succ_hop = hops[j+1] if j < len(ips) - 1 else bgp_hops[-1]+'$'
                            succ_ips[ips[j]][trace_idx] = succ_ip + stars_flag + '|' + succ_hop
                            if j > 0:
                                if not IsLegalASN(cur_hop):
                                    prev_k = j - 1
                                    while prev_k > 0 and not IsLegalASN(hops[prev_k]): prev_k = prev_k - 1
                                    prev_cpr_hop = hops[prev_k]
                                    last_prev_ip = ips[prev_k]
                                    if j - prev_k > 1:
                                        last_prev_ip = last_prev_ip + '*'
                                    else:
                                        ori_last_prev_ip_idx = ori_ips.index(last_prev_ip)
                                        if ori_ips[ori_last_prev_ip_idx+1] == '*':
                                            last_prev_ip = last_prev_ip + '*'
                                    succ_k = j + 1
                                    while succ_k < len(hops) and not IsLegalASN(hops[succ_k]): succ_k = succ_k + 1
                                    succ_cpr_hop = hops[succ_k] if succ_k < len(hops) else bgp_hops[-1]+'$'
                                    fst_succ_ip = ips[succ_k] if succ_k < len(hops) else dst_ip+'*'
                                    if succ_k < len(hops):
                                        if succ_k - j > 1:
                                            fst_succ_ip = fst_succ_ip + '*'
                                        else:
                                            ori_fst_succ_ip_idx = ori_ips.index(fst_succ_ip)
                                            if ori_ips[ori_fst_succ_ip_idx-1] == '*':
                                                fst_succ_ip = fst_succ_ip + '*'
                                    trip_AS[ips[j]][trace_idx] = last_prev_ip + ',' + fst_succ_ip + '|' + prev_cpr_hop + ',' + succ_cpr_hop
                                    continue
                                tmp_idx = cpr_hops.index(cur_hop)
                                prev_cpr_hop = cpr_hops[tmp_idx-1] if tmp_idx > 0 else cpr_hops[0]
                                succ_cpr_hop = cpr_hops[tmp_idx+1] if tmp_idx < len(cpr_hops) - 1 else bgp_hops[-1]
                                #trip_AS[ips[j]][trace_idx] = ips[j-1] + ',' + succ_ip + '|' + prev_cpr_hop + ',' + succ_cpr_hop
                                last_prev_ip = ips[0]
                                if tmp_idx > 0:
                                    last_prev_idx = hops.index(prev_cpr_hop)
                                    while hops[last_prev_idx + 1] == prev_cpr_hop: last_prev_idx += 1
                                    last_prev_ip = ips[last_prev_idx]
                                ori_last_prev_ip_idx = ori_ips.index(last_prev_ip)
                                #if any(not IsLegalASN(elem) for elem in ori_hops[ori_last_prev_ip_idx+1:ori_ip_idx]):
                                if ori_ip_idx - ori_last_prev_ip_idx > 1:
                                    last_prev_ip = last_prev_ip + '*'
                                fst_succ_ip = dst_ip
                                if tmp_idx < len(cpr_hops) - 1:                              
                                    fst_succ_idx = hops.index(succ_cpr_hop)
                                    fst_succ_ip = ips[fst_succ_idx]
                                    ori_fst_succ_ip_idx = ori_ips.index(fst_succ_ip)
                                    #if any(not IsLegalASN(elem) for elem in ori_hops[ori_ip_idx+1:ori_fst_succ_ip_idx]):
                                    if ori_fst_succ_ip_idx - ori_ip_idx > 1:
                                        fst_succ_ip = fst_succ_ip + '*'
                                elif dst_ip not in ips:
                                    fst_succ_ip = fst_succ_ip + '*'
                                    succ_cpr_hop = succ_cpr_hop + '$'
                                trip_AS[ips[j]][trace_idx] = last_prev_ip + ',' + fst_succ_ip + '|' + prev_cpr_hop + ',' + succ_cpr_hop
                    traces.append(' '.join(ips) + '|%s,%s'%(dst_ip, bgp_hops[-1]))
                    trace_idx += 1
                    if int(trace_idx) % 50000 == 0: print(trace_idx)
                if vp:
                    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/ipattr_is_ixp_%s_%s.json' %(vp, vp, date), 'w') as wf:
                        json.dump(is_ixp, wf, indent=1)
                    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/ipattr_prev_ips_%s_%s.json' %(vp, vp, date), 'w') as wf:
                        json.dump(prev_ips, wf, indent=1)
                    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/ipattr_succ_ips_%s_%s.json' %(vp, vp, date), 'w') as wf:
                        json.dump(succ_ips, wf, indent=1)
                    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/ipattr_trip_AS_%s_%s.json' %(vp, vp, date), 'w') as wf:
                        json.dump(trip_AS, wf, indent=1)  
                    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/ipattr_traces_%s_%s' %(vp, vp, date), 'w') as wf:
                        for line in traces:
                            wf.write(line+'\n')
                    print(vp + ' end GetIPAttr')
                else:
                    with open('/mountdisk2/common_vps/%s/atlas/ipattr_is_ixp_%s.json' %(date, date), 'w') as wf:
                        json.dump(is_ixp, wf, indent=1)
                    with open('/mountdisk2/common_vps/%s/atlas/ipattr_prev_ips_%s.json' %(date, date), 'w') as wf:
                        json.dump(prev_ips, wf, indent=1)
                    with open('/mountdisk2/common_vps/%s/atlas/ipattr_succ_ips_%s.json' %(date, date), 'w') as wf:
                        json.dump(succ_ips, wf, indent=1)
                    with open('/mountdisk2/common_vps/%s/atlas/ipattr_trip_AS_%s.json' %(date, date), 'w') as wf:
                        json.dump(trip_AS, wf, indent=1)  
                    with open('/mountdisk2/common_vps/%s/atlas/ipattr_traces_%s' %(date, date), 'w') as wf:
                        for line in traces:
                            wf.write(line+'\n')
                    print(vp + date + ' end GetIPAttr')
    
    ClearIxpPfxDict()
    ClearIxpAsSet()
                
def DebugModify4Attr():
    mapping_data = {}
    with open('/home/slt/code/ana_c_d_incongruity/modify4_ip_mapping_status.json', 'r') as rf:
        mapping_data = json.load(rf)    
    trip_as = {}
    with open('/home/slt/code/ana_c_d_incongruity/ipattr_trip_AS_ams-nl.json','r') as rf:
        trip_as = json.load(rf)
    scores = {}    
    with open('/home/slt/code/ana_c_d_incongruity/ipattr_score_ams-nl.json', 'r') as rf:
        scores = json.load(rf)
    prev_ips = {}
    with open('ipattr_prev_ips_ams-nl.json', 'r') as rf:
        prev_ips = json.load(rf)
    succ_ips = {}
    with open('ipattr_succ_ips_ams-nl.json', 'r') as rf:
        succ_ips = json.load(rf)
        
    concerns = set(trip_as.keys()) & set(mapping_data.keys())
    stat_trip_prevs = defaultdict(Counter)
    stat_trip_succs = defaultdict(Counter)
    stat_prevs = defaultdict(Counter)
    stat_succs = defaultdict(Counter)
    stat1_trip_prevs = []
    stat1_trip_succs = []
    stat1_prevs = []
    stat1_succs = []
    #for ip in concerns:
    for ip in trip_as.keys():
        for trip in trip_as[ip].values(): #trip_AS[ips[j]][trace_idx] = ips[j-1] + ',' + succ_ip + '|' + prev_cpr_hop + ',' + succ_cpr_hop
            trip_ip, _ = trip.split('|')
            prev_ip, succ_ip = trip_ip.split(',')
            stat_trip_prevs[ip][prev_ip in scores.keys() and scores[prev_ip] < 0.5] += 1
            stat_trip_succs[ip][succ_ip in scores.keys() and scores[succ_ip] < 0.5] += 1
        stat1_trip_prevs.append(stat_trip_prevs[ip][True] / (stat_trip_prevs[ip][True] + stat_trip_prevs[ip][False]))
        stat1_trip_succs.append(stat_trip_succs[ip][True] / (stat_trip_succs[ip][True] + stat_trip_succs[ip][False]))
        for prev in prev_ips[ip].values(): #prev_ips[ips[j]][trace_idx] = ips[j-1] + '|' + hops[j-1]
            prev_ip, _ = prev.split('|')
            stat_prevs[ip][prev_ip in scores.keys() and scores[prev_ip] < 0.5] += 1
        stat1_prevs.append(stat_prevs[ip][True] / (stat_prevs[ip][True] + stat_prevs[ip][False]))
        for succ in succ_ips[ip].values(): 
            succ_ip, _ = succ.split('|')
            stat_succs[ip][succ_ip in scores.keys() and scores[succ_ip] < 0.5] += 1
        stat1_succs.append(stat_succs[ip][True] / (stat_succs[ip][True] + stat_succs[ip][False]))
    
    # 绘制直方图，表示概率值的分布情况
    plt.hist(stat1_prevs, bins=10, range=(0, 1), edgecolor='black')
    plt.show()

def CmpAttr_SuccAsnRel():
    global vps
    status = {}
    res = {}
    all_data = {}
    with open('modify3_ip_mapping_status.json', 'r') as rf:
        all_data = json.load(rf)
    all_ips = set(all_data.keys())
    for vp in [vps[0]]:
        status[vp] = defaultdict()
        with open('modify_ip_mapping_status_%s.json' %vp, 'r') as rf:
            data1 = json.load(rf)
            common_ips = all_ips & set(data1.keys())
            for ip in common_ips:
                status[vp][ip] = data1[ip][0]
        res[vp] = defaultdict(defaultdict)
        with open('ipattr_succ_asnrel_%s.json' %vp, 'r') as rf:
            data2 = json.load(rf)
            common_ips = all_ips & set(data2.keys())
            for ip in common_ips:
                if ip not in status[vp].keys():
                    continue
                val = data2[ip]
                tmp_status = status[vp][ip]
                res[vp][tmp_status][ip] = [0, 0, 0, 0, 0, 0]
                tmp_sum = len(val)
                for subval in val.values():
                    if subval == '*':
                        subval = 6
                    res[vp][tmp_status][ip][subval-1] += 1 / tmp_sum
        # print('succ:')
        # for ip, val in res[vp]['succ'].items():
        #     print('{}:{}'.format(ip, val))
        # print('fail:')
        # for ip, val in res[vp]['fail'].items():
        #     print('{}:{}'.format(ip, val))
        draw_index = 1
        
        #fig, axs = plt.subplots(nrows=2, ncols=3, figsize=(10, 6))
        fig, axs = plt.subplots(nrows=6, ncols=2, figsize=(10, 6))
        labels = {0:'customer', 1:'provider', 2:'peer', 3:'unknown', 4:'sibling', 5: '*'}
        for draw_index in range(6):
            # Plot your data on each subplot
            succ = sorted([val[draw_index] for val in res[vp]['succ'].values()])
            fail = sorted([val[draw_index] for val in res[vp]['fail'].values()])
            # cdf1 = np.cumsum(np.ones_like(succ))/len(succ)
            # cdf2 = np.cumsum(np.ones_like(fail))/len(fail)
            # axs[int(draw_index/3)][draw_index%3].plot(succ, cdf1, c='r')
            # axs[int(draw_index/3)][draw_index%3].plot(fail, cdf2, c='g')
            axs[draw_index][0].plot(succ, c='r')
            axs[draw_index][1].plot(fail, c='g')
            axs[draw_index][0].set_title(labels[draw_index])
            
        #1: later is customer; 2: later is provider; 3: peer; 4: unknown   
        # axs[0, 0].set_title('customer')
        # axs[0, 1].set_title('provider')
        # axs[0, 2].set_title('peer')
        # axs[1, 0].set_title('unknown')
        # axs[1, 1].set_title('sibling')
        # axs[1, 2].set_title('*')
                
        plt.tight_layout()
        plt.show()
        break
    
def CmpAttr_SuccSameAsn():
    global vps
    status = {}
    res = {}
    for vp in vps:
        status[vp] = defaultdict()
        with open('modify_ip_mapping_status_%s.json' %vp, 'r') as rf:
            data1 = json.load(rf)
            for ip, val in data1.items():
                status[vp][ip] = val[0]
        res[vp] = defaultdict(defaultdict)
        with open('ipattr_succ_sameAS_%s.json' %vp, 'r') as rf:
            data2 = json.load(rf)
            for ip, val in data2.items():
                if ip not in status[vp].keys():
                    continue
                tmp_status = status[vp][ip]
                tmp_sum = len(val)
                true_num = len([subval for subval in val.values() if subval == True])
                res[vp][tmp_status][ip] = true_num / tmp_sum                
        # print('succ:')
        # for ip, val in res[vp]['succ'].items():
        #     print('{}:{}'.format(ip, val))
        succ = sorted(res[vp]['succ'].values())
        fail = sorted(res[vp]['fail'].values())
        cdf1 = np.cumsum(np.ones_like(succ))/len(succ)
        cdf2 = np.cumsum(np.ones_like(fail))/len(fail)
        plt.plot(succ, cdf1, c='r')
        plt.plot(fail, cdf2, c='g')
        # plt.scatter(range(len(succ)), succ, c = 'r')
        # plt.scatter(range(len(fail)), fail, c = 'g')
        plt.tight_layout()
        plt.show()
        # print('\n\n\nfail:')
        # for ip, val in res[vp]['fail'].items():
        #     print('{}:{}'.format(ip, val))
        break            
                
def CmpAttr_Trip():
    global vps
    status = {}
    res = {}
    rel_dict = {(1,1):0, (1,2):1, (1,3):2, (1,4):2, (1,5):0, (2,1):0, (2,2):0, (2,3):0, (2,4):2, (2,5):0, (3,1):0, (3,2):2, (3,3):2, (3,4):2, (3,5):0, (4,1):2, (4,2):2, (4,3):2, (4,4):1, (4,5):2, (5,1):0, (5,2):0, (5,3):0, (5,4):2, (5,5):0}
    all_data = {}
    with open('modify3_ip_mapping_status.json', 'r') as rf:
        all_data = json.load(rf)
    all_ips = set(all_data.keys())
    for vp in vps:
        status[vp] = defaultdict()
        with open('modify_ip_mapping_status_%s.json' %vp, 'r') as rf:
            data1 = json.load(rf)
            common_ips = all_ips & set(data1.keys())
            for ip in common_ips:
                val = data1[ip]
                status[vp][ip] = val[0]
        res[vp] = defaultdict(defaultdict)
        with open('ipattr_trip_%s.json' %vp, 'r') as rf:
            data2 = json.load(rf)
            common_ips = all_ips & set(data2.keys())
            for ip in common_ips:
                if ip not in status[vp].keys():
                    continue
                val = data2[ip]
                tmp_status = status[vp][ip]
                res[vp][tmp_status][ip] = [0, 0, 0]
                tmp_sum = len(val)
                for subval in val.values():
                    if '*' in subval:
                        continue
                    res[vp][tmp_status][ip][rel_dict[(subval[0], subval[1])]] += 1 / tmp_sum
        draw_index = 1
        
        fig, axs = plt.subplots(nrows=3, ncols=2, figsize=(10, 6))
        for draw_index in range(3):
            # Plot your data on each subplot
            succ = sorted([val[draw_index] for val in res[vp]['succ'].values()])
            fail = sorted([val[draw_index] for val in res[vp]['fail'].values()])
            # counts1, bin_edges1 = np.histogram(succ, bins=100, density=True)
            # cdf1 = np.cumsum(counts1)
            # axs[int(draw_index/3)][draw_index%3].plot(bin_edges1[1:], cdf1, c='r')
            # counts2, bin_edges2 = np.histogram(fail, bins=100, density=True)
            # cdf2 = np.cumsum(counts2)
            # axs[int(draw_index/3)][draw_index%3].plot(bin_edges2[1:], cdf2, c='g')
            cdf1 = np.cumsum(np.ones_like(succ))/len(succ)
            cdf2 = np.cumsum(np.ones_like(fail))/len(fail)
            # axs[draw_index].plot(succ, cdf1, c='r')
            # axs[draw_index].plot(fail, cdf2, c='g')
            axs[draw_index][0].plot(succ, c='r')
            axs[draw_index][1].plot(fail, c='g')
        # Set titles and axis labels for each subplot
        #1: later is customer; 2: later is provider; 3: peer; 4: unknown   
        axs[0][0].set_title('normal')
        axs[1][0].set_title('abnormal')
        axs[2][0].set_title('semi')
        
        plt.tight_layout()
        plt.show()
        break
    
def CmpAttr_PrevAsnRel():
    global vps
    status = {}
    res = {}
    for vp in [vps[5]]:
        status[vp] = defaultdict()
        with open('modify_ip_mapping_status_%s.json' %vp, 'r') as rf:
            data1 = json.load(rf)
            for ip, val in data1.items():
                status[vp][ip] = val[0]
        res[vp] = defaultdict(defaultdict)
        with open('ipattr_prev_asnrel_%s.json' %vp, 'r') as rf:
            data2 = json.load(rf)
            for ip, val in data2.items():
                if ip not in status[vp].keys():
                    continue
                tmp_status = status[vp][ip]
                res[vp][tmp_status][ip] = [0, 0, 0, 0, 0, 0]
                tmp_sum = len(val)
                for subval in val.values():
                    if subval == '*':
                        subval = 6
                    res[vp][tmp_status][ip][subval-1] += 1 / tmp_sum
        # print('succ:')
        # for ip, val in res[vp]['succ'].items():
        #     print('{}:{}'.format(ip, val))
        # print('fail:')
        # for ip, val in res[vp]['fail'].items():
        #     print('{}:{}'.format(ip, val))
        draw_index = 1
        
        #fig, axs = plt.subplots(nrows=2, ncols=3, figsize=(10, 6))
        fig, axs = plt.subplots(nrows=6, ncols=2, figsize=(10, 6))
        labels = {0:'customer', 1:'provider', 2:'peer', 3:'unknown', 4:'sibling', 5: '*'}
        for draw_index in range(6):
            # Plot your data on each subplot
            succ = sorted([val[draw_index] for val in res[vp]['succ'].values()])
            fail = sorted([val[draw_index] for val in res[vp]['fail'].values()])
            # cdf1 = np.cumsum(np.ones_like(succ))/len(succ)
            # cdf2 = np.cumsum(np.ones_like(fail))/len(fail)
            # axs[int(draw_index/3)][draw_index%3].plot(succ, cdf1, c='r')
            # axs[int(draw_index/3)][draw_index%3].plot(fail, cdf2, c='g')
            axs[draw_index][0].plot(succ, c='r')
            axs[draw_index][1].plot(fail, c='g')
            axs[draw_index][0].set_title(labels[draw_index])
            
        #1: later is customer; 2: later is provider; 3: peer; 4: unknown   
        # axs[0, 0].set_title('customer')
        # axs[0, 1].set_title('provider')
        # axs[0, 2].set_title('peer')
        # axs[1, 0].set_title('unknown')
        # axs[1, 1].set_title('sibling')
        # axs[1, 2].set_title('*')
                
        plt.tight_layout()
        plt.show()
        break

def cmp_mapping_status():
    orgs = ['ChinaTelecom', 'cernet']
    for org in orgs:
        print(org)
        fns = ['/home/slt/code/ana_c_d_incongruity/%s_202202' %org, '/home/slt/code/ana_c_d_incongruity/%s_202104' %org]
        data = {}
        for fn in fns:
            date = fn.split('_')[-1]
            data[date] = {}
            with open(fn, 'r') as rf:
                for line in rf:
                    if len(line.strip('\n').split(':')) != 3:
                        print(line)
                        continue
                    ip, status, asn = line.strip('\n').split(':')
                    status = status.strip('{').strip('\'')
                    asn = asn.strip(' ').strip('{').strip('}').strip('\'')
                    data[date][ip] = [asn, status]
        mismatch = 0
        newly = 0
        for ip, val in data['202202'].items():
            if ip in data['202104'].keys():
                if val != data['202104'][ip]:
                    mismatch += 1
            else:
                newly += 1
        print('\tmismatch: {}, newly: {}, total: {}'.format(mismatch, newly, len(data['202202'])))

def RefineSucceedIps_v3():
    global vps
    filtered = set()
    date = '20220215'
    all_ip_mappings = defaultdict(set)
    for vp in vps:
        os.chdir('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/' %vp)
        with open('modify_ip_mapping_status_%s.json' %vp, 'r') as rf:
            data = json.load(rf)
            if not data:
                continue
            trip_AS = {}
            with open('ipattr_trip_AS_%s_%s.json' %(vp, date), 'r') as rf:
                trip_AS = json.load(rf) #后面只取了trip_AS的keys()
            with open('ipattr_prev_ips_%s_%s.json' %(vp, date), 'r') as rf:
                prev_ips = json.load(rf) #prev_ips[ips[j]][trace_idx] = (ips[j-1], hops[j-1])
            with open('ipattr_succ_ips_%s_%s.json' %(vp, date), 'r') as rf:
                succ_ips = json.load(rf) 
            ips = set(data.keys()) & set(trip_AS.keys())
            for ip in ips:
                state, asn = data[ip]
                all_ip_mappings[ip].add((state, asn))
                if len(set(prev_ips[ip].values())) == 1 and len(set(succ_ips[ip].values())) == 1:
                    continue
                # if all(val.split('|')[1] == asn for val in prev_ips[ip].values()) and \
                #     all(val.split('|')[1] == asn for val in succ_ips[ip].values()):
                #         continue
                filtered.add(ip)
    print(len(all_ip_mappings))
    res = {}
    state_c = Counter()
    for ip in filtered:
        if len(all_ip_mappings[ip]) == 1:
            state, asn = list(all_ip_mappings[ip])[0]
            if asn and asn.isdigit() and int(asn) < 0xFFFFFF and int(asn) > 0:
                if state == 'succ' or state == 'fail':
                    res[ip] = [asn, state]
                    state_c[state] += 1
        # if any(elem[1] == 'succ' or elem[1] == 'fail' for elem in val):
        #     res[ip] = val
    print('res: %d' %len(res))
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/modify4_ip_mapping_status_2.json', 'w') as wf:
        json.dump(res, wf, indent=1)
    print(state_c)

def DebugTest2():
    key_asns = {'4809', '23764', '4812', '23724', '4811', '4816', '23650', '138950', '139721', '134756', '4835', '140061', '36678', '139018', '135386', '140330', '24545', '140903', '140292', '139019', '131285', '141771', '137702', '140485', '140647', '140553', '140265', '141739', '141998', '140345', '141679', '140527', '139203', '139201', '140636', '139220', '132147', '149178', '138982', '132225', '138169', '140638', '140083', '146966', '150145', '149837', '139462', '139887', '132153', '148969', '135089', '140278', '142608', '140276', '132833', '142404', '138991', '134425', '136167', '141157', '138949', '58518', '63527', '149979', '58517', '147038', '148981', '139767', '44218'}
    orgs = 'chinatelecom'
    #key_asns = {'24348', '24369', '24364', '24355', '24357', '24361', '24349', '24363', '24362', '24358', '24350', '24353', '24370', '24367', '24352', '24371', '24356', '24372', '23910', '45576'}
    #key_asns = {'4538', '24348', '138371', '24369', '24364', '133111', '24355', '45576', '23910', '38272', '24357', '24361', '24349', '24363', '24302', '24362', '38255', '139774', '24358', '138378', '24350', '24353', '24370', '24367', '138393', '24352', '138369', '24371', '138373', '24356', '24372'}
    #orgs = 'cernet'
    
    data = {}
    with open('/home/slt/code/ana_c_d_incongruity/modify3_ip_mapping_status.json', 'r') as rf:
        data = json.load(rf)
    rec = {}
    state_c = Counter()
    for ip, val in data.items():
        asn, state = val
        if asn in key_asns:
            rec[ip] = val
            state_c[state] += 1
    with open(orgs + '.json', 'w') as wf:
        json.dump(rec, wf, indent=1)
    print(state_c)

def reform(org):
    bgp_path_info = {}
    InitPref2ASInfo('/mountdisk1/ana_c_d_incongruity/out_ip2as_data/20220215.ip2as.prefixes', bgp_path_info)
    data = {}
    with open('%s.json' %org, 'r') as rf:
        data = json.load(rf)
    res = defaultdict(lambda:defaultdict(defaultdict))
    for ip, val in data.items():
        asn, state = val
        pref, rib_asn = GetBGPPath_Or_OriASN(bgp_path_info, ip, 'get_all_2')
        res[state][pref+'|'+rib_asn][ip] = asn
    with open('%s_v2.json' %org, 'w') as wf:
        json.dump(res, wf, indent=1)
        
def reform2():
    org = 'cernet'
    reform(org)
    data = {}
    with open('%s_v2.json' %org, 'r') as rf:
        data = json.load(rf)
    with open('%s_v3.txt' %org, 'w') as wf:
        for state, val in data.items():
            for key, subval in val.items():
                pref, rib_asn = key.split('|')
                s = sorted(subval.items(), key=lambda x:x[0])
                for elem in s:
                    if state == 'succ' and rib_asn == elem[1]:
                        continue
                    wf.write('{},{},{},{},{}\n'.format(state, pref, rib_asn, elem[0], elem[1]))

def debug():
    v = {'115.170.13.2': 'succ', '5.154.154.26': 'fail', '59.43.105.58': 'succ', '59.43.130.189': 'succ', '59.43.130.193': 'succ', '59.43.130.197': 'succ', '59.43.132.154': 'succ', '59.43.137.238': 'succ', '59.43.138.57': 'succ', '59.43.138.65': 'succ', '59.43.16.230': 'succ', '59.43.18.190': 'succ', '59.43.180.113': 'succ', '59.43.180.142': 'succ', '59.43.180.246': 'succ', '59.43.181.186': 'succ', '59.43.182.153': 'succ', '59.43.182.225': 'succ', '59.43.182.233': 'succ', '59.43.183.1': 'succ', '59.43.183.134': 'succ', '59.43.183.178': 'succ', '59.43.183.53': 'succ', '59.43.183.94': 'succ', '59.43.184.102': 'succ', '59.43.184.106': 'succ', '59.43.184.177': 'succ', '59.43.184.181': 'succ', '59.43.184.186': 'succ', '59.43.184.197': 'succ', '59.43.185.234': 'succ', '59.43.186.185': 'succ', '59.43.186.245': 'succ', '59.43.186.249': 'succ', '59.43.187.22': 'succ', '59.43.187.250': 'succ', '59.43.187.73': 'succ', '59.43.187.77': 'succ', '59.43.187.81': 'succ', '59.43.187.85': 'succ', '59.43.189.1': 'succ', '59.43.189.197': 'succ', '59.43.246.170': 'succ', '59.43.246.174': 'succ', '59.43.246.178': 'succ', '59.43.246.18': 'succ', '59.43.247.114': 'succ', '59.43.247.141': 'succ', '59.43.247.22': 'succ', '59.43.247.230': 'succ', '59.43.247.237': 'succ', '59.43.247.250': 'succ', '59.43.247.30': 'succ', '59.43.247.61': 'succ', '59.43.247.70': 'succ', '59.43.247.82': 'succ', '59.43.247.86': 'succ', '59.43.248.122': 'succ', '59.43.248.146': 'succ', '59.43.248.150': 'succ', '59.43.248.18': 'succ', '59.43.248.182': 'succ', '59.43.248.194': 'succ', '59.43.248.22': 'succ', '59.43.248.254': 'succ', '59.43.248.6': 'succ', '59.43.248.62': 'succ', '59.43.249.10': 'succ', '59.43.249.17': 'succ', '59.43.249.181': 'succ', '59.43.249.186': 'succ', '59.43.249.197': 'succ', '59.43.249.2': 'succ', '59.43.249.230': 'succ', '59.43.249.250': 'succ', '59.43.249.5': 'succ', '59.43.250.226': 'succ', '59.43.250.26': 'succ', '59.43.250.30': 'succ', '59.43.46.50': 'succ', '59.43.46.6': 'succ', '59.43.46.85': 'succ', '59.43.46.98': 'succ', '59.43.47.90': 'succ', '59.43.64.129': 'succ', '59.43.64.133': 'succ', '59.43.95.74': 'succ', '218.30.48.54': 'succ', '112.112.0.234': 'fail', '125.71.139.178': 'fail', '58.221.112.250': 'succ', '195.22.211.43': 'succ', '219.148.166.234': 'succ', '124.126.254.45': 'fail', '184.104.224.166': 'succ', '117.103.177.106': 'succ', '203.131.241.66': 'succ', '217.163.44.234': 'succ', '118.84.190.114': 'succ', '218.3.104.198': 'fail', '218.30.33.82': 'fail', '63.222.64.2': 'fail', '121.59.105.10': 'fail', '121.59.105.2': 'fail', '121.59.105.58': 'fail', '121.59.105.6': 'fail', '59.60.2.13': 'fail', '218.185.243.134': 'succ', '218.30.38.250': 'succ'}
    with open('chinatelecom.json', 'r') as rf:
        data = json.load(rf)
        s = defaultdict(Counter)
        for ip, state in v.items():
            if data[ip][1] == 'succ':
                s[ip][data[ip][1]==state] += 1
        print(s)

if __name__ == '__main__':  
    #DebugModify4Attr()
    #debug()
    
    #reform2()
    #cmp_mapping_status() 
    #CheckRefinedSucceedIPs()
    #RefineSucceedIps()   
    #CmpAttr_Trip()
    #CmpAttr_PrevAsnRel()
    #CmpAttr_SuccAsnRel()
    #CmpAttr_SuccSameAsn()
    
    # ModifyIPMappingStatus()
    # for vp in vps:
    #     GetIPAttr('20220215', vp)       
    RefineSucceedIps_v3()
    #DebugTest2()

    
    #DebugTest()
    #CheckBdrConformity()
    