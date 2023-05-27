
import os
import glob
import json
from utils_v2 import GetIxpAsSet, IsIxpAs, GetIxpPfxDict_2, IsIxpIp
from collections import Counter, defaultdict
from get_ip2as_from_bdrmapit import ConnectToBdrMapItDb, GetBdrCache, CloseBdrMapItDb, InitBdrCache, \
                                    ConstrBdrCache
from traceutils.bgp.bgp import BGP
from rect_bdrmapit import CheckSiblings
import matplotlib.pyplot as plt
import numpy as np
from compare_cd import InitPref2ASInfo, GetBGPPath_Or_OriASN

vps = ['ams-nl', 'jfk-us', 'nrt-jp', 'sao-br', 'sjc2-us', 'syd-au']

def ModifyIPMappingStatus():
    global vps
    GetIxpAsSet('20220215')
    for vp in vps:
    #for vp in ['syd-au']:
        ip_info = {}
        fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/match_*.202202*' %vp)
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
        fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/ana_compare_res/discrete_mm.*202202*' %vp)
        for fn in fns:
            with open(fn, 'r') as rf:
                data = json.load(rf)
                for val in data.values():
                    for i in range(0, len(val), 3):
                        trace_hops = val[i].strip('\n').split(']')[-1].split(' ')
                        bgp_hops = val[i+1].strip('\n').strip('\t').split(' ')
                        cur_splits = val[i+2].strip('\n').split(']')
                        mm_ips = cur_splits[0][1:].split(',')
                        pm_ips = cur_splits[1][1:].split(',')
                        ips = cur_splits[2].split(' ')
                        for i in range(len(ips)):
                            ip = ips[i]
                            if ip == '*':
                                continue
                            # if ip == '221.120.223.50':
                            #     a = 1
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
        with open('modify_ip_mapping_status_%s.json' %vp, 'w') as wf:
            json.dump(ip_stat, wf, indent=1)
            
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
    fn1 = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/match_%s.%s*' %(vp, vp, date[:6]))
    if fn1:
        with open(fn1[0], 'r') as rf:
            lines = rf.readlines()
    fn2 = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/ana_compare_res/discrete_mm.%s.%s*' %(vp, vp, date[:6]))
    if fn2:
        with open(fn2[0], 'r') as rf:
            data = json.load(rf)
            for val in data.values():
                lines = lines + val
    return lines

def GetMatchAndMmLines(vp, date):
    lines = []
    fn1 = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/match_%s.%s*' %(vp, vp, date[:6]))
    if fn1:
        with open(fn1[0], 'r') as rf:
            lines = rf.readlines()
    fn2 = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/mm_%s.%s*' %(vp, vp, date[:6]))
    if fn2:
        with open(fn2[0], 'r') as rf:
            lines = lines + rf.readlines()
    return lines

#bgp.reltype: 1: later is customer; 2: later is provider; 3: peer; 4: unknown   
def GetIPAttr():
    global vps
    date = '20220215'
    checksiblings = CheckSiblings('%s0401' %date[:4])
    GetIxpPfxDict_2(int(date[:4]), int(date[4:6]))
    GetIxpAsSet(date)
    for vp in vps:
        print(vp)
        lines = GetMatchAndMapErrorLines(vp, date)
        #lines = GetMatchAndMmLines(vp, date)
        print(len(lines))
        prev_asnrel = defaultdict(defaultdict)
        prev_ips = defaultdict(defaultdict)
        succ_ips = defaultdict(Counter)
        dst_ips = defaultdict(defaultdict)
        succ_asns = defaultdict(lambda:defaultdict(Counter))
        succ_asnrel = defaultdict(defaultdict)
        trip = defaultdict(Counter)
        prev_ixp = defaultdict(defaultdict)
        succ_ixp = defaultdict(defaultdict)
        is_ixp = {}
        prev_sameAS = defaultdict(defaultdict)
        succ_sameAS = defaultdict(defaultdict)
        trip_AS = defaultdict(Counter)
        if lines:
            for i in range(0, len(lines), 3):
                # if i > 1621014:
                #     tmp0 = lines[i]
                #     tmp2 = lines[i + 2]
                #     if tmp0.strip('\n').split(']')[0][1:] == '45.168.149.1':
                #         a = 1
                dst_ip, trace = lines[i].strip('\n').split(']')
                dst_ip = dst_ip[1:]
                bgp_hops = lines[i+1].strip('\n').strip('\t').split(' ')
                ori_hops = trace.split(' ')
                hops = [hop for hop in ori_hops if hop != '*']
                ori_ips = lines[i + 2].strip('\n').split(']')[-1].split(' ')
                ips = [ip for ip in ori_ips if ip != '*']
                for j in range(len(ips)):                    
                    if hops[j] == '*' or hops[j] == '?' or hops[j] == '-1':
                        continue
                    cur_hop = hops[j].strip('<').strip('>')
                    is_ixp[ips[j]] = (IsIxpIp(ips[j]) or IsIxpAs(cur_hop))
                    if j < len(ips) - 1:
                        succ_hop = hops[j+1].strip('<').strip('>')
                        succ_ips[ips[j]][ips[j+1] + '|' + succ_hop] += 1
                        succ_ixp[ips[j]][succ_hop] = (IsIxpIp(ips[j+1]) or IsIxpAs(succ_hop))
                        succ_sameAS[ips[j]][succ_hop] = (cur_hop == succ_hop)
                        succ_asns[ips[j]][succ_hop][ips[j+1]] += 1
                        if succ_hop not in succ_asnrel[ips[j]].keys():
                            if succ_hop == '*' or succ_hop == '?':
                                succ_asnrel[ips[j]][succ_hop] = '*'
                            else:
                                #if checksiblings.check_sibling(int(cur_hop), int(succ_hop)):
                                if int(cur_hop) == int(succ_hop):
                                    succ_asnrel[ips[j]][succ_hop] = 5
                                else:
                                    succ_asnrel[ips[j]][succ_hop] = checksiblings.bgp.reltype(int(cur_hop), int(succ_hop))
                    elif ips[j] != dst_ip: #最后一跳是dst_ip的，丢掉不管
                            dst_ips[ips[j]][dst_ip] = bgp_hops[-1]
                    if j > 0 and ips[j] != dst_ip:  #最后一跳是dst_ip的，丢掉不管
                        prev_hop = hops[j-1].strip('<').strip('>')
                        prev_ips[ips[j]][ips[j-1]] = prev_hop
                        prev_ixp[ips[j]][prev_hop] = (IsIxpIp(ips[j-1]) or IsIxpAs(prev_hop))
                        prev_sameAS[ips[j]][prev_hop] = (prev_hop == cur_hop)
                        if prev_hop not in prev_asnrel[ips[j]].keys():
                            if prev_hop == '*' or prev_hop == '?':
                                prev_asnrel[ips[j]][prev_hop] = '*'
                            else:
                                #if checksiblings.check_sibling(int(cur_hop), int(prev_hop)):
                                if int(cur_hop) == int(prev_hop):
                                    prev_asnrel[ips[j]][prev_hop] = 5
                                else:
                                    prev_asnrel[ips[j]][prev_hop] = checksiblings.bgp.reltype(int(cur_hop), int(prev_hop))
                        if j < len(ips) - 1:
                            trip[ips[j]][ips[j-1] + ',' + ips[j+1] + '|' + prev_hop + ',' + succ_hop] += 1
                            trip_AS[ips[j]][prev_hop + ',' + succ_hop] += 1
                        elif ips[j] != dst_ip: #最后一跳是dst_ip的，丢掉不管
                            trip[ips[j]][ips[j-1] + ',' + dst_ip + '|' + prev_hop + ',' + bgp_hops[-1]] += 1
                            trip_AS[ips[j]][prev_hop + ',' + bgp_hops[-1] + '$'] += 1
            with open('ipattr_prev_asnrel_%s.json' %vp, 'w') as wf:
                json.dump(prev_asnrel, wf, indent=1)
            with open('ipattr_succ_ips_%s.json' %vp, 'w') as wf:
                json.dump(succ_ips, wf, indent=1)
            with open('ipattr_succ_asns_%s.json' %vp, 'w') as wf:
                json.dump(succ_asns, wf, indent=1)
            with open('ipattr_succ_asnrel_%s.json' %vp, 'w') as wf:
                json.dump(succ_asnrel, wf, indent=1)
            with open('ipattr_trip_%s.json' %vp, 'w') as wf:
                json.dump(trip, wf, indent=1)
            with open('ipattr_prev_ixp_%s.json' %vp, 'w') as wf:
                json.dump(prev_ixp, wf, indent=1)
            with open('ipattr_succ_ixp_%s.json' %vp, 'w') as wf:
                json.dump(succ_ixp, wf, indent=1)
            with open('ipattr_is_ixp_%s.json' %vp, 'w') as wf:
                json.dump(is_ixp, wf, indent=1)
            with open('ipattr_prev_sameAS_%s.json' %vp, 'w') as wf:
                json.dump(prev_sameAS, wf, indent=1)
            with open('ipattr_succ_sameAS_%s.json' %vp, 'w') as wf:
                json.dump(succ_sameAS, wf, indent=1)
            with open('ipattr_prev_ips_%s.json' %vp, 'w') as wf:
                json.dump(prev_ips, wf, indent=1)
            with open('ipattr_dst_ips_%s.json' %vp, 'w') as wf:
                json.dump(dst_ips, wf, indent=1)            
            with open('ipattr_trip_AS_%s.json' %vp, 'w') as wf:
                json.dump(trip_AS, wf, indent=1)

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

def RefineSucceedIps():
    for fn in glob.glob('/home/slt/code/ana_c_d_incongruity/modify_ip_mapping_status_*'):
    #for fn in ['/home/slt/code/ana_c_d_incongruity/modify_ip_mapping_status_syd-au.json']:
        with open(fn, 'r') as rf:
            data = json.load(rf)
            if not data:
                continue
            print(fn)
            vp = fn.split('_')[-1].split('.')[0]
            succ_ips = {}
            with open('/home/slt/code/ana_c_d_incongruity/ipattr_succ_ips_%s.json' %vp, 'r') as rf:
                succ_ips = json.load(rf)
            prev_ips = {}
            with open('/home/slt/code/ana_c_d_incongruity/ipattr_prev_ips_%s.json' %vp, 'r') as rf:
                prev_ips = json.load(rf)
            dst_ips = {}
            with open('/home/slt/code/ana_c_d_incongruity/ipattr_dst_ips_%s.json' %vp, 'r') as rf:
                dst_ips = json.load(rf)
            filter_res = {}
            print('Origninal: %d' %len(data))
            for ip, val in data.items():
                if ip == "154.24.14.174":
                    a = 1
                filter = False
                state, asn = val
                if state == 'succ':
                    check_prev = False
                    if ip in succ_ips.keys() and len(succ_ips[ip]) == 1:
                        succ_ip = list(succ_ips[ip].keys())[0]
                        if succ_ip in data.keys():
                            succ_state, succ_asn = data[succ_ip]
                            if succ_asn != asn and succ_state == 'succ':
                                filter = True
                            elif succ_asn == asn:
                                check_prev = True
                    elif ip not in succ_ips.keys():
                        if ip not in dst_ips.keys():
                            print('{} error'.format(ip))
                        elif len(dst_ips[ip]) == 1:
                            dst_asn = list(dst_ips[ip].values())[0]
                            if dst_asn != asn:
                                filter = True
                            else:
                                check_prev = True
                    if check_prev:                            
                        if ip in prev_ips.keys():
                            tmp_prev_asns = {tmp_asn for tmp_asn in prev_ips[ip].values() if tmp_asn != '*'}
                            if len(tmp_prev_asns) == 1:
                                tmp_prev_asn = list(tmp_prev_asns)[0]
                                if tmp_prev_asn != asn:
                                    tmp_prev_ips = [tmp_ip for tmp_ip, tmp_asn in prev_ips[ip].items() if tmp_asn == tmp_prev_asn]
                                    if all(tmp_ip in data.keys() and data[tmp_ip][0] == 'succ' for tmp_ip in tmp_prev_ips):
                                        filter = True
                if state != 'other' and state != 'unmap' and not filter:
                    filter_res[ip] = val
            print('After: %d' %len(filter_res))
            with open('/home/slt/code/ana_c_d_incongruity/modify2_ip_mapping_status_%s.json' %vp, 'w') as wf:
                json.dump(filter_res, wf, indent =1 )

def CheckRefinedSucceedIPs():
    ip_info = defaultdict(defaultdict)
    for fn in glob.glob('/home/slt/code/ana_c_d_incongruity/modify2_ip_mapping_status_*'):
        vp = fn.split('_')[-1].split('.')[0]
        with open(fn, 'r') as rf:
            data = json.load(rf)
            for ip, val in data.items():
                state, asn = val
                if state == 'succ':
                    ip_info[ip][asn] = vp
    print('total: %d' %len(ip_info))
    susps = {ip:val for ip, val in ip_info.items() if len(val) > 1}
    #print(susps)
    print('suspects: %d' %len(susps))

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

def RefineSucceedIps_v2():
    filtered = defaultdict(set)
    all_ip_mappings = defaultdict(set)
    for fn in glob.glob('/home/slt/code/ana_c_d_incongruity/modify_ip_mapping_status_*'):
        with open(fn, 'r') as rf:
            data = json.load(rf)
            if not data:
                continue
            print(fn)
            vp = fn.split('_')[-1].split('.')[0]
            succ_ips = {}
            with open('/home/slt/code/ana_c_d_incongruity/ipattr_succ_ips_%s.json' %vp, 'r') as rf:
                succ_ips = json.load(rf)
            prev_ips = {}
            with open('/home/slt/code/ana_c_d_incongruity/ipattr_prev_ips_%s.json' %vp, 'r') as rf:
                prev_ips = json.load(rf)
            dst_ips = {}
            with open('/home/slt/code/ana_c_d_incongruity/ipattr_dst_ips_%s.json' %vp, 'r') as rf:
                dst_ips = json.load(rf)
            #print('Origninal: %d' %len(data))
            for ip, val in data.items():
                # if ip == "154.24.14.174":
                #     a = 1
                filter = False
                state, asn = val
                all_ip_mappings[ip].add((state, asn))
                if len(all_ip_mappings[ip]) > 1:
                    continue
                if state == 'succ':
                    check_prev = False
                    if ip in succ_ips.keys():
                        if all(succ_ip in data.keys() and data[succ_ip][1] == asn for succ_ip in succ_ips[ip].keys()):
                            check_prev = True
                    elif ip in dst_ips.keys() and all(dst_val == asn for dst_val in dst_ips[ip].values()):
                        check_prev = True
                    if check_prev:
                        if ip in prev_ips.keys() and all(prev_val == asn for prev_val in prev_ips[ip].values()):
                            filter = True
                    if not filter:
                        check_prev = False
                        if ip in succ_ips.keys() and len(succ_ips[ip]) == 1:
                            succ_ip = list(succ_ips[ip].keys())[0]
                            if succ_ip in data.keys():
                                succ_state, succ_asn = data[succ_ip]
                                if succ_asn != asn and succ_state == 'succ':
                                    filter = True
                                elif succ_asn == asn:
                                    check_prev = True
                        elif ip not in succ_ips.keys():
                            if ip not in dst_ips.keys():
                                print('{} error'.format(ip))
                            elif len(dst_ips[ip]) == 1:
                                dst_asn = list(dst_ips[ip].values())[0]
                                if dst_asn != asn:
                                    filter = True
                                else:
                                    check_prev = True
                        if check_prev:                            
                            if ip in prev_ips.keys():
                                tmp_prev_asns = {tmp_asn for tmp_asn in prev_ips[ip].values() if tmp_asn != '*'}
                                if len(tmp_prev_asns) == 1:
                                    tmp_prev_asn = list(tmp_prev_asns)[0]
                                    if tmp_prev_asn != asn:
                                        tmp_prev_ips = [tmp_ip for tmp_ip, tmp_asn in prev_ips[ip].items() if tmp_asn == tmp_prev_asn]
                                        if all(tmp_ip in data.keys() and data[tmp_ip][0] == 'succ' for tmp_ip in tmp_prev_ips):
                                            filter = True
                if not filter:
                    filtered[ip] = [asn, state]
    print(len(all_ip_mappings))
    res = {}
    state_c = Counter()
    for ip, val in filtered.items():
        if len(all_ip_mappings[ip]) == 1:
            asn, state = val
            if state == 'succ' or state == 'fail':
                res[ip] = [asn, state]
                state_c[state] += 1
        # if any(elem[1] == 'succ' or elem[1] == 'fail' for elem in val):
        #     res[ip] = val
    print('res: %d' %len(res))
    with open('/home/slt/code/ana_c_d_incongruity/modify3_ip_mapping_status.json', 'w') as wf:
        json.dump(res, wf, indent=1)
    print(state_c)

def RefineSucceedIps_v3():
    filtered = set()
    all_ip_mappings = defaultdict(set)
    for fn in glob.glob('/home/slt/code/ana_c_d_incongruity/modify_ip_mapping_status_*'):
        with open(fn, 'r') as rf:
            data = json.load(rf)
            if not data:
                continue
            print(fn)
            vp = fn.split('_')[-1].split('.')[0]
            trips = {}
            with open('/home/slt/code/ana_c_d_incongruity/ipattr_trip_%s.json' %vp, 'r') as rf:
                trips = json.load(rf)
            ips = set(data.keys()) & set(trips.keys())
            for ip in ips:
                state, asn = data[ip]
                all_ip_mappings[ip].add((state, asn))
                # if len(trips[ip]) == 1:
                #     filter = True
                #     state, asn = data[ip]
                #     for trip in trips[ip].keys():
                #         trip_ip, trip_AS = trip.split('|')
                #         prev_ip, succ_ip = trip_ip.split(',')
                #         prev_AS, succ_AS = trip_AS.split(',')
                #         if state == 'succ':
                #             if prev_ip in data.keys():
                #                 prev_state, prev_asn = data[prev_ip]
                #                 if prev_asn == 'succ' and prev_asn != asn:
                #                     if succ_ip in data.keys():
                #                         succ_state, succ_asn = data[succ_ip]
                if len(trips[ip]) > 1:
                    for trip in trips[ip]:
                        prev_AS, succ_AS = trip.split('|')[-1].split(',')
                        if prev_AS != asn or succ_AS != asn:
                            filtered.add(ip)
                            break
    print(len(all_ip_mappings))
    res = {}
    state_c = Counter()
    for ip in filtered:
        if len(all_ip_mappings[ip]) == 1:
            state, asn = list(all_ip_mappings[ip])[0]
            if state == 'succ' or state == 'fail':
                res[ip] = [asn, state]
                state_c[state] += 1
        # if any(elem[1] == 'succ' or elem[1] == 'fail' for elem in val):
        #     res[ip] = val
    print('res: %d' %len(res))
    with open('/home/slt/code/ana_c_d_incongruity/modify4_ip_mapping_status.json', 'w') as wf:
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
                

if __name__ == '__main__':  
    #reform2()
    #cmp_mapping_status() 
    #CheckRefinedSucceedIPs()
    #RefineSucceedIps()   
    #CmpAttr_Trip()
    #CmpAttr_PrevAsnRel()
    #CmpAttr_SuccAsnRel()
    #CmpAttr_SuccSameAsn()
    
    #ModifyIPMappingStatus()
    GetIPAttr()       
    RefineSucceedIps_v3()
    #RefineSucceedIps_v2()
    #DebugTest2()

    
    #DebugTest()
    #CheckBdrConformity()
    