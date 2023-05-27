
import os
import json
import sys
import glob
import struct, socket
from multiprocessing import Process, Queue
from collections import Counter, defaultdict

from traceutils.as2org.as2org import AS2Org
from traceutils.bgp.bgp import BGP
from get_ip2as_from_bdrmapit import ConnectToBdrMapItDb, GetIp2ASFromBdrMapItDb, CloseBdrMapItDb, InitBdrCache, \
                                    ConstrBdrCache, UpdateASInBdrDb
from utils_v2 import IsSib_2, GetSibRel, GetIxpPfxDict_2, IsIxpIp, GetIxpAsSet, IsIxpAs
from compare_cd import CompressTrace, InitPref2ASInfo, GetBGPPath_Or_OriASN, InitBGPPathInfo

class CheckSiblings():
    def __init__(self, use_date):
        self.as2orgs = {}
        as2orgs_dir = '/mountdisk1/ana_c_d_incongruity/as_org_data/'
        for filename in os.listdir(as2orgs_dir):
            date = filename.split('.')[0]
            if use_date <= date:
                #self.as2orgs[date] = AS2Org('/mountdisk1/ana_c_d_incongruity/as_org_data/%s01.as-org2info.txt' %use_date[:6])
                self.as2orgs[date] = AS2Org('/mountdisk1/ana_c_d_incongruity/as_org_data/%s.as-org2info.txt' %date)
            #print('{}'.format(self.as2orgs[date].name()))
        asrel_dir = '/mountdisk1/ana_c_d_incongruity/as_rel_cc_data/'
        self.bgp = BGP(asrel_dir + use_date[:6] + '01.as-rel3.txt', asrel_dir + use_date[:6] + '01.ppdc-ases.txt')
        self.date = use_date
        # self.bgp_v2 = defaultdict(defaultdict)
        # with open(asrel_dir + use_date[:6] + '01.as-rel3.txt', 'r') as rf:
        #     for line in rf:
        #         if line.startswith('#'):
        #             continue
        #         asn1, asn2, rel = line.strip('\n').split('|')
        #         if rel == '-1':
        #             self.bgp_v2[asn1][asn2] = 2
        #             self.bgp_v2[asn2][asn1] = 1
        #         elif rel == '0':
        #             self.bgp_v2[asn1][asn2] = 3
        #             self.bgp_v2[asn2][asn1] = 3
    
    def check_sibling(self, asn1, asn2):
        for d in self.as2orgs.keys():
            if d >= self.date:
                info = self.as2orgs[d]
                #print('{}'.format(info))
                if info.name(int(asn1)) == info.name(int(asn2)):
                    return True
        return False
    
    def get_all_siblings(self, asn):
        res = set()
        for as_orgs in self.as2orgs.values():
            res = res | as_orgs.siblings[int(asn)]
        return res

    def check_susp_sibling_trace(self, trace_list):
        susp_asns = {}#defaultdict(set)
        norm_asns = set()
        prev_hop = trace_list[0]
        if prev_hop == '*' or prev_hop == '?' or prev_hop.startswith('<'):
            prev_hop = None
        for i in range(1, len(trace_list)):
            hop = trace_list[i]
            if hop == '*' or hop == '?' or hop.startswith('<'):
                prev_hop = None
                continue
            if not prev_hop:
                prev_hop = hop
                continue
            if not self.check_sibling(prev_hop, hop):
                prev_hop = hop
                continue
            #siblings
            change_left = change_right = possi_change_left = possi_change_right = False
            if i > 1:
                left_hop = trace_list[i - 2]
                if left_hop == '*' or left_hop == '?' or left_hop.startswith('<'):
                    pass
                elif self.bgp.rel(int(left_hop), int(hop)):# and not self.bgp.rel(int(left_hop), int(prev_hop)):
                    if not self.bgp.rel(int(left_hop), int(prev_hop)):
                        change_left = True
                    elif not self.bgp.rel(int(prev_hop), int(hop)):
                        possi_change_left = True
            if i < len(trace_list) - 1:
                right_hop = trace_list[i + 1]
                if right_hop == '*' or right_hop == '?' or right_hop.startswith('<'):
                    pass
                elif self.bgp.rel(int(right_hop), int(prev_hop)):# and not self.bgp.rel(int(right_hop), int(hop)):
                    if not self.bgp.rel(int(right_hop), int(hop)):
                        change_right = True
                    elif not self.bgp.rel(int(prev_hop), int(hop)):
                        possi_change_right = True
            if change_left and change_right: pass #do nothing
            elif change_left: susp_asns[prev_hop] = hop
            elif change_right: susp_asns[hop] = prev_hop
            else:
                if possi_change_left and possi_change_right: pass
                elif possi_change_left: susp_asns[prev_hop] = hop
                elif possi_change_right: susp_asns[hop] = prev_hop                        
            prev_hop = hop
        return susp_asns

    def rec_sibling(self, trace_filename, first_asn = None):
        print(trace_filename)
        (vp, date) = trace_filename.split('/')[-1][len('trace_'):].split('.')
        print(vp)
        print(date)
        os.system('cp /mountdisk1/ana_c_d_incongruity/out_bdrmapit/sxt_bdr/before_rect_bdrmapit_%s_%s.db /mountdisk1/ana_c_d_incongruity/out_bdrmapit/sxt_bdr/bdrmapit_%s_%s.db' %(vp, date, vp, date))
        ConnectToBdrMapItDb('/mountdisk1/ana_c_d_incongruity/out_bdrmapit/sxt_bdr/bdrmapit_%s_%s.db' %(vp, date))
        #ConnectToBdrMapItDb('test1.db')
        ConstrBdrCache()
        stat_ips = {}
        debug_ip = '213.140.39.119'
        with open(trace_filename, 'r') as rf:
            for line in rf:
                asn_ips = defaultdict(set)
                dst_ip, trace_ip_path = line.strip('\n').split(':')
                if not trace_ip_path: #有的dst_ip没有trace
                    continue
                ip_list = trace_ip_path.split(',')
                ori_trace_list = []
                for _ip in ip_list:
                    if _ip == '*':
                        ori_trace_list.append('*')
                        continue
                    asn = GetIp2ASFromBdrMapItDb(_ip)
                    if not asn: ori_trace_list.append('?')
                    else:
                        if IsIxpAs(asn): ori_trace_list.append('<' + asn + '>')
                        else: ori_trace_list.append(asn)
                (trace_list, trace_to_ip_info, loop_flag) = CompressTrace(ori_trace_list, ip_list, '')
                if loop_flag:
                    continue
                if debug_ip in ip_list:
                    #print('')
                    pass
                susp_asns = self.check_susp_sibling_trace(trace_list)
                for hop in trace_list:
                    if hop == '*' or hop == '?' or hop.startswith('?'):
                        continue
                    fst_ip_index = ip_list.index(trace_to_ip_info[hop][0])
                    lst_ip_index = ip_list.index(trace_to_ip_info[hop][-1])
                    prev_ip = ip_list[fst_ip_index - 1] if fst_ip_index > 0 else None
                    next_ip = ip_list[lst_ip_index + 1] if lst_ip_index < len(ip_list) - 1 else None
                    for _ip in trace_to_ip_info[hop]:
                        if _ip not in stat_ips.keys():
                            stat_ips[_ip] = {'n': set(), 's': set()}
                        if hop in susp_asns.keys():
                            stat_ips[_ip]['s'].add((prev_ip, next_ip, susp_asns[hop]))
                        # else:
                        #     stat_ips[_ip]['n'].add((prev_ip, next_ip))

        susp_ips = defaultdict(Counter)
        for _ip, val in stat_ips.items():
            if _ip == debug_ip:
                print('')
            n_num = len(val['n'])
            s_num = len(val['s'])
            #if n_num < s_num:
            if s_num > 0:
                for (prev_ip, next_ip, asn) in val['s']:
                    susp_ips[_ip][asn] += 1
        susp_ips_res = {_ip:max(susp_ips[_ip].keys(), key=lambda x:susp_ips[_ip][x]) for _ip in susp_ips.keys()}
        #print('{}'.format(susp_ips_res))
        # with open('test.json', 'w') as wf:
        #     json.dump(susp_ips_res, wf, indent=1)
        #return susp_ips_res

        UpdateASInBdrDb(susp_ips_res)
        CloseBdrMapItDb()
        InitBdrCache()
        #os.system('cp test1.db /mountdisk1/ana_c_d_incongruity/out_bdrmapit/bdrmapit_%s_%s.db' %(vp, date))

    def verify_sibling(self, filename, vp, date):
        susp_ips = {}
        ip_accur = {}
        with open(filename, 'r') as rf:
            susp_ips = json.load(rf)
        with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/' + vp + '/bdrmapit/stat1_ipaccur_' + vp + '.' + date + '.json', 'r') as rf:
            ip_accur = json.load(rf)
        c_ips = set()
        n_ips = set()
        for _ip in susp_ips.keys():
            if _ip in ip_accur['fail'] or _ip in ip_accur['other']:
                c_ips.add(_ip)
            else:
                n_ips.add(_ip)
        with open('test_conform', 'w') as wf:
            wf.write('\n'.join(list(c_ips)))
        with open('test_notconform', 'w') as wf:
            wf.write('\n'.join(list(n_ips)))

def isPrivateIP(_ip):
    e1, e2, _, __ = _ip.split('.')
    if e1 == '10':
        return True
    if e1 == '172' and int(e2) >= 16 and int(e2) < 32:
        return True
    if e1 == '192' and e2 == '168':
        return True
    #100.64.0.0/10
    if e1 == '100' and int(e2) >= 64 and int(e2) < 128:
        return True
    return False

def non_rel_links_num(links):
    end_link = defaultdict(set)
    for end1, end2 in links:
        end_link[end1].add((end1, end2))
        end_link[end2].add((end1, end2))
    res = defaultdict(set)
    while True:
        end = max(end_link.keys(), key=lambda d:len(end_link[d]))
        links = end_link[end]
        if not links:
            break
        for link in links:
            res[end].add(link)
            (end1, end2) = link
            if end == end1: end_link[end2].remove(link)
            else: end_link[end1].remove(link)
        del end_link[end]
    return len(res)

def group_net_by_neighbors(date, group_flag = False): 
    os.chdir('/home/slt/code/ana_c_d_incongruity')
    neighs = {'neighs': defaultdict(set), 'multihop_neighs': defaultdict(set)}
    neighs_filename = 'test_neighs.json'
    neighs_asn_filename = 'test_neighs_asn.json'
    GetIxpPfxDict_2(int(date[:4]), int(date[4:6]))
    bgp_path_info = {}
    InitPref2ASInfo('/mountdisk1/ana_c_d_incongruity/out_ip2as_data/%s.ip2as.prefixes' %date, bgp_path_info)
    GetIxpAsSet(date)
    cache = {}
    debug_pref = '113.29.103.'
    if not os.path.exists(neighs_filename):
    #if True:
        trace_dir = '/mountdisk3/traceroute_download_all/result/'
        for filename in os.listdir(trace_dir):
            if not filename.endswith(date):
                continue
            print(filename)
            with open(trace_dir + filename, 'r') as rf:
                for line in rf:
                    if line.startswith('#'):
                        continue
                    #(dst_ip, ip_trace) = line.strip('\n').split(':')
                    prev_elem = 'q'
                    multihop = False
                    for elem in line.split('\t')[13:]:
                        if elem.__contains__(';'): elem = 'q'
                        elif elem != 'q': 
                            elem = elem.split(',')[0]
                            if IsIxpIp(elem) or isPrivateIP(elem):
                                elem = 'q'
                            else:
                                cur_prefix = elem[:elem.rindex('.') + 1]
                                if cur_prefix not in cache.keys():
                                    cache[cur_prefix] = GetBGPPath_Or_OriASN(bgp_path_info, elem, 'get_orias_2')
                                if IsIxpAs(cache[cur_prefix]):
                                    elem = 'q'
                        if elem != 'q':
                            if prev_elem != 'q':
                                prev_prefix = prev_elem[:prev_elem.rindex('.') + 1]
                                cur_prefix = elem[:elem.rindex('.') + 1]
                                if prev_prefix != cur_prefix:                                    
                                    # if prev_prefix not in cache.keys():
                                    #     cache[prev_prefix] = GetBGPPath_Or_OriASN(bgp_path_info, prev_elem, 'get_orias_2')
                                    # if cur_prefix not in cache.keys():
                                    #     cache[cur_prefix] = GetBGPPath_Or_OriASN(bgp_path_info, elem, 'get_orias_2')
                                    link = (prev_elem, elem) if prev_elem < elem else (elem, prev_elem)
                                    if not multihop:
                                        if prev_prefix < cur_prefix: neighs['neighs'][(prev_prefix, cur_prefix)].add(link)
                                        else: neighs['neighs'][(cur_prefix, prev_prefix)].add(link)
                                    else:
                                        if prev_prefix < cur_prefix: neighs['multihop_neighs'][(prev_prefix, cur_prefix)].add(link)
                                        else: neighs['multihop_neighs'][(cur_prefix, prev_prefix)].add(link)
                            prev_elem = elem
                            multihop = False
                        else:
                            multihop = True                        

        record_neighs = defaultdict(lambda: defaultdict(list))
        record_neighs_asn = defaultdict(lambda: defaultdict(list))
        for prefix_pair, links in neighs['neighs'].items():
            (pref1, pref2) = prefix_pair
            # if pref1 == debug_pref or pref2 == debug_pref:
            #     print('{}:{}'.format(prefix_pair, links))
            if non_rel_links_num(links) >= 3:
                record_neighs[pref1][pref2] = list(links)
                record_neighs[pref2][pref1] = list(links)  
                # if pref1 not in cache.keys():
                #     cache[pref1] = GetBGPPath_Or_OriASN(bgp_path_info, pref1 + '1', 'get_orias_2')
                # if pref2 not in cache.keys():
                #     cache[pref2] = GetBGPPath_Or_OriASN(bgp_path_info, pref2 + '1', 'get_orias_2')
                record_neighs_asn[pref1 + '|' + cache[pref1]][cache[pref2]].append(pref2)
                record_neighs_asn[pref2 + '|' + cache[pref2]][cache[pref1]].append(pref1)
        for prefix_pair, links in neighs['multihop_neighs'].items():
            (pref1, pref2) = prefix_pair
            if pref1 not in record_neighs.keys():
                if non_rel_links_num(links) >= 3:
                    record_neighs[pref1][pref2] = list(links)
                    record_neighs_asn[pref1 + '|' + cache[pref1]][cache[pref2]].append(pref2)
            if pref2 not in record_neighs.keys():
                if non_rel_links_num(links) >= 3:
                    record_neighs[pref2][pref1] = list(links)
                    record_neighs_asn[pref2 + '|' + cache[pref2]][cache[pref1]].append(pref1)
        with open(neighs_asn_filename, 'w') as wf:
            json.dump(record_neighs_asn, wf, indent=1)
        with open(neighs_filename, 'w') as wf:
            json.dump(record_neighs, wf, indent=1)
    
    record_neighs = {}
    with open(neighs_filename, 'r') as rf:
        record_neighs = json.load(rf)
    if not cache:
        for pref in record_neighs.keys():
            cache[pref] = GetBGPPath_Or_OriASN(bgp_path_info, pref + '1', 'get_orias_2')
    change = {}
    for pref, val in record_neighs.items():
        votes = Counter()
        for neigh, links in val.items():
            #votes[cache[neigh]] += len(links)
            votes[cache[neigh]] += non_rel_links_num(links)
        orders = sorted(votes.items(), key=lambda x: x[1], reverse=True)
        max_asn = orders[0][0]
        ori_asn = cache[pref]
        #if ori_asn == '?' or ori_asn == '-1' and max_asn != ori_asn:
        if max_asn != ori_asn:
            change[pref] = [max_asn, ori_asn, orders]
    with open('test_change.json', 'w') as wf:
        json.dump(change, wf, indent=1)
    print(len(change))
    #print('{}'.format(stat))

def modi_outip2as(date):
    ip2as = {}    
    with open('/mountdisk1/ana_c_d_incongruity/out_ip2as_data/%s.ip2as.prefixes_ori' %date, 'r') as rf:
        with open('test_ip2as_cp', 'w') as wf:
            for line in rf:
                (pref, asn) = line.strip('\n').split(' ')
                if pref.__contains__(':'):
                    break
                ip2as[pref] = asn
                wf.write(line)
    change = {}
    with open('test_change.json', 'r') as rf:        
        change = json.load(rf)
    #change[pref] = [orders[0][0], cache[pref], orders]
    for pref, val in change.items():
        ip2as[pref + '0/24'] = val[0]
    #socket.ntohl(struct.unpack("I",socket.inet_aton(ip1))[0])
    # for pref in ip2as.keys():
    #     print(pref.split('/')[0])
    #     socket.ntohl(struct.unpack("I",socket.inet_aton(pref.split('/')[0]))[0])
    sort = sorted(ip2as.items(), key=lambda x: \
                                (socket.ntohl(struct.unpack("I",socket.inet_aton(x[0].split('/')[0]))[0]), \
                                 int(x[0].split('/')[1])))
    with open('/mountdisk1/ana_c_d_incongruity/out_ip2as_data/%s.ip2as.prefixes' %date, 'w') as wf:
        for pref, asn in sort:
            wf.write('{} {}\n'.format(pref, asn))

def PerTask(date, trace_filename):
    checksiblings = CheckSiblings(date)  
    checksiblings.rec_sibling(trace_filename)

if __name__ == '__main__':  
    if sys.argv[1] == '1':  #效果不好，废弃
        date = '20201215'
        group_net_by_neighbors(date)
        modi_outip2as(date)

    #os.system('bdrmapit json -c ')

    if sys.argv[1] == '2':
        trace_filename = '/mountdisk1/ana_c_d_incongruity/traceroute_data/result/trace_nrt-jp.20201215'
        (vp, date) = trace_filename.split('/')[-1].strip('trace_').split('.')
        checksiblings = CheckSiblings(date)  
        #print(checksiblings.bgp.rel(3356, 395174))    
        checksiblings.rec_sibling(trace_filename, '7660')
        #checksiblings.verify_sibling('test', 'nrt-jp', '20201215')

    if sys.argv[1] == '3':
        tracevp_bgpvp_info = {'ams-nl': '80.249.208.34', 'jfk-us': '198.32.160.61', 'sjc2-us': '64.71.137.241', 'syd-au': '45.127.172.46', 'zrh2-ch': '109.233.180.32', 'nrt-jp': '203.181.248.168'}
        tracevp_as_info = {'ams-nl': '1103', 'jfk-us': '6939', 'sjc2-us': '6939', 'syd-au': '7575', 'zrh2-ch': '34288', 'nrt-jp': '7660'}
        bgp_dir = '/mountdisk1/ana_c_d_incongruity/rib_data/bgpdata/'
        trace_dir = '/mountdisk1/ana_c_d_incongruity/traceroute_data/result/'
        task_list = []
        bdrmapit_filenames = os.listdir('/mountdisk1/ana_c_d_incongruity/out_bdrmapit/sxt_bdr/')
        for bdrmapit_filename in bdrmapit_filenames:
            if bdrmapit_filename.startswith('before'):
                continue
            (vp, date) = bdrmapit_filename[len('bdrmapit_'):-3].split('_')
            print(vp)
            print(date)
            if date[:4] != '2021':
                continue
            print(bdrmapit_filename)
            trace_filename = '/mountdisk1/ana_c_d_incongruity/traceroute_data/result/trace_%s.%s' %(vp, date)
            # if vp != 'ams-nl':
            #     continue         
            task = Process(target=PerTask, args=(date, trace_filename))
            if len(task_list) > 10:
                for t_task in task_list:
                    t_task.join()
                task_list.clear()
            task_list.append(task)
            task.start()     
        for task in task_list:  
            task.join()
            # checksiblings = CheckSiblings(date)  
            # checksiblings.rec_sibling(trace_filename)
