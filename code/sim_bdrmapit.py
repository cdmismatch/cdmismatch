
import os
import json
from collections import Counter

from compare_cd import InitBGPPathInfo, GetBGPPath_Or_OriASN, InitPref2ASInfo
from get_ip2as_from_bdrmapit import ConnectToBdrMapItDb, GetIp2ASFromBdrMapItDb, CloseBdrMapItDb, InitBdrCache, \
                                    ConstrBdrCache
from compare_cd import CompressTrace

def RecordBdrInfo(filename, out_dir):
    out_filename = out_dir + filename.split('/')[-1].strip('trace_') + '_bdrinfo.json'
    if os.path.exists(out_filename):
        with open(out_filename, 'r') as rf:
            bdr_info = json.load(rf)
            return bdr_info

    bdr_info = {}
    with open(filename, 'r') as rf:
        curlines = rf.readlines(100000)
        while curlines:
            for curline in curlines:
                (dst_ip, path) = curline.strip('\n').split(':')
                path_list = path.split(',')
                for i in range(0, len(path_list) - 1):
                    hop = path_list[i]
                    if hop not in bdr_info.keys():
                        bdr_info[hop] = [set(), set()]
                    bdr_info[hop][0].add(path_list[i + 1])
                last_hop = path_list[-1]
                if last_hop != dst_ip:
                    if last_hop not in bdr_info.keys():
                        bdr_info[last_hop] = [set(), set()]
                    bdr_info[last_hop][1].add(dst_ip)
            curlines = rf.readlines(100000)
    for (_key, val) in bdr_info.items():
        bdr_info[_key] = [list(val[0]), list(val[1])]
    with open(out_filename, 'w') as wf:
        json.dump(bdr_info, wf, indent=1)
    return bdr_info

def LastHopAnno(trace_filename, out_dir):    
    bdr_info = RecordBdrInfo(trace_filename, out_dir)
    
    (vp, date) = trace_filename.split('/')[-1].strip('trace_').split('.')        
    tracevp_bgpvp_info = {'ams-nl': '80.249.208.34', 'jfk-us': '198.32.160.61', 'sjc2-us': '64.71.137.241', 'syd-au': '198.32.176.177', 'per-au': '198.32.176.177', 'zrh2-ch': '109.233.180.32', 'nrt-jp': '203.181.248.168'}
    bgp_path_info = {}
    #InitBGPPathInfo('/mountdisk1/ana_c_d_incongruity/rib_data/bgpdata/bgp_%s_%s' %(tracevp_bgpvp_info[vp], date), bgp_path_info)
    InitPref2ASInfo('/mountdisk1/ana_c_d_incongruity/out_ip2as_data/' + date + '.ip2as.prefixes', bgp_path_info)

    last_ip_info = {}
    for (_key, val) in bdr_info.items():
        if val[0]: #有succ，不管
            continue
        last_ip_info[_key] = {}
        for dst_ip in val[1]:
            if dst_ip == '*':
                continue
            asns = GetBGPPath_Or_OriASN(bgp_path_info, dst_ip, 'get_orias_2')
            if asns == '?':
                continue
            if asns not in last_ip_info[_key].keys():
                last_ip_info[_key][asns] = 1
            else:
                last_ip_info[_key][asns] += 1
        if len(last_ip_info[_key]) == 0:
            del last_ip_info[_key]
    last_ip_anno = {}
    for (_key, asns_info) in last_ip_info.items():
        sort_list = sorted(asns_info.items(), key=lambda d:d[1], reverse=True)
        last_ip_anno[_key] = [sort_list[0][0], sort_list]
    with open(out_dir + vp + '.' + date + '_lastipanno.json', 'w') as wf:
        json.dump(last_ip_anno, wf, indent=1)

def CmpLastHopAnnoWithBdr(filename):
    last_ip_anno = {}
    with open(filename, 'r') as rf:
        last_ip_anno = json.load(rf)
    #nrt-jp.20180815_lastipanno.json
    (vp, date) = filename.split('/')[-1].split('_')[0].split('.') 
    ConnectToBdrMapItDb('/mountdisk1/ana_c_d_incongruity/out_bdrmapit/bdrmapit_%s_%s.db' %(vp, date))
    ConstrBdrCache()
    diff_ip_info = {}
    for (_ip, val) in last_ip_anno.items():
        anno_asn = val[0]
        bdr_asn = GetIp2ASFromBdrMapItDb(_ip)
        if anno_asn != bdr_asn:
            diff_ip_info[_ip] = [[anno_asn, bdr_asn], val[1]]
    CloseBdrMapItDb()
    with open(filename[:filename.rindex('.')] + '_diffbdr.json', 'w') as wf:
        json.dump(diff_ip_info, wf, indent=1)
    print(len(last_ip_anno))
    print(len(diff_ip_info))

    diff_ip_stat = {'succ': {}, 'fail': {}, 'other': {}, 'unmap': {}}
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/' + vp + '/bdrmapit/stat2_nodstip_ipaccur_' + vp + '.' + date + '.json', 'r') as rf:
        ip_stat = json.load(rf)
        for (_ip, val) in diff_ip_info.items():
            if val[0][1] == '':
                diff_ip_stat['unmap'][_ip] = val
            else:
                for (_type, t_val) in ip_stat.items():
                    if _ip in t_val.keys():
                        diff_ip_stat[_type][_ip] = val
                        break
    for (_type, val) in diff_ip_stat.items():
        print('%s: %d; ' %(_type, len(val.keys())), end='')
    print('')
    with open(filename[:filename.rindex('.')] + '_diffbdr_stat.json', 'w') as wf:
        json.dump(diff_ip_stat, wf, indent=1)

#只关注最后一跳是否带来新的正确的linkslt
def AnaLastLink(filename):
    (vp, date) = filename.split('_')[-1].split('.')
    ip2as_info = {}
    InitPref2ASInfo('/mountdisk1/ana_c_d_incongruity/out_ip2as_data/' + date + '.ip2as.prefixes', ip2as_info)
    last_ip_info = {}
    mid_ips = set()
    last_possi_visible_links = set()
    last_ab_bdr_links = set()
    with open(filename, 'r') as rf:
        curline_trace = rf.readline()
        while curline_trace:
            curline_bgp = rf.readline()
            curline_ip = rf.readline()
            if curline_ip.__contains__('203.181.249.39'):
                curline_trace = rf.readline()
                continue
            (dst_ip, trace) = curline_trace[1:-1].split(']')
            ip_list = curline_ip.strip('\n').split(' ')
            last_ip = ip_list[-1]
            for mid_hop in ip_list[:-1]:
                mid_ips.add(mid_hop)
            if dst_ip == last_ip:
                curline_trace = rf.readline()
                continue
            ori_trace_list = trace.split(' ')
            (trace_list, no_use, no_use_flag) = CompressTrace(ori_trace_list, ip_list)
            bgp_list = curline_bgp.strip('\n').strip('\t').split(' ')
            prev_asn = ''
            write_flag = False
            for i in range(len(bgp_list) - 1, -1, -1):
                bgp_hop = bgp_list[i]
                if bgp_hop in trace_list:
                    prev_asn = bgp_hop
                    if i < len(bgp_list) - 1:
                        last_possi_visible_links.add((bgp_list[i], bgp_list[i + 1]))
                        # wf.write(curline_trace + curline_bgp + curline_ip)
                        write_flag = True
                    #for j in range(i, len(bgp_list) - 1):
                    #    last_possi_visible_links.add((bgp_list[j], bgp_list[j + 1]))
                    # else:
                    #     last_possi_visible_links.add([bgp_hop, '$'])
                    trace_index = trace_list.index(bgp_hop)
                    for j in range(trace_index, len(trace_list) - 1):
                        if trace_list[j] != '*' and trace_list[j] != '?' and \
                            trace_list[j + 1] != '*' and trace_list[j + 1] != '?':
                            last_ab_bdr_links.add((trace_list[j], trace_list[j + 1]))
                            # if not write_flag:
                            #     wf.write(curline_trace + curline_bgp + curline_ip)
                            write_flag = True
                    break
            if last_ip not in last_ip_info.keys():
                ori_asn_last = GetBGPPath_Or_OriASN(ip2as_info, last_ip, 'get_orias_2')
                last_ip_info[last_ip] = [write_flag, (ori_asn_last, ori_trace_list[-1]), {prev_asn}, Counter(), []]
            if write_flag:
                last_ip_info[last_ip][0] = write_flag    
            last_ip_info[last_ip][2].add(prev_asn)
            last_ip_info[last_ip][3][GetBGPPath_Or_OriASN(ip2as_info, dst_ip, 'get_orias_2')] += 1
            last_ip_info[last_ip][4].append('\t' + curline_trace + '\t' + curline_bgp + '\t' + curline_ip)
            curline_trace = rf.readline()
    for mid_hop in mid_ips:
        if mid_hop in last_ip_info.keys():
            del last_ip_info[mid_hop]
    print(len(last_ab_bdr_links))
    print(len(last_possi_visible_links))    
    count = [0, 0]
    with open('test', 'w') as wf:
        for (last_ip, val) in last_ip_info.items():
            if not val[0]:
                count[0] += 1
                continue
            count[1] += 1
            wf.write(last_ip + '\n')
            wf.write('\t%s, %s, %s\n' %(val[1], tuple(val[2]), str(val[3])))
            wf.write(''.join(val[4]))
            wf.write('\n')
    print(count)

if __name__ == '__main__':
    # RecordBdrInfo('/mountdisk1/ana_c_d_incongruity/traceroute_data/result/trace_nrt-jp.20180815', \
    #              '/mountdisk1/ana_c_d_incongruity/sim_bdrmapit/')
    # LastHopAnno('/mountdisk1/ana_c_d_incongruity/traceroute_data/result/trace_nrt-jp.20180815', \
    #             '/mountdisk1/ana_c_d_incongruity/sim_bdrmapit/')
    #CmpLastHopAnnoWithBdr('/mountdisk1/ana_c_d_incongruity/sim_bdrmapit/nrt-jp.20180815_lastipanno.json')
    AnaLastLink('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/nrt-jp/bdrmapit/ab_nrt-jp.20180815')
