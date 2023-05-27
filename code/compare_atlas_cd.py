
from mimetypes import common_types
import os
import json
import glob
import struct, socket
import datetime
import pybgpstream
import subprocess
from collections import defaultdict, Counter
from traceutils.ixps import create_peeringdb
from traceutils.bgp.bgp import BGP
from get_ip2as_from_bdrmapit import ConnectToBdrMapItDb, GetIp2ASFromBdrMapItDb, CloseBdrMapItDb, InitBdrCache, \
                                    ConstrBdrCache
from utils_v2 import GetIxpPfxDict_2, IsIxpIp, ClearIxpPfxDict, SetCurMidarTableDate_2, GetAsOfIpByMi, ConnectToDb, \
                    InitMidarCache, CloseDb, GetSibRel_2, IsSib_2, GetOrgOfAS, GetPeerASNByIp, GetIxpAsSet, ClearIxpAsSet, \
                    IsIxpAs, GetClosestDate
from compare_cd import InitPref2ASInfo_2, MapTrace, CompressTrace, SelCloseBGP, CompareCD_PerTrace, pfx2as_info, StatIP, \
                        extract_ip2as_from_peeringdb, InitBGPPathInfo, GetBGPPath_Or_OriASN, StatIP_StripDstIP
from rect_bdrmapit import CheckSiblings
from multiprocessing import Process, Pool
import sys
sys.path.append('/home/slt/code/el_git/')
from roa_new import load_roa, check_pref_valid

map_method = 'ml_map'#'ori_bdr'#'sxt_bdr'#'coa_rib_based'#'rib_peeringdb'#'hoiho_s_bdr'#'midar' #'rib_based'
filter_snd_mm = True
g_parell_num = os.cpu_count()

def CompareAtlasCD(atlas_filename, bgp_filename, date, asn, filter_dsts=None):
    global map_method
    global pfx2as_info
    global filter_snd_mm

    if not os.path.exists('/mountdisk2/common_vps/%s/cmp_res' %date):
        os.mkdir('/mountdisk2/common_vps/%s/cmp_res/' %date)
    dir_suffix = '_filter_sndmm' if filter_snd_mm else ''
    if not os.path.exists('/mountdisk2/common_vps/%s/cmp_res/%s%s/' %(date, map_method, dir_suffix)):
        try:
            os.mkdir('/mountdisk2/common_vps/%s/cmp_res/%s%s/' %(date, map_method, dir_suffix))
        except Exception as e:
            pass
    bgp_path_info = {}
    with open(bgp_filename, 'r') as rf:
        bgp_path_info = json.load(rf)
    atlas_vp = atlas_filename.split('/')[-1].split('_')[-1]
    ip_accur_info = {}
    wf_no_bgp = open('/mountdisk2/common_vps/%s/cmp_res/%s%s/nobgp_%s' %(date, map_method, dir_suffix, atlas_vp), 'w')
    wf_mal_pos = open('/mountdisk2/common_vps/%s/cmp_res/%s%s/malpos_%s' %(date, map_method, dir_suffix, atlas_vp), 'w')
    wf_loop = open('/mountdisk2/common_vps/%s/cmp_res/%s%s/loop_%s' %(date, map_method, dir_suffix, atlas_vp), 'w')
    wf_ab = open('/mountdisk2/common_vps/%s/cmp_res/%s%s/mm_%s' %(date, map_method, dir_suffix, atlas_vp), 'w')
    wf_match = open('/mountdisk2/common_vps/%s/cmp_res/%s%s/match_%s' %(date, map_method, dir_suffix, atlas_vp), 'w')
    trace_links = set()
    bgp_links = set()
    trace_link_info = {}
    count_nobgp = count_malpos = count_loop = count_match = count_total = 0
    GetIxpAsSet(date)
    ori_asn_cache = {} #ori_asn_cache只在rib-based方法中有用，为了函数参数统一，其它方法中只是占位符
    if map_method.__contains__('bdr'):     
        if map_method == 'sxt_bdr':
            ConnectToBdrMapItDb('/mountdisk2/common_vps/%s/atlas/bdrmapit/sxt_bdr.db' %date)
        else:
            ConnectToBdrMapItDb('/mountdisk2/common_vps/%s/atlas/bdrmapit/ori_bdr.db' %date)
        ConstrBdrCache()
    elif map_method == 'midar':
        ConnectToDb()
        SetCurMidarTableDate_2(date)
        InitMidarCache()
    elif map_method == 'coa_rib_based' or map_method == 'rib_peeringdb':
        #InitPref2ASInfo('/mountdisk1/ana_c_d_incongruity/out_ip2as_data/' + date + '.ip2as.prefixes', pfx2as_info)
        InitPref2ASInfo_2('/mountdisk1/ana_c_d_incongruity/rib_data/rv_prefix2as/' + date + '.pfx2as', pfx2as_info)
    elif map_method == 'ml_map':
        with open('/mountdisk2/common_vps/%s/atlas/model_modified_ip_mappings_%s.json' %(date, date), 'r') as rf:
            ori_asn_cache = json.load(rf)
    if map_method == 'rib_peeringdb':# or map_method == 'sxt_bdr':
        extract_ip2as_from_peeringdb(date, ori_asn_cache) #加入peeringdb的内容
    # end_time = datetime.datetime.now()
    # print((end_time - start_time).seconds)
    
    # start_time = datetime.datetime.now()
    ip_remap = defaultdict(Counter)
    with open(atlas_filename, 'r') as rf:
        for curline in rf:
            (dst_ip, trace_ip_path) = curline.strip('\n').split(':')
            if filter_dsts and dst_ip in filter_dsts:
                continue
            if not trace_ip_path: #有的dst_ip没有trace
                continue
            debug_flag = False
            count_total += 1
            ip_list = trace_ip_path.split(',')
            if dst_ip not in bgp_path_info.keys():
                print('BGP path for %s not found' %dst_ip)
                continue
            bgps = list({path for val in bgp_path_info[dst_ip].values() for path in val})
            if not bgps:
                wf_no_bgp.write(curline)
                count_nobgp += 1
                continue
            if debug_flag: print('bgps: {}'.format(bgps))
            ori_trace_list = MapTrace(None, ip_list, map_method, ori_asn_cache)
            (trace_list, trace_to_ip_info, loop_flag) = CompressTrace(ori_trace_list, ip_list, asn)
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
    if map_method.__contains__('bdr'):
        CloseBdrMapItDb()
        InitBdrCache()
    elif map_method == 'midar':
        CloseDb()
    elif map_method.__contains__('rib'):
        pfx2as_info.clear()
    # end_time = datetime.datetime.now()
    # print((end_time - start_time).seconds)
    wf_no_bgp.close()
    wf_loop.close()
    wf_mal_pos.close()
    wf_ab.close()
    wf_match.close()
    with open('/mountdisk2/common_vps/%s/cmp_res/%s%s/mm_ip_remap_%s.json' %(date, map_method, dir_suffix, atlas_vp), 'w') as wf:
        json.dump(ip_remap, wf, indent = 1)
    #ip_accur_info[_ip] = [[0, 0.0, set()], [0, 0.0, set()], [0, 0.0, set()], [0, 0.0, set()], [0, 0.0, set()]]
    for _ip, val in ip_accur_info.items():
        for elem in val:
            elem[2] = list(elem[2])
    with open('/mountdisk2/common_vps/%s/cmp_res/%s%s/ipaccur_%s.json' %(date, map_method, dir_suffix, atlas_vp), 'w') as wf:
        json.dump(ip_accur_info, wf, indent=1)  # 写为多行
    count_cancompare = count_total - count_nobgp
    if count_cancompare > 0:
        with open('/mountdisk2/common_vps/%s/cmp_res/%s%s/trace_stat_%s' %(date, map_method, dir_suffix, atlas_vp), 'w') as wf:
            stat = {'total':count_total, 'nobgp':count_nobgp / count_total, \
                    'can_compare': count_cancompare, 'loop': count_loop / count_cancompare, \
                    'malpos': count_malpos / count_cancompare, 'match': count_match / count_cancompare}
            json.dump(stat, wf, indent=1)
    with open('/mountdisk2/common_vps/%s/cmp_res/%s%s/link_stat_%s' %(date, map_method, dir_suffix, atlas_vp), 'w') as wf:
        link_total = len(bgp_links)
        if link_total > 0:
            tp = len(trace_links & bgp_links) / link_total
            fp = len(trace_links.difference(bgp_links)) / link_total
            fn = len(bgp_links.difference(trace_links)) / link_total
            wf.write('total bgp link num: %d\n' %link_total)
            wf.write('total trace link num: %d\n' %len(trace_links))
            wf.write('tp: %.2f, fp: %.2f, fn: %.2f\n' %(tp, fp, fn))
    with open('/mountdisk2/common_vps/%s/cmp_res/%s%s/falselink_%s' %(date, map_method, dir_suffix, atlas_vp), 'w') as wf:
        ip_link_info = defaultdict(set)
        for link in trace_links.difference(bgp_links):
            for (ip1, ip2) in trace_link_info[link]:
                ip_link_info[ip1].add((0, link))
                ip_link_info[ip2].add((1, link))
        sorted_ips = dict(sorted(ip_link_info.items(), key=lambda d:len(d[1]), reverse=True))
        for (_ip, links) in sorted_ips.items():
            wf.write('{}[{}]\n'.format(_ip, links))
    with open('/mountdisk2/common_vps/%s/cmp_res/%s%s/missinglink_%s' %(date, map_method, dir_suffix, atlas_vp), 'w') as wf:
        wf.write('\n'.join(list(bgp_links.difference(trace_links))))
    # print('here:', end='')
    # print(ip_accur_info['77.67.76.34'])
    #os.system('cat /mountdisk2/common_vps/%s/cmp_res/%s/trace_stat_%s' %(date, map_method, atlas_vp))
    ip_accur_info.clear()
    bgp_path_info.clear()
    ClearIxpAsSet()
    
    #StatIP2(map_method, date)

    return count_match / count_cancompare if count_cancompare else 0

def MapTraceroutePath_OneDay(filename, date, wfn, db_fn): #use ori_bdr
    global map_method
    if os.path.exists(wfn) and os.path.getsize(wfn) > 0:
        return
    print(filename)
    wf_mapped = open(wfn, 'w')
    GetIxpAsSet(date)
    ConnectToBdrMapItDb(db_fn)
    ConstrBdrCache()
    pfx2as_info = {}
    InitPref2ASInfo_2('/mountdisk1/ana_c_d_incongruity/rib_data/rv_prefix2as/' + date + '.pfx2as', pfx2as_info)
    with open(filename, 'r') as rf:
        for curline in rf:
            (dst_ip, trace_ip_path) = curline.strip('\n').split(':')
            if not trace_ip_path: #有的dst_ip没有trace
                continue
            dst_asn = GetBGPPath_Or_OriASN(pfx2as_info, dst_ip, 'get_orias_2')
            ip_list = trace_ip_path.split(',')
            ori_trace_list = MapTrace(None, ip_list, map_method, None)            
            wf_mapped.write('[%s]%s\n' %(dst_ip, ' '.join(ori_trace_list)))
            wf_mapped.write('\t\t%s\n' %dst_asn)
            wf_mapped.write(' '.join(ip_list) + '\n')
    CloseBdrMapItDb()
    InitBdrCache()
    wf_mapped.close()
    ClearIxpAsSet()

def get_asn_org(date):
    filenames = glob.glob('/mountdisk1/ana_c_d_incongruity/as_org_data/*')
    dates = [filename.split('/')[-1][:8] for filename in filenames]
    closest_date = GetClosestDate(date[:6] + '01', dates)
    tmp_dict = dict()
    as_orgs = dict()
    with open('/mountdisk1/ana_c_d_incongruity/as_org_data/' + closest_date + '.as-org2info.txt', 'r', encoding='utf-8') as rf:
        for curline in rf:
            if curline.startswith('#'):
                continue
            elems = curline.split('|')
            if len(elems) == 5: #format: org_id|changed|name|country|source            
                tmp_dict[elems[0]] = elems[2]
            elif len(elems) == 6:   #format: aut|changed|aut_name|org_id|opaque_id|source
                as_orgs[elems[0]] = elems[3]
            else:
                print("Format error. Exit")
    for key in as_orgs.keys():
        if as_orgs[key] in tmp_dict.keys():
            as_orgs[key] = tmp_dict[as_orgs[key]]
    return as_orgs

def get_asn_tier(date):
    filenames = glob.glob('/mountdisk1/ana_c_d_incongruity/as_rel_cc_data/*ppdc-ases.txt')
    dates = [filename.split('/')[-1][:8] for filename in filenames]
    closest_date = GetClosestDate(date[:6] + '01', dates)
    as_tier = {}
    with open('/mountdisk1/ana_c_d_incongruity/as_rel_cc_data/' + closest_date + '.ppdc-ases.txt', 'r', encoding='utf-8') as rf:
        for line in rf:
            if line.startswith('#'):
                continue
            elems = line.split(' ')
            num = len(elems) - 1
            if num > 50:
                as_tier[elems[0]] = 'large-ISP'
            elif num > 5:
                as_tier[elems[0]] = 'small-ISP'
            else:
                as_tier[elems[0]] = 'stub'
    return as_tier

def get_asn_info(date):
    asns = ['205112', '14061', '13030', '7575', '34927', '52863', '6830', '58308', '1403', '58057', '57695', '8218', '59414', '8426', '3399', '3491']
    as_orgs = get_asn_org(date)
    as_tier = get_asn_tier(date)
    asn_info = {asn:[as_orgs.get(asn), as_tier.get(asn)] for asn in asns}
    print(asn_info)

def get_res(date):
    filenames = glob.glob('/mountdisk2/common_vps/%s/cmp_res/%s/trace_stat_*' %(date, map_method))
    for filename in filenames:
        with open(filename, 'r') as rf:
            for line in rf:
                if line[:5] == 'match':
                    res = float(line.strip('\n').split(':')[-1])
                    if res > 0:
                        print(res)

def StatIP2(cur_map_method, date):
    global filter_snd_mm
    if filter_snd_mm:
        if os.path.exists('/mountdisk2/common_vps/%s/cmp_res/%s_filter_sndmm' %(date, cur_map_method)):
            os.chdir('/mountdisk2/common_vps/%s/cmp_res/%s_filter_sndmm' %(date, cur_map_method))
        else:
            return
    else:
        os.chdir('/mountdisk2/common_vps/%s/cmp_res/%s' %(date, cur_map_method))
    spec_filenames = glob.glob('ipaccur_*.json')
    for f in spec_filenames:
        spec_filename = f[len('ipaccur_'):-5]
        # if os.path.exists('ip_stat_nodstip_' + spec_filename):
        #     continue
        StatIP(spec_filename)
        StatIP_StripDstIP(spec_filename)

def stat_trace_match_base(): #atlas+ark, all time, for rib, rib+peeringdb, bdrmapit, sxt_bdr
    cur_date = '20220301'
    matches = defaultdict(Counter)
    totals = defaultdict(Counter)
    mappings = ['coa_rib_based', 'rib_peeringdb', 'ori_bdr', 'ml_map']
    # for year in range(2018, 2023):
    #     for month in range(1, 13):
    #         date = str(year) + str(month).zfill(2) + '15'
    #         if date > cur_date:
    #             break
    #         # if date < '20220415':
    #         #     continue
    #         print(date)
    #         for mapping in mappings:
    #             filenames = glob.glob('/mountdisk2/common_vps/%s/cmp_res/%s/trace_stat_*' %(date, mapping))
    #             for filename in filenames:
    #                 with open(filename, 'r') as rf:
    #                     stat = json.load(rf)
    #                     n = stat['can_compare']
    #                     if n > 80:
    #                         matches[mapping][date[:6]] += int(stat['match'] * n)
    #                         totals[mapping][date[:6]] += n
    for arkvp in ['sjc2-us','ams-nl','syd-au','sao-br','nrt-jp']:
        for mapping in mappings:
            filenames = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/%s_filtersndmm/trace_stat_*' %(arkvp, mapping))
            for filename in filenames:
                with open(filename, 'r') as rf:
                    date = filename.split('/')[-1].split('.')[-1]
                    for line in rf:
                        if line.startswith('can'):
                            n = int(line.split(',')[0].split(':')[-1][1:])
                            if n < 100:
                                break
                            totals[mapping][date[:6]] += n
                        if line.startswith('match'):
                            matches[mapping][date[:6]] += int(float(line.strip('\n').split(':')[-1]) * n)
    res = defaultdict(list)
    for mapping, val in matches.items():
        for date, subval in val.items():
            res[mapping].append(subval / totals[mapping][date])
    with open('/mountdisk2/common_vps/cmp_trace_match_base1.json', 'w') as wf:
        json.dump(res, wf, indent=1)

def stat_trace_match_midar(): #ark, all time, for rib+peeringdb, bdrmapit, midar, hoiho
    matches = defaultdict(Counter)
    totals = defaultdict(Counter)
    midar_dates = ['201803', '201901', '201904', '202001', '202008', '202103']
    for arkvp in ['sjc2-us','ams-nl','syd-au','sao-br','nrt-jp']:
        for mapping in ['midar', 'hoiho_s_bdr', 'ori_bdr']: #
            tmp_mapping = mapping
            filenames = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/%s_filtersndmm/trace_stat_*' %(arkvp, tmp_mapping))
            for filename in filenames:
                date = filename.split('/')[-1].split('.')[-1]
                # if mapping == 'midar_exact':                    
                #     if date[:6] not in midar_dates:
                #         continue
                with open(filename, 'r') as rf:
                    for line in rf:
                        if line.startswith('can_compare'):
                            compare_num = int(line.split(',')[0].split(':')[-1][1:])
                            if compare_num < 100:
                                print(arkvp + '.' + date)
                                break
                            totals[mapping][date[:6]] += compare_num
                        if line.startswith('match'):
                            matches[mapping][date[:6]] += int(float(line.strip('\n').split(':')[-1]) * compare_num)
    res = defaultdict(list)
    for mapping, val in matches.items():
        for date, subval in val.items():
            res[mapping].append(subval / totals[mapping][date])
    with open('/mountdisk2/common_vps/cmp_trace_match_midar.json', 'w') as wf:
        json.dump(res, wf, indent=1)

def stat_ip_base(type): #atlas+ark, all time, for rib, rib+peeringdb, bdrmapit, sxt_bdr
    cur_date = '20220301'
    filenames = []
    #mappings = ['coa_rib_based', 'rib_peeringdb', 'sxt_bdr', 'ori_bdr']
    mappings = ['coa_rib_based', 'rib_peeringdb', 'ori_bdr', 'ml_map']
    # for year in range(2018, 2023):
    #     for month in range(1, 13):
    #         date = str(year) + str(month).zfill(2) + '15'
    #         if date > cur_date:
    #             break
    #         # if date < '20220415':
    #         #     continue
    #         print(date)
    #         for mapping in mappings:
    #             tmp_filenames = glob.glob('/mountdisk2/common_vps/%s/cmp_res/%s/ip_stat_*' %(date, mapping))
    #             filenames = filenames + tmp_filenames
    for arkvp in ['sjc2-us','ams-nl','syd-au','jfk-us','sao-br']:
        for mapping in mappings:
            tmp_filenames = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/%s_filtersndmm/ip_stat_*' %(arkvp, mapping))
            filenames = filenames + tmp_filenames
    
    matches = defaultdict(Counter)
    totals = defaultdict(Counter)
    for filename in filenames:
        date, mapping = None, None
        if filename.__contains__('common_vps'):
            _, _, _, date, _, mapping, _ = filename.split('/')
        else:
            _, _, _, _, _, mapping, last = filename.split('/')
            date = last.split('/')[-1].split('.')[-1]
        # if mapping != 'rib_peeringdb' or date[:6] != '201901':
        #     continue
        if mapping.__contains__('_filtersndmm'):
            mapping = mapping[:len(mapping)-len('_filtersndmm')]
        with open(filename, 'r') as rf:
            for line in rf:
                if line.startswith(type):
                    if not line.__contains__('('):
                        break
                    n = int(line.split('(')[-1].split(')')[0])
                    if n < 80:
                        break
                    matches[mapping][date[:6]] += n
                if line.startswith('total'):
                    t = int(line.strip('\n').split(':')[-1][1:])
                    totals[mapping][date[:6]] += t
            # print(filename)
            # print(matches[mapping][date[:6]])
            # print(totals[mapping][date[:6]])
    
    res = defaultdict(defaultdict)
    for mapping, val in matches.items():
        for date, subval in val.items():
            res[mapping][date] = subval / totals[mapping][date]
    s = {}
    for mapping, val in res.items():
        s[mapping] = sorted(val.items(), key=lambda x:x[0])
    with open('/mountdisk2/common_vps/cmp_ip_%s_base1.json' %type, 'w') as wf:
        json.dump(s, wf, indent=1)

def stat_ip_midar(type): #atlas+ark, all time, for rib, rib+peeringdb, bdrmapit, sxt_bdr
    cur_date = '20210801'
    filenames = []
    matches = defaultdict(Counter)
    totals = defaultdict(Counter)
    midar_dates = ['201803', '201901', '201904', '202001', '202008', '202103']
    for arkvp in ['sjc2-us','ams-nl','syd-au','jfk-us','sao-br','nrt-jp']:
        for mapping in ['rib_peeringdb', 'ori_bdr', 'midar', 'hoiho_s_bdr', 'midar_exact']:
            tmp_mapping = mapping
            if mapping == 'midar_exact': tmp_mapping = 'midar'
            filenames = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/%s/ip_stat_nodstip_*' %(arkvp, tmp_mapping))
            for filename in filenames:
                date = filename.split('/')[-1].split('.')[-1]
                if mapping == 'midar_exact':                    
                    if date[:6] not in midar_dates:
                        continue
                with open(filename, 'r') as rf:
                    for line in rf:
                        if line.startswith(type):
                            if not line.__contains__('('):
                                break
                            n = int(line.split('(')[-1].split(')')[0])
                            if n < 80:
                                break
                            matches[mapping][date[:6]] += n
                        if line.startswith('total'):
                            t = int(line.strip('\n').split(':')[-1][1:])
                            totals[mapping][date[:6]] += t
    
    res = defaultdict(list)
    for mapping, val in matches.items():
        for date, subval in val.items():
            res[mapping].append(subval / totals[mapping][date])
    with open('/mountdisk2/common_vps/cmp_ip_%s_midar.json' %type, 'w') as wf:
        json.dump(res, wf, indent=1)

def stat_ip_snmp(type): #atlas+ark, 20210415, for rib+peeringdb, ori_bdr, snmp_bdr
    date = '20210415'
    filenames = []
    mappings = ['rib_peeringdb', 'snmp_bdr', 'ori_bdr']
    for mapping in mappings:
        tmp_filenames = glob.glob('/mountdisk2/common_vps/%s/cmp_res/%s/ip_stat_*' %(date, mapping))
        filenames = filenames + tmp_filenames
    for arkvp in ['sjc2-us','ams-nl','syd-au','jfk-us','sao-br']:
        for mapping in mappings:
            tmp_filenames = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/%s/ip_stat_*%s*' %(arkvp, mapping, date[:6]))
            filenames = filenames + tmp_filenames
    
    matches = Counter()
    totals = Counter()
    for filename in filenames:
        date, mapping = None, None
        if filename.__contains__('common_vps'):
            _, _, _, date, _, mapping, _ = filename.split('/')
        else:
            _, _, _, _, _, mapping, last = filename.split('/')
            date = last.split('/')[-1].split('.')[-1]
        # if mapping != 'rib_peeringdb' or date[:6] != '201901':
        #     continue
        with open(filename, 'r') as rf:
            for line in rf:
                if line.startswith(type):
                    if not line.__contains__('('):
                        break
                    n = int(line.split('(')[-1].split(')')[0])
                    if n < 80:
                        break
                    matches[mapping] += n
                if line.startswith('total'):
                    t = int(line.strip('\n').split(':')[-1][1:])
                    totals[mapping] += t
            # print(filename)
            # print(matches[mapping][date[:6]])
            # print(totals[mapping][date[:6]])
    
    res = {}
    for mapping, val in matches.items():
        res[mapping] = val / totals[mapping]
    with open('/mountdisk2/common_vps/cmp_ip_%s_snmp.json' %type, 'w') as wf:
        json.dump(res, wf, indent=1)

def CheckSndMM():
    stat = defaultdict(defaultdict)
    debug_info = defaultdict(lambda:defaultdict(list))
    for vp in  ['ams-nl', 'sjc2-us', 'syd-au', 'sao-br', 'nrt-jp']:
        tmp_stat = defaultdict(lambda:defaultdict(set))
        for method in ['rib_peeringdb', 'coa_rib_based', 'ori_bdr']:
            fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/%s/mm_%s.*' %(vp, method, vp))
            for fn in fns:
                print(fn)
                vp, date = fn.split('_')[-1].split('.')
                debug_info[vp][date].append(method)
                with open(fn, 'r') as rf:            
                    lines = [rf.readline() for _ in range(3)]
                    while lines[0]:
                        dst_ip, tmp = lines[0][1:].split(']')
                        trace_list = tmp.strip('\n').split(' ')
                        bgp_list = lines[1].strip('\t').strip('\n').split(' ')
                        if len(bgp_list) == 1:
                            lines = [rf.readline() for _ in range(3)]
                            continue    
                        for i in range(len(trace_list)):
                            if trace_list[i] != trace_list[0] and trace_list[i] != '*' and trace_list[i] != '?' and trace_list[i] != '-1':
                                if trace_list[i] != bgp_list[1]:
                                    tmp_stat[date][method].add(dst_ip)
                                break
                        lines = [rf.readline() for _ in range(3)]
        for date, val in tmp_stat.items():
            mid = None
            fst = True
            for cur_set in val.values():
                if fst:
                    mid = {elem for elem in cur_set}
                    fst = False
                else:
                    mid = mid & cur_set
            stat[date][vp] = list(mid)
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/filter_dsts', 'w') as wf:
        json.dump(stat, wf, indent=1)
    #print(debug_info)
    
def CheckSndMM_ForAtlas():
    dirs = glob.glob('/mountdisk2/common_vps/20*15')
    dates = [tmp.split('/')[-1] for tmp in dirs]
    for date in dates:
        stat = {}
        for method in ['rib_peeringdb', 'coa_rib_based', 'ori_bdr']:
            fns = glob.glob('/mountdisk2/common_vps/%s/cmp_res/%s/mm_*' %(date, method))
            tmp_stat = defaultdict(lambda:defaultdict(set))            
            for fn in fns:
                elems = fn.split('/')
                if not elems[-1][3].isdigit():
                    continue                
                print(fn)
                date = elems[3]
                vp = elems[-1].split('_')[-1]
                with open(fn, 'r') as rf:            
                    lines = [rf.readline() for _ in range(3)]
                    while lines[0]:
                        dst_ip, tmp = lines[0][1:].split(']')
                        trace_list = tmp.strip('\n').split(' ')
                        bgp_list = lines[1].strip('\t').strip('\n').split(' ')
                        if len(bgp_list) == 1:
                            lines = [rf.readline() for _ in range(3)]
                            continue    
                        for i in range(len(trace_list)):
                            if trace_list[i] != trace_list[0] and trace_list[i] != '*' and trace_list[i] != '?' and trace_list[i] != '-1':
                                if trace_list[i] != bgp_list[1]:
                                    tmp_stat[vp][method].add(dst_ip)
                                break
                        lines = [rf.readline() for _ in range(3)]
        for vp, val in tmp_stat.items():
            mid = None
            fst = True
            for cur_set in val.values():
                if fst:
                    mid = {elem for elem in cur_set}
                    fst = False
                else:
                    mid = mid & cur_set
            stat[vp] = list(mid)
        with open('/mountdisk2/common_vps/%s/atlas/filter_dsts_atlas_%s' %(date, date), 'w') as wf:
            json.dump(stat, wf, indent=1)
            
def CountTracesAfterFilterSndMm():
    dirs = glob.glob('/mountdisk2/common_vps/20*15')
    dates = [tmp.split('/')[-1] for tmp in dirs]
    for date in dates:
        filtered = {}
        with open('/mountdisk2/common_vps/%s/atlas/filter_dsts_atlas_%s' %(date, date), 'r') as rf:
            filtered = json.load(rf)
        fns = glob.glob('/mountdisk2/common_vps/%s/atlas/trace_*' %date)
        t = 0
        for fn in fns:
            with open(fn, 'r') as rf:
                t += len(rf.readlines())
        print(date + ':' + str(t-sum([len(val) for val in filtered.values()])))

def CollectTracesAfterFilterSndMm():
    dirs = glob.glob('/mountdisk2/common_vps/20*15')
    dates = [tmp.split('/')[-1] for tmp in dirs]
    for date in dates:
        filtered = {}
        with open('/mountdisk2/common_vps/%s/atlas/filter_dsts_atlas_%s' %(date, date), 'r') as rf:
            filtered = json.load(rf)
        fns = glob.glob('/mountdisk2/common_vps/%s/atlas/trace_*' %date)
        t = 0
        with open('/mountdisk2/common_vps/%s/atlas/filtered_trace_%s' %(date,date), 'w') as wf:
            for fn in fns:
                vp = fn.split('/')[-1][len('trace_'):]
                with open(fn, 'r') as rf:
                    for line in rf:
                        dst_ip = line.split(':')[0]
                        if vp not in filtered.keys() or dst_ip not in filtered[vp]:
                            wf.write(line)                        
        
# def GetOriBdrMapAfterFilteredSndMm():
#     dirs = glob.glob('/mountdisk2/common_vps/20*15')
#     dates = [tmp.split('/')[-1] for tmp in dirs]
#     for date in dates:
#         filtered = {}
#         with open('/mountdisk2/common_vps/%s/atlas/filter_dsts_atlas_%s' %(date, date), 'r') as rf:
#             filtered = json.load(rf)
#         fns = glob.glob()
    
def RecalculateTraceMetricsForAtlas():
    new_trace_match = defaultdict(lambda:defaultdict(defaultdict))
    filter_dsts = {}
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/filter_dsts_atlas', 'r') as rf:
        filter_dsts = json.load(rf)
    dirs = glob.glob('/mountdisk2/common_vps/20*')
    dates = [tmp.split('/')[-1] for tmp in dirs]
    for date in dates:
        if date[:6] > '202202':
            continue
        for method in ['rib_peeringdb', 'coa_rib_based', 'ori_bdr']:
            fns = glob.glob('/mountdisk2/common_vps/%s/cmp_res/%s/mm_*' %(date, method))
            for fn in fns:
                elems = fn.split('/')
                if not elems[-1][3].isdigit():
                    continue                
                print(fn)
                to_filter = 0
                date = elems[3]
                vp = elems[-1].split('_')[-1]
                cur_filter_dsts = set(filter_dsts[date][vp]) if date in filter_dsts and vp in filter_dsts[date] else set()
                with open(fn, 'r') as rf:            
                    lines = [rf.readline() for _ in range(3)]
                    while lines[0]:
                        dst_ip, tmp = lines[0][1:].split(']')
                        if dst_ip in cur_filter_dsts:
                            to_filter += 1
                        lines = [rf.readline() for _ in range(3)]
                with open('/mountdisk2/common_vps/%s/cmp_res/%s/trace_stat_%s' %(date, method, vp), 'r') as rf:
                    tmp_data = json.load(rf)
                    if tmp_data['total'] > to_filter:
                        new_trace_match[date][vp][method] = (tmp_data['total'] * tmp_data['match']) / (tmp_data['total'] - to_filter)
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/new_trace_match_rate_atlas', 'w') as wf:
        json.dump(new_trace_match, wf, indent=1)

def stat_trace_match_base_v2(): #atlas+ark, all time, for rib, rib+peeringdb, bdrmapit, sxt_bdr
    mappings = ['coa_rib_based', 'rib_peeringdb', 'ori_bdr', 'ml_map']
    res = defaultdict(list)
    # data_atlas = {}
    # with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/new_trace_match_rate_atlas', 'r') as rf:
    #     data_atlas = json.load(rf)
    # for val in data_atlas.values():
    #     for subval in val.values():
    #         for mapping, subsubval in subval.items():
    #             res[mapping].append(subsubval)
    for arkvp in ['sjc2-us','ams-nl','syd-au','sao-br','nrt-jp']:
        for mapping in mappings:
            filenames = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/%s_filtersndmm/trace_stat_*' %(arkvp, mapping))
            for filename in filenames:
                with open(filename, 'r') as rf:
                    for line in rf:
                        if line.startswith('can'):
                            n = int(line.split(',')[0].split(':')[-1][1:])
                            # if n < 100:
                            #     break
                        if line.startswith('match'):
                            res[mapping].append(float(line.strip('\n').split(':')[-1]))
    with open('/mountdisk2/common_vps/cmp_trace_match_base2.json', 'w') as wf:
        json.dump(res, wf, indent=1)


def stat_trace_match_base_atlas():
    mappings = ['coa_rib_based', 'rib_peeringdb', 'ori_bdr', 'ml_map']
    res = defaultdict(list)
    fns = glob.glob('/mountdisk2/common_vps/20*15')
    dates = [fn.split('/')[-1] for fn in fns]
    for date in sorted(dates):
        for mapping in mappings:
            filenames = glob.glob('/mountdisk2/common_vps/%s/cmp_res/%s_filter_sndmm/trace_stat_*' %(date, mapping))
            t = 0
            m = 0.0
            for filename in filenames:
                with open(filename, 'r') as rf:
                    tmp = 0
                    for line in rf:
                        if line.__contains__('can'):
                            tmp = int(line.split(',')[0].split(':')[-1].strip(' '))
                            if tmp == 0:
                                break
                            t += tmp
                            # if n < 100:
                            #     break
                        if line.__contains__('match'):
                            m += (tmp * float(line.strip('\n').split(':')[-1].strip(' ')))
            if t > 0:
                res[mapping].append(m / t)
    with open('/mountdisk2/common_vps/cmp_trace_match_atlas.json', 'w') as wf:
        json.dump(res, wf, indent=1)

def stat_ip_base_v2(): #atlas+ark, all time, for rib, rib+peeringdb, bdrmapit, sxt_bdr
    res = defaultdict(lambda:defaultdict(list))
    status = ['succ', 'fail', 'unmap']
    for mapping in ['coa_rib_based', 'rib_peeringdb', 'ori_bdr', 'ml_map']:
        for arkvp in ['sjc2-us','ams-nl','syd-au','nrt-jp','sao-br']:        
            fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/%s_filtersndmm/ip_stat_nodstip_*' %(arkvp, mapping))
            for fn in fns:
                tmp = {}
                total = 0
                with open(fn, 'r') as rf:
                    for line in rf:
                        elems = line.split(':')
                        if elems[0] in status:
                            a = elems[1].split('(')[-1].split(')')[0]
                            if a.isdigit():
                                tmp[elems[0]] = int(a)
                            else:
                                tmp[elems[0]] = 0
                        elif elems[0] == 'total_count':
                            total = int(elems[1].strip('\n').strip(' '))
                for tmp_type in status:
                    res[tmp_type][mapping].append(tmp[tmp_type]/total)
    for tmp_type, val in res.items():
        with open('/mountdisk2/common_vps/cmp_ip_%s_base2.json' %tmp_type, 'w') as wf:
            json.dump(val, wf, indent=1)

def stat_ip_base_v2_midar(): #atlas+ark, all time, for rib, rib+peeringdb, bdrmapit, sxt_bdr
    res = defaultdict(lambda:defaultdict(list))
    status = ['succ', 'fail', 'unmap']
    for mapping in ['midar', 'ori_bdr', 'hoiho_s_bdr']:
        for arkvp in ['sjc2-us','ams-nl','syd-au','nrt-jp','sao-br']:        
            fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/%s_filtersndmm/ip_stat_nodstip_*' %(arkvp, mapping))
            for fn in sorted(fns):
                tmp = {}
                total = 0
                with open(fn, 'r') as rf:
                    for line in rf:
                        elems = line.split(':')
                        if elems[0] in status:
                            a = elems[1].split('(')[-1].split(')')[0]
                            if a.isdigit():
                                tmp[elems[0]] = int(a)
                            else:
                                tmp[elems[0]] = 0
                        elif elems[0] == 'total_count':
                            total = int(elems[1].strip('\n').strip(' '))
                for tmp_type in status:
                    res[tmp_type][mapping].append(tmp[tmp_type]/total)
    for tmp_type, val in res.items():
        with open('/mountdisk2/common_vps/cmp_ip_%s_midar.json' %tmp_type, 'w') as wf:
            json.dump(val, wf, indent=1)
            
            
def stat_ip_base_v2_snmp(): #atlas+ark, all time, for rib, rib+peeringdb, bdrmapit, sxt_bdr
    res = defaultdict(lambda:defaultdict(list))
    status = ['succ', 'fail', 'unmap']
    
    for arkvp in ['sjc2-us','ams-nl','syd-au','nrt-jp','sao-br']:        
        fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/snmp_bdr_filtersndmm/ip_stat_nodstip_%s.*' %(arkvp, arkvp))
        dates = [fn.split('.')[-1] for fn in fns]
        for date in dates:
            for mapping in ['snmp_bdr', 'ori_bdr']:
                tmp = {}
                total = 0
                with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/%s_filtersndmm/ip_stat_nodstip_%s.%s' %(arkvp, mapping, arkvp, date), 'r') as rf:
                    for line in rf:
                        elems = line.split(':')
                        if elems[0] in status:
                            a = elems[1].split('(')[-1].split(')')[0]
                            if a.isdigit():
                                tmp[elems[0]] = int(a)
                            else:
                                tmp[elems[0]] = 0
                        elif elems[0] == 'total_count':
                            total = int(elems[1].strip('\n').strip(' '))
                for tmp_type in status:
                    res[tmp_type][mapping].append(tmp[tmp_type]/total)
    for tmp_type, val in res.items():
        with open('/mountdisk2/common_vps/cmp_ip_%s_snmp.json' %tmp_type, 'w') as wf:
            json.dump(val, wf, indent=1)
            
def stat_trace_match_snmp(): #atlas+ark, all time, for rib, rib+peeringdb, bdrmapit, sxt_bdr
    mappings = ['snmp_bdr', 'ori_bdr']
    res = defaultdict(list)
    # data_atlas = {}
    # with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/new_trace_match_rate_atlas', 'r') as rf:
    #     data_atlas = json.load(rf)
    # for val in data_atlas.values():
    #     for subval in val.values():
    #         for mapping, subsubval in subval.items():
    #             res[mapping].append(subsubval)
    for arkvp in ['sjc2-us','ams-nl','syd-au','sao-br','nrt-jp']:
        filenames = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/snmp_bdr_filtersndmm/trace_stat_%s.2021*' %(arkvp, arkvp))
        dates = [fn.split('.')[-1] for fn in filenames]
        for mapping in mappings:
            for date in dates:
                with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/%s_filtersndmm/trace_stat_%s.%s' %(arkvp, mapping, arkvp, date), 'r') as rf:
                    for line in rf:
                        if line.startswith('can'):
                            n = int(line.split(',')[0].split(':')[-1][1:])
                            # if n < 100:
                            #     break
                        if line.startswith('match'):
                            res[mapping].append(float(line.strip('\n').split(':')[-1]))
    with open('/mountdisk2/common_vps/cmp_trace_match_snmp.json', 'w') as wf:
        json.dump(res, wf, indent=1)
            
def stat_ip_base_v2_midar_sort_by_date(): #atlas+ark, all time, for rib, rib+peeringdb, bdrmapit, sxt_bdr
    res = defaultdict(lambda:defaultdict(Counter))
    c = defaultdict(lambda:defaultdict(Counter))
    status = ['succ', 'fail', 'unmap']
    for mapping in ['midar', 'ori_bdr']:
        for arkvp in ['sjc2-us','ams-nl','syd-au','nrt-jp','sao-br']:        
            fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/%s_filtersndmm/ip_stat_nodstip_*' %(arkvp, mapping))
            for fn in sorted(fns):
                date = fn.split('.')[-1]
                tmp = {}
                total = 0
                with open(fn, 'r') as rf:
                    for line in rf:
                        elems = line.split(':')
                        if elems[0] in status:
                            a = elems[1].split('(')[-1].split(')')[0]
                            if a.isdigit():
                                tmp[elems[0]] = int(a)
                            else:
                                tmp[elems[0]] = 0
                        elif elems[0] == 'total_count':
                            total = int(elems[1].strip('\n').strip(' '))
                for tmp_type in status:
                    res[tmp_type][mapping][date] += (tmp[tmp_type]/total)
                    c[tmp_type][mapping][date] += 1
    for tmp_type, val in res.items():
        for mapping, subval in val.items():
            for date, subsubval in subval.items():
                res[tmp_type][mapping][date] = subsubval / c[tmp_type][mapping][date]
    for tmp_type, val in res.items():
        with open('/mountdisk2/common_vps/cmp_ip_%s_midar_date.json' %tmp_type, 'w') as wf:
            json.dump(val, wf, indent=1)            
            
def stat_ip_base_atlas(): #atlas+ark, all time, for rib, rib+peeringdb, bdrmapit, sxt_bdr
    res = defaultdict(lambda:defaultdict(list))
    status = ['succ', 'fail', 'unmap']
    fns = glob.glob('/mountdisk2/common_vps/20*15')
    dates = [fn.split('/')[-1] for fn in fns]
    for date in dates:
        for mapping in ['coa_rib_based', 'rib_peeringdb', 'ori_bdr', 'ml_map']:
            filenames = glob.glob('/mountdisk2/common_vps/%s/cmp_res/%s_filter_sndmm/ip_stat_nodst*' %(date, mapping))
            t = 0
            m = Counter()
            for filename in filenames:
                with open(filename, 'r') as rf:
                    for line in rf:
                        for cur_s in status:
                            if line.startswith(cur_s):
                                #print(line)
                                if line.__contains__('('):
                                    m[cur_s] += int(line.split('(')[-1].split(')')[0])
                        if line.__contains__('total'):
                            t += int(line.split(':')[-1].strip('\n').strip(' '))
            if t > 0:
                for cur_s in status:
                    res[cur_s][mapping].append(m[cur_s]/t)
    for cur_s in status:
        with open('/mountdisk2/common_vps/cmp_ip_%s_atlas.json' %cur_s, 'w') as wf:
            json.dump(res[cur_s], wf, indent=1)

def ana_rm_reach_originAS():
    fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/*/ori_bdr_filtersndmm/ana_compare_res/continuous_mm.*')
    same_dst = 0
    extra_dst = 0
    diff_dst = 0
    total = 0
    longer = 0
    shorter = 0
    same_len = 0
    for fn in fns:
        _, vp, date = fn.split('.')
        with open(fn, 'r') as rf:
            lines = [rf.readline() for _ in range(3)]
            while lines[0]:
                dst_ip, tmp = lines[0][1:].split(']')
                trace_list = tmp.strip('\n').split(' ')
                compress_trace_list = [trace_list[i] for i in range(len(trace_list)) if trace_list[i] != '*' and trace_list[i] != '?' and trace_list[i] != '-1' and (i==0 or trace_list[i] != trace_list[i-1])]
                bgp_list = lines[1].strip('\t').strip('\n').split(' ')                
                origin_asn = bgp_list[-1]
                if origin_asn == compress_trace_list[-1]:
                    same_dst += 1
                    if len(compress_trace_list) > len(bgp_list):
                        longer += 1
                    elif len(compress_trace_list) < len(bgp_list):
                        shorter += 1
                    else:
                        same_len += 1
                elif origin_asn in compress_trace_list:
                    extra_dst += 1
                else:
                    diff_dst += 1
                total += 1
                lines = [rf.readline() for _ in range(3)]
    print(same_dst)
    print(extra_dst)
    print(diff_dst)
    print(total)
    print('longer: %f' %(longer/same_dst))
    print('shorter: %f' %(shorter/same_dst))
    print('same: %f' %(same_len/same_dst))
    
def find_possi_hh():
    fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/*/ml_map_filtersndmm/ana_compare_res/continuous_mm.*.202202*')
    ra = 0
    c = 0
    t = 0
    t1 = 0
    sure_hh = defaultdict(list)
    possi_hh = defaultdict(list)
    checksiblings = CheckSiblings('20220215')
    roa_pref_asn = load_roa('20220215')
    tracevp_bgpvp_info = {'ams-nl': '80.249.208.34', 'jfk-us': '198.32.160.61', 'sjc2-us': '64.71.137.241', 'syd-au': '45.127.172.46', 'nrt-jp': '203.181.248.168', 'sao-br': '187.16.217.17'}
    tracevp_as_info = {'ams-nl': '1103', 'jfk-us': '6939', 'sjc2-us': '6939', 'syd-au': '7575', 'nrt-jp': '7660', 'sao-br': '22548'}
    pref_asns = {}
    with open('/mountdisk2/pref_oriasns/20220215', 'r') as rf:
        pref_asns = json.load(rf)
    for fn in fns:
        print(fn)
        vp = fn.split('.')[-2]
        bgp_filename = '/mountdisk1/ana_c_d_incongruity/rib_data/bgpdata/bgp_' + tracevp_bgpvp_info[vp] + '_20220215'
        bgp_path_info = {}
        InitBGPPathInfo(bgp_filename, bgp_path_info)
        with open(fn, 'r') as rf:
            lines = [rf.readline() for _ in range(3)]
            while lines[0]:
                dst_ip, tmp = lines[0][1:].split(']')
                prefix = GetBGPPath_Or_OriASN(bgp_path_info, dst_ip, 'get_prefix')
                trace_list = tmp.strip('\n').split(' ')
                ind = trace_list.index(tracevp_as_info[vp])
                trace_list = trace_list[ind:]
                compress_trace_list = [trace_list[i] for i in range(len(trace_list)) if trace_list[i] != '*' and trace_list[i] != '?' and trace_list[i] != '-1' and (i==0 or trace_list[i] != trace_list[i-1])]
                bgp_list = lines[1].strip('\t').strip('\n').split(' ')                
                origin_asn = bgp_list[-1]
                dst_asn = compress_trace_list[-1]
                if len(bgp_list) > 1 and len(compress_trace_list) > 1 and bgp_list[1] != compress_trace_list[1]:
                    lines = [rf.readline() for _ in range(3)]
                    continue
                if origin_asn in compress_trace_list:
                    if origin_asn != dst_asn:
                        ra += 1
                    lines = [rf.readline() for _ in range(3)]
                    continue      
                if compress_trace_list[-1] in bgp_list:
                    lines = [rf.readline() for _ in range(3)]
                    continue
                if checksiblings.check_sibling(origin_asn, dst_asn):       
                    lines = [rf.readline() for _ in range(3)]
                    continue
                t1 += 1
                preflen = 32
                ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton(dst_ip))[0])
                pref_ip, preflen = prefix.split('/')
                preflen = int(preflen)
                ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton(pref_ip))[0])
                for mask_len in range(preflen, 7, -1):
                    if mask_len < 32:
                        mask = ~(1 << (31 - mask_len))
                        ip_int = ip_int & mask
                    pref = str(socket.inet_ntoa(struct.pack('I',socket.htonl(ip_int)))) + '/' + str(mask_len)
                    print(pref)
                    print(origin_asn)
                    flag = check_pref_valid(pref, [origin_asn], roa_pref_asn)
                    print(flag[0])
                    if flag[0] == 'valid':
                        flag1 = check_pref_valid(pref, [dst_asn], roa_pref_asn)
                        print(flag1[0])
                        if flag1[0] == 'invalid-asn':
                            lines[1] = lines[1].strip('\t')
                            lines[1] = '[' + pref + ']' + lines[1]
                            ip_int1 = socket.ntohl(struct.unpack("I",socket.inet_aton(dst_ip))[0])
                            found = False
                            for masklen1 in range(31, preflen, -1):
                                mask1 = ~(1 << (31 - masklen1))
                                ip_int1 = ip_int1 & mask1
                                pref1 = str(socket.inet_ntoa(struct.pack('I',socket.htonl(ip_int1)))) + '/' + str(masklen1)
                                if pref1 in pref_asns.keys():
                                    if dst_asn in pref_asns[pref1]:
                                        sure_hh[dst_ip] += lines
                                        found = True
                                        break
                            if not found:
                                possi_hh[dst_ip] += lines
                            c += 1
                        t += 1
                        break
                lines = [rf.readline() for _ in range(3)]
    print(ra)
    print(c)
    print(t)
    print(len(possi_hh))
    print(len(sure_hh))
    print(t1)
    with open('possi_hh_from_mm', 'w') as wf:
        json.dump(possi_hh, wf, indent=1)
    with open('sure_hh_from_mm', 'w') as wf:
        json.dump(sure_hh, wf, indent=1)
            
def find_possi_hh_filter_export_policy():
    bgp = BGP('/mountdisk1/ana_c_d_incongruity/as_rel_cc_data/20220201.as-rel3.txt', '/mountdisk1/ana_c_d_incongruity/as_rel_cc_data/20220201.ppdc-ases.txt')
    lines = []
    with open('/home/slt/code/ana_c_d_incongruity/sure_hh_from_mm', 'r') as rf:
        data = json.load(rf)
        for val in data.values():
            for k in range(0, len(val), 3):
                trace_list = val[k].strip('\n').split(']')[-1].split(' ')
                bgp_list = val[k + 1].strip('\n').split(']')[-1].split(' ')
                trace_list = trace_list[trace_list.index(bgp_list[0]):]
                trace_list = [trace_list[i] for i in range(len(trace_list)) if trace_list[i] != '*' and trace_list[i] != '?' and trace_list[i] != '-1' and (i==0 or trace_list[i] != trace_list[i-1])]
                cur, nxt_c_hop, nxt_d_hop = None, None, None
                for i in range(len(trace_list)):
                    if trace_list[i+1] not in bgp_list:
                        cur = int(trace_list[i])
                        nxt_c_hop = int(bgp_list[bgp_list.index(trace_list[i])+1])
                        nxt_d_hop = int(trace_list[i+1])
                        break
                if bgp.reltype(cur, nxt_c_hop) == bgp.reltype(cur, nxt_d_hop):
                    lines = lines + val[k:k+3]
    print(len(lines) / 3)
    with open('sure_hh_from_mm_1', 'w') as wf:
        wf.write(''.join(lines))

def get_one_day_bgp_links(date, collector):
    print('%s begin' %collector)
    links = set()
    try:
        stream = pybgpstream.BGPStream(
            from_time = '%s-%s-%s 00:00:00' %(date[:4], date[4:6], date[6:8]), 
            until_time = '%s-%s-%s 02:00:00' %(date[:4], date[4:6], date[6:8]), 
            record_type="ribs",
            collectors=[collector])
        for elem in stream:
            pathlist = elem.fields['as-path'].split(' ')
            for i in range(1, len(pathlist)):
                if pathlist[i][0] != '{' and pathlist[i] != pathlist[i - 1]:      
                    links.add(pathlist[i - 1]+','+pathlist[i])
                    links.add(pathlist[i]+','+pathlist[i - 1])
        print('get ribs done %s' %collector)
    except Exception as e:
        print(e)
        return
    print('download bgp end %s' %collector)
    with open('/mountdisk1/ana_c_d_incongruity/rib_data/all_collectors_one_date_bgpdata/links_%s_%s' %(date, collector), 'w') as wf:
        wf.write('\n'.join(links))
        
def cal_bgp_links():
    date = '20220415'
    collectors = ['route-views2', 'route-views.nwax', 'route-views2.saopaulo', 'route-views.ny', 'route-views3', 'route-views.perth', 'route-views4', 'route-views.peru', 'route-views5', 'route-views.phoix', 'route-views6', 'route-views.rio', 'route-views.amsix', 'route-views.saopaulo', 'route-views.bdix', 'route-views.sfmix', 'route-views.bknix', 'route-views.sg', 'route-views.chicago', 'route-views.siex', 'route-views.chile', 'route-views.soxrs', 'route-views.eqix', 'route-views.sydney', 'route-views.flix', 'route-views.telxatl', 'route-views.fortaleza', 'route-views.uaeix', 'route-views.gixa', 'route-views.wide', 'route-views.gorex', 'route-views.isc', 'route-views.jinx', 'route-views.kixp', 'route-views.linx', 'route-views.mwix', 'route-views.napafrica', 'rrc08', 'rrc09', 'rrc10', 'rrc11', 'rrc12', 'rrc13', 'rrc14', 'rrc15', 'rrc16', 'rrc17', 'rrc18', 'rrc19', 'rrc20', 'rrc21', 'rrc22', 'rrc00', 'rrc23', 'rrc01', 'rrc24', 'rrc03', 'rrc25', 'rrc04', 'rrc26', 'rrc05', 'rrc06', 'rrc07']
    # pool = Pool(processes=len(collectors))
    # paras = []
    # for collector in collectors:
    #     paras.append((date, collector))
    # pool.starmap(get_one_day_bgp_links, paras)
    # pool.close()
    # pool.join()
    res = set()
    fns = glob.glob('/mountdisk1/ana_c_d_incongruity/rib_data/all_collectors_one_date_bgpdata/links_%s_*' %date)
    for fn in fns:
        with open(fn, 'r') as rf:
            for elem in rf.readlines():
                if not elem.__contains__('{'):
                    res.add(elem.strip('\n'))
    with open('/mountdisk1/ana_c_d_incongruity/rib_data/all_collectors_one_date_bgpdata/links_%s_all' %date, 'w') as wf:
        wf.write('\n'.join(res))
    print(len(res) / 2)
    
def CollectTraceLinks():
    fns = ['/mountdisk2/common_vps/20220415/atlas/mapped_20220415'] + glob.glob('/mountdisk3/traceroute_download_all/202204/map_*')
    links = set()
    for fn in fns:
        print(fn)
        with open(fn, 'r') as rf:
            lines = [rf.readline() for _ in range(3)]
            while lines[0]:
                trace_list = lines[0].split(']')[-1].strip('\n').split(' ')
                trace_list = [elem for elem in trace_list if elem != '*' and elem != '?']
                trace_list = [trace_list[i] for i in range(len(trace_list)) if i==0 or trace_list[i] != trace_list[i-1]]
                for i in range(1, len(trace_list)):
                    links.add(trace_list[i-1]+','+trace_list[i])   
                    links.add(trace_list[i]+','+trace_list[i-1])
                lines = [rf.readline() for _ in range(3)]
    with open('/mountdisk3/traceroute_download_all/202204/links', 'w') as wf:
        wf.write('\n'.join(links))
    print(len(links) / 2)
    
def CheckBGPLinksInTraceLinks():
    date = '20220415'
    bgp_links = None
    bgp_nodes = set()
    with open('/mountdisk1/ana_c_d_incongruity/rib_data/all_collectors_one_date_bgpdata/links_%s_all' %date, 'r') as rf:
        bgp_links = set(rf.readlines())        
        for link in bgp_links:
            asn1, asn2 = link.strip('\n').split(',')
            bgp_nodes.add(asn1)
            bgp_nodes.add(asn2)  
    print(len(bgp_nodes))
    trace_links = None
    trace_nodes = set()
    with open('/mountdisk3/traceroute_download_all/%s/links' %date[:6], 'r') as rf:
        trace_links = set(rf.readlines())
        for link in trace_links:
            asn1, asn2 = link.strip('\n').split(',')
            trace_nodes.add(asn1)
            trace_nodes.add(asn2)  
    print(len(trace_nodes))
    print(len(bgp_nodes.difference(trace_nodes)))          
    concerns = set()
    for elem in bgp_links:
        if elem not in trace_links:
            if len(elem.strip('\n').split(',')) != 2:
                print(elem)
            asn1, asn2 = elem.strip('\n').split(',')
            if asn1 in trace_nodes and asn2 in trace_nodes:
                concerns.add(elem.strip('\n'))
    print(len(concerns) / 2)

def ConstruProbeToPref():    
    atlas_asn_info = {}
    with open('/mountdisk2/common_vps/probe_asn.json', 'r') as rf:
        atlas_asn_info = json.load(rf)
    probe_prefs = defaultdict(lambda:defaultdict(set))
    fns = glob.glob('/mountdisk1/ana_c_d_incongruity/rib_data/all_collectors_one_date_bgpdata/for_bogus_links_pref_paths_20230513_*')
    links = set()
    for fn in fns:
        with open(fn, 'r') as rf:
            data = json.load(rf)
            for pref, val in data.items():
                for elem in val:
                    path_list = elem.split(' ')
                    if len(path_list) < 3:
                        continue
                    links.add(path_list[-2]+' '+path_list[-1])
                    for cur in path_list[-3:-1*len(path_list)-1:-1]:
                        if cur in atlas_asn_info.keys():
                            probe_prefs[cur][pref].add(' '.join(path_list[path_list.index(cur):]))
                            break
    rec = defaultdict(defaultdict)
    for cur, val in probe_prefs.items():
        for pref, subval in val.items():
            rec[cur][pref] = list(subval)
    with open('possi_bogus_links_probe_info2', 'w') as wf:
        json.dump(rec, wf, indent=1)
    print(sum([len(val) for val in probe_prefs.values()]))
    print(len(links))
    
    
def get_liveip_v3():
    asns = set()
    with open('/home/slt/code/ana_c_d_incongruity/possi_bogus_links_to_traceroute', 'r') as rf:
        for line in rf:
            asns.add(line.strip('\n').split(' ')[-1])
    asn_prefs = defaultdict(set)
    with open('/mountdisk1/ana_c_d_incongruity/rib_data/rv_prefix2as/20230515.pfx2as', 'r') as rf:
        for line in rf:
            pref, _len, asn = line.strip('\n').split('\t')
            if asn in asns:
                asn_prefs[asn].add(pref+'/'+str(_len))
    fns = glob.glob('/home/slt/code/ana_c_d_incongruity/liveips_liveips_*')
    done_prefs = [fn.split('_')[-1] for fn in fns]
    for asn, prefs in asn_prefs.items():
        find = False
        for pref in prefs:
            key = pref.split('/')[0]
            if key in done_prefs:
                os.system('cp /home/slt/code/ana_c_d_incongruity/liveips_liveips_%s /home/slt/code/ana_c_d_incongruity/liveips_%s_ip_%s' %(key, asn, key))
                find = True
                break
            
        if find:
            continue
        print(asn)
        for pref in prefs:
            print(pref)
            try:
                if len(glob.glob('/home/slt/code/ana_c_d_incongruity/liveips_%s_ip_*' %asn)) >= 1:
                    break
                outfn = '/home/slt/code/ana_c_d_incongruity/liveips_%s_ip_%s' %(asn, pref.split('/')[0])
                if os.path.exists(outfn) and os.path.getsize(outfn) > 0:
                    continue
                #sudo zmap --probe-module=icmp_echoscan 182.61.200.0/24 -N 5 -B 10M -o test
                subprocess.check_output("echo %s | sudo -S zmap --probe-module=icmp_echoscan -G 30:b0:37:d3:7e:f9 -N 1 -B 10M -o %s %s" %('_sxt3021', outfn, pref), shell=True)
            except subprocess.CalledProcessError:
                print("Unavailable prefix %s!" % pref)

def rerecord_tasks():
    rec = set()
    fns = glob.glob('/home/slt/code/ana_c_d_incongruity/liveips_*')
    asn_pref = {}
    for fn in fns:
        if os.path.getsize(fn) == 0:
            continue
        _, asn, _, pref = fn.split('/')[-1].split('_')
        with open(fn, 'r') as rf:
            ip = rf.readlines()[0].strip('\n')
            asn_pref[asn] = ip
    ip_probes = defaultdict(set)
    with open('/home/slt/code/ana_c_d_incongruity/possi_bogus_links_to_traceroute', 'r') as rf:
        for line in rf:
            asn1, asn2 = line.strip('\n').split(' ')
            if asn2 in asn_pref.keys():
                ip_probes[asn_pref[asn2]].add(asn1)
    rec = {ip:list(probes) for ip, probes in ip_probes.items()}
    with open('/home/slt/code/ana_c_d_incongruity/possi_bogus_links_probe_info_liveips', 'w') as wf:
        json.dump(rec, wf, indent=1)
    
def main_func():
    #get_liveip_v3()
    rerecord_tasks()
    #CheckBGPLinksInTraceLinks()
    #CollectTraceLinks()
    #cal_bgp_links()
    #find_possi_bogus_links()
    #FilterPossiBogusLinks()
    #CheckSerialHijackers()
    #GetPrfsOfLinkVictim()
    # ConstruProbeToPref()
    # return
    # fns = glob.glob('/mountdisk2/common_vps/20*15')
    # dates = [fn.split('/')[-1] for fn in fns]
    # paras = []
    # for date in dates:
    #     paras.append(('/mountdisk2/common_vps/%s/atlas/traces_all' %date, date, '/mountdisk2/common_vps/%s/atlas/mapped_%s' %(date, date), '/mountdisk2/common_vps/%s/atlas/bdrmapit/ori_bdr.db' %date))
    # pool = Pool(processes=len(paras))
    # print('task num: %d' %len(paras))
    # pool.starmap(MapTraceroutePath_OneDay, paras)
    # pool.close()
    # pool.join()   
    # return
    # fns = glob.glob('/mountdisk3/traceroute_download_all/202204/trace_*')
    # for fn in fns:
    #     wfn = fn.replace('trace_', 'map_')
    #     vp, date = fn.split('_')[-1].split('.')
    #     db_fn = '/mountdisk1/ana_c_d_incongruity/out_bdrmapit/ori_bdr/bdrmapit_%s_%s.db' %(vp, date)
    #     MapTraceroutePath_OneDay(fn, date, wfn, db_fn)
    #cal_bgp_links()
    #find_possi_hh_filter_export_policy()
    #detect_possi_hh()
    #get_spec_prefs()
    #find_possi_hh()
    #ana_rm_reach_originAS()
    #RecalculateTraceMetricsForAtlas()
    #stat_trace_match_base_v2()
    #stat_trace_match_base_atlas()
    #stat_trace_match_base()
    #stat_trace_match_midar()
    #stat_ip_base_v2_midar()
    #stat_ip_base_v2_midar_sort_by_date()
    #stat_trace_match_snmp()
    #stat_ip_base_v2_snmp()
    #stat_ip_base_v2()
    #stat_ip_base_atlas()
    #stat_ip_base('succ')
    # stat_ip_base('fail')
    #stat_ip_base('unmap')
    #stat_ip_midar('succ')
    #stat_ip_snmp('succ')
    #CheckSndMM_ForAtlas()
    #CountTracesAfterFilterSndMm()
    # CollectTracesAfterFilterSndMm()
    return
    global map_method
    cur_date = '20221001'
    paras = []
    map_methods = ['rib_peeringdb', 'ori_bdr', 'ml_map'] #'coa_rib_based'
    for year in range(2019, 2023):
        for month in range(1, 13):
            date = str(year) + str(month).zfill(2) + '15'
            for m in map_methods:
                #paras.append((m, date))
                StatIP2(m, date)
            continue
            # if date != '20190115':
            #     continue
            if map_method == 'snmp_bdr' and date != '20210415':
                continue
            if date > cur_date:
                break            
            filter_dsts = set()
            if filter_snd_mm:
                with open('/mountdisk2/common_vps/%s/atlas/filter_dsts_atlas_%s' %(date, date), 'r') as rf:
                    data = json.load(rf)
                    for val in data.values():
                        filter_dsts = filter_dsts | set(val)
            print(date)
            common_vps = {} #common_vps[asn][atlas_vp][bgp_vp] = dist
            with open('/mountdisk2/common_vps/%s/common_vp_%s.json' %(date, date), 'r') as rf:
                common_vps = json.load(rf)
            for asn, val in common_vps.items():
                for atlas_vp, subval in val.items():
                    bgp_vp = min(subval.keys(), key=lambda x:subval[x])
                    atlas_filename = '/mountdisk2/common_vps/%s/atlas/trace_%s' %(date, atlas_vp)
                    bgp_filename = '/mountdisk2/common_vps/%s/bgp/bgp_%s.json' %(date, bgp_vp)
                    if not os.path.exists(atlas_filename):
                        print('err! {} not exists!'.format(atlas_filename))
                        continue
                    if not os.path.exists(bgp_filename):
                        print('err! {} not exists!'.format(bgp_filename))
                        continue
                    paras.append((atlas_filename, bgp_filename, date, asn, filter_dsts))
                    #CompareAtlasCD(atlas_filename, bgp_filename, date, asn)
    #print(match_rate)    
    if paras:
        pool = Pool(processes=g_parell_num)
        print('task num: %d' %len(paras))
        #pool.starmap(CompareAtlasCD, paras)
        pool.starmap(StatIP2, paras)
        pool.close()
        pool.join()   

def get_one_day_bgp(date, prefs, collector):
    print('%s begin' %collector)
    data = defaultdict(set)
    try:
        stream = pybgpstream.BGPStream(
            from_time = '%s-%s-%s 00:00:00' %(date[:4], date[4:6], date[6:8]), 
            until_time = '%s-%s-%s 02:00:00' %(date[:4], date[4:6], date[6:8]), 
            record_type="ribs",
            collectors=[collector],
            #filter="peer 1103")
            filter="prefix more %s" %' '.join(prefs))
            #filter="prefix exact %s" %pref)
        for elem in stream:
            path = elem.fields['as-path']
            if path.__contains__('{'):
                continue
            data[elem.fields['prefix']].add(path.split(' ')[-1])
        print('get ribs done %s' %collector)
        for i in range(0, 24, 2):
            print('updates in time: %d' %i)
            stream = pybgpstream.BGPStream(
                from_time = '%s-%s-%s %s:00:00' %(date[:4], date[4:6], date[6:8], str(i).zfill(2)), 
                until_time = '%s-%s-%s %s:59:59' %(date[:4], date[4:6], date[6:8], str(i+1).zfill(2)), 
                record_type="updates",
                collectors=[collector],
                #filter="elemtype announcements and peer 1103")
                filter="elemtype announcements and prefix more %s" %' '.join(prefs))
                #filter="prefix exact %s" %pref)
            for elem in stream:
                path = elem.fields['as-path']
                if path.__contains__('{'):
                    continue
                data[elem.fields['prefix']].add(path.split(' ')[-1])
    except Exception as e:
        print(e)
        return
    print('download bgp end %s' %collector)
    #with open('bgp_1103_%s', 'w') as wf:
    rec = {key:list(val) for key, val in data.items()}
    with open('spec_pref_asns_%s' %collector, 'w') as wf:
        json.dump(rec, wf, indent=1)

def get_spec_prefs():
    concerned = set()
    with open('/home/slt/code/ana_c_d_incongruity/possi_hh_from_mm', 'r') as rf:
        data = json.load(rf)
        for val in data.values():
            for i in range(0, len(val), 3):
                pref = val[i+1].split(']')[0][1:]
                concerned.add(pref)
    collectors = ['route-views2', 'route-views.nwax', 'route-views2.saopaulo', 'route-views.ny', 'route-views3', 'route-views.perth', 'route-views4', 'route-views.peru', 'route-views5', 'route-views.phoix', 'route-views6', 'route-views.rio', 'route-views.amsix', 'route-views.saopaulo', 'route-views.bdix', 'route-views.sfmix', 'route-views.bknix', 'route-views.sg', 'route-views.chicago', 'route-views.siex', 'route-views.chile', 'route-views.soxrs', 'route-views.eqix', 'route-views.sydney', 'route-views.flix', 'route-views.telxatl', 'route-views.fortaleza', 'route-views.uaeix', 'route-views.gixa', 'route-views.wide', 'route-views.gorex', 'route-views.isc', 'route-views.jinx', 'route-views.kixp', 'route-views.linx', 'route-views.mwix', 'route-views.napafrica', 'rrc08', 'rrc09', 'rrc10', 'rrc11', 'rrc12', 'rrc13', 'rrc14', 'rrc15', 'rrc16', 'rrc17', 'rrc18', 'rrc19', 'rrc20', 'rrc21', 'rrc22', 'rrc00', 'rrc23', 'rrc01', 'rrc24', 'rrc03', 'rrc25', 'rrc04', 'rrc26', 'rrc05', 'rrc06', 'rrc07']
    pool = Pool(processes=len(collectors))
    paras = []
    for collector in collectors:
        paras.append(('20220215', concerned, collector))
    pool.starmap(get_one_day_bgp, paras)
    pool.close()
    pool.join()

def detect_possi_hh():
    pref_asns = defaultdict(set)
    fns = glob.glob('/home/slt/code/ana_c_d_incongruity/spec_pref_asns_*')
    for fn in fns:
        with open(fn, 'r') as rf:
            cur_pref_asns = json.load(rf)
            for pref, asns in cur_pref_asns.items():
                pref_asns[pref] = pref_asns[pref] | set(asns)
    hh = defaultdict(list)
    with open('/home/slt/code/ana_c_d_incongruity/possi_hh_from_mm', 'r') as rf:
        data = json.load(rf)
        for dst_ip, val in data.items():
            for i in range(0, len(val), 3):
                dst_asn = val[i].split(' ')[-1].strip('\n')
                pref = val[i+1].split(']')[0][1:]
                preflen = int(pref.split('/')[1])
                ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton(dst_ip))[0])
                for mask_len in range(32, preflen, -1):
                    if mask_len < 32:
                        mask = ~(1 << (31 - mask_len))
                        ip_int = ip_int & mask
                    tmp_pref = str(socket.inet_ntoa(struct.pack('I',socket.htonl(ip_int)))) + '/' + str(mask_len)
                    if tmp_pref in pref_asns.keys():
                        tmp_asns = pref_asns[tmp_pref]
                        if dst_asn in tmp_asns:
                            hh[dst_ip] = hh[dst_ip] + val[i:i+3]
    with open('reassure_hh', 'w') as wf:
        json.dump(hh, wf, indent=1)
    print(len(hh))

def find_possi_bogus_links():
    tracevp_as_info = {'ams-nl': '1103', 'jfk-us': '6939', 'sjc2-us': '6939', 'syd-au': '7575', 'nrt-jp': '7660', 'sao-br': '22548'}
    # fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/*/ml_map_filtersndmm/match_*.20220215')# + \
    #     #glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/*/ml_map_filtersndmm/mm_*.20220215')
    # all_trace_links = set()
    # for fn in fns:
    #     vp = fn.split('_')[-1].split('.')[0]
    #     with open(fn, 'r') as rf:
    #         lines = [rf.readline() for _ in range(3)]
    #         while lines[0]:
    #             dst_ip, tmp = lines[0][1:].split(']')
    #             trace_list = tmp.strip('\n').split(' ')
    #             if tracevp_as_info[vp] not in trace_list:
    #                 lines = [rf.readline() for _ in range(3)]
    #                 continue
    #             ind = trace_list.index(tracevp_as_info[vp])
    #             trace_list = trace_list[ind:]
    #             trace_list = [elem for elem in trace_list if elem != '*' and elem != '?' and elem != '-1']
    #             trace_list = [trace_list[i] for i in range(len(trace_list)) if (i==0 or trace_list[i] != trace_list[i-1])]
    #             for i in range(1, len(trace_list)):
    #                 all_trace_links.add(trace_list[i-1] + ' ' + trace_list[i])
    #             lines = [rf.readline() for _ in range(3)]
    
    all_trace_links = set()
    with open('/mountdisk3/traceroute_download_all/202204/links', 'r') as rf:
        for line in rf:
            all_trace_links.add(line.strip('\n').replace(',', ' '))
    fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/*/ml_map_filtersndmm/ana_compare_res/continuous_mm.*.202202*')
    susp_bgp_links = defaultdict(lambda:defaultdict(set))
    for fn in fns:
        print(fn)
        vp = fn.split('.')[-2]
        with open(fn, 'r') as rf:
            lines = [rf.readline() for _ in range(3)]
            while lines[0]:
                dst_ip, tmp = lines[0][1:].split(']')
                trace_list = tmp.strip('\n').split(' ')
                ind = trace_list.index(tracevp_as_info[vp])
                trace_list = trace_list[ind:]
                trace_list = [elem for elem in trace_list if elem != '*' and elem != '?' and elem != '-1']
                trace_list = [trace_list[i] for i in range(len(trace_list)) if (i==0 or trace_list[i] != trace_list[i-1])]
                bgp_list = lines[1].strip('\t').strip('\n').split(' ')                
                trace_links = set()
                for i in range(1, len(trace_list)):
                    trace_links.add(trace_list[i-1] + ' ' + trace_list[i])
                for i in range(1, len(bgp_list)):
                    bgp_link = bgp_list[i-1] + ' ' + bgp_list[i]
                    if bgp_link not in trace_links and bgp_link not in all_trace_links:
                        if bgp_list[i-1] in trace_list and bgp_list[i] in trace_list:
                            ind1 = trace_list.index(bgp_list[i-1])
                            ind2 = trace_list.index(bgp_list[i])
                            susp_bgp_links[bgp_list[i-1]][bgp_list[i]].add(' '.join(trace_list[ind1:ind2+1]))
                lines = [rf.readline() for _ in range(3)]
    print(len(susp_bgp_links))
    print(sum([len(val) for val in susp_bgp_links.values()]))
    rec = defaultdict(defaultdict)
    for key, val in susp_bgp_links.items():
        for subkey, subval in val.items():
            rec[key][subkey] = list(subval)
    with open('possi_bogus_link_from_mm', 'w') as wf:
        json.dump(rec, wf, indent=1)

def FilterPossiBogusLinks():
    possi_bogus_links = {}
    with open('possi_bogus_link_from_mm', 'r') as rf:
        possi_bogus_links = json.load(rf)
    atlas_asn_info = {}
    with open('/mountdisk2/common_vps/probe_asn.json', 'r') as rf:
        atlas_asn_info = json.load(rf)
    # rec = {}
    # for key, val in possi_bogus_links.items():
    #     if key not in atlas_asn_info.keys():
    #         rec[key] = val
    # with open('possi_bogus_link_from_mm_2', 'w') as wf:
    #     json.dump(rec, wf, indent=1)
    to_trace = set()
    others = {}
    for asn1, val in possi_bogus_links.items():
        for asn2, subval in val.items():
            if asn1 in atlas_asn_info.keys():
                to_trace.add(asn1+' '+asn2)
            elif asn2 in atlas_asn_info.keys():
                to_trace.add(asn2+' '+asn1)
            else:
                others[asn1+' '+asn2] = subval
    print(len(to_trace))
    print(len(others))
    with open('possi_bogus_links_to_traceroute', 'w') as wf:
        wf.write('\n'.join(to_trace))
    with open('possi_bogus_links_remains.json', 'w') as wf:
        json.dump(others, wf, indent=1)


def get_one_day_spec_pref_paths_for_bogus_links(v_h, prefs_v, date, collector):
    print('%s begin' %collector)
    pref_paths = defaultdict(set)
    try:
        stream = pybgpstream.BGPStream(
            from_time = '%s-%s-%s 00:00:00' %(date[:4], date[4:6], date[6:8]), 
            until_time = '%s-%s-%s 02:00:00' %(date[:4], date[4:6], date[6:8]), 
            record_type="ribs",
            collectors=[collector],
            filter="prefix exact %s" %' '.join(prefs_v.keys()))
        for elem in stream:
            prefix = elem.fields['prefix']
            if prefix not in prefs_v.keys():
                continue
            v = prefs_v[prefix]
            for h in v_h[v]:
                if h + ' ' + v not in elem.fields['as-path']:
                    continue
                pathlist = elem.fields['as-path'].split(' ')
                pathlist = [pathlist[i] for i in range(len(pathlist)) if i==0 or pathlist[i]!=pathlist[i-1]]
                pref_paths[prefix].add(' '.join(pathlist))
                #print(' '.join(pathlist))
        print('get ribs done %s' %collector)
    except Exception as e:
        print(e)
        return
    print('download bgp end %s' %collector)
    with open('/mountdisk1/ana_c_d_incongruity/rib_data/all_collectors_one_date_bgpdata/for_bogus_links_pref_paths_%s_%s' %(date, collector), 'w') as wf:
        rec = {key:list(val) for key, val in pref_paths.items()}
        json.dump(rec, wf, indent=1)
        
def GetPrfsOfLinkVictim():
    v_h = defaultdict(set)
    with open('/home/slt/code/ana_c_d_incongruity/possi_bogus_links_remains.json', 'r') as rf:
        data = json.load(rf)
        for key in data.keys():
            h, v = key.split(' ')
            v_h[v].add(h)     
    prefs_v = {}
    with open('/mountdisk1/ana_c_d_incongruity/rib_data/rv_prefix2as/20230513.pfx2as', 'r') as rf:
        for line in rf:
            elems = line.strip('\n').split('\t')
            if elems[-1] in v_h.keys():
                prefs_v[elems[0]+'/'+elems[1]] = elems[-1]
    collectors = ['route-views2', 'route-views.nwax', 'route-views2.saopaulo', 'route-views.ny', 'route-views3', 'route-views.perth', 'route-views4', 'route-views.peru', 'route-views5', 'route-views.phoix', 'route-views6', 'route-views.rio', 'route-views.amsix', 'route-views.saopaulo', 'route-views.bdix', 'route-views.sfmix', 'route-views.bknix', 'route-views.sg', 'route-views.chicago', 'route-views.siex', 'route-views.chile', 'route-views.soxrs', 'route-views.eqix', 'route-views.sydney', 'route-views.flix', 'route-views.telxatl', 'route-views.fortaleza', 'route-views.uaeix', 'route-views.gixa', 'route-views.wide', 'route-views.gorex', 'route-views.isc', 'route-views.jinx', 'route-views.kixp', 'route-views.linx', 'route-views.mwix', 'route-views.napafrica', 'rrc08', 'rrc09', 'rrc10', 'rrc11', 'rrc12', 'rrc13', 'rrc14', 'rrc15', 'rrc16', 'rrc17', 'rrc18', 'rrc19', 'rrc20', 'rrc21', 'rrc22', 'rrc00', 'rrc23', 'rrc01', 'rrc24', 'rrc03', 'rrc25', 'rrc04', 'rrc26', 'rrc05', 'rrc06', 'rrc07']
    pool = Pool(processes=len(collectors))
    paras = []
    for collector in collectors:
        paras.append((v_h, prefs_v, '20230513', collector))
    pool.starmap(get_one_day_spec_pref_paths_for_bogus_links, paras)
    pool.close()
    pool.join()
    

def CheckSerialHijackers():
    groundtruth = {'10512','57129','205944','34991','9498','11695','133955','3266','202746','29632','44582','9009','203040','327814','197426','8011','35916','62135','43350','203418','29073','42229','12506'}
    inferred = {'64670','40960','60010','393559','55830','137186','136850','63023','134094','64902','33529','12586','65060','64035','16913','55789','46230','4761','396319','265237','88888','64520','9542','63317','201224','57578','394844','206639','47117','134705','11878','7979','48551','16422','7701','203496','265705','204591','267286','394738','52060','48359','134833','134830','30277','263571','25926','23452','23456','134190','395667','65524','65529','43181','12127','26972','58182','197113','62878','61391','33440','26984','206698','200617','45889','262487','65444','11645','32875','27136','42293','24768','56096','134548','14061','17575','7363','8660','61317','8082','40034','6488','131297','22769','29354','200736','47869','39272','37182','7','63827','28409','58552','2639','200080','37135','201640','65110','65111','16164','63018','62052','60974','200039','60025','15083','35886','10741','135003','135004','131936','65090','201432','7512','48115','198949','55803','199312','133357','133354','45316','12679','395033','65539','65536','65537','65532','64489','43151','17175','197923','54239','59772','28067','56389','201735','39536','32714','45757','5580','50841','13682','65017','65014','65013','65012','65011','65010','136800','36131','65600','11409','49004','4874','198596','58879','36511','60800','19437','393899','204710','133201','395800','46261','52481','65027','37958','64770','26711','262820','132934','52302','132068','14179','31377','12720','7333','395105','63294','204010','61813','40459','136970','12684','23649','199290','63410','24000','36656','15562','395880','60539','12345','51563','9','55222','19136','135386','7411','19529','41204','30479','393960','134176','44098','56129','16253','64533','65505','65504','65501','65500','65503','65502','133752','64611','21637','8529','61036','65200','65206','32242','263783','263258','131334','53086','204105','2','65040','9178','8888','23738','263903','58908','58905','135028','204223','4785','132721','15','53648','51852','65515','62805','265534','400','37473','2035','265270','38266','199297','40000','201133','13346','65410','41018','10100','207083','333','22113','61574','61575','61576','22119','134520','37451','137571','59117','202786','53876','57724','8095','63440','137085','63849','135596','204895','43212','11368','60485','395363','63930','65330','14670','60117','18986','14207','13896','64804','264430','57695','204135','14046','14043','134785','201351','51977','7346','135509','62508','15393','132231','28126','7321','12180','21788','35773','265721','22549','27501','23338','202015','48030','65135','60562','45456','65103','393927','31972','136593','135026','200775','204043','395734','65075','54574','10505','24544','63119','57792','55008','396076','62240','205262','133477','14333','394094','206776','65550','65551','65555','17158','60458','327711','4','33387','134451','29713','60084','30103','16437','327717','21301','394279','264488','55081','55084','58964','135607','135605','39855','135402','13169','13161','198942','1337','8697','267294','8346','37405','134823','135834','203162','8100','206264','201288','42624','12156','9312','59592','199995','133229','14277','48944','58305','51913','65456','2208','14664','20473','30985','136620','64650','56984','37692','135091','13478','15701','1000','11019','393780','62741','23139','42962','7014','14558','64500','64501','64504','262181','56554','43239','197329','65101','65100','65102','65104','45489','17216','17213','134175','23541','205450','205422','136897','59749','7825','201793','55293','26026','131477','196716','203734','204211','35800','9504','65521','65525','43147','36828','48057','49302','204057','14455','5673','64634','40925','205021','135330','6589','25369','136384','136782','32708','36137','133676','8071','40676','65023','65020','65021','65025','65028','42427','12790','6','206706','46657','32092','14340','41717','262239','60394','60392','64252','265515','3177','133731','24875','59703','197890','133293','133219','21351','19916','29006','27176','202491','55863','6500','135377','135376','13445','29572','28226','264979','64900','131788','205474','35913','63990','65412','40440','136945','52223','49981','37238','64555','64550','64552','6391','134134','204063','200811','65150','65480','10241','10247','58752','263103','62010','327980','203724','64535','201333','395970','203406','36217','54600','204136','203649','133398','31798','47171','64280','200859','201108','16265','327815','137650','3214','19969','100','101','205361','203380','132839','8053','64601','64602','62093','33205','60932','60068','137191','137443','62468','60274','6762','131320','65405','203959','65052','65051','13647','64013','54680','11','10','12','59210','48262','49121','133865','262206','47729','62734','26658','136162','20','37468','205820','8','17440','42081','20150','54931','21859','28490','40311','65430','65433','65432','62352','203704','65112','7514','64646','136038','135663','41267','204188','29606','132972','40907','34637','55933','62445','57731','197595','265908','49','23198','327687','200002','136933','19148','395358','134963','30186','132471','133448','201105','133798','138415','46198','1','55002','264284','500','206617','49877','60349','201341','201838','58127','202883','49461','17048','137017','131284','61440','31863','134671','16555','58115','10480','40824','64600','58252','50360','23098','136268','133717','80','64651','200983','32959','52125','207046','49741','207200','42831','65088','203061','60929','200','64023','21287','10575','19571','133136','133131','3689','39572','65530','65531','6453','48851','65540','65543','43160','49195','64534','56647','3223','47065','5998','14821','28078','197991','64511','64513','64512','64515','64517','64516','63889','44812','201233','203602','203786','30058','327931','65008','65000','65001','65002','65003','65004','65005','65007','53587','393430','1321','22427','196854','60171','54825','200429','62000','65428','200872','64271','65333','44150','62490','198551','137502','290','46664','45204','203543','29020','206213','10973','15412','63975','61138','132927','204287','38757','395088','45382','11919','395111','207230','58366','201987','29386','34600','206751','200358','53107','134687','135886','3','65534','49084','65533','62058','200759','62597','62599','62263','203872','59699','59692','59344','61102','55987','26857','62610','395524','394695','37115','203661','42710','62387','58738','199979','266409','32181','3731','204175','204170','52776','18254','56478','5313','201106','63956','35557','135777','37420','45572','393502','25051','33333','65516','65514','65512','65513','65510','65511','58202','132335','61220','205718','12025','19905','64621','64620','135407','59796','59795','59790','37544','65232','64540','53889','38808','203930','133645','33576','7489','23470','46281','46284','11259','65031','65035','11784','43588','7971','23748','27052','64999','64594','39523','31732','133847','7727','65400','43289','65401','64249','45034','58806','51429','37449','48644','50245','65535','25319','57624','61971','43072','62662','200100','64666','135642','134512','18863','132918','33688','62228','30083','7844','64702','200141','197794','202829','132790','34636','22730','33182','36007','203125','47836','38176','136950','203827','48592','57976','59377','65301','63888','37674','33438','263824','137551','43955','14029','65300','202766','22','20406','44814','65499','393676','198747','65018','5','61580','3298','206893','59623','137273','202054','133547','201229','21226','46368','201746','136743','394380','138195','5772','205827','63293','46614','394021','64474','198584','58895','200534','16','58065','133771','133196','395561','202020','201613'}
    with open('possi_bogus_link_from_mm_2', 'r') as rf:
        data = json.load(rf)
        asns = set(data.keys())
        print(len(asns))
        print(len(asns & groundtruth))
        print(len(asns & inferred))
        print((asns & inferred))

if __name__ == '__main__':
    main_func()
