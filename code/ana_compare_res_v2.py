
import pybgpstream
import os
import json
import datetime
from collections import defaultdict, Counter
from find_vp_v2 import CompressBGPPath
from multiprocessing import Process, Pool
import glob
from compare_cd import CompareCD_PerTrace, SelCloseBGP, CompressTrace, InitBGPPathInfo, GetBGPPath_Or_OriASN
import sys
sys.path.append('/home/slt/code/el_git/')
from find_common_vps_3 import get_ixp_ases, g_collectors, get_all_prefs_for_ip, g_parell_num
import numpy as np

def download_specdst_bgp_per_c(prefs, date, c, ixp_asns):
    print('{} {} begin'.format(date, c))
    #filename = '/mountdisk2/common_vps/%s/cmp_res/sxt_bdr/ana_compare_res/rm_dst_bgp_%s.json' %(date, c)
    filename = '/mountdisk1/ana_c_d_incongruity/rib_data/all_collectors_one_date_bgpdata/bgp_%s_%s.json' %(date, c)
    bgp_info = defaultdict(set)
    if not os.path.exists(filename):
        dt = datetime.datetime.strptime('%s-%s-%s 00:00:00' %(date[:4], date[4:6], date[6:8]), '%Y-%m-%d %H:%M:%S')
        # chunk_size = 20
        # for i in range(0, len(prefs), chunk_size):
        #     cur_prefs = prefs[i:i + chunk_size]
        try:
            #f = 'prefix exact %s' %(' '.join(cur_prefs))
            stream = pybgpstream.BGPStream(
                from_time = dt.strftime('%Y-%m-%d %H:%M:%S'),
                until_time = (dt + datetime.timedelta(hours=2)).strftime('%Y-%m-%d %H:%M:%S'),
                collectors=[c],
                record_type="ribs")#,
                #filter=f)
            for elem in stream:
                pref = elem.fields['prefix']
                if pref not in prefs:
                    # if pref != '0.0.0.0/0':
                    #     print('error')
                    continue
                path = elem.fields['as-path']
                if not path.__contains__('{'):
                    bgp_info[pref].add(CompressBGPPath(path, ixp_asns))
            stream = pybgpstream.BGPStream(
                from_time = dt.strftime('%Y-%m-%d %H:%M:%S'),
                until_time = (dt + datetime.timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S'),
                collectors=[c],
                record_type="updates")#,
                #filter=f)
            for elem in stream:
                pref = elem.fields['prefix']
                if pref not in prefs:
                    continue
                path = elem.fields['as-path']
                if not path.__contains__('{'):
                    bgp_info[pref].add(CompressBGPPath(path, ixp_asns))
        except Exception as e:
            pass
        if bgp_info:
            with open(filename, 'w') as wf:
                for pref, paths in bgp_info.items():
                    for path in paths:
                        wf.write('%s|%s\n' %(pref, path))
    print('{} {} end'.format(date, c))

def download_specdst_bgp_per_c_for_ark(prefs_oriasn, date, c, ixp_asns):
    print('{} {} begin'.format(date, c))
    #filename = '/mountdisk2/common_vps/%s/cmp_res/sxt_bdr/ana_compare_res/rm_dst_bgp_%s.json' %(date, c)
    filename = '/mountdisk1/ana_c_d_incongruity/rib_data/all_collectors_one_date_bgpdata/bgp_%s_%s.json' %(date, c)
    prefs = prefs_oriasn.keys()
    bgp_info = defaultdict(set)
    if not os.path.exists(filename):
        dt = datetime.datetime.strptime('%s-%s-%s 00:00:00' %(date[:4], date[4:6], date[6:8]), '%Y-%m-%d %H:%M:%S')
        # chunk_size = 20
        # for i in range(0, len(prefs), chunk_size):
        #     cur_prefs = prefs[i:i + chunk_size]
        try:
            #f = 'prefix exact %s' %(' '.join(cur_prefs))
            stream = pybgpstream.BGPStream(
                from_time = dt.strftime('%Y-%m-%d %H:%M:%S'),
                until_time = (dt + datetime.timedelta(hours=2)).strftime('%Y-%m-%d %H:%M:%S'),
                collectors=[c],
                record_type="ribs")#,
                #filter=f)
            for elem in stream:
                pref = elem.fields['prefix']
                if pref not in prefs:
                    continue
                path = elem.fields['as-path']
                if not path.__contains__('{'):
                    debug_a = prefs_oriasn[pref]
                    if path.split(' ')[-1] != prefs_oriasn[pref]:
                        bgp_info[pref].add(CompressBGPPath(path, ixp_asns))
            stream = pybgpstream.BGPStream(
                from_time = dt.strftime('%Y-%m-%d %H:%M:%S'),
                until_time = (dt + datetime.timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S'),
                collectors=[c],
                record_type="updates")#,
                #filter=f)
            for elem in stream:
                pref = elem.fields['prefix']
                if pref not in prefs:
                    continue
                path = elem.fields['as-path']
                if not path.__contains__('{'):
                    if path.split(' ')[-1] != prefs_oriasn[pref]:
                        bgp_info[pref].add(CompressBGPPath(path, ixp_asns))
        except Exception as e:
            pass
        if bgp_info:
            with open(filename, 'w') as wf:
                for pref, paths in bgp_info.items():
                    for path in paths:
                        wf.write('%s|%s\n' %(pref, path))
    print('{} {} end'.format(date, c))
    
def download_rm_dst_bgp(date):
    ixp_asns = get_ixp_ases(date)
    filenames = glob.glob('/mountdisk2/common_vps/%s/cmp_res/sxt_bdr/ana_compare_res/continuous_mm.*' %date)
    # for arkvp in ['ams-nl', 'jfk-us', 'sjc2-us', 'syd-au', 'nrt-jp', 'sao-br']:
    #     find = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/ana_compare_res/continuous_mm.%s.%s*' %(arkvp, arkvp, date[:6]))
    #     if find: filenames.append(find[0])
    prefs = set()
    for filename in filenames:
        with open(filename, 'r') as rf:
            lines = [rf.readline() for _ in range(3)]
            while lines[0]:
                dst = lines[0].split(']')[0][1:]
                prefs = prefs | set(get_all_prefs_for_ip(dst))
                #prefs.add(dst)
                lines = [rf.readline() for _ in range(3)]
    print('date: {}, pref_num: {}'.format(date, len(prefs)))
    return (ixp_asns, list(prefs))

def InitBGPPathInfo_ForAtlas(date, bgp_path_info):
    for filename in glob.glob('/mountdisk2/common_vps/%s/cmp_res/sxt_bdr/ana_compare_res/rm_dst_bgp_*' %date):
        tmp_info = {}
        InitBGPPathInfo(filename, tmp_info)
        for key, val in tmp_info.items():
            if key not in bgp_path_info.keys():
                bgp_path_info[key] = [set(), set()]
            bgp_path_info[key][0] = bgp_path_info[key][0] | set(val[0])
            bgp_path_info[key][1] = bgp_path_info[key][1] | val[1]
        #print(bgp_path_info.get('218.150.183.0/24'))

def get_mm_part_of_trace_per_bgp(trace_list, bgp_list):
    rec_index = 0
    for i in range(len(trace_list)):
        hop = trace_list[i]
        if hop != '*' and hop != '?' and not hop.startswith('<'):
            if hop in bgp_list:
                rec_index = i
            else:
                return ' '.join(trace_list[rec_index:])
    return None

def get_mm_parts_of_trace_by_bgps(trace_list, bgps):
    res = set()
    for bgp in bgps:
        tmp = get_mm_part_of_trace_per_bgp(trace_list, bgp.split(' '))
        if not tmp:
            print('err, find match')
            return None
        res.add(tmp)
    l = list(res)
    s = sorted(l, key=lambda x:len(x))
    return s

def check_partial_match(mm_part, ori_trace_list, bgp, ip_list, trace_to_ip_info, ip_accur_info, mm_ips, pm_ips, ip_remap):
    mm_part_list = mm_part.split(' ')
    hop = mm_part_list[0]
    trace_ind = len(ori_trace_list) - ori_trace_list[-1::-1].index(hop) - 1
    bgp_list = bgp.split(' ')
    if hop not in bgp_list:
        return False
    bgp_ind = bgp_list.index(hop)
    return CompareCD_PerTrace(' '.join(bgp_list[bgp_ind:]), mm_part_list, ip_list, trace_to_ip_info, ip_accur_info, mm_ips, pm_ips, ip_remap)

def get_all_bgpvp_paths(date, atlas_vp, atlas_bgp_info):
    bgpvp_paths = set()
    with open('/mountdisk2/common_vps/%s/bgp/bgp_%s.json' %(date, atlas_bgp_info[atlas_vp]), 'r') as rf:
        data = json.load(rf)
        for val in data.values():
            for paths in val.values():
                bgpvp_paths = bgpvp_paths | set(paths)
        return bgpvp_paths

def find_matched_bgp(date):
    os.chdir('/mountdisk2/common_vps/%s/cmp_res/sxt_bdr/ana_compare_res/' %date)
    bgp_path_info = {}
    InitBGPPathInfo_ForAtlas(date, bgp_path_info)
    rm_files = glob.glob('continuous_mm.*')
    ip_accur_info = {}
    ip_remap = defaultdict(Counter)
    mm_ips = []
    pm_ips = []
    with open('/mountdisk2/common_vps/%s/common_vp_%s.json' %(date, date), 'r') as rf:
        common_vps = json.load(rf)
    atlas_bgp_info = {}
    for asn, val in common_vps.items():
        for atlas_vp, subval in val.items():
            atlas_bgp_info[atlas_vp] = min(subval.keys(), key=lambda x:subval[x])
    stat = defaultdict(lambda:Counter())
    for rm_file in rm_files:
        print(rm_file)
        atlas_vp0 = rm_file[rm_file.index('.'):rm_file.rindex('.')]
        atlas_vp = atlas_vp0[1:]
        bgpvp_paths = get_all_bgpvp_paths(date, atlas_vp, atlas_bgp_info)
        wf_whole_match = None
        wf_splice_match = None
        wf_nobgp_match = None
        with open(rm_file, 'r') as rf:
            lines = [rf.readline() for _ in range(3)]
            while lines[0]:
                dst_ip, ori_trace = lines[0][1:].strip('\n').split(']')
                bgps = GetBGPPath_Or_OriASN(bgp_path_info, dst_ip, 'get_path')
                if not bgps:
                    if not wf_nobgp_match:
                        wf_nobgp_match = open('rm_nobgp_match' + atlas_vp0, 'w')
                    wf_nobgp_match.write(''.join(lines))
                    stat[atlas_vp]['nomatch'] += 1
                    lines = [rf.readline() for _ in range(3)]
                    continue
                ip_list = lines[2].strip('\n').split(']')[-1].split(' ')
                ori_trace_list = ori_trace.split(' ')
                (trace_list, trace_to_ip_info, loop_flag) = CompressTrace(ori_trace_list, ip_list, ori_trace_list[0])
                (sel_bgp, min_ab_count, mal_pos_flag) = SelCloseBGP(bgps, trace_list, trace_to_ip_info)
                if mal_pos_flag: #FIX-ME. mal pos的情况暂不解决
                    continue
                match_flag = False
                if sel_bgp.split(' ')[0] == trace_list[0]:
                    match_flag = CompareCD_PerTrace(sel_bgp, trace_list, ip_list, trace_to_ip_info, ip_accur_info, mm_ips, pm_ips, ip_remap)
                if match_flag:
                    if not wf_whole_match:
                        wf_whole_match = open('rm_whole_match' + atlas_vp0, 'w')
                    wf_whole_match.write(''.join(lines))
                    stat[atlas_vp]['wholematch'] += 1
                    lines = [rf.readline() for _ in range(3)]
                    continue
                mm_parts = get_mm_parts_of_trace_by_bgps(trace_list, bgpvp_paths)
                if not mm_parts: #abnormal,应该在上一步CompareCD_PerTrace()时找到
                    if not wf_whole_match:
                        wf_whole_match = open('rm_whole_match' + atlas_vp0, 'w')
                    wf_whole_match.write(''.join(lines))
                    stat[atlas_vp]['wholematch'] += 1
                    lines = [rf.readline() for _ in range(3)]
                    continue
                splice_match = False
                for mm_part in mm_parts:
                    for bgp in bgps:
                        if check_partial_match(mm_part, ori_trace_list, bgp, ip_list, trace_to_ip_info, ip_accur_info, mm_ips, pm_ips, ip_remap):
                            splice_match = True
                            break
                    if splice_match: break
                if splice_match:
                    if not wf_splice_match:
                        wf_splice_match = open('rm_splice_match' + atlas_vp0, 'w')
                    wf_splice_match.write(''.join(lines))
                    stat[atlas_vp]['splicematch'] += 1
                else:
                    if not wf_nobgp_match:
                        wf_nobgp_match = open('rm_nobgp_match' + atlas_vp0, 'w')
                    wf_nobgp_match.write(''.join(lines))
                    stat[atlas_vp]['nomatch'] += 1
                lines = [rf.readline() for _ in range(3)]
        if wf_whole_match: wf_whole_match.close()
        if wf_splice_match: wf_splice_match.close()
        if wf_nobgp_match: wf_nobgp_match.close()
    with open('/mountdisk2/common_vps/real_mm_rates.json', 'r') as rf:
        rm_info = json.load(rf)
        rm_info1 = {key.split('|')[1]:val for key, val in rm_info.items()}
        for key, val in stat.items():
            t = sum(val.values())
            if key not in rm_info1.keys() or date not in rm_info1[key].keys():
                continue
            for subkey, subval in val.items():
                stat[key][subkey] = subval / t
            stat[key]['rm_rate'] = rm_info1[key][date]
        with open('stat_rm_match_%s' %date, 'w') as wf:
            json.dump(stat, wf, indent=1)
        os.system('cat stat_rm_match_%s' %date)

def stat_all_rm_match():
    res = defaultdict(Counter)
    cur_date = '20220720'
    for year in range(2019, 2023):
        for month in range(1, 13):
            date = str(year) + str(month).zfill(2) + '15'
            if date > cur_date:
                break
            total = 0
            with open('/mountdisk2/common_vps/%s/cmp_res/sxt_bdr/ana_compare_res/stat_rm_match_%s' %(date, date), 'r') as rf:
                data = json.load(rf)
                for vp, val in data.items():
                    if 'rm_rate' not in val.keys():
                        continue
                    tmp_total = 0
                    with open('/mountdisk2/common_vps/%s/cmp_res/sxt_bdr/trace_stat_%s' %(date, vp), 'r') as rf1:
                        tmp = json.load(rf1)
                        tmp_total = tmp["can_compare"]
                    total += tmp_total
                    for _type, subval in val.items():
                        if _type != "rm_rate":
                            res[_type][date] += subval * val["rm_rate"] * tmp_total
            for _type in res.keys():
                res[_type][date] = res[_type][date] / total
    #print(res)
    # for _type, val in res.items():
    #     print('{}:{}'.format(_type, np.mean(list(val.values()))))
    for _type, val in res.items():
        print('{}:{}'.format(_type, val['20190115']))


def download_rm_dst_bgp_all():
    cur_date = '20220920'
    paras = []
    for year in range(2019, 2023):
        for month in range(1, 13):
            date = str(year) + str(month).zfill(2) + '15'
            if date > cur_date:
                break
            # if date < '20200415':
            #     continue
            ixp_asns, prefs = download_rm_dst_bgp(date)
            for c in g_collectors:
                paras.append((prefs, date, c, ixp_asns))
    pool = Pool(processes=g_parell_num)
    results = pool.starmap(download_specdst_bgp_per_c, paras)
    pool.close()
    pool.join()

def stat_rm_patterns(date):
    tracestat_filenames = glob.glob('/mountdisk2/common_vps/%s/cmp_res/sxt_bdr/trace_stat*' %date)
    stat = defaultdict(defaultdict)
    for fn in tracestat_filenames:
        vp = fn.split('/')[-1].split('_')[-1] #"trace_stat_96.127.249.195"
        with open(fn, 'r') as rf:
            data = json.load(rf)
            stat['total'][vp] = data['can_compare']
    s_rm = set()
    l_rm = set()
    with open('/mountdisk2/common_vps/%s/cmp_res/sxt_bdr/ana_compare_res/stat_rm_match_%s' %(date, date), 'r') as rf:
        data = json.load(rf)
        for vp, val in data.items():
            if 'rm_rate' not in stat.keys():
                print('err')
                continue
            if vp not in stat['total'].keys():
                print('err')
                continue
            stat['rm_rate'][vp] = val['rm_rate'] * stat['total'][vp]
            for _type, subval in val.items():
                if _type != 'rm_rate':
                    stat[_type][vp] = subval * stat['rm_rate'][vp]
            if val['rm_rate'] < 0.1: s_rm.add(vp)
            else: l_rm.add(vp)
    all = {}
    s = {}
    l = {}
    for _type, val in stat.items():
        all[_type] = sum(val.values())
        s[_type] = sum([sub_val for sub_key, sub_val in val.items() if sub_key in s_rm])
        l[_type] = sum([sub_val for sub_key, sub_val in val.items() if sub_key in l_rm])
    for _type in all.keys():
        if _type != 'total' and _type != 'rm_rate':
                all[_type] = all[_type] / all['rm_rate']
                s[_type] = s[_type] / s['rm_rate']
                l[_type] = l[_type] / l['rm_rate']
    if 'rm_date' not in all.keys():
        return (None, None, None)
    all['rm_rate'] = all['rm_rate'] / all['total']
    s['rm_rate'] = s['rm_rate'] / s['total']
    l['rm_rate'] = l['rm_rate'] / l['total']
    print(all)
    print(s)
    print(l)
    return (all, s, l)

def stat_rm_patterns_all():
    cur_date = '20220920'
    for year in range(2019, 2023):
        for month in range(1, 13):
            date = str(year) + str(month).zfill(2) + '15'
            if date > cur_date:
                break
            if date == '20190115':
                continue
            find_matched_bgp(date)
            stat_rm_patterns(date)

def get_all_bgpvp_paths_ark(date, bgpvp):
    bgpvp_paths = set()
    with open('/mountdisk1/ana_c_d_incongruity/rib_data/bgpdata/bgp_%s_%s' %(bgpvp, date), 'r') as rf:
        data = json.load(rf)
        for val in data.values():
            for paths in val.values():
                bgpvp_paths = bgpvp_paths | set(paths)
        return bgpvp_paths

def fst_step_check_whole_match_forark(date):
    tracevp_bgpvp_info = {'ams-nl': '80.249.208.34'}#, 'sjc2-us': '64.71.137.241', 'syd-au': '45.127.172.46', 'nrt-jp': '203.181.248.168', 'sao-br': '187.16.217.17'}
    tracevp_as_info = {'ams-nl': '1103', 'sjc2-us': '6939', 'syd-au': '7575', 'nrt-jp': '7660', 'sao-br': '22548'}
    total = 0
    spec = 0
    for arkvp, bgpvp in tracevp_bgpvp_info.items():
        #bgpvp_paths = get_all_bgpvp_paths_ark(date, bgpvp)
        filenames = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/ams-nl/sxt_bdr/ana_compare_res/continuous_mm.%s.%s*' %(arkvp, date[:6]))
        with open(filenames[0], 'r') as rf:
            lines = [rf.readline() for _ in range(3)]
            while lines[0]:
                ori_trace_list = lines[0].strip('\n').split(']')[-1].split(' ')
                ip_list = lines[2].strip('\n').split(']')[-1].split(' ')
                (trace_list, trace_to_ip_info, loop_flag) = CompressTrace(ori_trace_list, ip_list, ori_trace_list[0])
                if trace_list[:3] == ['1103', '224', '16509'] or trace_list[:3] == ['1103', '224', '11537']:
                    spec += 1
                total += 1
                lines = [rf.readline() for _ in range(3)]
    print('{}: {}, {}'.format(date, total, spec))

def download_spec_peer_bgp(c, peer, date, ixp_asns):
    print('{} {} begin'.format(date, c))
    filename = '/mountdisk2/common_vps/%s/bgp/all_bgp_%s_%s.json' %(date, peer, c)
    bgp_info = defaultdict(set)
    if not os.path.exists(filename):
        dt = datetime.datetime.strptime('%s-%s-%s 00:00:00' %(date[:4], date[4:6], date[6:8]), '%Y-%m-%d %H:%M:%S')
        try:
            f = 'peer %s' %peer
            stream = pybgpstream.BGPStream(
                from_time = dt.strftime('%Y-%m-%d %H:%M:%S'),
                until_time = (dt + datetime.timedelta(hours=2)).strftime('%Y-%m-%d %H:%M:%S'),
                collectors=[c],
                record_type="ribs",
                filter=f)
            for elem in stream:
                pref = elem.fields['prefix']
                path = elem.fields['as-path']
                if not path.__contains__('{'):
                    bgp_info[pref].add(CompressBGPPath(path, ixp_asns))
            stream = pybgpstream.BGPStream(
                from_time = dt.strftime('%Y-%m-%d %H:%M:%S'),
                until_time = (dt + datetime.timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S'),
                collectors=[c],
                record_type="updates",
                filter=f)
            for elem in stream:
                pref = elem.fields['prefix']
                path = elem.fields['as-path']
                if not path.__contains__('{'):
                    bgp_info[pref].add(CompressBGPPath(path, ixp_asns))
        except Exception as e:
            pass
        if bgp_info:
            with open(filename, 'w') as wf:
                for pref, paths in bgp_info.items():
                    for path in paths:
                        wf.write('%s|%s\n' %(pref, path))
    print('{} {} end'.format(date, c))

def fst_step_get_full_bgp_atlas():
    paras = []
    check_keys = set()
    cur_date = '20220920'
    for year in range(2019, 2023):
        for month in range(1, 13):
            date = str(year) + str(month).zfill(2) + '15'
            if date > cur_date:
                break
            ixp_asns = get_ixp_ases(date)
            atlas_bgp_info = {}
            with open('/mountdisk2/common_vps/%s/common_vp_%s.json' %(date, date), 'r') as rf:
                data = json.load(rf)
                for asn, val in data.items():
                    for atlasvp, subval in val.items():
                        bgpvp = min(subval.keys(), key=lambda x:subval[x])
                        atlas_bgp_info[atlasvp] = [bgpvp, asn]
            bgp_c_info = {}
            with open('/mountdisk2/common_vps/%s/bgp_vp_collectors_%s.json' %(date, date), 'r') as rf:
                bgp_c_info = json.load(rf)
            rm_fns = glob.glob('/mountdisk2/common_vps/%s/cmp_res/sxt_bdr/ana_compare_res/continuous_mm.*' %date)
            for rm_fn in rm_fns:
                atlasvp = rm_fn[rm_fn.index('.')+1:rm_fn.rindex('.')]
                bgpvp, asn = atlas_bgp_info[atlasvp]
                c = bgp_c_info[bgpvp][0]
                if (c, asn, date) not in check_keys:
                    check_keys.add((c, asn, date))
                    paras.append((c, asn, date, ixp_asns))
    pool = Pool(processes=g_parell_num)
    results = pool.starmap(download_spec_peer_bgp, paras)
    pool.close()
    pool.join()

def fst_step_check_splice_match_atlas():
    filenames = []
    for arkvp in ['ams-nl', 'jfk-us', 'sjc2-us', 'syd-au', 'nrt-jp', 'sao-br']:
        find = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/ana_compare_res/continuous_mm.%s.%s*' %(arkvp, arkvp, date[:6]))
        if find: filenames.append(find[0])

def stat_mm_ip_atlas():
    real_mm_rates = {}
    with open('/mountdisk2/common_vps/real_mm_rates.json', 'r') as rf:
        real_mm_rates = json.load(rf)
    modi_real_mm_rates = {key.split('|')[-1]:val for key, val in real_mm_rates.items()}

    res = defaultdict(defaultdict)
    cur_date = '20220920'
    err_cnt = 0
    for year in range(2019, 2023):
        for month in range(1, 13):
            date = str(year) + str(month).zfill(2) + '15'
            if date > cur_date:
                break
            fns = glob.glob('/mountdisk2/common_vps/%s/cmp_res/sxt_bdr/ana_compare_res/continuous_mm.*' %date)
            for fn in fns:
                tmp = fn.split('/')[-1]
                vp = tmp[tmp.index('.')+1:tmp.rindex('.')]
                if vp not in modi_real_mm_rates.keys() or date not in modi_real_mm_rates[vp].keys():
                    #print(err_cnt)
                    err_cnt += 1
                    continue
                if modi_real_mm_rates[vp][date] < 0.1:
                    continue
                c = defaultdict(list)
                total = 0
                with open(fn, 'r') as rf:
                    lines = [rf.readline() for _ in range(3)]
                    while lines[0]:
                        total += 1
                        mm_hops = set()
                        ori_trace_list = lines[0].strip('\n').split(']')[-1].split(' ')
                        bgp_list = lines[1].strip('\n').strip('\t').split(' ')
                        ip_list = lines[-1].strip('\n').split(']')[-1].split(' ')
                        trace_list, _, __ = CompressTrace(ori_trace_list, ip_list, bgp_list[0])
                        tmp_fst = 0
                        for i in range(len(trace_list)):
                            if trace_list[i] != '*' and trace_list[i] != '?' and trace_list[i] != '<':
                                tmp_fst = i
                                break
                        trace_list = trace_list[i:]
                        for hop in trace_list:
                            if hop != '*' and hop != '?' and hop[0] != '<':
                                if hop not in bgp_list:
                                    mm_hops.add(hop)
                        for ip in lines[-1].split(']')[0][1:].split(','):
                            tmp_subs = None
                            if ip not in ip_list:
                                #print(ip)
                                continue
                            ind = ip_list.index(ip)
                            hop = ori_trace_list[ind]
                            trace_ind = trace_list.index(hop)
                            #locate[ip][trace_ind] += 1
                            if trace_ind < len(trace_list) - 1:
                                left_hop = None
                                for i in range(trace_ind - 1, -1, -1):
                                    tmp_hop = trace_list[i]
                                    if tmp_hop in bgp_list:
                                        left_hop = tmp_hop
                                        break
                                right_hop = None
                                for i in range(trace_ind + 1, len(trace_list)):
                                    tmp_hop = trace_list[i]
                                    if tmp_hop in bgp_list:
                                        right_hop = tmp_hop
                                        break
                                if left_hop and right_hop:
                                    bgp_left_ind = bgp_list.index(left_hop)
                                    bgp_right_ind = bgp_list.index(right_hop)
                                    tmp_subs = bgp_list[bgp_left_ind + 1:bgp_right_ind]
                                elif left_hop:
                                    tmp_subs = bgp_list[bgp_left_ind + 1:]
                                # elif right_hop:
                                #     tmp_subs = set(bgp_list[:bgp_right_ind])
                            if len(mm_hops) == 1 and not tmp_subs:
                                a = 1
                            c[ip].append([len(mm_hops), trace_ind, tmp_subs]) #c[ip]的每个elem代表一次traceroute的数据，elem包括：
                            # if tmp_subs:
                            #     subs[ip].add(' '.join(tmp_subs))
                        lines = [rf.readline() for _ in range(3)]
                s = sorted(c.items(), key=lambda x:len(x), reverse=True)
                if s:
                    ip, val = s[0]
                    ip_subs = []
                    mm_hops_nums = Counter()
                    locate = Counter()
                    sub_num = None
                    for mm_hops_num, trace_ind, tmp_subs in val:
                        mm_hops_nums[mm_hops_num] += 1
                        # if mm_hops_num > 1:
                        #     continue
                        locate[trace_ind] += 1
                        if tmp_subs:
                            common = False
                            for check in ip_subs:
                                if set(check) & set(tmp_subs):
                                    common = True
                                    break
                            if not common:
                                ip_subs.append(tmp_subs)
                    sub_num = len(ip_subs)
                    if sub_num == 1:
                        with open('/mountdisk2/common_vps/%s/cmp_res/sxt_bdr/ipaccur_%s.json' %(date, vp), 'r') as rf_ipaccur:
                            ipaccur = json.load(rf_ipaccur)
                            if ipaccur[ip][0][0] > len(val):
                                sub_num = -1
                    res[vp][date] = [modi_real_mm_rates[vp][date], #real_mm_rate
                                    len(val) / total, #最popular的IP占?%的trace
                                    mm_hops_nums, #最popular的IP所在的trace含mm_hops的数量
                                    locate, #最popular的IP所在含一个mm_hops的trace中，IP对应trace hop在trace_list中的位置
                                    sub_num] #最popular的IP所在含一个mm_hops的trace中，IP对应substitute的数量
    mm_hops_nums = Counter()
    locates = Counter()
    subs = Counter()
    for vp, val in res.items():
        for date, subval in val.items():
            mm_hops_nums.update(subval[2])
            locates.update(subval[3])
            subs[subval[4]] += 1
    stat = {'mm_hops_nums': mm_hops_nums, 'locates': locates, 'subs': subs}
    with open('/mountdisk2/common_vps/stat_popular_mm_ip_atlas_2.json', 'w') as wf:
        json.dump(stat, wf, indent=1)
    print(stat)
    # a = []
    # b = []
    # for vp, val in res.items():
    #     for date, subval in val.items():
    #         if subval[1]:
    #             if subval[0] >= 0.1:
    #                 a.append(subval[1][0])
    #             b.append(subval[1][0])
    # rec = {'large_mm': a, 'all': b}
    # with open('/mountdisk2/common_vps/stat_popular_mm_ip_atlas.json', 'w') as wf:
    #     json.dump(rec, wf, indent=1)
    
def ana_atlas_asn():
    asns = set()
    with open('/mountdisk2/common_vps/real_mm_rates.json', 'r') as rf:
        real_mm_rates = json.load(rf)
        asns = {key.split('|')[0] for key in real_mm_rates.keys()}
    cone_size = {}
    with open('/mountdisk1/ana_c_d_incongruity/as_rel_cc_data/20210501.ppdc-ases.txt', 'r') as rf:
        for line in rf:
            if line.startswith('#'):
                continue
            elems = line.split(' ')
            cone_size[elems[0]] = len(elems) - 1
    net_type = {}
    #ixp = create_peeringdb('/mountdisk1/ana_c_d_incongruity/peeringdb_data/peeringdb_2022_07_26.json')
    with open('/mountdisk1/ana_c_d_incongruity/peeringdb_data/peeringdb_2022_07_26.json', 'r') as rf:
        ixp = json.load(rf)        
        for val in ixp['net']['data']:
            net_type[str(val['asn'])] = val['info_type']
    res = {}
    reverse_res = defaultdict(set)
    for asn in asns:
        if asn in net_type.keys():
            if net_type[asn] == 'Content':
                res[asn] = 'content'
                reverse_res['content'].add(asn)
        if asn not in res.keys():
            if asn in cone_size.keys():
                cs = cone_size[asn]
                if cs > 50:
                    res[asn] = 'large_provider'
                    reverse_res['large_provider'].add(asn)
                elif cs > 5:
                    res[asn] = 'small_provider'
                    reverse_res['small_provider'].add(asn)
                else:
                    res[asn] = 'stub'
                    reverse_res['stub'].add(asn)
    res['57777'] = 'stub'
    res['8298'] = 'small_provider'
    with open('/mountdisk2/common_vps/atlas_asn_type.json', 'w') as wf:
        json.dump(res, wf, indent=1)
    print(reverse_res)

def filter_susp_altas_vp():
    res = defaultdict(defaultdict)
    fns = glob.glob('/mountdisk2/common_vps/*15/cmp_res/sxt_bdr/trace_stat_*')
    #fns = glob.glob('/mountdisk2/common_vps/*15/cmp_res/coa_rib_based/trace_stat_*')
    for fn in fns:
        try:
            with open(fn, 'r') as rf:
                _, _, _, date, _, _, last = fn.split('/')
                vp = last.split('_')[-1]
                data = json.load(rf)
                res[vp][date] = data['match']
        except Exception as e:
            print(fn)
    filter_key = set()
    for vp, val in res.items():
        for date, subval in val.items():
            if subval < 0.6:
                #print(vp + '_' + date)
                filter_key.add(vp + '_' + date)
    #print(res)
    #print(filter_key)
    
    filter_res = defaultdict(defaultdict)
    with open('/mountdisk2/common_vps/real_mm_rates.json', 'r') as rf:
        data = json.load(rf)
        for key, val in data.items():
            vp = key.split('|')[-1]
            for date, subval in val.items():
                if vp + '_' + date == '132.147.91.107_20211115':
                    a = 2
                if vp + '_' + date not in filter_key:
                    filter_res[key][date] = subval
    with open('/mountdisk2/common_vps/real_mm_rates_filter.json', 'w') as wf:
        json.dump(filter_res, wf, indent=1)
    with open('/mountdisk2/common_vps/real_mm_rates_2.json', 'r') as rf:
        data = json.load(rf)
        for key, val in data.items():
            vp = key.split('|')[-1]
            for date, subval in val.items():
                if vp + '_' + date not in filter_key:
                    filter_res[key][date] = subval
    with open('/mountdisk2/common_vps/real_mm_rates_2_filter.json', 'w') as wf:
        json.dump(filter_res, wf, indent=1)

def vp_distribution():
    asn_type = {}
    with open('/mountdisk2/common_vps/atlas_asn_type.json', 'r') as rf:
        asn_type = json.load(rf) #vp_type[asn] = 'stub'
    vp_type = {}
    tier1_asns = ['3356', '1299', '174', '2914', '6762', '3257', '6939', '6453', '6461', '3491', '1273', '9002', '5511', '4637', '7473', '12956', '12389', '16735', '7018', '701', '9498', '31133', '3320', '3216', '1239', '262589', '20485', '6830', '7922', '3549', '7195', '20764', '4826', '37468', '10429', '33891', '209', '7738', '8359', '58453', '4230']
    fns = glob.glob('/mountdisk2/common_vps/*15/common_vp_*.json')
    for fn in fns:
        with open(fn, 'r') as rf:
            data = json.load(rf)
            for asn, val in data.items():
                if asn not in asn_type.keys():
                    #print(asn)
                    continue
                for vp in val.keys():
                    if asn in tier1_asns:
                        vp_type[vp] = 'tier1'
                    else:
                        vp_type[vp] = asn_type[asn]
    type_vps = defaultdict(set)
    for vp, _type in vp_type.items():
        type_vps[_type].add(vp)
    for _type, vps in type_vps.items():
        print('{}: {}'.format(_type, len(vps)))

def get_long_time_atlas_vp():
    vps = set()
    with open('/mountdisk2/common_vps/real_mm_rates.json', 'r') as rf:
        data = json.load(rf)
        for key, val in data.items():
            if len(val) > 12:
                vps.add(key)
    print(len(vps))

def main_func():
    get_long_time_atlas_vp()
    #vp_distribution()
    #filter_susp_altas_vp()
    #ana_atlas_asn()
    # stat_mm_ip_atlas()
    #stat_all_rm_match()
    # fst_step_get_full_bgp_atlas()
    # download_rm_dst_bgp_all()
    #stat_rm_patterns_all()
    #fst_step_check_whole_match_forark_all()

if __name__ == '__main__':
    main_func()
