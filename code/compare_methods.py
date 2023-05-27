
from collections import defaultdict
import os
import json
import glob
import re

from compare_cd import InitBGPPathInfo, GetBGPPath_Or_OriASN
from get_ip2as_from_bdrmapit import ConnectToBdrMapItDb, GetIp2ASFromBdrMapItDb, CloseBdrMapItDb, InitBdrCache, \
                                    ConstrBdrCache

def MapSame(map1, map2):
    if set(map1.split('_')) & set(map2.split('_')):
        return True
    return False

def CmpIpAccurOfRibBdr(vp, date):
    bgp_path_info = {}
    tracevp_bgpvp_info = {'ams-nl': '80.249.208.34', 'jfk-us': '198.32.160.61', 'sjc2-us': '64.71.137.241', 'syd-au': '198.32.176.177', 'per-au': '198.32.176.177', 'zrh2-ch': '109.233.180.32', 'nrt-jp': '203.181.248.168'}
    InitBGPPathInfo('/mountdisk1/ana_c_d_incongruity/rib_data/bgpdata/bgp_' + tracevp_bgpvp_info[vp] + '_' + date, bgp_path_info)
    ConnectToBdrMapItDb('/mountdisk1/ana_c_d_incongruity/out_bdrmapit/bdrmapit_' + vp + '_' + date + '.db')
    ConstrBdrCache()

    print('%s.%s' %(vp, date))
    os.chdir('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/' + vp + '/')
    stat_info = {'rib_based': {}, 'bdrmapit': {}}
    with open('rib_based/stat2_nodstip_ipaccur_%s.%s.json' %(vp, date), 'r') as rf:
        stat_info['rib_based'] = json.load(rf)
    with open('bdrmapit/stat2_nodstip_ipaccur_%s.%s.json' %(vp, date), 'r') as rf:
        stat_info['bdrmapit'] = json.load(rf)
    ip_info = {'rib_based': {}, 'bdrmapit': {}}
    for (map_method, stat_val) in stat_info.items():
        for (status, ip_val) in stat_val.items():
            for _ip in ip_val.keys():
                ip_info[map_method][_ip] = status
    comm_ips = set(ip_info['rib_based'].keys()) & set(ip_info['bdrmapit'].keys())
    comm_ips_num = len(comm_ips)
    total_ips_num = len(set(ip_info['rib_based'].keys()) | set(ip_info['bdrmapit'].keys()))
    cmp_stat = {'both_map': \
                    {'map_same': \
                        {'succ': 0, 'fail': 0, 'other': 0}, \
                    'map_diff': \
                        {'both_succ': {}, 'r_succ': {}, 'b_succ': {}, 'none_succ': {}}\
                    }, \
                'r_map': \
                    {'succ': 0, 'fail': 0, 'other': 0}, \
                'b_map': \
                    {'succ': 0, 'fail': 0, 'other': 0}, \
                'none_map': {}}
    #each base dict contains {_ip: [avg_span, avg_pos]}
    for _ip in comm_ips:
        if ip_info['rib_based'][_ip] != 'unmap' and ip_info['bdrmapit'][_ip] != 'unmap':
            #both mapped
            rib_based_res = GetBGPPath_Or_OriASN(bgp_path_info, _ip, 'get_orias')
            bdrmapit_res = GetIp2ASFromBdrMapItDb(_ip)
            if MapSame(rib_based_res, bdrmapit_res):
                #mapped same
                cmp_stat['both_map']['map_same'][ip_info['bdrmapit'][_ip]] += 1
            else:
                #mapped different
                if ip_info['rib_based'][_ip] == 'succ' and ip_info['bdrmapit'][_ip] == 'succ':
                    # if stat_info['rib_based']['succ'][_ip][0] > 1 and stat_info['rib_based']['succ'][_ip][1] < 1:
                    #     print('%s (AS1: %s, AS2: %s) [%d, %.2f]' %(_ip, rib_based_res, bdrmapit_res, stat_info['rib_based']['succ'][_ip][0], stat_info['rib_based']['succ'][_ip][1]))
                    #return
                    cmp_stat['both_map']['map_diff']['both_succ'][_ip] = stat_info['rib_based']['succ'][_ip]
                elif ip_info['rib_based'][_ip] == 'succ':
                    if stat_info['rib_based']['succ'][_ip][0] > 27:
                        print('%s (AS1: %s, AS2: %s) [%d, %.2f]' %(_ip, rib_based_res, bdrmapit_res, stat_info['rib_based']['succ'][_ip][0], stat_info['rib_based']['succ'][_ip][1]))
                    cmp_stat['both_map']['map_diff']['r_succ'][_ip] = stat_info['rib_based']['succ'][_ip]
                elif ip_info['bdrmapit'][_ip] == 'succ':
                    cmp_stat['both_map']['map_diff']['b_succ'][_ip] = stat_info['bdrmapit']['succ'][_ip]
                else:
                    cmp_stat['both_map']['map_diff']['none_succ'][_ip] = stat_info['bdrmapit'][ip_info['bdrmapit'][_ip]][_ip]
        elif ip_info['rib_based'][_ip] != 'unmap':
            cmp_stat['r_map'][ip_info['rib_based'][_ip]] += 1
        elif ip_info['bdrmapit'][_ip] != 'unmap':
            cmp_stat['b_map'][ip_info['bdrmapit'][_ip]] += 1
        else:
            cmp_stat['none_map'][_ip] = stat_info['bdrmapit']['unmap'][_ip]   
    CloseBdrMapItDb()
    InitBdrCache()
    if not os.path.exists('cmp_methods/'):
        os.mkdir('cmp_methods/')
    with open('cmp_methods/cmp.json', 'w') as wf:
        json.dump(cmp_stat, wf, indent=1)
    with open('cmp_methods/cmp_brief', 'w') as wf:
        wf.write('total ip num: %d(%.2f)\n' %(comm_ips_num, comm_ips_num / total_ips_num))
        for _type in ['r_map', 'b_map']:
            tmp = sum(cmp_stat[_type].values())
            wf.write('%s num: %.2f(%d)\n' %(_type, tmp / comm_ips_num, tmp))
            wf.write('\tin %s, ' %_type)
            for sub_type in cmp_stat[_type].keys():
                wf.write('%s rate: %.2f; ' %(sub_type, cmp_stat[_type][sub_type] / tmp))
            wf.write('\n')
        tmp = len(cmp_stat['none_map'].keys())
        wf.write('none mapped num: %.2f(%d)\n' %(tmp / comm_ips_num, tmp))
        count = {'map_same': sum(cmp_stat['both_map']['map_same'].values()), \
                'map_diff': [0, {}]}
        for (_type, val) in cmp_stat['both_map']['map_diff'].items():            
            tmp1 = tmp2 = 0
            for sub_val in val.values():
                tmp1 += sub_val[0]
                tmp2 += sub_val[1]
            tmp3 = len(val)
            count['map_diff'][1][_type] = [tmp3, tmp1 / tmp3, tmp2 / tmp3]
            count['map_diff'][0] += tmp3
        tmp = count['map_same'] + count['map_diff'][0]
        wf.write('both mapped num: %.2f(%d)\n' %(tmp / comm_ips_num, tmp))
        wf.write('\tin both mapped, same: %.2f, diff: %.2f\n' %(count['map_same'] / tmp, count['map_diff'][0] / tmp))
        wf.write('\t\tin both mapped and same, ')
        for _type in cmp_stat['both_map']['map_same']:
            wf.write('%s: %.2f; ' %(_type, cmp_stat['both_map']['map_same'][_type] / count['map_same']))
        wf.write('\n\t\tin both mapped but diff, ')
        for _type in cmp_stat['both_map']['map_diff']:
            wf.write('%s: %.2f[%d, %.2f]; ' %(_type, count['map_diff'][1][_type][0] / count['map_diff'][0], count['map_diff'][1][_type][1], count['map_diff'][1][_type][2]))
        
    os.system('cat cmp_methods/cmp_brief')
    print('done')

def cmp_ori_sxt_bdr():
    os.chdir('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/')
    trace_match_rate = defaultdict(defaultdict)
    ip_fail_rate = defaultdict(defaultdict)
    for vp in ['ams-nl', 'jfk-us', 'sjc2-us', 'syd-au', 'zrh2-ch', 'nrt-jp']:
        os.chdir(vp + '/')
        for method in ['ori_bdr', 'sxt_bdr']:
            os.chdir(method + '/')
            for filename in os.listdir('.'):
                if filename.startswith('trace_stat_'):
                    date = filename.split('.')[-1]
                    with open(filename, 'r') as rf:
                        data = rf.read()
                        res = re.findall('match:(\d\.\d.*)', data)                   
                        trace_match_rate[(vp, date)][method] = res[0]
                if filename.startswith('ip_stat_nodstip_'):
                    date = filename.split('.')[-1]
                    with open(filename, 'r') as rf:
                        data = rf.read()
                        res = re.findall('fail: \d\.\d.+\((\d+?)\)', data)                   
                        ip_fail_rate[(vp, date)][method] = res[0]
            os.chdir('../')
        os.chdir('../')
    for key, val in trace_match_rate.items():
        print('{}: {}, {}'.format(key, val['ori_bdr'], val['sxt_bdr']))
    for key, val in ip_fail_rate.items():
        diff_rate = (int(val['ori_bdr']) - int(val['sxt_bdr'])) / int(val['ori_bdr'])
        print('{}: {}'.format(key, diff_rate))

if __name__ == '__main__':
    cmp_ori_sxt_bdr()
    # for vp in ['ams-nl', 'jfk-us', 'sjc2-us', 'syd-au', 'zrh2-ch', 'nrt-jp']:
    # for vp in ['nrt-jp']:
    #     trace_filenames = glob.glob(r'/mountdisk1/ana_c_d_incongruity/traceroute_data/result/trace_%s*' %vp)
    #     for trace_filename in trace_filenames:
    #         date = trace_filename[trace_filename.rindex('.') + 1:]
    #         if date != '20180815':
    #             continue
    #         CmpIpAccurOfRibBdr(vp, date)
    #         break
    #     break
