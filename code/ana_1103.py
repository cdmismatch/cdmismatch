
import pybgpstream
import datetime
from collections import defaultdict, Counter
from find_vp_v2 import CompressBGPPathListAndTestLoop
from utils_v2 import GetIxpAsSet
import glob
import json
from ana_compare_res import cal_mm_segs
import matplotlib.pyplot as plt

def debug_get_one_day_bgp(date, pref, vp):
    start = datetime.datetime.now()
    try:
        stream = pybgpstream.BGPStream(
            # from_time = '%s-%s-%s 00:00:00' %(str(dt.year), str(dt.month).zfill(2), str(dt.day).zfill(2)), 
            # until_time = '%s-%s-%s 02:00:00' %(str(dt.year), str(dt.month).zfill(2), str(dt.day).zfill(2)), 
            from_time = '%s-%s-%s 08:00:00' %(date[:4], date[4:6], date[6:8]), 
            until_time = '%s-%s-%s 10:00:00' %(date[:4], date[4:6], date[6:8]), 
            record_type="ribs",
            collectors=['route-views.amsix', 'rrc03'],
            filter="peer %s and prefix exact %s" %(vp, pref))
            #filter="prefix exact %s" %pref)
        for elem in stream:
            tmp_pref = elem.fields['prefix']
            if tmp_pref != pref:
                continue
            # if elem.peer_asn != vp:
            #     continue
            print(elem.record.rec.collector)
            print(elem.fields['as-path'])
    except Exception as e:
        pass
    print('Elapse time: {}s'.format(datetime.datetime.now() - start).seconds)

def rec_elem(elem, data):
    pref = elem.fields['prefix']
    path = elem.fields['as-path']
    if path.__contains__('{'):
        return
    path_list = path.split(' ')
    if any(int(asn) >= 64512 and int(asn) <= 65536 for asn in path_list):
        return
    comp_path_list = CompressBGPPathListAndTestLoop(path_list)
    if comp_path_list:
        data[pref].add(' '.join(comp_path_list))
    return
        
def get_one_day_bgp(date):
    start = datetime.datetime.now()
    data = defaultdict(set)
    GetIxpAsSet(date)
    try:
        stream = pybgpstream.BGPStream(
            from_time = '%s-%s-%s 00:00:00' %(date[:4], date[4:6], date[6:8]), 
            until_time = '%s-%s-%s 02:00:00' %(date[:4], date[4:6], date[6:8]), 
            record_type="ribs",
            #collectors=['route-views.amsix', 'rrc03'],
            collectors=['route-views.sg', 'rrc23'],
            #filter="peer 1103")
            filter="peer 18106")
            #filter="prefix exact %s" %pref)
        for elem in stream:
            rec_elem(elem, data) 
        print('get ribs done')
        for i in range(0, 24, 2):
            print('updates in time: %d' %i)
            stream = pybgpstream.BGPStream(
                from_time = '%s-%s-%s %s:00:00' %(date[:4], date[4:6], date[6:8], str(i).zfill(2)), 
                until_time = '%s-%s-%s %s:59:59' %(date[:4], date[4:6], date[6:8], str(i+1).zfill(2)), 
                record_type="updates",
                #collectors=['route-views.amsix', 'rrc03'],
                collectors=['route-views.sg', 'rrc23'],
                #filter="elemtype announcements and peer 1103")
                filter="elemtype announcements and peer 18106")
            for elem in stream:
                rec_elem(elem, data)
    except Exception as e:
        print(e)
        return
    print('download bgp end')
    #with open('bgp_1103_%s', 'w') as wf:
    with open('bgp_18106_%s' %date, 'w') as wf:
        for pref, val in data.items():
            for elem in val:
                wf.write('{}|{}\n'.format(pref, elem))
    print('write file done')    

def debug_succips(date):
    key_ip = '109.105.97.143'
    info = Counter()
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/ams-nl/sxt_bdr/mm_ams-nl.%s' %date, 'r') as rf:
            lines = [rf.readline() for _ in range(3)]
            while lines[0]:
                if lines[2].__contains__(key_ip):
                    trace_list = lines[0][1:].strip('\n').split(']')[-1].split(' ')
                    ip_list = lines[2].strip('\n').split(']')[-1].split(' ')
                    ind = ip_list.index(key_ip)
                    if ind < len(ip_list) - 1:
                        info[(ip_list[ind+1], trace_list[ind+1])] += 1
                    else:
                        info[('N, N')] += 1
                lines = [rf.readline() for _ in range(3)]
    print(info)

def ana_dst_in_Brazil():
    fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/*/sxt_bdr/ana_compare_res/continuous_mm.*.20*')
    stat = defaultdict(list)
    for fn in sorted(fns):
        elems = fn.split('.')
        vp, date = elems[-2], elems[-1]        
        asn_country = {}
        orgid_country = {}
        with open('/mountdisk1/ana_c_d_incongruity/as_org_data/%s0401.as-org2info.txt' %date[:4], 'r') as rf:
            for line in rf:
                if line.startswith('#'):
                    continue
                elems = line.strip('\n').split('|')
                if len(elems) == 5: #format: org_id|changed|name|country|source            
                    orgid_country[elems[0]] = elems[-2]
                elif len(elems) == 6:   #format: aut|changed|aut_name|org_id|opaque_id|source
                    if elems[3] in orgid_country.keys():
                        asn_country[elems[0]] = orgid_country[elems[3]]
        country_counts = Counter()
        with open(fn, 'r') as rf:
            lines = [rf.readline() for _ in range(3)]
            while lines[0]:
                ori_asn = lines[1].strip('\n').split(' ')[-1]
                if ori_asn in asn_country.keys():
                    country_counts[asn_country[ori_asn]] += 1
                else:
                    country_counts['unknown'] += 1
                lines = [rf.readline() for _ in range(3)]
        total = sum(country_counts.values())
        #s = sorted(country_counts.items(), key=lambda x:x[1], reverse=True)
        #print('\t{}: {}:{}, BR:{}'.format(date, s[0][0], s[0][1]/total, country_counts['BR']/total))
        stat[vp].append(country_counts['BR']/total)
    rec = {}
    for vp, val in stat.items():
        rec[vp] = sum(stat[vp]) / len(stat[vp])
    print(rec)

def ana_pref24_in_Brazil():
    date = '202101'
    orgid_country = {}
    asn_country = {}
    with open('/mountdisk1/ana_c_d_incongruity/as_org_data/%s0401.as-org2info.txt' %date[:4], 'r') as rf:
        for line in rf:
            if line.startswith('#'):
                continue
            elems = line.strip('\n').split('|')
            if len(elems) == 5: #format: org_id|changed|name|country|source            
                orgid_country[elems[0]] = elems[-2]
            elif len(elems) == 6:   #format: aut|changed|aut_name|org_id|opaque_id|source
                if elems[3] in orgid_country.keys():
                    asn_country[elems[0]] = orgid_country[elems[3]]
    c = Counter()
    # with open('/mountdisk1/ana_c_d_incongruity/rib_data/rv_prefix2as/%s.pfx2as' %date, 'r') as rf:
    #     for line in rf:
    #         pref, preflen, asn = line.strip('\n').split('\t')
    #         preflen = int(preflen)
    #         if preflen > 24:
    #             continue
    #         if asn in asn_country.keys():
    #             c[asn_country[asn]] += (1<<(24-int(preflen)))
    for vp in ['ams-nl', 'jfk-us', 'nrt-jp', 'sao-br', 'sjc2-us', 'syd-au']:
        fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/match_%s.%s*' %(vp, vp, date))
        fns = fns + glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/mm_%s.%s*' %(vp, vp, date))
        t = 0
        for fn in fns:
            with open(fn, 'r') as rf:
                lines = [rf.readline() for _ in range(3)]
                while lines[0]:
                    ori_asn = lines[1].strip('\n').split(' ')[-1]
                    if ori_asn in asn_country.keys():
                        c[asn_country[ori_asn]] += 1
                    t += 1
                    lines = [rf.readline() for _ in range(3)]
        #s = sorted(c.items(), key=lambda x:x[1], reverse=True)
        #print(s[:10])
        if t > 0 and 'BR' in c:
            print('{}:{}'.format(vp, c['BR'] / t))
                
def stat_7660():
    fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/nrt-jp/sxt_bdr/ana_compare_res/continuous_mm.nrt-jp.20*')
    ratio = {}
    for fn in sorted(fns):
        date = fn.split('.')[-1]
        rec = []
        #print(date)
        f = 0
        total = 0
        with open(fn, 'r') as rf:
            lines = [rf.readline() for _ in range(3)]
            while lines[0]:
                if lines[0].__contains__('7660 6939') or \
                    lines[0].__contains__('7660 <9355> 6939') or \
                    lines[0].__contains__('7660 9355 6939') or \
                    lines[0].__contains__('7660 9355 9355 6939') or \
                    lines[0].__contains__('7660 2516 6939'):
                    f += 1
                elif (rec or date[:6] == '202112'):
                    rec = rec + lines
                total += 1
                lines = [rf.readline() for _ in range(3)]
            ratio[date[:6]+'15'] = f / total
        if rec:
            with open('test', 'w') as wf:
                for elem in rec:
                    wf.write(elem)
        #print('\t{}'.format(ratio[date]))
    print(ratio)
    ori_date = {}
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/real_mm_rates.json', 'r') as rf:
        ori_data = json.load(rf)
    res = {}
    for date, val in ori_data['nrt-jp'].items():
        res[date] = val * (1 - ratio[date])
    print(res)
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/real_mm_rates_nrt-jp_modi.json', 'w') as wf:
        json.dump(res, wf, indent=1)
        
def stat_ra_1103():
    fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/ams-nl/sxt_bdr/ana_compare_res/continuous_mm.ams-nl.*')
    data = {}
    subdata = defaultdict(Counter)
    for fn in fns:
        date = fn.split('.')[-1]
        c, t = 0, 0
        with open(fn, 'r') as rf:
            lines = [rf.readline() for _ in range(3)]
            while lines[0]:
                #if lines[0].__contains__('1103 137409 201011') and lines[1].__contains__('1103 33891'):
                # if lines[0].__contains__('1103 201011 ') and not lines[1].__contains__(' 201011 '):
                #     elems = lines[1].strip('\t').strip('\n').split(' ')
                #     if len(elems) > 1:
                #         subdata[date][elems[1]] += 1
                #if lines[0].__contains__('9121 47331') and lines[1].endswith('9121\n'):
                if lines[0].__contains__('11164 11537 11537 ') and lines[1].__contains__('11164 4637'):
                #if lines[0].__contains__('1103 3257 ') and lines[1].__contains__('1103 6461 '):
                    c += 1
                t += 1
                lines = [rf.readline() for _ in range(3)]
        data[date] = [c, t, c / t]
    s = sorted(data.items(), key=lambda x:x[0])
    for elem in s:
        print(elem)
    sub_s = sorted(subdata.items(), key=lambda x:x[0])
    for elem in sub_s:
        print(elem)
        
def ana_mm_form():
    fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/ams-nl/sxt_bdr/ana_compare_res/continuous_mm.ams-nl.*')
    detour = [0 for _ in range(len(fns))]
    extra_tail = [0 for _ in range(len(fns))]
    fst_bifork = [0 for _ in range(len(fns))]
    t = [0 for _ in range(len(fns))]
    i = 0
    for fn in sorted(fns):
        date = fn.split('.')[-1]
        with open(fn, 'r') as rf:
            lines = [rf.readline() for _ in range(3)]
            while lines[0]:
                trace_list = []
                for elem in lines[0].strip('\n').split(']')[-1].split(' '):
                    if elem != '*' and elem != '?' and elem != '-1' and elem not in trace_list:
                        trace_list.append(elem)
                bgp_path = lines[1].strip('\n').strip('\t')
                bgp_list = bgp_path.split(' ')
                if len(bgp_list) > 1 and len(trace_list) > 1 and bgp_list[1] != trace_list[1]:
                    fst_bifork[i] += 1
                (mm_seg_num, mm_segs) = cal_mm_segs(bgp_path, trace_list)
                tmp_detour = False
                tmp_extra_tail = False
                for seg in mm_segs:
                    [mm_bgp_seg, mm_trace_seg] = seg
                    if mm_bgp_seg[-1] == mm_trace_seg[-1]:
                        if len(mm_trace_seg) > len(mm_bgp_seg):
                            tmp_detour = True
                    else:
                        if len(mm_bgp_seg) == 1 and mm_bgp_seg[0] in mm_trace_seg:
                            tmp_extra_tail = True
                if tmp_detour: detour[i] += 1
                if tmp_extra_tail: extra_tail[i] += 1
                t[i] += 1
                lines = [rf.readline() for _ in range(3)]
        i += 1
    # print('detour:')
    # for date, val in detour.items():
    #     print('{}:{}'.format(date, val / t[date]))
    # print('extra_tail:')
    # for date, val in extra_tail.items():
    #     print('{}:{}'.format(date, val / t[date]))
    # print('fst_bifork:')
    # for date, val in fst_bifork.items():
    #     print('{}:{}'.format(date, val / t[date]))
    #detour_rate = [0.3413, 0.3842, 0.3220, 0.2195, 0.2973, 0.4330, 0.3316, 0.3597, 0.3800, 0.2917, 0.3483, 0.3164, 0.3801, 0.2320, 0.2644, 0.4602, 0.3307, 0.4487, 0.1983, 0.2899, 0.2629, 0.4537, 0.3247, 0.2941, 0.8549, 0.2703, 0.3478, 0.2638, 0.2284, 0.2319, 0.4401, 0.2367, 0.4751, 0.3995, 0.5059, 0.3480, 0.2613, 0.2720, 0.2706, 0.2324, 0.4508, 0.3975, 0.4670, 0.4704, 0.2554, 0.4309, 0.3244, 0.5290]
    #extra_tail_rate = [0.02346, 0.04217, 0.04184, 0.00600, 0.02205, 0.00461, 0.05643, 0.03387, 0.03737, 0.03984, 0.02539, 0.02634, 0.01400, 0.00429, 0.03912, 0.01319, 0.03653, 0.01166, 0.02166, 0.01402, 0.04038, 0.03275, 0.01338, 0.03660, 0.00674, 0.01134, 0.01162, 0.01232, 0.02955, 0.02593, 0.01417, 0.02494, 0.00863, 0.01463, 0.00605, 0.06701, 0.03434, 0.03749, 0.03017, 0.02121, 0.00865, 0.01182, 0.01007, 0.05767, 0.01335, 0.01649, 0.02944, 0.01146]
    fig, ax = plt.subplots()
    ax.plot(range(len(detour)), detour, c='r')
    ax.plot(range(len(extra_tail)), extra_tail, c='g')
    ax.plot(range(len(fst_bifork)), fst_bifork, c='b')
    ax.legend(loc='upper right', prop={'size': 15})
    plt.tight_layout()
    plt.show()
    
def ana_diff_date():
    pre_ = '/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/ams-nl/sxt_bdr/ana_compare_res/continuous_mm.ams-nl.'
    date1 = '20210915'
    date2 = '20211015'
    fn1 = pre_ + date1
    fn2 = pre_ + date2
    
    dst_ips1 = set()
    with open(fn1, 'r') as rf:
        lines = [rf.readline() for _ in range(3)]
        while lines[0]:
            dst_ips1.add(lines[0].split(']')[0][1:])
            lines = [rf.readline() for _ in range(3)]
    rec = []
    with open(fn2, 'r') as rf:
        lines = [rf.readline() for _ in range(3)]
        while lines[0]:
            dst_ip = lines[0].split(']')[0][1:]
            if dst_ip not in dst_ips1:
                rec = rec + lines[:3]
            lines = [rf.readline() for _ in range(3)]
    with open('test', 'w') as wf:
        for line in rec:
            wf.write(line)
    print(len(rec))
    
def filter_cases():
    record = []
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/ams-nl/sxt_bdr/ana_compare_res/continuous_mm.ams-nl.20220215', 'r') as rf:
        lines = [rf.readline() for _ in range(3)]
        while lines[0]:
            filter = False
            if lines[0].__contains__('1103 137409') and not lines[1].__contains__(' 137409 '):
                filter = True
            if lines[0].__contains__('9121 47331') and lines[1].endswith('9121\n'):
                filter = True
            if lines[0].__contains__('1103 3257 ') and lines[1].__contains__('1103 6461 '):
                filter = True
            if not filter:
                record = record + lines
            lines = [rf.readline() for _ in range(3)]
    with open('test', 'w') as wf:
        for line in record:
            wf.write(line)
    print(len(record))
                
def draw_spec_mm_1103():
    #dates = ['20180115', '20180215', '20180315', '20180415', '20180515', '20180615', '20180715', '20180815', '20180915', '20181015', '20181115', '20181215', '20190115', '20190215', '20190315', '20190415', '20190515', '20190615', '20190715', '20190815', '20190915', '20191015', '20191115', '20191215', '20200115', '20200215', '20200315', '20200415', '20200515', '20200615', '20200715', '20200815', '20200915', '20201015', '20201115', '20201215', '20210115', '20210415', '20210515', '20210615', '20210715', '20210815', '20210915', '20211015', '20211115', '20211215', '20220115', '20220215']
    dates = ['20190315', '20210715', '20210815', '20210915', '20211015', '20211115', '20211215', '20220115', '20220215']
    t = [0.01491392717327184, 0.015634286692001512, 0.014036865173527834, 0.0204930966469428, 0.01398603733765511, 0.01872741890387919, 0.019124018666875076, 0.01772741890387919, 0.03972741890387919]
    cases = {}
    cases['9121_47331'] = {'20200715': 0.1, '20211215': 0.25, '20220115': 0.26, '20220215': 0.13}
    cases['1103_137409'] = {'20210715': 0.56, '20210815': 0.56, '20210915': 0.42, '20220115': 0.4, '20220215': 0.2}
    cases['11164_11537'] = {'20190315': 0.72, '20210915': 0.15, '20211015': 0.22}
    cases['3257_6141'] = {'20220215': 0.42}
    cases_arr = {}
    for key, val in cases.items():
        cases_arr[key] = [0 for _ in range(len(dates))]
        for date, subval in val.items():
            if date in dates:
                cases_arr[key][dates.index(date)] = t[dates.index(date)] * subval
                t[dates.index(date)] -= cases_arr[key][dates.index(date)]
            
    fig, ax = plt.subplots(figsize=(8, 8))
    width = 0.3
    #ax.plot(dates, t, linestyle=':', c='grey')
    ax.bar(dates, t, width, label='other')#, color='yellow')
    j = 1
    for key, val in cases_arr.items():
        # if key != '9121_47331':
        #     ax.bar(dates, val, width, bottom=t, label='ri_%d' %j)
        #     j += 1
        # else:
        #     ax.bar(dates, val, width, bottom=t, label='ra_0')
        ax.bar(dates, val, width, bottom=t, label='form-%d' %j)
        j += 1
        t = [t[i] + val[i] for i in range(len(dates))]
    ax.legend(bbox_to_anchor=(0, 1.02, 1, 0.2), loc='lower left', mode='expand', ncol=3, prop={'size': 15})    
    
    dates_str = []    
    for year in range(2018, 2023):
        dates_str.append(str(year) + '.01')
        dates_str.append(str(year) + '.07')
    dates_str = dates_str[:-1]
    dates_tick = [i*6 for i in range(len(dates_str))]
    dates_tick = [i for i in range(len(dates))]
    dates_str = [date[2:4]+'.'+date[4:6] for date in dates]
    ax.set_xticks(dates_tick)
    ax.set_xticklabels(dates_str)
    y_tick = [i * 0.01 for i in range(5)]
    y_tickstr = [i * 8000 for i in range(5)]
    ax.set_yticks(y_tick)
    ax.set_yticklabels(y_tickstr)
    ax.tick_params(labelsize=10)
    ax.set_ylim(0, 0.045)
    
    plt.tight_layout()
    eps_fig = plt.gcf() # 'get current figure'
    eps_fig.savefig('ana_1103_spec_cases.eps', format='eps')
    plt.show()

def extract_diff():
    date = '202105'
    dst_ips_mm = set()
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/ams-nl/sxt_bdr/mm_ams-nl.%s15' %date, 'r') as rf:
        lines = [rf.readline() for _ in range(3)]
        while lines[0]:
            dst_ips_mm.add(lines[0].split(']')[0][1:])
            lines = [rf.readline() for _ in range(3)]
    dst_ips_cm = set()
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/ams-nl/sxt_bdr/ana_compare_res/continuous_mm.ams-nl.%s15' %date, 'r') as rf:
        lines = [rf.readline() for _ in range(3)]
        while lines[0]:
            dst_ips_cm.add(lines[0].split(']')[0][1:])
            lines = [rf.readline() for _ in range(3)]
    c_mms = []
    c_cms = []
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/sao-br/sxt_bdr/ana_compare_res/continuous_mm.sao-br.%s15' %date, 'r') as rf:
        lines = [rf.readline() for _ in range(3)]
        while lines[0]:
            dst_ip = lines[0].split(']')[0][1:]
            if dst_ip not in dst_ips_mm:
                c_mms = c_mms + lines
            elif dst_ip not in dst_ips_cm:
                c_cms = c_cms + lines
            lines = [rf.readline() for _ in range(3)]
    with open('test', 'w') as wf:
        wf.write(''.join(c_mms))
    print(len(c_mms))
    with open('test1', 'w') as wf:
        wf.write(''.join(c_cms))
    print(len(c_cms))

def stat_ra_22548():
    fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/sao-br/sxt_bdr/ana_compare_res/continuous_mm.sao-br.*')
    data = defaultdict(defaultdict)
    dates = []
    for fn in sorted(fns):
        date = fn.split('.')[-1]
        dates.append(date)
        c = Counter()
        t = 0
        with open(fn, 'r') as rf:
            lines = [rf.readline() for _ in range(3)]
            while lines[0]:
                if lines[0].__contains__('22548 6939 ') and not lines[1].__contains__('22548 6939 '):
                    c['6939'] += 1    
                elif lines[0].__contains__('22548 3491 ') and not lines[1].__contains__('22548 3491 '):
                    c['3491'] += 1
                elif lines[0].__contains__('22548 2914 ') and not lines[1].__contains__('22548 2914 '):
                    c['2914'] += 1
                t += 1
                lines = [rf.readline() for _ in range(3)]
        for key, val in c.items():
            data[key][date] = val / t
    print(data)
    base = [0.02549,  0.01228,  0.01064,  0.01141,  0.01455,  0.02428,  0.01867,  0.01700,  0.02051,  0.02913,  0.01081,  0.01258,  0.01290,  0.01161,  0.01663,  0.01943,  0.01812,  0.03301,  0.02069,  0.01682,  0.01293,  0.01164,  0.01233,  0.01560,  0.01280,  0.01614]
    t = [i for i in base]
    cases_arr = {}
    for key, val in data.items():
        cases_arr[key] = [0 for _ in range(len(dates))]
        for date, subval in val.items():
            cases_arr[key][dates.index(date)] = base[dates.index(date)] * subval
            t[dates.index(date)] -= cases_arr[key][dates.index(date)]
            
    fig, ax = plt.subplots()
    width = 0.5
    #ax.plot(dates, t, linestyle=':', c='blue')
    ax.bar(dates, t, width, label='other')
    j = 0
    for key, val in cases_arr.items():
        ax.bar(dates, val, width, bottom=t, label=key)
        t = [t[i] + val[i] for i in range(len(dates))]
    ax.legend(bbox_to_anchor=(0, 1.02, 1, 0.2), loc='lower left', mode='expand', ncol=4, prop={'size': 10})    
    
    # dates_str = []    
    # for year in range(2018, 2023):
    #     dates_str.append(str(year) + '.01')
    #     dates_str.append(str(year) + '.07')
    # dates_str = dates_str[:-1]
    # dates_tick = [i*6 for i in range(len(dates_str))]
    # ax.set_xticks(dates_tick)
    dates_str = [date[2:4]+'.'+date[4:6] for date in dates]
    ax.set_xticklabels(dates_str)
    ax.tick_params(labelsize=5)
    ax.grid(axis='y',linestyle=':',color='grey',alpha=0.3, which='major')
    
    plt.tight_layout()
    eps_fig = plt.gcf() # 'get current figure'
    eps_fig.savefig('ana_22548_spec_cases.eps', format='eps')
    plt.show()

def stat_22548():
    fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/sao-br/sxt_bdr/ana_compare_res/continuous_mm.sao-br.20*')
    ratio = {}
    for fn in sorted(fns):
        date = fn.split('.')[-1]
        rec = []
        #print(date)
        f = 0
        total = 0
        with open(fn, 'r') as rf:
            lines = [rf.readline() for _ in range(3)]
            while lines[0]:
                if (lines[0].__contains__('22548 6939 ') and not lines[1].__contains__('22548 6939 ')) or \
                    (lines[0].__contains__('22548 3491 ') and not lines[1].__contains__('22548 3491 ')) or \
                    (lines[0].__contains__('22548 2914 ') and not lines[1].__contains__('22548 2914 ')):
                    f += 1
                # elif (rec or date[:6] == '202112'):
                #     rec = rec + lines
                total += 1
                lines = [rf.readline() for _ in range(3)]
            ratio[date[:6]+'15'] = f / total
        if rec:
            with open('test', 'w') as wf:
                for elem in rec:
                    wf.write(elem)
        #print('\t{}'.format(ratio[date]))
    print(ratio)
    ori_date = {}
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/real_mm_rates.json', 'r') as rf:
        ori_data = json.load(rf)
    res = {}
    for date, val in ori_data['sao-br'].items():
        res[date] = val * (1 - ratio[date])
    print(res)
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/real_mm_rates_sao-br_modi.json', 'w') as wf:
        json.dump(res, wf, indent=1)
                    
if __name__ == '__main__':
    #stat_22548()
    #stat_ra_22548()
    #extract_diff()
    #stat_ra_1103()
    #filter_cases()
    #draw_spec_mm_1103()
    #ana_diff_date()
    #ana_mm_form()
    #stat_7660()
    #ana_dst_in_Brazil()
    #ana_pref24_in_Brazil()
    #dt = datetime.datetime.now()
    get_one_day_bgp('20210815')
    #debug_succips('20190215')
    