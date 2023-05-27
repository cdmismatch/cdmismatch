
import json
import glob

def CheckRM():
    threshold = 0.1
    stat = {True: [0, 0], False: [0, 0]}
    change = []
    with open('/mountdisk2/common_vps/real_mm_rates_filter.json', 'r') as rf:
        data = json.load(rf)
        for key, val in data.items():
            asn, vp = key.split('|')
            for date, ratio in val.items():
                stat_key = (ratio >= threshold)
                with open('/mountdisk2/common_vps/%s/cmp_res/sxt_bdr/ana_compare_res/continuous_mm.%s.%s' %(date, vp, date), 'r') as rf1:
                    t = 0
                    c = 0
                    lines = [rf1.readline() for _ in range(3)]
                    while lines[0]:
                        t += 1
                        stat[stat_key][1] += 1
                        trace_list = lines[0].split(']')[-1].strip('\n').split(' ')
                        bgp_list = lines[1].strip('\t').strip('\n').split(' ')
                        if len(bgp_list) == 1:
                            lines = [rf1.readline() for _ in range(3)]
                            continue    
                        for i in range(len(trace_list)):
                            if trace_list[i] != trace_list[0] and trace_list[i] != '*' and trace_list[i] != '?' and trace_list[i] != '-1':
                                if trace_list[i] != bgp_list[1]:
                                    stat[stat_key][0] += 1
                                else:
                                    c += 1
                                    if stat_key == True:
                                        a = 1
                                break
                        lines = [rf1.readline() for _ in range(3)]
                        if t > 0:
                            change.append(ratio * c / t)
                        else:
                            change.append(0)
    #print(stat)
    print(change)
    
    
def CheckRM_Ark():
    stat = [0, 0]
    rm = {}
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/real_mm_rates.json', 'r') as rf:
        rm = json.load(rf)
    fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/*/sxt_bdr/ana_compare_res/continuous_mm.*.*')
    for fn in fns:
        _, vp, date = fn.split('.')
        with open(fn, 'r') as rf1:
            t = 0
            c = 0
            lines = [rf1.readline() for _ in range(3)]
            while lines[0]:
                t += 1
                stat[1] += 1
                trace_list = lines[0].split(']')[-1].strip('\n').split(' ')
                bgp_list = lines[1].strip('\t').strip('\n').split(' ')
                if len(bgp_list) == 1:
                    lines = [rf1.readline() for _ in range(3)]
                    continue    
                for i in range(len(trace_list)):
                    if trace_list[i] != trace_list[0] and trace_list[i] != '*' and trace_list[i] != '?' and trace_list[i] != '-1':
                        if trace_list[i] != bgp_list[1]:
                            stat[0] += 1
                        else:
                            c += 1
                        break
                lines = [rf1.readline() for _ in range(3)]
        if vp not in rm:
            continue
        if date not in rm[vp]:
            continue
        rm[vp][date] = rm[vp][date] * c / t
    #print(stat)
    a =  [subval for val in rm.values() for subval in val.values()]
    print(a)

if __name__ == '__main__':
    #CheckRM()
    CheckRM_Ark()
    