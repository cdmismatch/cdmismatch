
import os
import re
import socket
import struct

import global_var
from gen_ip2as_command import GetCloseDateFile, PreGetSrcFilesInDirs
from download_irrdata import GetOrgAsnFromIRROnLine
from ana_inconformity import FindTraceAsInBgpPath, ClassifyAbTrace2, PrintDetourDict
from ana_prefix_traceroute_group_by_prefix_v2 import CompressAsPath, CompressAsPathToMin, CheckTracesByIRRData
from utils_v2 import GetSibRel, AsnInTracePathList, DropStarsInTraceList, PathHasValley, GetBgpByPrefix, GetBgpPathFromBgpPrefixDict_2, ClearBGPByPrefix

def GetIpPath(trace_seg):
    print(trace_seg)
    ip_seg_dict = dict()
    dst_as_set = set()
    num = 0
    modi_trace_seg1 = ' ' + trace_seg + ' '
    modi_trace_seg2 = ' ' + trace_seg + '\n'
    for year in range(2018,2021):
        for month in range(1,13):
            if (year == 2016 and month < 4) or (year == 2020 and month > 4): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
                continue
            month_str = str(month).zfill(2)
            date = str(year) + month_str + '15'
            for vp in global_var.vps:
                filename = global_var.par_path + global_var.out_my_anatrace_dir + '/' + vp + '_' + date + '/ribs_midar_bdrmapit/ana_ab_4_detour'
                if not os.path.exists(filename):
                    continue
                #print(filename)
                with open(filename, 'r') as rf:
                    curline_trace = rf.readline()
                    while curline_trace:
                        curline_bgp = rf.readline()
                        curline_ip = rf.readline()
                        if (not curline_trace.__contains__(modi_trace_seg1)) and \
                            (not curline_trace.__contains__(modi_trace_seg2)):
                            curline_trace = rf.readline()
                            continue
                        num += 1
                        trace_seg_list = trace_seg.split(' ')
                        trace_list = curline_trace.strip('\n').split(' ')
                        ip_list = curline_ip.strip('\n').split(' ')
                        first_index = -1
                        if curline_trace.__contains__(modi_trace_seg1):
                            first_index = (curline_trace[:curline_trace.index(modi_trace_seg1)]).count(' ') + 1
                        else:
                            first_index = (curline_trace[:curline_trace.index(modi_trace_seg2)]).count(' ') + 1
                        last_index = first_index + trace_seg.count(' ')
                        print(trace_list[first_index:last_index + 1])
                        ip_seg = ' '.join(ip_list[first_index:last_index + 1])
                        if ip_seg not in ip_seg_dict.keys():
                            ip_seg_dict[ip_seg] = set()
                        ip_seg_dict[ip_seg].add(filename)
                        dst_as_set.add(trace_list[1].strip(']'))
                        curline_trace = rf.readline()
    '''
    first_ab_ip_set = set()
    for (key, val) in ip_seg_dict.items():
        print(key)
        for elem in val:
            print('\t' + elem)
        first_ab_ip_set.add(key.split(' ')[1])
    print('\n')
    #for elem in dst_as_set:
        #print(elem)
    #print('\nnum: %d' %num)
    return first_ab_ip_set
    '''
    return ip_seg_dict

ip_irr_cache = dict()
def GetIrrDataOfIp(ip):
    global ip_irr_cache
    org_set = set()
    asn_set = set()
    if ip not in ip_irr_cache.keys():
        (org_set, asn_set) = GetOrgAsnFromIRROnLine(ip)
        ip_irr_cache[ip] = [org_set, asn_set]
    else:
        [org_set, asn_set] = ip_irr_cache[ip]
    return (org_set, asn_set)

def AnaDetourSeg(filename, w_filename):
    rf = open(filename, 'r')
    wf = open(w_filename, 'w')
    curline = rf.readline()
    bgp_seg = ''
    while curline:
        if not curline.startswith('\t'): #(src, dst)对，不分析
            curline = rf.readline()
            continue
        if not curline.startswith('\t\t'): #bgp seg
            bgp_seg = curline.strip('\t').split(':')[0]
            curline = rf.readline()
            continue
        #curline.startswith('\t\t'): trace seg
        trace_seg = curline.strip('\t').split(':')[0]
        wf.write(trace_seg + '\n')
        ip_seg_dict = GetIpPath(trace_seg)
        for ip_seg in ip_seg_dict.keys():
            wf.write('\t')
            ip_seg_list = ip_seg.split(' ')
            for ip in ip_seg_list:
                if ip.__contains__('<') or ip == '*':
                    continue
                (org_set, asn_set) = GetIrrDataOfIp(ip)
                wf.write('{')
                for elem in org_set:
                    wf.write(elem + ',')
                wf.write('} ')
            wf.write('\n')
        curline = rf.readline()
    rf.close()
    wf.close()

def AnaDetourSeg_2(filename):
    print(filename)
    rf = open(filename, 'r')
    wf_sub = open(filename[:filename.rindex('/')] + '/detour_sub', 'w')
    wf_inc = open(filename[:filename.rindex('/')] + '/detour_inc', 'w')
    wf_dec = open(filename[:filename.rindex('/')] + '/detour_dec', 'w')
    inc_info_dict = dict()
    curline = rf.readline()
    src = ''
    dst = ''
    bgp_mid_list = []
    bgp_seg = ''
    while curline:
        if not curline.startswith('\t'): #(src, dst)对
            src_dst_pair = curline[:curline.index(':')].split(' ')
            src = src_dst_pair[0]
            dst = src_dst_pair[1]
            curline = rf.readline()
            continue
        if not curline.startswith('\t\t'): #bgp seg
            bgp_seg = curline.strip('\t').split(':')[0]
            bgp_mid_list = []
            for elem in bgp_seg.split(' '):
                if not (elem.__contains__(src) or elem.__contains__(dst)):
                    bgp_mid_list.append(elem)
            curline = rf.readline()
            continue
        #curline.startswith('\t\t'): trace seg
        trace_seg = curline.strip('\t').split(':')[0]
        trace_seg_list = trace_seg.split(' ')
        trace_mid_list = []
        for elem in trace_seg_list:
            if (not elem.__contains__(src)) and (not elem.__contains__(dst)) and elem != '?' and elem != '*':
                if not AsnInTracePathList(elem, trace_mid_list):
                    trace_mid_list.append(elem)
        if len(trace_mid_list) == len(bgp_mid_list): #sub
            wf_sub.write("%s\n" %bgp_seg)
            wf_sub.write("\t%s\n" %trace_seg)
        elif len(trace_mid_list) < len(bgp_mid_list): #dec
            wf_dec.write("%s\n" %bgp_seg)
            wf_dec.write("\t%s\n" %trace_seg)
        else: #inc，这里要分类，具体inc了多少
            norm_count = 0
            for i in range(0, len(trace_seg_list)):
                elem = trace_seg_list[i]
                if elem.__contains__(src) or elem == '?' or elem == '*':
                    norm_count += 1
                else:
                    break
            for i in range(len(trace_seg_list) - 1, 0, -1):
                elem = trace_seg_list[i]
                if elem.__contains__(dst) or elem == '?' or elem == '*':
                    norm_count += 1
                else:
                    break
            if trace_seg in inc_info_dict.keys(): #按理说不应该出现这种情况，打印，先覆盖掉原来的信息
                print("NOTE. Duplicate trace: %s" %trace_seg)
                print("bgp_seg1: %s" %inc_info_dict[trace_seg][0])
                print("bgp_seg2: %s" %bgp_seg)
                print("curline: %s" %curline)
                return
            inc_info_dict[trace_seg] = [bgp_seg, len(trace_mid_list) - len(bgp_mid_list), len(trace_seg_list) - norm_count]
        curline = rf.readline()
    rf.close()
    trace_sort_list = sorted(inc_info_dict.items(), key=lambda d:(d[1][1], d[1][2]), reverse=True)
    bgp_dict = dict() #按bgp_seg将trace_sort_list分类后再写入文件
    for elem in trace_sort_list:
        bgp_seg = elem[1][0]
        if bgp_seg not in bgp_dict.keys():
            bgp_dict[bgp_seg] = []
        bgp_dict[bgp_seg].append([elem[0], elem[1][1], elem[1][2]])
    for bgp_seg in bgp_dict.keys():
        wf_inc.write(bgp_seg + '\n')
        for elem in bgp_dict[bgp_seg]:
            wf_inc.write("\t%s(%d, %d)\n" %(elem[0], elem[1], elem[2]))
    wf_sub.close()
    wf_dec.close()
    wf_inc.close()

trace_dict = dict()
def SearchTraceDateInSpecFiles(filename, obj_trace): #在名为filename的文件中找trace，返回文件的日期
    global trace_dict
    path_set = set()

    if len(trace_dict) == 0:
        for year in range(2018,2021):
            for month in range(1,13):
                if (year == 2016 and month < 4) or (year == 2020 and month > 4): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
                    continue
                date = str(year) + str(month).zfill(2) + '15'
                for vp in global_var.vps:
                    cur_file = global_var.par_path +  global_var.out_my_anatrace_dir + '/' + vp + '_' + date + '/ribs_midar_bdrmapit/' + filename
                    trace_dict[vp + '_' + date] = []
                    with open(cur_file, 'r') as rf:
                        trace_line = rf.readline()
                        while trace_line:
                            bgp_line = rf.readline()
                            ip_line = rf.readline()
                            trace_dict[vp + '_' + date].append(trace_line)
                            trace_line = rf.readline()
    for (key, trace_list) in trace_dict.items():
        for elem in trace_list:
            if elem.__contains__(' ' + obj_trace + '\n') or  elem.__contains__(' ' + obj_trace + ' '):
                path_set.add(key)
                break
    return path_set

def FilterInstDetour(filename):
    rf = open(filename, 'r')
    wf = open(filename[:filename.rindex('/')] + '/detour_dict_stable', 'w')
    new_detour_dict = dict()
    src_dst_pair = ''
    src_dst_pair_count = 0
    bgp_seg = ''
    bgp_count = 0
    curline = rf.readline()
    while curline:
        if not curline.startswith('\t'): #(src, dst)对
            #print(curline)
            if bgp_seg: #先处理前面的bgp_seg
                if len(new_detour_dict[src_dst_pair][0][bgp_seg][0]) == 0: #该bgp_seg不记录
                    new_detour_dict[src_dst_pair][0].pop(bgp_seg)
                else:
                    new_detour_dict[src_dst_pair][0][bgp_seg][1] = bgp_count
                bgp_seg = ''
                bgp_count = 0
            if src_dst_pair: #处理前面的src_dst_pair
                if len(new_detour_dict[src_dst_pair][0]) == 0: #该src_dst_pair不记录
                    new_detour_dict.pop(src_dst_pair)
                else:
                    new_detour_dict[src_dst_pair][1] = src_dst_pair_count
            src_dst_pair = curline[:curline.index(':')]
            src_dst_pair_count = 0
            new_detour_dict[src_dst_pair] = [dict(), 0]
            curline = rf.readline()
            continue
        if not curline.startswith('\t\t'): #bgp seg
            if bgp_seg: #处理前面的bgp_seg
                if len(new_detour_dict[src_dst_pair][0][bgp_seg][0]) == 0: #该bgp_seg不记录
                    new_detour_dict[src_dst_pair][0].pop(bgp_seg)
                else:
                    new_detour_dict[src_dst_pair][0][bgp_seg][1] = bgp_count
            bgp_seg = curline.strip('\t').split(':')[0]
            bgp_count = 0
            new_detour_dict[src_dst_pair][0][bgp_seg] = [dict(), 0]
            curline = rf.readline()
            continue
        #curline.startswith('\t\t'): trace seg
        trace_seg = curline.strip('\t').split(':')[0]
        trace_count = int(curline[(curline.index(':') + 2):curline.index('(')])
        writable = True
        if trace_count < 10:
            path_set = SearchTraceDateInSpecFiles('ana_ab_4_detour', trace_seg)
            if len(path_set) < 2: #只在一个文件里出现过
                writable = False #transient trace，不记录
        if writable:
            #print("write trace %s in bgp_seg %s" %(trace_seg, bgp_seg))
            new_detour_dict[src_dst_pair][0][bgp_seg][0][trace_seg] = trace_count
            bgp_count += trace_count
            src_dst_pair_count += trace_count
        curline = rf.readline()
    rf.close()
    if bgp_seg: #先处理前面的bgp_seg
        if len(new_detour_dict[src_dst_pair][0][bgp_seg][0]) == 0: #该bgp_seg不记录
            new_detour_dict[src_dst_pair][0].pop(bgp_seg)
        else:
            new_detour_dict[src_dst_pair][0][bgp_seg][1] = bgp_count
        bgp_seg = ''
        bgp_count = 0
    if src_dst_pair: #处理前面的src_dst_pair
        if len(new_detour_dict[src_dst_pair][0]) == 0: #该src_dst_pair不记录
            new_detour_dict.pop(src_dst_pair)
        else:
            new_detour_dict[src_dst_pair][1] = src_dst_pair_count
    total_count = 0
    for (src_dst_pair, val) in new_detour_dict.items():
        total_count += val[1]
    for (src_dst_pair, val) in new_detour_dict.items():
        wf.write("%s:%d(%.2f)\n" %(src_dst_pair, val[1], val[1] / total_count))
        for (bgp_seg, val1) in val[0].items():
            wf.write("\t%s:%d(%.2f)\n" %(bgp_seg, val1[1], val1[1] / total_count))
            for (trace_seg, val2) in val1[0].items():
                wf.write("\t\t%s:%d(%.2f)\n" %(trace_seg, val2, val2 / total_count))
    wf.close()

def StatDetourSegCount(filename):
    rf = open(filename, 'r')
    curline = rf.readline()
    bgp_seg = ''
    total_num = 0
    num = 0
    count = 0
    total_count = 0
    while curline:
        if not curline.startswith('\t'): #(src, dst)对
            total_num += 1
            tmp_count = int(curline[(curline.index(':') + 1):curline.index('(')])
            total_count += tmp_count
            if tmp_count < 10:
                num += 1
                count += tmp_count
        curline = rf.readline()
    rf.close()
    print("total_num: %d" %total_num)
    print("num: %d" %num)
    print("total_count: %d" %total_count)
    print("count: %d" %count)

#re_trace_seg = re.compile(r'(3491 \d+_58593)')
def GetTraceOfSpecSeg(re_trace_seg):
    res_dict = dict()
    for year in range(2018,2021):
        for month in range(1,13):
            if (year == 2016 and month < 4) or (year == 2020 and month > 4): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
                continue
            month_str = str(month).zfill(2)
            date = str(year) + month_str + '15'
            for vp in global_var.vps:
                parent_dir = global_var.par_path + global_var.out_my_anatrace_dir + '/' + vp + '_' + date + '/ribs_midar_bdrmapit/'
                filenames = ['3_single_path_fin', '4_multi_path_fin', '5_has_ixp_ip']
                for filename in filenames:
                    path = parent_dir + filename
                    if not os.path.exists(path):
                        continue
                    with open(path, 'r') as rf:
                        curline_trace = rf.readline()
                        while curline_trace:
                            curline_ip = rf.readline()
                            res = re.findall(re_trace_seg, curline_trace)
                            if res:
                                for elem in res:
                                    if elem not in res_dict.keys():
                                        res_dict[elem] = set()
                                    res_dict[elem].add(path)
                            curline_trace = rf.readline()
    for (key, val) in res_dict.items():
        print(key)
        for elem in val:
            print('\t' + elem)

def GetBgpOfSpecBgpSeg(re_bgp_seg):
    res_dict = dict()
    dir_path = global_var.par_path + global_var.rib_dir + '/bgpdata/'
    file_list = os.listdir(dir_path)
    for filename in file_list:
        if os.path.isfile(dir_path + filename):
            print(filename)
            with open(dir_path + filename, 'r') as rf:
                curline = rf.readline()
                while curline:
                    elems = curline.split('|')
                    res = re.findall(re_bgp_seg, elems[2])
                    if res:
                        for elem in res:
                            if elem not in res_dict.keys():
                                res_dict[elem] = set()
                            res_dict[elem].add(filename)
                    curline = rf.readline()
    for (key, val) in res_dict.items():
        print(key)
        for elem in val:
            print('\t' + elem)

def GetWhatsFiltered(filename1, filename2, filename3):
    tmp_set = set()
    with open(filename2, 'r') as rf2:
        curline_trace = rf2.readline()
        while curline_trace:
            curline_bgp = rf2.readline()
            curline_ip = rf2.readline()
            tmp_set.add(curline_trace)
            curline_trace = rf2.readline()
    wf = open(filename3, 'w')
    with open(filename1, 'r') as rf1:
        curline_trace = rf1.readline()
        while curline_trace:
            curline_bgp = rf1.readline()
            curline_ip = rf1.readline()
            if curline_trace not in tmp_set:
                wf.write("%s%s%s" %(curline_trace, curline_bgp, curline_ip))
            curline_trace = rf1.readline()
    wf.close()

def TmpCheckDetourByIrr():
    PreGetSrcFilesInDirs()
    detour_dict = dict()
    #for year in range(2018,2021):
    for year in range(2018,2019):
        #for month in range(1,13):
        for month in range(12,13):
            if (year == 2016 and month < 4) or (year == 2020 and month > 4): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
                continue
            GetSibRel(year, month)
            #for vp in global_var.vps:
            for vp in ['tmp']:
                date = str(year) + str(month).zfill(2) + '15'
                #filename = global_var.par_path +  global_var.out_my_anatrace_dir + '/' + vp + '_' + date + '/ribs_midar_bdrmapit/ana_ab_4_detour' #'nrt-jp.2019030115/'
                filename = 'tmp_detour_test'
                (count_ab, count_total) = CheckTracesByIRRData(filename, True, False)
                ClassifyAbTrace2(filename + '_ab', 'tmp_last_extra', 'tmp_first_hop_ab', 'tmp_detour', 'tmp_bifurc', None, 'a', detour_dict)
                os.remove(filename + '_ab')
    if len(detour_dict) > 0:
        PrintDetourDict(detour_dict, 'tmp_detour_dict')

def BgpSegInTraceList(bgp_seg, trace_list):
    #print('check begin')
    bgp_list = bgp_seg.split(' ')
    start_index_list = []
    start_bgp = bgp_list[0]
    for i in range(0, len(trace_list)):
        if start_bgp in trace_list[i].split('_'):
            start_index_list.append(i)    
    for start_index in start_index_list:
        bgp_index = 0
        for trace_index in range(start_index, len(trace_list)):
            if bgp_list[bgp_index] in trace_list[trace_index].split('_'):
                continue
            if (bgp_index + 1) == len(bgp_list): #匹配结束
                break
            if bgp_list[bgp_index + 1] in trace_list[trace_index].split('_'):
                bgp_index += 1
                continue
            break #not match
        if (bgp_index + 1) == len(bgp_list): #匹配结束
            #print('check end')
            return True
    #print('check end')
    return False

all_traces_set = set()
#重写需要删除global_var.par_path + global_var.other_middle_data_dir + 'all_traces'文件
def PreGetAllTraces():
    global all_traces_set
    obj_filename = global_var.par_path + global_var.other_middle_data_dir + 'all_traces'
    if not os.path.exists(obj_filename):
        print('Construct all_traces file')
        for year in range(2018,2021):
            for month in range(1,13):
                if (year == 2016 and month < 4) or (year == 2020 and month > 4): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
                    continue
                month_str = str(month).zfill(2)
                date = str(year) + month_str + '15'
                for vp in global_var.vps:
                    parent_dir = global_var.par_path + global_var.out_my_anatrace_dir + '/' + vp + '_' + date + '/ribs_midar_bdrmapit/'
                    filenames = ['final_normal_fin', 'final_unmap_fin', 'final_ab_fin']
                    for filename in filenames:
                        path = parent_dir + filename
                        print(path)
                        if not os.path.exists(path):
                            continue
                        with open(path, 'r') as rf:
                            curline_trace = rf.readline()
                            while curline_trace:
                                #print(curline_trace)
                                curline_bgp = rf.readline()
                                curline_ip = rf.readline()
                                ori_trace_path = curline_trace[curline_trace.index(']') + 1:].strip('\n').strip(' ')
                                trace_path = CompressAsPathToMin(CompressAsPath(ori_trace_path))
                                all_traces_set.add(trace_path)
                                curline_trace = rf.readline()
        print(len(all_traces_set))
        with open(obj_filename, 'w') as wf:
            wf.write(';'.join(all_traces_set))
    else:
        print('Get all traces from file')
        with open(obj_filename, 'r') as rf:
            data = rf.read()
        all_traces_set = set(data.strip(';').split(';'))
        print(len(all_traces_set))

#7575 7575_1221 1221 1221_4637 4637 4637_9498 9498 9498_58717 58717 58717_138403 58717 58717_138403
def TransOneTraceToUniASTraces(trace):
    #print(trace)
    if not trace.__contains__(' '):
        return set(trace.split('_'))
    res_trace_set = set()
    for elem in trace.split(' ')[0].split('_'):
        sub_trace_set = TransOneTraceToUniASTraces(trace[trace.index(' ') + 1:])
        for sub_trace in sub_trace_set:
            tmp = CompressAsPathToMin(CompressAsPath(elem + ' ' + sub_trace))
            res_trace_set.add(tmp)
    return res_trace_set

def PreTransAllTraceToUniASTraces():
    global all_traces_set
    PreGetAllTraces()
    res_trace_set = set()
    for trace in all_traces_set:
        #print(trace)
        cur_trace_set = TransOneTraceToUniASTraces(trace)
        res_trace_set |= cur_trace_set
    print(len(res_trace_set))
    with open(global_var.par_path + global_var.other_middle_data_dir + 'all_traces_uni_as', 'w') as wf:
        wf.write(';'.join(res_trace_set))
    return res_trace_set

all_trace_set_uni_as = set()
def PreGetAllTracesWithUniAs():
    global all_trace_set_uni_as
    with open(global_var.par_path + global_var.other_middle_data_dir + 'all_traces_uni_as', 'r') as rf:
        data = rf.read()
    all_trace_set_uni_as = data.split(';')
    print(len(all_trace_set_uni_as))

def FindBgpSegListInTraces(bgp_seg_list):    
    global all_trace_set_uni_as
    PreGetAllTracesWithUniAs()
    checked_seg_list = []
    for bgp_seg in bgp_seg_list:
        print(bgp_seg)
        for trace in all_trace_set_uni_as:
            #if BgpSegInTraceList(bgp_seg, trace.split(' ')):
            if trace.__contains__(bgp_seg):
                checked_seg_list.append(bgp_seg)
                break
    for bgp_seg in checked_seg_list:
        bgp_seg_list.remove(bgp_seg)                       
    print("Remain bgp_seg num: %d" %len(bgp_seg_list))   
    return bgp_seg_list

def CheckBgpSegInTraces(bgp_seg):    
    global all_trace_set_uni_as
    for trace in all_trace_set_uni_as:
        #if BgpSegInTraceList(bgp_seg, trace.split(' ')):
        if trace.__contains__(bgp_seg):
            return True
    return False
    
def StatAstInTraces():    
    total_count = 0
    ast_count = 0
    ab_count = 0
    ab_ast_count = 0
    for year in range(2018,2021):
        for month in range(1,13):
            if (year == 2016 and month < 4) or (year == 2020 and month > 4): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
                continue
            month_str = str(month).zfill(2)
            date = str(year) + month_str + '15'
            for vp in global_var.vps:
                parent_dir = global_var.par_path + global_var.out_my_anatrace_dir + '/' + vp + '_' + date + '/ribs_midar_bdrmapit/'
                print(parent_dir)
                filenames_1 = ['3_single_path_fin', '4_multi_path_fin', '5_has_ixp_ip']
                for filename in filenames_1:
                    with open(parent_dir + filename, 'r') as rf:
                        curline_trace = rf.readline()
                        while curline_trace:
                            total_count += 1
                            curline_ip = rf.readline()
                            if curline_trace.__contains__('*'):
                                ast_count += 1
                            curline_trace = rf.readline()
                filenames_2 = ['final_ab']
                for filename in filenames_2:
                    with open(parent_dir + filename, 'r') as rf:
                        curline_trace = rf.readline()
                        while curline_trace:
                            ab_count += 1
                            curline_ip = rf.readline()
                            if curline_trace.__contains__('*'):
                                ab_ast_count += 1
                            curline_trace = rf.readline()
    print("total_count: %d" %total_count)
    print("ast_count: %d" %ast_count)
    print("ab_count: %d" %ab_count)
    print("ab_ast_count: %d" %ab_ast_count)


def CheckIfBgpPathExistsInTrace_Deleted(filename):    
    rf = open(filename, 'r')
    bgp_seg_list = []
    curline = rf.readline()
    while curline:
        if curline.startswith('\t') and not curline.startswith('\t\t'): #bgp_seg
            bgp_seg_list.append(curline[curline.index('\t') + 1:curline.index(':')])
        curline = rf.readline()
    rf.close()
    print(len(bgp_seg_list))
    print(bgp_seg_list[:3])
    rem_bgp_seg_list = FindBgpSegListInTraces(bgp_seg_list)
    wf = open(filename[:filename.rindex('/')] + '/detour_dict_bgppath_not_in_trace', 'w')
    rf = open(filename, 'r')
    curline = rf.readline()
    while curline:
        if not curline.startswith('\t'): #bgp_seg
            if curline.strip('\n') in rem_bgp_seg_list: #abnormal bgp_seg
                wf.write(curline)
                writable = True
            else:
                writable = False
        else:
            if writable:
                wf.write(curline)
        curline = rf.readline()
    rf.close()
    wf.close()

trace_links_set = set()
def GetAllTraceLinks():
    global trace_links_set
    global all_trace_set_uni_as
    PreGetAllTracesWithUniAs()
    data_path = global_var.par_path + global_var.other_middle_data_dir + 'tracelinks'
    if not os.path.exists(data_path):
        for trace in all_trace_set_uni_as:
            trace_list = trace.split(' ')
            for i in range(0, len(trace_list) - 1):
                link = trace_list[i] + ' ' + trace_list[i + 1]
                if link not in trace_links_set:
                    trace_links_set.add(link)
        with open(data_path, 'w') as wf:
            wf.write('\n'.join(list(trace_links_set)))
    else:
        with open(data_path, 'r') as rf:
            data = rf.read()
            trace_links_set = set(data.split('\n'))

def AnaNextHopOfOneTracePath(src, dst, nexthop_list = None):
    total_count = 0
    count_list = []
    count_dict = dict()
    if nexthop_list:
        count_list = [0 for i in range(len(nexthop_list))]
    for year in range(2018,2021):
        for month in range(1,13):
            if (year == 2016 and month < 4) or (year == 2020 and month > 4): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
                continue
            month_str = str(month).zfill(2)
            date = str(year) + month_str + '15'
            for vp in global_var.vps:
                parent_dir = global_var.par_path + global_var.out_my_anatrace_dir + '/' + vp + '_' + date + '/ribs_midar_bdrmapit/'
                #print(parent_dir)
                filenames = ['final_normal_fin', 'final_unmap_fin', 'final_ab_fin']
                for filename in filenames:
                    path = parent_dir + filename
                    if not os.path.exists(path):
                        continue
                    print(path)
                    with open(path, 'r') as rf:
                        curline_trace = rf.readline()
                        while curline_trace:
                            curline_bgp = rf.readline()
                            curline_ip = rf.readline()
                            ori_trace_path = curline_trace[curline_trace.index(']') + 1:].strip('\n').strip(' ')
                            trace_path = CompressAsPathToMin(CompressAsPath(ori_trace_path))
                            comp_trace_path = ' '.join(DropStarsInTraceList(trace_path.split(' '))) #2021.7.11不应删去*和？
                            if comp_trace_path.__contains__(src + ' ') and comp_trace_path.__contains__(' ' + dst):
                                total_count += 1
                                index = comp_trace_path.index(src + ' ')
                                if comp_trace_path[index + len(src + ' '):].__contains__(' '):
                                    nexthop = comp_trace_path[index + len(src + ' '):comp_trace_path.index(' ', index + len(src + ' '))]
                                else:
                                    nexthop = comp_trace_path[index + len(src + ' '):]
                                if nexthop_list:
                                    for i in range(0, len(nexthop_list)):
                                        if nexthop.__contains__(nexthop_list[i]):
                                            count_list[i] += 1
                                else:
                                    if nexthop not in count_dict.keys():
                                        count_dict[nexthop] = 0
                                    count_dict[nexthop] += 1
                            curline_trace = rf.readline()
    print(total_count)
    if count_list:
        for i in range(0, len(nexthop_list)):
            print(nexthop_list[i] + ': ' + str(count_list[i]))
    else:
        for (nexthop, count) in count_dict.items():
            print(nexthop + ': ' + str(count))

def AnaNextHopOfOneBgpPath(src, dst, nexthop_list = None):
    total_count = 0
    count_list = None
    count_dict = dict()
    if nexthop_list:
        count_list = [set() for i in range(len(nexthop_list))]
    for year in range(2018,2021):
        for month in range(1,13):
            if (year == 2016 and month < 4) or (year == 2020 and month > 4): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
                continue
            month_str = str(month).zfill(2)
            date = str(year) + month_str + '15'
            for vp in global_var.vps:
            #for vp in ['nrt-jp']:
                filename = 'bgp_' + global_var.trace_as_dict[vp] + '_' + date
                print(filename)
                with open(global_var.par_path + global_var.rib_dir + '/bgpdata/' + filename, 'r') as rf:
                    curline = rf.readline()
                    while curline:
                        elems = curline.split('|')
                        cur_path = elems[2]
                        comp_trace_path = CompressAsPathToMin(CompressAsPath(cur_path))
                        if comp_trace_path.__contains__(src + ' ') and comp_trace_path.__contains__(' ' + dst):
                            total_count += 1
                            index = comp_trace_path.index(src + ' ')
                            if comp_trace_path[index + len(src + ' '):].__contains__(' '):
                                nexthop = comp_trace_path[index + len(src + ' '):comp_trace_path.index(' ', index + len(src + ' '))]
                            else:
                                nexthop = comp_trace_path[index + len(src + ' '):]
                            if nexthop_list:
                                for i in range(0, len(nexthop_list)):
                                    if nexthop.__contains__(nexthop_list[i]):
                                        count_list[i].add(str(date + '_' + vp))
                            else:
                                if nexthop not in count_dict.keys():
                                    count_dict[nexthop] = set()
                                count_dict[nexthop].add(str(date + '_' + vp))
                        curline = rf.readline()
    print(total_count)
    if nexthop_list:
        for i in range(0, len(count_list)):
            print(nexthop_list[i] + ': ')
            for elem in count_list[i]:
                print('\t' + elem)
    else:
        for (nexthop, date_set) in count_dict.items():
            print(nexthop + ': ')
            for elem in date_set:
                print('\t' + elem)

def BgpLinksInTrace(link):
    global trace_links_set
    GetAllTraceLinks()
    elems = link.split(' ')
    rev_link = elems[1] + ' ' + elems[0]
    if link in trace_links_set or rev_link in trace_links_set:
        return True
    return False

def GetTraceDate(trace):
    date_set = set()
    for year in range(2018,2021):
        for month in range(1,13):
            if (year == 2016 and month < 4) or (year == 2020 and month > 4): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
                continue
            month_str = str(month).zfill(2)
            date = str(year) + month_str + '15'
            for vp in global_var.vps:
                parent_dir = global_var.par_path + global_var.out_my_anatrace_dir + '/' + vp + '_' + date + '/ribs_midar_bdrmapit/'
                filenames = ['ana_ab_4_detour']
                for filename in filenames:
                    path = parent_dir + filename
                    if not os.path.exists(path):
                        continue
                    with open(path, 'r') as rf:
                        curline_trace = rf.readline()
                        while curline_trace:
                            curline_bgp = rf.readline()
                            curline_ip = rf.readline()
                            if curline_trace.__contains__(trace):
                                date_set.add(date)
                            curline_trace = rf.readline()
    return date_set

g_bgp_paths_dict = dict()
# def GetAllBgpPathsWithDate():
#     global bgp_paths_dict
#     for year in range(2017,2021):
#         for month in range(1,13):
#             if (year == 2016 and month < 4) or (year == 2020 and month > 4): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
#                 continue
#             month_str = str(month).zfill(2)
#             date = str(year) + month_str + '15'
#             bgp_paths_dict[date] = set()
#             for vp in global_var.vps:
#                 filename = 'bgp_' + global_var.trace_as_dict[vp] + '_' + date
#                 print(filename)
#                 with open(global_var.par_path + global_var.rib_dir + '/bgpdata/' + filename, 'r') as rf:
#                     curline = rf.readline()
#                     while curline:
#                         elems = curline.split('|')
#                         cur_path = CompressAsPathToMin(CompressAsPath(elems[2]))
#                         bgp_paths_dict[date].add(cur_path)
#                         curline = rf.readline()

def PreGetAllBgpPaths():
    global g_bgp_paths_dict
    filepath = global_var.par_path + global_var.other_middle_data_dir + 'all_bgp_paths'
    if not os.path.exists(filepath):
        for year in range(2018,2021):
            for month in range(1,13):
                if (year == 2016 and month < 4) or (year == 2020 and month > 4): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
                    continue
                month_str = str(month).zfill(2)
                date = str(year) + month_str + '15'
                for vp in global_var.vps:
                    filename = 'bgp_' + global_var.trace_as_dict[vp] + '_' + date
                    print(filename)
                    with open(global_var.par_path + global_var.rib_dir + '/bgpdata/' + filename, 'r') as rf:
                        curline = rf.readline()
                        while curline:
                            elems = curline.split('|')
                            cur_path = CompressAsPathToMin(CompressAsPath(elems[2]))
                            if cur_path not in g_bgp_paths_dict.keys():
                                g_bgp_paths_dict[cur_path] = dict()
                            locate = date + '_' + vp
                            if locate not in g_bgp_paths_dict[cur_path].keys():
                                g_bgp_paths_dict[cur_path][locate] = 0
                            g_bgp_paths_dict[cur_path][locate] += 1
                            curline = rf.readline()
        with open(filepath, 'w') as wf:
            for (bgp_path, info) in g_bgp_paths_dict.items():
                wf.write(bgp_path + ':')
                for (locate, num) in info.items():
                    wf.write(locate + ' ' + str(num) + ',')
                wf.write(';')
    else:
        with open(filepath, 'r') as rf:
            data_list = rf.read().strip(';').split(';')
            for elem in data_list:
                items = elem.split(':')
                g_bgp_paths_dict[items[0]] = dict()
                for sub_elem in items[1].strip(',').split(','):
                    tmp = sub_elem.split(' ')
                    g_bgp_paths_dict[items[0]][tmp[0]] = int(tmp[1])
            print(len(g_bgp_paths_dict))

def CheckTraceSegInBgp(trace_seg):
    global g_bgp_paths_dict
    trace_seg_set = TransOneTraceToUniASTraces(trace_seg)
    for tmp_trace_seg in trace_seg_set:
        for bgp_path in g_bgp_paths_dict.keys():
            if bgp_path.__contains__(tmp_trace_seg):
                return True
    return False

def PickEarliestDate(date_set):
    year = 3000
    month = 0
    for elem in date_set:
        if (int(elem[:4]) < year) or ((int(elem[:4]) == year) and (int(elem[4:6]) < month)):
            year = int(elem[:4])
            month = int(elem[4:6])
    return (year, month)

def BgpPathExistsEarlier(given_year, given_month, path):
    global g_bgp_paths_dict
    for (bgp_path, info) in g_bgp_paths_dict:
        if bgp_path.__contains__(path):
            for locate in info.keys():
                year = int(locate[:4])
                month = int(locate[4:6])
                if (year < given_year) or ((year == given_year) and (month < given_month)):
                    return True
    return False

def GetBgpDates(bgp_seg):
    global g_bgp_paths_dict
    dates_set = set()
    for (bgp_path, info) in g_bgp_paths_dict.items():
        if bgp_path.__contains__(bgp_seg):
            for locate in info.keys():
                date = locate[:6]
            dates_set.add(date)
    return dates_set


transient_bgp_date_count = 1
transient_bgp_occur_count = 20
def BgpSegIsTransient(bgp_seg):
    global g_bgp_paths_dict
    dates_set = set()
    occur_count = 0
    for (bgp_path, info) in g_bgp_paths_dict.items():
        if bgp_path.__contains__(bgp_seg):
            for (locate, count) in info.items():
                date = locate[:6]
                dates_set.add(date)
                occur_count += count
    if len(dates_set) > transient_bgp_date_count or occur_count > transient_bgp_occur_count:
        return False #not transient
    return True

def CheckBgpPathNorm(filename):
    global trace_links_set
    PreGetAllBgpPaths()
    GetAllTraceLinks()
    rf = open(filename, 'r')
    wf_ab_link = open(filename[:filename.rindex('/')] + '/detour_dict_ab_link', 'w')
    wf_has_valley = open(filename[:filename.rindex('/')] + '/detour_dict_has_valley', 'w')
    count_total = 0
    count_ab_link = 0
    count_has_valley = 0
    curline = rf.readline()
    while curline:
        count_total += 1
        tmp = curline.strip('\n').strip('\t')
        bgp_seg = tmp[tmp.index(';') + 1:tmp.index(':')]
        cur_wf = None
        #step 1, check if bgp link newly announced
        # print('step 1')
        # (year, month) = PickEarliestDate(date_set)
        # print(str(year) + str(month).zfill(2))
        # if not BgpPathExistsEarlier(year, month, bgp_path):
        #     count_new_bgp_path += 1
        #     cur_wf = wf_new_bgp_path
        #step 2, check bgp link ever existed in traceroute
        if not cur_wf:
            prev_elem = ''
            for elem in bgp_seg.split(' '):
                if prev_elem != '':
                    link1 = prev_elem + ' ' + elem
                    link2 = elem + ' ' + prev_elem
                    if (link1 not in trace_links_set) and (link2 not in trace_links_set): #abnormal link
                        count_ab_link += 1
                        cur_wf = wf_ab_link
                        break
                prev_elem = elem
        #step 3, check if bgp path has valley
        if not cur_wf:
            print('step 3')
            fin_has_no_valley = False
            dates_set = GetBgpDates(bgp_seg)
            for date in dates_set:
                (has_rel, has_valley) = PathHasValley(int(date[:4]), int(date[4:6]), bgp_seg)
                if not has_rel:
                    print("not has_rel")
                if has_rel and (not has_valley): #宽容做法，只要有一个日期内bgp path正常即认为正常
                    fin_has_no_valley = True
                    break
            if not fin_has_no_valley:
                count_has_valley += 1
                cur_wf = wf_has_valley
        #step 4, record
        if cur_wf:
            print('step 4')
            cur_wf.write(curline)
    rf.close()
    #wf_new_bgp_path.close()
    wf_ab_link.close()
    wf_has_valley.close()
    print("count_total: %d" %count_total)
    #print("count_new_bgp_path: %d" %count_new_bgp_path)
    print("count_ab_link: %d" %count_ab_link)
    print("count_has_valley: %d" %count_has_valley)

def AnaCount(filename):    
    rf = open(filename, 'r')
    curline = rf.readline()
    count = 0
    count1 = 0
    while curline:
        if not curline.startswith('\t'): #(src, dst)对，不分析
            count += 1
            curline = rf.readline()
            continue
        #if not curline.startswith('\t\t'): #bgp seg
            #curline = rf.readline()
            #continue
        count1 += 1
        curline = rf.readline()
    print(count)
    print(count1)

def TmpAnaMulBgpUpdates(filename):
    #total_count = 0
    for year in range(2018,2021):
        for month in range(1,13):
            if (year == 2016 and month < 4) or (year == 2020 and month > 4): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
                continue
            month_str = str(month).zfill(2)
            date = str(year) + month_str + '15'
            for vp in global_var.vps:
                g_asn = global_var.trace_as_dict[vp]    
                bgp_filename = global_var.par_path + global_var.rib_dir + 'bgpdata/bgp_' + g_asn + '_' + date
                GetBgpByPrefix(bgp_filename) #step 2 #get bgp_path to dst_prefix
                parent_dir = global_var.par_path + global_var.out_my_anatrace_dir + '/' + vp + '_' + date + '/ribs_midar_bdrmapit/'
                path = parent_dir + filename
                if not os.path.exists(path):
                    continue
                print(path)
                wf = open(parent_dir + filename + '_all_bgp_paths', 'w')
                with open(path, 'r') as rf:
                    curline_trace = rf.readline()
                    while curline_trace:
                        curline_bgp = rf.readline()
                        curline_ip = rf.readline()
                        dst_prefix = curline_trace[1:curline_trace.index(' ')]
                        bgp_path_list = GetBgpPathFromBgpPrefixDict_2(dst_prefix)
                        wf.write(curline_trace)
                        for bgp_path in bgp_path_list:
                            wf.write('\t%s\n' %bgp_path)
                        wf.write(curline_ip)
                        curline_trace = rf.readline()
                wf.close()
                ClearBGPByPrefix()
    #print("end: %d" %total_count)

def GroupTracePathOfDetourDict(filename):
    wf = open(filename + '_group_trace_as', 'w')
    with open(filename, 'r') as rf:
        curline = rf.readline()
        cur_pair = None
        cur_bgp_seg = None
        trace_bgp_dict = dict()
        while curline:
            if not curline.startswith('\t'): #(src, dst)对，不分析
                if cur_pair:
                    wf.write(cur_pair)
                    for (key, val) in trace_bgp_dict.items():
                        wf.write("\t%s:%d(%.02f)\n" %(key, val[0], val[1]))
                cur_pair = curline #整句不用动
                cur_bgp_seg = None
                trace_bgp_dict.clear()
                curline = rf.readline()
                continue
            if not curline.startswith('\t\t'): #bgp seg
                cur_bgp_seg = curline.strip('\t').split(':')[0]
                curline = rf.readline()
                continue
            #curline.startswith('\t\t'): trace seg
            elems = curline.strip('\t').split(':')
            trace_seg = elems[0]
            count = int(elems[1][:elems[1].index('(')])
            perc = float(elems[1][elems[1].index('(') + 1:elems[1].index(')')])
            comp_trace_seg = ' '.join(DropStarsInTraceList(CompressAsPathToMin(CompressAsPath(trace_seg)).split(' ')))
            key = comp_trace_seg + ';' + cur_bgp_seg
            if key not in trace_bgp_dict.keys():
                trace_bgp_dict[key] = [0,0.0]
            trace_bgp_dict[key][0] += count
            trace_bgp_dict[key][1] += perc
            curline = rf.readline()
    wf.close()

def ClassifyDetour(filename):    
    PreGetAllTracesWithUniAs()
    PreGetAllBgpPaths()
    trace_seg_dict = dict()
    bgp_seg_dict = dict()
    wf_backup = open(filename[:filename.rindex('/')] + '/detour_backup_paths', 'w')
    wf_unannouce = open(filename[:filename.rindex('/')] + '/detour_unannounce_trace', 'w')
    wf_false_bgp = open(filename[:filename.rindex('/')] + '/detour_false_bgp', 'w')
    wf_malicious_bgp = open(filename[:filename.rindex('/')] + '/detour_malicious_bgp', 'w')
    with open(filename, 'r') as rf:
        curline = rf.readline()
        i = 0
        cur_pair = None
        while curline:
            print(i)
            i += 1
            if not curline.startswith('\t'): #(src, dst)对，不分析
                curline = rf.readline()
                continue
            #curline.startswith('\t\t'): trace seg
            elems = curline.strip('\t').strip('\n').split(';')
            trace_seg = elems[0]
            bgp_seg = elems[1][:elems[1].index(':')]
            if trace_seg not in trace_seg_dict.keys():
                trace_seg_dict[trace_seg] = CheckTraceSegInBgp(trace_seg)
            if bgp_seg not in bgp_seg_dict.keys():
                bgp_seg_dict[bgp_seg] = CheckBgpSegInTraces(bgp_seg)
            trace_res = trace_seg_dict[trace_seg]
            bgp_res = bgp_seg_dict[bgp_seg]
            if trace_res & bgp_res:
                wf_backup.write(curline)
            elif trace_res:
                wf_false_bgp.write(curline)
            elif bgp_res:
                wf_unannouce.write(curline)
            else:
                wf_malicious_bgp.write(curline)
            curline = rf.readline()
    wf_backup.close()
    wf_false_bgp.close()
    wf_unannouce.close()
    wf_malicious_bgp.close()

def GetAllBgpOfAbTrace(filename):
    os.chdir(global_var.par_path +  global_var.out_my_anatrace_dir + '/')
    for year in range(2018,2021):
    #for year in range(2018,2019):
        for month in range(1,13):
            date = str(year) + str(month).zfill(2) + '15'
            for vp in global_var.vps:
                g_asn = global_var.trace_as_dict[vp]    
                bgp_filename = global_var.par_path + global_var.rib_dir + 'bgpdata/bgp_' + g_asn + '_' + date
                GetBgpByPrefix(bgp_filename)
                cur_sub_dir = vp + '_' + date + '/ribs_midar_bdrmapit/' #'nrt-jp.2019030115/'
                wf = open(cur_sub_dir + filename + '_all_bgp', 'w')
                print(date + '_' + vp)
                with open(cur_sub_dir + filename, 'r') as rf:
                    curline_trace = rf.readline()
                    while curline_trace:
                        curline_bgp = rf.readline()
                        curline_ip = rf.readline()
                        dst_prefix = curline_trace[1:curline_trace.index(' ')]
                        bgp_list = GetBgpPathFromBgpPrefixDict_2(dst_prefix)
                        wf.write(curline_trace)
                        for bgp_path in bgp_list:
                            wf.write("\t%s\n" %CompressAsPath(bgp_path))
                        wf.write(curline_ip)
                        curline_trace = rf.readline()
                ClearBGPByPrefix()



if __name__ == '__main__':
    test = False
    test = True
    if test:
        #TmpAnaMulBgpUpdates('ana_ab_4_detour')
        #PreTransAllTraceToUniASTraces()
        #PreGetAllTracesWithUniAs()
        #AnaNextHopOfOneBgpPath('3356', '52055')
        #AnaNextHopOfOneTracePath('3356', '52055')
        #PreTransAllTraceToUniASTraces()
        #PreGetAllBgpPaths()
        #PreGetAllTracesWithUniAs()
        
        GetAllBgpOfAbTrace('ana_ab_4')
    else:
        #steps = ['step0', 'step1', 'step2', 'step3']
        steps = ['step2']

        if steps.__contains__('step0'):
            PreGetAllTraces()
            PreTransAllTraceToUniASTraces()
            PreGetAllTracesWithUniAs()
        
        if steps.__contains__('step1'):
            FilterInstDetour(global_var.par_path + global_var.out_my_anatrace_dir + '/detour_dict')
            #AnaDetourSeg_2这一步暂时不做，2021.7.17
            #AnaDetourSeg_2(global_var.par_path + global_var.out_my_anatrace_dir + '/detour_dict_stable')

        if steps.__contains__('step2'):
            #GroupTracePathOfDetourDict(global_var.par_path + global_var.out_my_anatrace_dir + '/detour_dict_stable')
            ClassifyDetour(global_var.par_path + global_var.out_my_anatrace_dir + '/detour_dict_stable_group_trace_as')
            CheckBgpPathNorm(global_var.par_path + global_var.out_my_anatrace_dir + '/detour_malicious_bgp')
        
        if steps.__contains__('step3'):
            PreGetSrcFilesInDirs()
            
