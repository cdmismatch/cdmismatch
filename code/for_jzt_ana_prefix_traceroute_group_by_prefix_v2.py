import sys
import re
import ujson
import socket
import struct
import os
import shutil
import copy

cur_map_method = 'bdrmapit'

import global_var
from utils_v2 import GetPfx2ASByRv, GetLongestMatchPrefixByRv, GetAsListOfIpByRv, GetAsStrOfIpByRv, \
    GetAsStrOfPrefixByRv, GetAsOfIpByMi, GetIxpPfxDict, IsIxpIp, GetIxpAsSet, IsIxpAs, IsSib, \
    GetDiffList, IsSib_2, CalIpFreq, PreOpenRouterIpFiles, CloseRouterIpFiles, PreConstrRouterIpDict, \
    PreOpenAsRouterFiles, CloseAsRouterFiles, GetGeoOfIpByMi, GeoDistance, PreOpenRouterGeoFiles, \
    CloseGeoRouterFiles, GetSibRel, AsIsEqual, ClearIxpAsSet, ClearIxpPfxDict, ClearRouterIpDict, \
    GetAsOfRouterByMi, GetGeoOfRouterByMi, Get2AsRel, AsnInBgpPathList, AsnInTracePathList, \
    FstPathContainedInSnd, ClearIp2AsDict, ClearSibRel, GetPfx2ASByBgp, GetBgpByPrefix, GetBgpPathFromBgpPrefixDict, \
    GetBgpPathFromBgpPrefixDict_2, GetPathAsDict, ClearPathAsDict, GetBgpPathByAs, FindTraceAsInBgpPath, \
    GetDistOfIps, ConnectToDb, CloseDb, ClearBGPByPrefix, CombineFiles, IfPossibleIxp_ForASList, InitMidarCache, \
    SetCurMidarTableDate, InitGeoCache, FindTraceAsSetInBgpPath, IsSibByIRR, IsSibByMultiDataFiles_2, \
    GetSibRelByMultiDataFiles, ClearSibRelByMultiDataFiles
from get_ip2as_from_bdrmapit import ConnectToBdrMapItDb, GetIp2ASFromBdrMapItDb, CloseBdrMapItDb, InitBdrCache, \
                                    ConstrBdrCache
from gen_ip2as_command import PreGetSrcFilesInDirs

def CompressAsPath(ori_as_path):
    ori_as_path = ori_as_path.strip(' ')
    elems = ori_as_path.split(' ')
    as_trace_compress = ""
    pre_elem = ''
    temp_stars = ""
    for elem in elems:
        if elem == pre_elem:
            temp_stars = ""
        elif elem == '?' or elem == '*' or elem.startswith('<'):
            temp_stars = temp_stars + elem + ' '
        else:
            if temp_stars != "":
                as_trace_compress = as_trace_compress + temp_stars
                temp_stars = ""
            as_trace_compress = as_trace_compress + elem + ' '
            pre_elem = elem
    as_trace_compress = as_trace_compress + temp_stars
    as_trace_compress = as_trace_compress.strip(' ')
    return as_trace_compress

#2021.2.19修改，对于moas的hop，如果包含前一跳或者后一跳的as，则舍弃该hop
def CompressAsPathToMin(path):
    return path #2021.5.19不应该像下面一样压缩，可能有错
    debug_modi = False
    elems = path.split(' ')
    res_elems = []
    for elem in elems:
        #if elem == '*' or elem == '?':
        if elem == '*' or elem == '?' or elem.startswith('<'): #把ixp情况考虑进去
            continue
        if not res_elems.__contains__(elem):
            same = False
            if elem.__contains__('_'):
                if res_elems:
                    for prev_elem in res_elems[-1].split('_'):
                        if prev_elem in elem.split('_'):
                            same = True
                            debug_modi = True
                            break
                if not same:
                    res_elems.append(elem)
            else:
                if res_elems and elem in res_elems[-1].split('_'):
                    res_elems.pop()
                    debug_modi = True
                res_elems.append(elem)
    #if debug_modi:
        #print(" ".join(res_elems))
    return " ".join(res_elems)

def AreTwoPathSame(path1, path2):
    return CompressAsPathToMin(path1) == CompressAsPathToMin(path2)

def HasLoop(path):
    elems = path.split(' ')
    temp_set = set()
    for elem in elems:
        if elem == '*' or elem == '?' or elem.startswith('<'):
            continue
        if temp_set.__contains__(elem):
            return True
        else:
            temp_set.add(elem)
    return False

def PrependDstOnPath(dst_as, path):
    dst_as_list = dst_as.split('_')
    index = path.rfind(' ')
    if index == -1:
        last_as = path
    else:
        last_as = path[index+1:]
    if last_as not in dst_as_list:
        path = path + ' ' + dst_as
    return path

def SuppleMiDataToTwoDifTrace(ip_as_path1, ip_as_path2): #ip_as_path: [ip_path, ori_as_path, update_flag, ixp_index]
    #print("%s %s %s" %(ip_as_path1[0], ip_as_path1[1], ip_as_path1[2]))
    #print("%s %s %s" %(ip_as_path2[0], ip_as_path2[1], ip_as_path2[2]))
    (ip_as_path1[1], ip_as_path1[2]) = AddMiAsToTracePath(ip_as_path1[1], ip_as_path2[1], ip_as_path1[0], ip_as_path1[2])
    (ip_as_path2[1], ip_as_path2[2]) = AddMiAsToTracePath(ip_as_path2[1], ip_as_path1[1], ip_as_path2[0], ip_as_path2[2])
    #sort_list1[0] = CompressAsPathToMin(CompressAsPath(ip_as_path1[1]))
    #sort_list1[1] = ip_as_path1
    #sort_list2[0] = CompressAsPathToMin(CompressAsPath(ip_as_path2[1]))
    #sort_list2[1] = ip_as_path2
    return (ip_as_path1, ip_as_path2)

def FilterSimilarPath(dst_key, ip_as_path_list, use_mi_data):
    dst_as = dst_key[dst_key.find(' ')+1:]
    #首先建立一个以min_path为key的字典，val是ip_as_path
    #然后以min_path包含个数多少从大到小排序
    #排序后从大到小遍历，将包含在更大key中的小key和对应的ip_as_path过滤掉
    #所谓包含：1.较大的key加上dst_as再和较小的key比较 
    # 2.使用key_list，只要较小的key_list的所有元素包含在较大的key_list中，即认为包含。不需要string之间的严格包含
    cmp_dict = dict()
    for ip_as_path in ip_as_path_list: #ip_as_path = [ip_path, ori_as_path, ixp_index]， ixp_index后来不用了，但暂时不做修改
        min_path = CompressAsPath(ip_as_path[1])
        min_path = CompressAsPathToMin(min_path)
        if min_path not in cmp_dict.keys():            
            if use_mi_data:
                update_flag = [ False for i in range(0, len(ip_as_path[1]))]
                ip_as_path.insert(2, update_flag) #ip_as_path = [ip_path, ori_as_path, update_flag, ixp_index]
                #ip_as_path.append(update_flag) 
            cmp_dict[min_path] = ip_as_path #对于重复的min_path，只记录一条路径
    sort_list = sorted(cmp_dict.items(), key=lambda d:len(d[0].split(' ')), reverse = True)
    #print(sort_list)
    elem_list = []
    for elem in sort_list:
        elem_list.append(elem[1]) #elem: [ip_path, ori_as_path, update_flag, ixp_index]
    res_list = [elem_list[0]]
    list_len = len(elem_list)
    for i in range(1, list_len):
        #cur_min_path = sort_list[i][0]
        #print(cur_min_path)
        is_contained = False
        for j in range(0, i):
            #larger_min_path = PrependDstOnPath(dst_as, sort_list[j][0])
            #if FstPathContainedInSnd(cur_min_path, larger_min_path):
            if use_mi_data:
                #print("%d %d" %(i, j))
                tmp_i = elem_list[i] #tmp_i: [ip_path, ori_as_path, update_flag, ixp_index]
                tmp_j = elem_list[j] #tmp_j: [ip_path, ori_as_path, update_flag, ixp_index]
                #(sort_list[i][1], sort_list[j][1]) = SuppleMiDataToTwoDifTrace(sort_list[i][1], sort_list[j][1])
                (tmp_i, tmp_j) = SuppleMiDataToTwoDifTrace(elem_list[i], elem_list[j])
                elem_list[i] = tmp_i
                elem_list[j] = tmp_j
            min_path1 = ''
            min_path2 = ''
            if use_mi_data:
                min_path1 = CompressAsPathToMin(CompressAsPath(elem_list[i][1]))
                min_path2 = CompressAsPathToMin(CompressAsPath(elem_list[j][1]))
            else:
                min_path1 = sort_list[i][0]
                min_path2 = sort_list[j][0]
            if FstPathContainedInSnd(min_path1, min_path2):
                is_contained = True
                break
        if not is_contained: #find a different min_path
            res_list.append(elem_list[i])
    #for i in range(0, len(res_list)):
        #res_list[i].pop()
    return res_list 

def GetLongestPath(path_list):
    max_num = 0
    longest_path = ""
    for path in path_list:
        num = path.count(' ') + 1 - path.count('*') - path.count('?')
        if max_num < num:
            max_num = num
            longest_path = path
    return longest_path

#def SuppleMapASByMi(ip, as_str):
    #mi_as = GetAsOfIpByMi(ip)
    #if mi_as and mi_as not in as_str:
        #as_str += '_' + mi_as
    #return as_str

#def SuppleMapASByBdr(ip, as_str):
    #bdr_as = GetIp2ASFromBdrMapItDb(ip)
    #if bdr_as and bdr_as not in as_str:
        #as_str += '_' + bdr_as
    #return as_str

def GetIpAndAsPathFromTraceline(elems, record_ixp = False):
    global cur_map_method
    ip_path = ''
    as_path = ''
    #ixp_index = ''
    for i in range(13, len(elems)):
        curhops = elems[i]
        if curhops.__contains__('q'):
            ip_path = ip_path + ' *'
            as_path = as_path + ' *'
        else:
            #curhops: "210.171.224.41,1.210,1;210.171.224.41,1.216,1"
            hopelems = curhops.split(';')
            hop_as_list = []
            hop_ip_list = []
            for elem in hopelems:
                #elem: "210.171.224.41,1.210,1"
                temp = elem.split(',')
                #先检查ip是否是ixp的ip
                #temp[0]: "210.171.224.41"
                is_ixp_ip = False
                #print("temp[0]: %s" %temp[0])
                if IsIxpIp(temp[0]):
                    is_ixp_ip = True #从打印来看挺多的
                #2021.4.26修改
                as_str = ''
                #print(cur_map_method)
                if cur_map_method.__contains__('ribs'):
                    as_str = GetAsStrOfIpByRv(temp[0]) #moas之间以'_'隔开
                elif cur_map_method.__contains__('midar'):
                    as_str = GetAsOfIpByMi(temp[0])
                elif cur_map_method.__contains__('bdrmapit'):
                    as_str = GetIp2ASFromBdrMapItDb(temp[0])
                if as_str:
                    for asn in as_str.split('_'):
                        if IsIxpAs(asn):
                            is_ixp_ip = True
                            break
                    #if not is_ixp_ip and as_str not in hop_as_list:
                    if as_str not in hop_as_list:
                        hop_as_list.append(as_str)
                else:
                    if '?' not in hop_as_list:
                        hop_as_list.append('?')
                if False:
                    if is_ixp_ip:
                        #ixp_index += ' ' + str(i - 13)
                        #continue #忽略是ixp的ip #2021.1.30 不忽略
                        #hop_as_list = ['<' + '_'.join(hop_as_list) + '>'] #2021.1.30 做标记
                        hop_as_list = ['<>'] #2021.1.30 做标记，暂时先这样做
                        hop_ip_list = ['<' + temp[0] + '>'] #2021.1.30 做标记
                        break #有一个ixp ip就记当前位置为ixp ip
                    else:
                        hop_ip_list.append(temp[0])
                else:
                    hop_ip_list.append(temp[0])
                
            if len(hop_as_list) == 0: #2021.1.27这里原来没有这一步，有bug，应该考虑过滤掉IXP后没有ip和AS的情况
                print('NOTE: hop not exist')
            elif len(hop_as_list) == 1:
                ip_path = ip_path + ' ' + hop_ip_list[0]
                as_path = as_path + ' ' + hop_as_list[0]
            else:
                ip_path = ip_path + ' {' + ' '.join(hop_ip_list) + '}'
                as_path = as_path + ' {' + ' '.join(hop_as_list) + '}'
    #ixp_index.strip(' ')
    #return (ixp_index, ip_path, as_path)
    return (ip_path, as_path)


#def FilterSimilarPathAndRecord(cur_dst_key, ip_as_path_list, wf_single, wf_multi, wf_ixp_index, use_mi_data):
def FilterSimilarPathAndRecord(cur_dst_key, ip_as_path_list, wf_single, wf_multi, use_mi_data):
    filtered_list = FilterSimilarPath(cur_dst_key, ip_as_path_list, use_mi_data)
    '''
    if wf_ixp_index:
        for ip_as_path in filtered_list:
            if ip_as_path[-1]:
                wf_ixp_index.write("[%s]%s\n" %(cur_dst_key, ip_as_path[-1]))
                wf_ixp_index.write("[%s]%s\n" %(cur_dst_key, ip_as_path[1]))
                wf_ixp_index.write("[%s]%s\n" %(cur_dst_key, ip_as_path[0]))
    '''
    if len(filtered_list) == 1:
        wf_single.write("[%s]%s\n" %(cur_dst_key, filtered_list[0][1]))
        wf_single.write("[%s]%s\n" %(cur_dst_key, filtered_list[0][0]))
        #return 0
    else: #a dst prefix has multiple paths 
        for ip_as_path in filtered_list:
            wf_multi.write("[%s]%s\n" %(cur_dst_key, ip_as_path[1]))
            wf_multi.write("[%s]%s\n" %(cur_dst_key, ip_as_path[0]))
        #return 1
    return len(filtered_list)

def TmpRecordIxpPath(trace_file_name):
    rf = open(trace_file_name, 'r', encoding='utf-8')
    w_path_ixp_filename = '5_has_ixp_ip'
    wf_ixp = open(w_path_ixp_filename, 'w', encoding='utf-8')
    #w_path_ixp_index_filename = '5_has_ixp_ip_index_1'
    #wf_ixp_index = open(w_path_ixp_index_filename, 'w', encoding='utf-8')

    trace_dict = dict()
    curline = rf.readline()
    while curline:
        if not curline.startswith('T'):
            curline = rf.readline()
            continue
        curline = curline.strip('\n')
        elems = curline.split('\t')
        #print(elems)
        #src_as = get_as_by_ip(elems[1])
        dst_ip = elems[2]
        dst_prefix = GetLongestMatchPrefixByRv(dst_ip)
        dst_as_str = GetAsStrOfPrefixByRv(dst_prefix) #moas之间以'_'隔开
        if dst_prefix == "" or dst_as_str == "":
            curline = rf.readline()
            continue
        #wf.write("[%s]" %dst_prefix)
        dst_key = dst_prefix + ' ' + dst_as_str
        (ip_path, ori_as_path) = GetIpAndAsPathFromTraceline(elems, True)
        if not ori_as_path.__contains__('<'): 
            curline = rf.readline()
            continue    
        #wf_ixp_index.write("[%s]%s\n" %(dst_key, ixp_index))     
        if dst_key not in trace_dict.keys():
            trace_dict[dst_key] = []
        #if as_path not in trace_dict[dst_key]: #其实这句话没用，是个bug，暂且不删除
        trace_dict[dst_key].append([ip_path, ori_as_path])
        curline = rf.readline()        
    rf.close()
    #wf_ixp_index.close()

    multi_num = 0
    for (dst_key, ip_as_path_list) in trace_dict.items():    
        multi_num += FilterSimilarPathAndRecord(dst_key, ip_as_path_list, wf_ixp, wf_ixp, False)
    wf_ixp.close()
    print("multi_num: %d" %multi_num)
    

def ChgTrace2ASPath(trace_file_name):
    rf = open(trace_file_name, 'r', encoding='utf-8')
    #w_path_set_filename = str('res_as_path_has_set_of_prefix_') + trace_file_name
    wf = open('../out/as_' + trace_file_name, 'w', encoding='utf-8')
    #wf_incomplete = = open('as_incomplete_' + trace_file_name, 'w', encoding='utf-8')
    count = 0
    count_unmap = 0
    count_incomplete = 0

    curline = rf.readline()
    while curline:
        if not curline.startswith('T'):
            curline = rf.readline()
            continue
        curline = curline.strip('\n')
        elems = curline.split('\t')
        #print(elems)
        #src_as = get_as_by_ip(elems[1])
        (ip_path, ori_as_path) = GetIpAndAsPathFromTraceline(elems, False)
        #print(ori_as_path)
        if not ip_path or not ori_as_path:
            curline = rf.readline()
            continue
        as_path = CompressAsPath(ori_as_path)
        if as_path.__contains__('{'):
            pass
        elif as_path.__contains__('*'):
            #wf_incomplete.write("%s\n" %ori_as_path)
            count_incomplete += 1
        elif as_path.__contains__('?'):
            #wf_incomplete.write("%s\n" %ori_as_path)
            count_unmap += 1
        else:
            wf.write("%s\n" %as_path)
            count += 1
        curline = rf.readline()        
    rf.close()
    wf.close()
    print("count: %d" %count)
    print("count_incomplete: %d" %count_incomplete)
    print("count_unmap: %d" %count_unmap)

'''
def GetAsPathFstByMi(ips_str):
    as_path = ""
    ip_list = ips_str.split(' ')
    for ip in ip_list:
        if ip == '*':
            as_path = as_path + ' *'
            continue
        asn = GetAsOfIpByMi(ip)
        if not asn: #Mi里找不到，在RV里找
            asn = GetAsStrOfIpByRv(ip)
        if not asn:
            as_path += ' ?'
        else:
            as_path += ' ' + asn
    return as_path.strip(' ')
'''

def CheckMultiPathByMi(filename, record_file_name, open_mode): #将Mi mapping添加到trace path中
    rf = open(filename, 'r')
    #w_single_path_filename = str('res_3_single_as_path_of_prefix_') + trace_file_name
    w_single_path_filename = filename + '_single'
    wf_single = open(w_single_path_filename, 'w', encoding='utf-8')
    count_single = 0
    #w_multi_path_filename = str('res_3_multi_as_path_of_prefix_') + trace_file_name
    w_multi_path_filename = filename + '_multi'
    wf_multi = open(w_multi_path_filename, 'w', encoding='utf-8')
    count_multi = 0

    curline_trace = rf.readline()
    cur_dst_key = ""
    ip_as_path_list = []
    while curline_trace:
        curline_ip = rf.readline()
        #if curline_ip.__contains__('[71.1.176.0/20 11398]'):
            #print('')
        elems_trace = curline_trace.strip('\n').strip(' ').split(']')
        elems_ip = curline_ip.strip('\n').strip(' ').split(']')
        if len(elems_ip) < 2 or len(elems_trace) < 2:
            curline_trace = rf.readline()
            continue
        tmp_dst_key = elems_ip[0][1:]
        ip_path = elems_ip[1].strip('\n').strip(' ')
        if ip_path == '':
            curline_trace = rf.readline()
            continue
        trace_path = elems_trace[1].strip('\n').strip(' ')
        #new_trace_path = AddMiAsToTracePath(trace_path, '', ip_path)
        if (not cur_dst_key) or (tmp_dst_key == cur_dst_key):            
            #ip_as_path_list.append([ip_path, new_trace_path])
            ip_as_path_list.append([ip_path, trace_path])
            if not cur_dst_key:
                cur_dst_key = tmp_dst_key
        else:
            #multi_num += FilterSimilarPathAndRecord(cur_dst_key, ip_as_path_list, wf_single, wf_multi, None, True)
            res = FilterSimilarPathAndRecord(cur_dst_key, ip_as_path_list, wf_single, wf_multi, True)
            if res == 1:
                count_single += res
            else:
                count_multi += res
            cur_dst_key = tmp_dst_key
            #ip_as_path_list = [[ip_path, new_trace_path]]
            ip_as_path_list = [[ip_path, trace_path]]
        curline_trace = rf.readline()
    #multi_num += FilterSimilarPathAndRecord(cur_dst_key, ip_as_path_list, wf_single, wf_multi, None, True)
    res = FilterSimilarPathAndRecord(cur_dst_key, ip_as_path_list, wf_single, wf_multi, True)
    if res == 1:
        count_single += res
    else:
        count_multi += res
    
    rf.close()
    wf_single.close()
    wf_multi.close()
    wf = open(record_file_name, open_mode)
    wf.write("In multi trace, after mi mapping, became single num: %d\n" %count_single)
    wf.write("In multi trace, after mi mapping, still multi num: %d\n" %count_multi)
    wf.close()

bgp_dict = dict()
def GetBgp(filename_asn): #从原始格式中提取数据
    global bgp_dict
    rf = open(filename_asn, 'r')
    curline = rf.readline()
    while curline:
        as_path = curline.split('|')[2]
        if as_path:
            dst_as = as_path.split(' ')[-1]
            if dst_as not in bgp_dict.keys():
                bgp_dict[dst_as] = []
            compress_as_path = CompressAsPath(as_path)
            if compress_as_path not in bgp_dict[dst_as]:
                bgp_dict[dst_as].append(compress_as_path)
        curline = rf.readline()
    rf.close()

def ClearBGP():
    global bgp_dict
    bgp_dict.clear()

def TracePathListEqualsBgpPathList(trace_list, bgp_list): #这里考虑moas的情况
    length = len(trace_list)
    if length == len(bgp_list):
        for i in range(0, length):
            if not bgp_list[i] in trace_list[i].split('_'):
                return False
        return True
    else:
        return False

min_dist_limit = 100
def GeoAdj(ip_list, ori_left_index, ori_right_index):
    geo_left = ""
    geo_right = ""
    if ori_left_index != -1:
        if ip_list[ori_left_index].startswith('<'):
            print('NOTE! ip_list[ori_left_index] == <ip>')
            return False
        geo_left = GetGeoOfIpByMi(ip_list[ori_left_index])
    if ori_right_index != len(ip_list):
        if ip_list[ori_right_index].startswith('<'):
            print('NOTE! ip_list[ori_right_index] == <ip>')
            return False
        geo_right = GetGeoOfIpByMi(ip_list[ori_right_index])
    for i in range(ori_left_index + 1, ori_right_index):
        if ip_list[i] == '*' or ip_list[i].startswith('<'):
            continue
        geo_cur = GetGeoOfIpByMi(ip_list[i])
        if not geo_cur:
            return False
        if (geo_left and GeoDistance(geo_cur, geo_left) < min_dist_limit) or \
            (geo_right and GeoDistance(geo_cur, geo_right) < min_dist_limit):
            pass
        else:
            return False
    return True

def AddMiAsToTracePath(ori_trace_path, base_path, ip_path, update_flag = []):
    global cur_map_method
    if not ori_trace_path or not ip_path:
        print(ori_trace_path)
        print(base_path)
        print(ip_path)
        return (ori_trace_path, update_flag)
    
    base_path = base_path.replace('_', ' ')
    trace_list = ori_trace_path.strip('\n').strip(' ').split(' ')
    ip_list = ip_path.strip('\n').strip(' ').split(' ')
    #print('ip_path: %s' %ip_path)
    for i in range(0, len(trace_list)):
        cur_trace_as = trace_list[i]
        #if cur_trace_as == '*' or cur_trace_as == '?' or cur_trace_as.startswith('<'):
        if cur_trace_as == '*' or cur_trace_as.startswith('<'):
            continue
        find = False
        if cur_trace_as != '?':
            for tmp_as in cur_trace_as.split('_'):
                if tmp_as in base_path.split(' '):
                    find = True
                    break
        if not find:
            if not update_flag or not update_flag[i]:
                #print('i: %d AddMi: %s' %(i, ip_list[i]))
                if cur_map_method.__contains__('midar'):
                    mi_as = GetAsOfIpByMi(ip_list[i])                
                    if mi_as and mi_as not in cur_trace_as.split('_'):
                        if trace_list[i] != '?':
                            trace_list[i] += '_' + mi_as
                        else:
                            trace_list[i] = mi_as
                if cur_map_method.__contains__('bdrmapit'):
                    bdr_as = GetIp2ASFromBdrMapItDb(ip_list[i])                
                    if bdr_as and bdr_as not in cur_trace_as.split('_'):
                        if trace_list[i] != '?':
                            trace_list[i] += '_' + bdr_as
                        else:
                            trace_list[i] = bdr_as
            if update_flag and not update_flag[i]:
                update_flag[i] = True
    if not trace_list:
        print(ori_trace_path)
        print(base_path)
        print(ip_path)
    return (' '.join(trace_list), update_flag)


#和v1相比，条件严格了，不同的跳只有sibling关系才认为正常
def TracePathIsNormal_back(ori_trace_path, ori_bgp_path, ip_path, use_mi_data):
    trace_path = CompressAsPathToMin(CompressAsPath(ori_trace_path))
    bgp_path = CompressAsPathToMin(ori_bgp_path)

    if trace_path == bgp_path:
        #print(1)
        return (True, 0)
    trace_list = trace_path.split(' ')
    bgp_list = bgp_path.split(' ')
    ip_list = ip_path.split(' ')
    if TracePathListEqualsBgpPathList(trace_list, bgp_list):
        return (True, 0)
    dif_list = GetDiffList(trace_list, bgp_list)
    for dif_seg in dif_list:
        (dif_trace_range, dif_bgp) = dif_seg
        if not dif_trace_range: #bgp多出来几跳，认为正常
            #print(2)
            continue
        dif_trace = [x for x in dif_trace_range]
        left_index = dif_trace[0] - 1
        right_index = dif_trace[-1] + 1
        #if left_index != -1:
            #print("left: %s" %trace_list[left_index])
        #if right_index != len(trace_list):
            #print("right: %s" %trace_list[right_index])
        ori_trace_list = ori_trace_path.split(' ')
        ori_left_index = -1
        ori_right_index = len(ori_trace_list)
        if left_index != -1:
            ori_left_index = ori_trace_list.index(trace_list[left_index])
            while ori_left_index + 1 < len(ori_trace_list) and ori_trace_list[ori_left_index + 1] == ori_trace_list[ori_left_index]:
                ori_left_index += 1
        if right_index != len(trace_list):
            ori_right_index = ori_trace_list.index(trace_list[right_index])     
        normal = True
        for i in dif_trace:
            #print("cur: %s" %trace_list[i])            
            #和"最左边的或最右边的正常的hop"是sibling关系或者离得特别近，可能是IXP引起的不一致，注意这里不是多出来的几跳之间是sibling关系
            #或和对应的bgp hop比
            if (left_index != -1 and IsSib_2(trace_list[i], trace_list[left_index])) or \
                (right_index != len(trace_list) and IsSib_2(trace_list[i], trace_list[right_index])) or \
                (dif_bgp and len(dif_bgp) == 1 and IsSib_2(trace_list[i], bgp_list[dif_bgp[0]])):
                pass
            else:
                normal = False
                break
        #如果ip的位置很近，可能是IXP引起的不一致
        if not normal and use_mi_data and GeoAdj(ip_list, ori_left_index, ori_right_index):
            normal = True
        if not normal:
            dif_num = 0
            for elem in trace_list:
                if not AsnInBgpPathList(elem, bgp_list):
                    dif_num += 1
            #return (False, len(dif_list)) #把有几处不一致返回，用于挑选最接近的bgp_path
            return (False, dif_num) #2021.2.13，把有几处不一致的计算方法进行修改
    return (True, 0)

#原来的版本可能有bug，重新写
def TracePathIsNormal(ori_trace_path, ori_bgp_path, ip_path, use_geo_data):
    global cur_map_method
    bgp_path_list = CompressAsPathToMin(ori_bgp_path).split(' ')
    ori_trace_path_list = ori_trace_path.split(' ')
    ip_path_list = ip_path.split(' ')
    normal = True
    bgp_prev_index = 0
    ip_prev_index = 0
    bgp_next_index = 0
    for i in range(0, len(ori_trace_path_list)):
        cur_hop = ori_trace_path_list[i]
        if cur_hop.__contains__('*') or cur_hop.__contains__('?') or cur_hop.__contains__('<'):
            continue
        if AsnInBgpPathList(cur_hop, bgp_path_list):
            bgp_prev_index = FindTraceAsInBgpPath(cur_hop, bgp_path_list)
            ip_prev_index = i
            bgp_next_index = bgp_prev_index + 1
            while bgp_next_index < len(bgp_path_list):
                if AsnInTracePathList(bgp_path_list[bgp_next_index], ori_trace_path_list):
                    break
                bgp_next_index += 1
            continue  #正常，看下一跳
        ip_next_index = i + 1
        while ip_next_index < len(ip_path_list) and (ip_path_list[ip_next_index].__contains__('*') or ip_path_list[ip_next_index].__contains__('<')):
            ip_next_index += 1
        bgp_prev_hop = bgp_path_list[bgp_prev_index]
        if IsSib_2(bgp_prev_hop, cur_hop):  #和左边正常的hop是sibling关系
            continue    #正常，看下一跳
        #if False:   #2021.4.29 先不考虑地理位置
        if use_geo_data:
            (res, dist) = GetDistOfIps(ip_path_list[i], ip_path_list[ip_prev_index])
            #if res and dist < min_dist_limit:   #和左边正常的hop离得特别近，可能是IXP引起的不一致
            if res and dist < min_dist_limit:   #和左边正常的hop离得特别近，可能是IXP引起的不一致
                #print("1(%s(%s), %s(%s), %d)" %(ip_path_list[i], ori_trace_path_list[i], ip_path_list[ip_prev_index], ori_trace_path_list[ip_prev_index], dist))
                continue    #正常，看下一跳
        if bgp_next_index == 0: #abnormal
            print(ori_trace_path)
            print(ori_bgp_path)
        if bgp_next_index < len(bgp_path_list):
            bgp_next_hop = bgp_path_list[bgp_next_index]
            if IsSib_2(bgp_next_hop, cur_hop):   #和右边的hop是sibling关系或者离得特别近，可能是IXP引起的不一致
                continue    #正常，看下一跳
        #if False:   #2021.4.29 先不考虑地理位置
        if ip_next_index < len(ip_path_list) and use_geo_data:
            (res, dist) = GetDistOfIps(ip_path_list[i], ip_path_list[ip_next_index])
            if res and dist < min_dist_limit:   #和右边的hop离得特别近，可能是IXP引起的不一致
                #print("2(%s(%s), %s(%s), %d)" %(ip_path_list[i], ori_trace_path_list[i], ip_path_list[ip_next_index], ori_trace_path_list[ip_next_index], dist))
                continue    #正常，看下一跳
        normal = False
        if bgp_prev_index + 1 < bgp_next_index:
            for j in range(bgp_prev_index + 1, bgp_next_index):
                bgp_hop = bgp_path_list[j]
                if IsSib_2(bgp_hop, cur_hop):   #和对应的bgp hop是sibling关系
                    normal = True    #正常，看下一跳
                    break
            #2021.4.24 加一条，如果ab_hop的/24子网内有ip和ab_hop离得特别近，且该ip属于bgp_hop，则认为可能是IXP
            #if False:   #2021.4.29 先不考虑地理位置
            if not normal and use_geo_data:
                if False:
                #if IfPossibleIxp_ForASList(ip_path_list[i], bgp_path_list[bgp_prev_index + 1:bgp_next_index]):
                    normal = True
        if not normal:
            return (False, len(bgp_path_list) - bgp_prev_index)    #有一跳不正常，即为不正常，把有几处不一致的计算方法进行修改
    return (True, 0)
    
def TracePathIsNormalByIRRData(ori_trace_path, ori_bgp_path, ip_path, check_irr, check_newer_org):
    global cur_map_method
    bgp_path_list = CompressAsPathToMin(ori_bgp_path).split(' ')
    ori_trace_path_list = ori_trace_path.split(' ')
    ip_path_list = ip_path.split(' ')
    normal = True
    bgp_prev_index = 0
    ip_prev_index = 0
    bgp_next_index = 0
    for i in range(0, len(ori_trace_path_list)):
        cur_hop = ori_trace_path_list[i]
        if cur_hop.__contains__('*') or cur_hop.__contains__('?') or cur_hop.__contains__('<'):
            continue
        if AsnInBgpPathList(cur_hop, bgp_path_list):
            bgp_prev_index = FindTraceAsInBgpPath(cur_hop, bgp_path_list)
            ip_prev_index = i
            bgp_next_index = bgp_prev_index + 1
            while bgp_next_index < len(bgp_path_list):
                if AsnInTracePathList(bgp_path_list[bgp_next_index], ori_trace_path_list):
                    break
                bgp_next_index += 1
            continue  #正常，看下一跳
        bgp_prev_hop = bgp_path_list[bgp_prev_index]
        if check_irr and IsSibByIRR(bgp_prev_hop, ip_path_list[i]):  #和左边正常的hop是sibling关系
            continue    #正常，看下一跳
        if check_newer_org and IsSibByMultiDataFiles_2(bgp_prev_hop, cur_hop):
            continue
        if bgp_next_index == 0: #abnormal
            print(ori_trace_path)
            print(ori_bgp_path)
        if bgp_next_index < len(bgp_path_list):
            bgp_next_hop = bgp_path_list[bgp_next_index]
            if check_irr and IsSibByIRR(bgp_next_hop, ip_path_list[i]):  #和右边正常的hop是sibling关系
                continue    #正常，看下一跳
            if check_newer_org and IsSibByMultiDataFiles_2(bgp_next_hop, cur_hop):
                continue
        normal = False
        if bgp_prev_index + 1 < bgp_next_index:
            for j in range(bgp_prev_index + 1, bgp_next_index):
                bgp_hop = bgp_path_list[j]
                if check_irr and IsSibByIRR(bgp_hop, ip_path_list[i]):   #和对应的bgp hop是sibling关系
                    normal = True    #正常，看下一跳
                    break
                if check_newer_org and IsSibByMultiDataFiles_2(bgp_hop, cur_hop):   #和对应的bgp hop是sibling关系
                    normal = True    #正常，看下一跳
                    break
        if not normal:
            return (False, len(bgp_path_list) - bgp_prev_index)    #有一跳不正常，即为不正常，把有几处不一致的计算方法进行修改
    return (True, 0)

def CheckTraces(filename, use_mi_data, use_geo_data):
    rf = open(filename, 'r')
    wf_ab = open(filename + '_ab', 'w')
    wf_unmap = open(filename + '_unmap', 'w')
    count_ab = 0
    count_unmap = 0
    count_total = 0

    curline = rf.readline()
    while curline:
        count_total += 1
        curline_bgp = ''
        if use_mi_data or use_geo_data: #use_mi_data和use_geo_data都为false，说明是第一次，没有bgp_path
            curline_bgp = rf.readline()
        curline_ip = rf.readline()
        elems = curline.strip('\n').strip(' ').split(']')
        if len(elems) < 2:
            curline = rf.readline()
            continue
        temp = curline_ip.strip('\n').strip(' ').split(']')
        if len(temp) < 2:
            curline = rf.readline()
            continue
        ip_path = temp[1].strip('\n').strip(' ')
        if not ip_path:
            curline = rf.readline()
            continue
        dst_key = elems[0][1:]
        dst_prefix = dst_key.split(' ')[0]
        ori_trace_path = elems[1].strip(' ')
        normal = False
        min_dif_size = 100
        similar_bgp_path = ''
        ori_trace_path_2 = ''
        if use_mi_data:
            (ori_trace_path_2, not_use) = AddMiAsToTracePath(ori_trace_path, curline_bgp.strip('\n').strip('\t'), ip_path, None)
        else:
            ori_trace_path_2 = ori_trace_path
        bgp_path_list = GetBgpPathFromBgpPrefixDict_2(dst_prefix)
        if not bgp_path_list:
            print("NOTICE: CheckTraces() prefix %s not found in bgp table" %dst_prefix)
            curline = rf.readline()
            continue
        for ori_bgp_path in bgp_path_list:
            (normal, dif_size) = TracePathIsNormal(ori_trace_path_2, ori_bgp_path, ip_path, use_geo_data)
            if normal:
                break
            if min_dif_size > dif_size:
                similar_bgp_path = ori_bgp_path
                min_dif_size = dif_size
        if not normal:
            wf_ab.write("[%s] %s\n" %(dst_key, ori_trace_path_2))
            wf_ab.write("\t\t%s\n" %similar_bgp_path)
            wf_ab.write("%s" %curline_ip)
            count_ab += 1
        elif ori_trace_path_2.__contains__('?'): #2021.5.18 记录ip没有map的trace
            wf_unmap.write("[%s] %s\n" %(dst_key, ori_trace_path_2))
            wf_unmap.write("\t\t%s\n" %similar_bgp_path)
            wf_unmap.write("%s" %curline_ip)
            count_unmap += 1
        curline = rf.readline()
    
    wf_ab.close()
    wf_unmap.close()
    rf.close()
    return (count_ab, count_unmap, count_total)

def CheckTracesByIRRData(filename, check_irr, check_newer_org):
    rf = open(filename, 'r')
    wf_ab = open(filename + '_ab', 'w')
    count_ab = 0
    count_total = 0

    curline = rf.readline()
    while curline:
        count_total += 1
        curline_bgp = rf.readline()
        curline_ip = rf.readline()
        elems = curline.strip('\n').strip(' ').split(']')
        if len(elems) < 2:
            curline = rf.readline()
            continue
        temp = curline_ip.strip('\n').strip(' ').split(']')
        if len(temp) < 2:
            curline = rf.readline()
            continue
        ip_path = temp[1].strip('\n').strip(' ')
        if not ip_path:
            curline = rf.readline()
            continue
        dst_key = elems[0][1:]
        dst_prefix = dst_key.split(' ')[0]
        ori_trace_path = elems[1].strip(' ')
        ori_bgp_path = CompressAsPath(curline_bgp.strip('\n').strip('\t'))
        normal = False
        (normal, dif_size) = TracePathIsNormalByIRRData(ori_trace_path, ori_bgp_path, ip_path, check_irr, check_newer_org)
        if not normal:
            wf_ab.write("%s%s%s" %(curline, curline_bgp, curline_ip))
            count_ab += 1
        curline = rf.readline()
    
    wf_ab.close()
    rf.close()
    print("Still ab: %d, percent: %.2f" %(count_ab, count_ab / count_total))
    return (count_ab, count_total)

'''
def CheckAbPathByMi(file_name): #用Mi的方法
    rf = open(file_name, 'r')
    w_filename = file_name + '_2'
    wf = open(w_filename, 'w')

    curline_as = rf.readline()
    ab_num = 0
    while curline_as:
        curline_bgp = rf.readline()
        curline_ip = rf.readline()
        elems = curline_ip.strip('\n').split(']')
        if len(elems) <= 1:
            curline_as = rf.readline() 
            continue
        ip_path = elems[1].strip(' ')
        as_path_mi = GetAsPathFstByMi(ip_path)
        #print(as_path_mi)
        #print(curline_bgp)
        (normal, dif_size) = TracePathIsNormal(as_path_mi, curline_bgp.strip('\n').strip('\t'), ip_path.strip('\n'), True)
        if not normal:
            new_curline_as = curline_as.split('] ')[0] + '] ' + as_path_mi + '\n'
            #print(new_curline_as)
            #print(curline_bgp)
            #print(curline_ip)
            wf.write(new_curline_as)
            #wf.write(curline_bgp) #以后跑程序这行不写，以便后面统一格式处理
            wf.write(curline_ip)
            ab_num += 1
        curline_as = rf.readline()    
    rf.close()
    wf.close()
    print(ab_num)
'''

def TmpChgPathFile(files):
    for filename in files:
        rf = open(filename, 'r')
        wf = open(filename + '_2', 'w')
        curline = rf.readline()
        while curline:
            if curline.__contains__('['):
                wf.write(curline)
            curline = rf.readline()
        rf.close()
        wf.close()

def TwoGeoSetClose(geo1_set, geo2_set):
    for geo1 in geo2_set:
        for geo2 in geo2_set:
            if GeoDistance(geo1, geo2) < min_dist_limit:
                return True
    return False

def GetSimilarAs(asn, ip):
    #if GetAsOfIpByMi(ip) == asn:
        #print("NOTICE in GetSimilarAs: ip %s, asn %s" %(ip, asn))
        #return asn
    
    fst_dot_index = ip.find('.')
    pre = ip[0:fst_dot_index]
    mid = ip[fst_dot_index + 1:ip.rfind('.')]
    rf = open('mi\\sorted_ip2node_' + pre, 'r')
    content = rf.read().strip('\n').strip(',')
    rf.close()
    if not content:
        print("NOTICE: 'mi\\sorted_ip2node_%s' has no content" %pre)
        return ""
    key = str(',') + mid
    start_index = content.find(key)
    if start_index == -1:   #没找到后怎么处理还没想好
        return asn
    as_info = dict()
    temp_list = content[start_index + 1:].split(',')
    for info in temp_list:
        elems = info.split(' ')
        if len(elems) < 2:
            continue
        tmp_mid_ip = elems[0]
        if mid == tmp_mid_ip[0:tmp_mid_ip.rfind('.')]:
            tmp_asn = GetAsOfRouterByMi(elems[1])
            if tmp_asn not in as_info.keys():
                as_info[tmp_asn] = set()
            as_info[tmp_asn].add(GetGeoOfRouterByMi(elems[1]))
        else:
            break
    add_asn = set()
    for cur_asn in asn.split('_'):
        if cur_asn in as_info.keys():
            cur_geos = as_info[cur_asn]
            for (tmp_asn, tmp_geos) in as_info.items():
                if cur_asn != tmp_asn and TwoGeoSetClose(cur_geos, tmp_geos):
                    add_asn.add(tmp_asn)
    if not add_asn and len(as_info) == 1:
        for tmp_asn in as_info.keys():
            if tmp_asn not in asn.split('_'):
                add_asn.add(tmp_asn)
    for new_asn in add_asn:
        asn += '_' + new_asn
    return asn

#2021.2.12将/24内mi mapping的所有AS返回，不考虑地理位置
def GetSimilarAs_2(ip):
    res = []
    fst_dot_index = ip.find('.')
    pre = ip[0:fst_dot_index]
    mid = ip[fst_dot_index + 1:ip.rfind('.')]
    #if not os.path.exists('..\\srcdata\\mi\\sorted_ip2node_' + pre):
        #return res
    rf = open('../../data/mi/sorted_ip2node_' + pre, 'r')
    content = rf.read().strip('\n').strip(',')
    rf.close()
    if not content:
        print("NOTICE: '../../data/mi/sorted_ip2node_%s' has no content" %pre)
        return ""
    key = str(',') + mid
    start_index = content.find(key)
    if start_index == -1:   #没找到后怎么处理还没想好
        return res
    temp_list = content[start_index + 1:].split(',')
    for info in temp_list:
        elems = info.split(' ')
        if len(elems) < 2:
            continue
        tmp_mid_ip = elems[0]
        if mid == tmp_mid_ip[0:tmp_mid_ip.rfind('.')]:
            tmp_asn = GetAsOfRouterByMi(elems[1])
            if tmp_asn not in res:
                res.append(tmp_asn)
        else:
            break    
    return res

'''
def CheckTraces3(filename):
    rf = open(filename, 'r')
    wf_ab = open("res_bgpcheck_ab_" + filename, 'w')
    cur_num_ab = 0

    curline_trace = rf.readline()
    while curline_trace:
        discard = rf.readline()
        curline_ip = rf.readline()
        elems = curline_trace.split(']')
        if len(elems) < 2:
            curline_trace = rf.readline()
            continue
        ori_trace_path = elems[1].strip('\n').strip(' ')
        ori_trace_list = ori_trace_path.split(' ')
        multi_hop_as_list = []
        for elem in ori_trace_list:
            if elem != '*' and elem != '?' and elem not in multi_hop_as_list and ori_trace_list.count(elem) > 1:
                multi_hop_as_list.append(elem)
        temp = curline_ip.split(']')
        if len(temp) < 2:
            curline_trace = rf.readline()
            continue
        ip_path = temp[1].strip('\n').strip(' ')
        if not ip_path:
            curline_trace = rf.readline()
            continue
        ip_list = ip_path.split(' ')
        dst_key = elems[0][1:]
        dst_as_str = dst_key.split(' ')[1]
        dst_as_list = dst_as_str.split('_') #考虑moas
        sel_bgp_path_list = []
        for dst_as in dst_as_list:
            if dst_as not in bgp_dict.keys():
                continue
            for bgp_path in bgp_dict[dst_as]:
                bgp_path_list = bgp_path.split(' ')
                contained = True
                for asn in multi_hop_as_list:
                    if not AsnInBgpPathList(asn, bgp_path_list):
                        contained = False
                        break
                if contained and bgp_path not in sel_bgp_path_list:
                    sel_bgp_path_list.append(bgp_path)
        
        normal = False
        min_dif_size = 100
        similar_bgp_path = ''
        update_flag = [ False for i in range(0, len(ori_trace_list))]
        for ori_bgp_path in sel_bgp_path_list:
            bgp_path_list = CompressAsPathToMin(ori_bgp_path).split(' ')
            for i in range(0, len(ori_trace_list)):
                asn = ori_trace_list[i]
                if asn == '*' or asn == '?' or AsnInBgpPathList(asn, bgp_path_list) or update_flag[i]:
                    continue
                ip = ip_list[i]
                new_asn = GetSimilarAs(asn, ip)
                ori_trace_list[i] = new_asn
                update_flag[i] = True
            (normal, dif_size) = TracePathIsNormal(' '.join(ori_trace_list), ori_bgp_path, ip_path, True)
            if normal:
                break
            if min_dif_size > dif_size:
                similar_bgp_path = ori_bgp_path
                min_dif_size = dif_size
        if not normal:
            wf_ab.write("[%s] %s\n" %(dst_key, ' '.join(ori_trace_list)))
            wf_ab.write("\t\t%s\n" %similar_bgp_path)
            wf_ab.write("%s" %curline_ip)
            cur_num_ab += 1
        curline_trace = rf.readline()
        
    print("[%s]num_ab: %d" %(filename, cur_num_ab))    
    wf_ab.close()
    rf.close()
'''
def CalDifAsNumInTrace(trace_list, bgp_path_list):    
    num = 0
    for asn in trace_list:
        if not AsnInBgpPathList(asn, bgp_path_list):
            num += 1
    return num

def CheckTraces3(filename): #即使trace_as重复出现也找相似的as
    rf = open(filename, 'r')
    wf_ab = open("res_bgpcheck_ab_" + filename, 'w')
    cur_num_ab = 0

    curline_trace = rf.readline()
    done = 0
    while curline_trace:
        print(done)
        done += 1
        discard = rf.readline()
        curline_ip = rf.readline()
        elems = curline_trace.strip('\n').strip(' ').split(']')
        if len(elems) < 2:
            curline_trace = rf.readline()
            continue
        ori_trace_path = elems[1].strip('\n').strip(' ')
        ori_trace_list = ori_trace_path.split(' ')
        trace_path = CompressAsPathToMin(CompressAsPath(ori_trace_path))
        trace_list = trace_path.split(' ')
        temp = curline_ip.strip('\n').strip(' ').split(']')
        if len(temp) < 2:
            curline_trace = rf.readline()
            continue
        ip_path = temp[1].strip('\n').strip(' ')
        if not ip_path:
            curline_trace = rf.readline()
            continue
        ip_list = ip_path.split(' ')
        dst_key = elems[0][1:]
        dst_as_str = dst_key.split(' ')[1]
        #all_bgp_path_list = []
        bgp_path_dict = dict()
        for dst_as in dst_as_str.split('_'):
            if dst_as in bgp_dict.keys():
                for bgp_path in bgp_dict[dst_as]:
                    if bgp_path not in bgp_path_dict.keys():
                        bgp_path_dict[bgp_path] = CalDifAsNumInTrace(trace_list, bgp_path.split(' '))
        sort_list = sorted(bgp_path_dict.items(), key=lambda d:d[1])
    
        normal = False
        min_dif_size = 100
        similar_bgp_path = ''
        update_flag = [ False for i in range(0, len(ori_trace_list))]
        for (ori_bgp_path, dif_num) in sort_list:
            bgp_path_list = ori_bgp_path.split(' ')
            for i in range(0, len(ori_trace_list)):
                asn = ori_trace_list[i]
                if asn == '*' or asn == '?' or asn.startswith('<') or AsnInBgpPathList(asn, bgp_path_list) or update_flag[i]:
                    continue
                ip = ip_list[i]
                new_asn = GetSimilarAs(asn, ip)
                ori_trace_list[i] = new_asn
                update_flag[i] = True
            (normal, dif_size) = TracePathIsNormal(' '.join(ori_trace_list), ori_bgp_path, ip_path, True)
            if normal:
                break
            if min_dif_size > dif_size:
                similar_bgp_path = ori_bgp_path
                min_dif_size = dif_size
        if not normal:
            wf_ab.write("[%s] %s\n" %(dst_key, ' '.join(ori_trace_list)))
            wf_ab.write("\t\t%s\n" %similar_bgp_path)
            wf_ab.write("%s" %curline_ip)
            cur_num_ab += 1
        curline_trace = rf.readline()
        
    print("[%s]num_ab: %d" %(filename, cur_num_ab))    
    wf_ab.close()
    rf.close()

def CheckTraces4(filename): #只根据文件中给出的bgp_path对比
    rf = open(filename, 'r')
    wf_ab = open("res_bgpcheck_ab_" + filename, 'w')
    cur_num_ab = 0

    curline_trace = rf.readline()
    done = 0
    while curline_trace:
        print(done)
        done += 1
        curline_bgp = rf.readline()
        curline_ip = rf.readline()
        elems = curline_trace.strip('\n').strip(' ').split(']')
        if len(elems) < 2:
            curline_trace = rf.readline()
            continue
        ori_trace_path = elems[1].strip('\n').strip(' ')
        ori_trace_list = ori_trace_path.split(' ')
        bgp_list = curline_bgp.strip('\n').strip('\t').split(' ')
        temp = curline_ip.strip('\n').strip(' ').split(']')
        if len(temp) < 2:
            curline_trace = rf.readline()
            continue
        ip_path = temp[1].strip('\n').strip(' ')
        if not ip_path:
            curline_trace = rf.readline()
            continue
        ip_list = ip_path.split(' ')
        normal = True
        for i in range(0, len(ori_trace_list)):
            cur_as = ori_trace_list[i]
            if cur_as == '*' or cur_as == '?' or cur_as.startswith('<') or AsnInBgpPathList(cur_as, bgp_list):
                continue
            new_as = GetSimilarAs(cur_as, ip_list[i])
            if not AsnInBgpPathList(new_as, bgp_list):
                normal = False
                break
        if not normal:
            wf_ab.write("%s" %curline_trace)
            wf_ab.write("%s" %curline_bgp)
            wf_ab.write("%s" %curline_ip)
            cur_num_ab += 1
        curline_trace = rf.readline()
        
    print("[%s]num_ab: %d" %(filename, cur_num_ab))    
    wf_ab.close()
    rf.close()


def TracePathIsInBgpPath(trace_list, bgp_list):
    for elem in trace_list:
        if elem == '*' or elem == '?' or elem.startswith('<'):
            continue
        if not AsnInBgpPathList(elem, bgp_list):
            return False
    return True

def CheckAbUseSimilarAs():
    for i in range(2,3):
        rf = open("res_last_hop_ab_" + str(i), 'r')
        wf_ab = open("res_last_hop_ab_" + str(i) + "_remain", 'w')
        wf_normal = open("res_last_hop_ab_" + str(i) + "_normal", 'w')
        curline_trace = rf.readline()
        num = 0
        while curline_trace:
            curline_bgp = rf.readline()
            curline_ip = rf.readline()
            elems = curline_trace.strip('\n').strip(' ').split(']')
            if len(elems) < 2:
                curline_trace = rf.readline()
                continue
            dst_key = elems[0][1:]
            dst_as_str = dst_key.split(' ')[1]
            ori_trace_path = elems[1].strip('\n').strip(' ')
            ori_trace_list = ori_trace_path.split(' ')
            trace_path = CompressAsPathToMin(CompressAsPath(ori_trace_path))
            trace_list = trace_path.split(' ')
            #former_trace_list = trace_list[0:len(trace_list) - i]
            begin_as = trace_list[-1 * i]
            begin_index = ori_trace_list.index(begin_as)
            temp = curline_ip.strip('\n').strip(' ').split(']')
            if len(temp) < 2:
                curline_trace = rf.readline()
                continue
            ip_path = temp[1].strip('\n').strip(' ')
            if not ip_path:
                curline_trace = rf.readline()
                continue
            ip_list = ip_path.split(' ')
            for j in range(begin_index, len(ori_trace_list)):
                if ori_trace_list[j] == '*' or ori_trace_list[j] == '?' or ori_trace_list[j].startswith('<'):
                    continue
                ori_trace_list[j] = GetSimilarAs(ori_trace_list[j], ip_list[j])
            find = False
            for dst_as in dst_as_str.split('_'):
                if dst_as in bgp_dict.keys():
                    for bgp_path in bgp_dict[dst_as]:
                        if TracePathIsInBgpPath(ori_trace_list, bgp_path.split(' ')):
                            find  = True
                            wf_normal.write("%s\n" %(' '.join(ori_trace_list)))
                            wf_normal.write("%s\n" %bgp_path)
                            wf_normal.write("%s" %curline_ip)
                            break
                if find:
                    break
            if not find:
                wf_ab.write("%s" %curline_trace)
                wf_ab.write("%s" %curline_bgp)
                wf_ab.write("%s" %curline_ip)
                num += 1
            curline_trace = rf.readline()
        print("%d still remain %d ab" %(i, num))
        rf.close()
        wf_ab.close()
        wf_normal.close()

lastip_dstip_dict = dict()
def GetTraceLastIpDstIpDict(filename):
    global lastip_dstip_dict
    rf = open(filename, 'r')
    curline_trace = rf.readline()

    while curline_trace:
        if curline_trace.startswith('T'):
            elems = curline_trace.strip('\n').split('\t')
            dst_ip = elems[2]
            last_ip = elems[-1]
            if last_ip != 'q':
                last_ip = last_ip.split(',')[0]
                if last_ip not in lastip_dstip_dict.keys():
                    lastip_dstip_dict[last_ip] = []
                lastip_dstip_dict[last_ip].append(dst_ip)
            else:
                i = 1
                while elems[-1 * i] == 'q':
                    i += 1
                last_ip = elems[-1 * i]
                key = '*' + last_ip
                if key not in lastip_dstip_dict.keys():
                    lastip_dstip_dict[key] = []
                lastip_dstip_dict[key].append(dst_ip)
        curline_trace = rf.readline()

def ClearTraceLastIpDstIpDict():
    lastip_dstip_dict.clear()

def SelectIp(dst_prefix, dst_ip_list):
    elems = dst_prefix.split('/')
    mask_len = int(elems[1])
    dst_ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton(elems[0]))[0])
    mask = 0xFFFFFFFF - (1 << (32 - mask_len)) + 1
    res_list = []
    for elem in dst_ip_list:
        elem_int = socket.ntohl(struct.unpack("I",socket.inet_aton(elem))[0])        
        if dst_ip_int == (elem_int & mask):
            res_list.append(elem)
    if not res_list:
        print('')
    return res_list

def GetMiAsOfDstIpAndRecheck(filename, wf):
    rf = open(filename, 'r')
    curline_trace = rf.readline()

    while curline_trace:
        curline_bgp = rf.readline()
        curline_ip = rf.readline()
        dst_key = curline_trace.split(']')[0]
        elems = dst_key.split(' ')
        dst_prefix = elems[0][1:]
        dst_as = elems[1]
        as_trace = curline_trace.split(']')[1].strip('\n').strip(' ')
        as_list = as_trace.split(' ')
        if len(as_list) <= 1:
            curline_trace = rf.readline()
            continue
        while as_list and as_list[-1] == '*' or as_list[-1] == '?':
            as_list.pop()
        if not as_list:
            curline_trace = rf.readline()
            continue
        find = False
        last_as = as_list[-1].strip('<').strip('>')
        for cur_dst_as in dst_as.split('_'):
            if cur_dst_as in last_as.split('_'):
                find = True
                break
        if find:
            wf.write(curline_trace)
            wf.write(curline_bgp)
            wf.write(curline_ip)
            curline_trace = rf.readline()
            continue
        res_list = [dst_prefix[:dst_prefix.rindex('.')] + '.1']
        if dst_prefix.split('/')[1] != '24':
            ip_list = curline_ip.strip('\n').split(' ')
            last_ip = ip_list[-1].strip('<').strip('>')
            dst_ip_list = []
            if last_ip != '*':
                if last_ip not in lastip_dstip_dict.keys():
                    print('NOTE! GetMiAsOfDstIpAndRecheck dict key not found_1')
                    print(curline_ip, end='')
                    return False
                dst_ip_list = lastip_dstip_dict[last_ip]
            else:
                i = 1
                while elems[-1 * i] == 'q':
                    i += 1
                last_ip = elems[-1 * i]
                key = '*' + last_ip
                if key not in lastip_dstip_dict.keys():
                    print('NOTE! GetMiAsOfDstIpAndRecheck dict key not found_2')
                    print(curline_ip, end='')
                    return False
                dst_ip_list = lastip_dstip_dict[key]
            res_list = SelectIp(dst_prefix, dst_ip_list)
            if not res_list:
                print('NOTE! GetMiAsOfDstIpAndRecheck dst_ip not found')
                print(curline_ip, end='')
                return False
        match = False
        similar_bgp_path = ''
        min_dif_size = 1000
        new_dst_asn = ''
        for ip in res_list:
            #cur_as = GetAsOfIpByMi(ip)
            new_as_list = GetSimilarAs_2(ip) #2012.2.12改为同一/24子网内的模糊匹配
            if not new_as_list:
                continue
            match = False
            for cur_as in new_as_list:
                if cur_as not in dst_as.split('_') and cur_as in last_as.split('_'):
                    if cur_as in bgp_dict.keys():
                        for bgp_path in bgp_dict[cur_as]:
                            (match, dif_size) = TracePathIsNormal(as_trace, bgp_path, curline_ip.split(']')[1].strip('\n').strip(' '), False)
                            if match:
                                break
                            if min_dif_size > dif_size:
                                similar_bgp_path = bgp_path
                                min_dif_size = dif_size
                                new_dst_asn = cur_as
                if match:
                    break
        if not match:
            if new_dst_asn:
                wf.write("[%s %s] %s\n" %(dst_prefix, new_dst_asn, as_trace))
                wf.write("\t\t%s\n" %similar_bgp_path)
                wf.write("[%s] %s" %(' '.join(dst_ip_list), curline_ip.split(']')[1]))
            else:
                wf.write(curline_trace)
                wf.write(curline_bgp)
                wf.write(curline_ip)
        curline_trace = rf.readline()
    rf.close()

def TmpReCheckTracesUseNewGetDiffList(dir):
    rf = open(dir + 'ab_filter1' , 'r')
    wf = open(dir + 'ab_filter2' , 'w')

    curline_trace = rf.readline()
    while curline_trace:
        curline_bgp = rf.readline()
        curline_ip = rf.readline()
        ori_trace_path = curline_trace.split(']')[1].strip('\n').strip(' ')
        trace_path = CompressAsPathToMin(CompressAsPath(ori_trace_path))
        bgp_path = CompressAsPathToMin(curline_bgp.strip('\n').strip('\t'))
        if trace_path == bgp_path:
            curline_trace = rf.readline()
            continue
        trace_list = trace_path.split(' ')
        bgp_list = bgp_path.split(' ')
        if TracePathListEqualsBgpPathList(trace_list, bgp_list):
            curline_trace = rf.readline()
            continue
        dif_list = GetDiffList(trace_list, bgp_list)
        for dif_seg in dif_list:
            (dif_trace_range, dif_bgp) = dif_seg
            if dif_trace_range: 
                wf.write(curline_trace)
                wf.write(curline_bgp)
                wf.write(curline_ip)
                break
        curline_trace = rf.readline()
    rf.close()
    wf.close()


def CheckAbPathByDstAs(filename, w_filename):
    rf = open(filename, 'r')
    wf = open(w_filename, 'w')
    curline_trace = rf.readline()
    count_ab = 0
    count_total = 0

    while curline_trace:
        count_total += 1
        curline_bgp = rf.readline()
        curline_ip = rf.readline()
        elems = curline_trace.strip('\n').split(']')
        dst_as_str = elems[0].split(' ')[1]
        ori_trace_path = elems[1].strip(' ')
        normal = False
        for dst_as in dst_as_str.split('_'):
            bgp_path_list = GetBgpPathByAs(dst_as)
            if not bgp_path_list:
                print("NOTICE: CheckTraces() dst_as %s not found in bgp table" %dst_as)
            for ori_bgp_path in bgp_path_list:
                (normal, dif_size) = TracePathIsNormal(ori_trace_path, ori_bgp_path, '', False)
                if normal:
                    break
            if normal:
                break
        if not normal:
            wf.write(curline_trace)
            wf.write(curline_bgp)
            wf.write(curline_ip)
            count_ab += 1
        curline_trace = rf.readline()
    rf.close()
    wf.close()
    return (count_ab, count_total)

def TagStepInRecordFile(record_file_name, step):
    res = ''
    if os.path.isfile(record_file_name):
        with open(record_file_name, 'r') as f:
            res = f.read()
            if res.__contains__(step):
                if res.index(step) == 0:
                    res = ''
                else:
                    res = res[0:res.index(step)]
    wf = open(record_file_name, 'w')
    wf.write("%s\n\n%s\n" %(res, step))
    wf.close()

def ClearTagInRecordFile(record_file_name, step):
    res = ''
    if os.path.isfile(record_file_name):
        with open(record_file_name, 'r') as f:
            res = f.read()
            if res.__contains__(step):
                if res.index(step) == 0:
                    res = ''
                else:
                    res = res[0:res.index(step)]
    wf = open(record_file_name, 'w')
    wf.write("%s\n" %res)
    wf.close()

'''
def main_back():
    #dirs = ['hkg-cn.20190301\\', 'syd-au.20190301\\', 'sjc2-us.20190301\\', 'nrt-jp.20190301\\', 'zrh2-ch.20190302\\']
    #dirs = ['hkg-cn.20190315\\', 'syd-au.20190315\\', 'sjc2-us.20190315\\', 'nrt-jp.20190315\\', 'zrh2-ch.20190314\\']
    #dirs = ['hkg-cn.20190401\\', 'syd-au.20190401\\', 'nrt-jp.20190401\\', 'zrh2-ch.20190402\\']
    dirs = ['nrt-jp.20190315\\']
    steps = ['step 1', 'step 2', 'step 3', 'step 4', 'step 5']

    ConnectToDb()

    for cur_dir in dirs:
        print(cur_dir)
        #if not os.path.isdir(cur_dir):
            #os.makedirs(cur_dir)
        os.chdir(cur_dir)
        trace_file_name = cur_dir.strip('\\')
        #os.system("copy ..\\%s %s" %(trace_file_name, trace_file_name))
        g_asn = trace_as_dict[trace_file_name.split('.')[0]]
        #os.system("copy ..\\bgp_%s bgp_%s" %(g_asn, g_asn))
        record_file_name = 'record_' + trace_file_name

        if steps.__contains__('step 1'):
            print('step 1')
            if os.path.exists(record_file_name):
                os.remove(record_file_name)
            TagStepInRecordFile(record_file_name, 'step 1')
            #GetPfx2ASByRv() #step 1, 3, 4
            GetPfx2ASByBgp('bgp_' + g_asn) #2021.3.2使用VP本身的bgp表进行匹配，而不是用routeviews-rv2-20190301-1200.pfx2as_coalesced
            GetSibRel() #step 1, 3, 5，这步一开始错过了！在进行不一致分析的时候发现，后来补上
            GetIxpPfxDict() #step 1
            GetIxpAsSet() #step 1
            ChgTrace2ASPath(trace_file_name, record_file_name, 'a') #step 1
            #TmpRecordIxpPath(trace_file_name)
            ClearIxpAsSet()
            ClearIxpPfxDict()
            ClearSibRel()
            ClearIp2AsDict()
        
        #step 2 #经过改进，mi mapping的效率有所提高
        if steps.__contains__('step 2'):
            print('step 2')
            TagStepInRecordFile(record_file_name, 'step 2')
            filename = '4_multi_path'
            CheckMultiPathByMi(filename, record_file_name, 'a') #step 3 #这一步非常耗时

        if steps.__contains__('step 3'):
            print('step 3')
            TagStepInRecordFile(record_file_name, 'step 3')
            #GetPfx2ASByRv() #step 1, 3, 4
            GetPfx2ASByBgp('bgp_' + g_asn) #2021.3.2使用VP本身的bgp表进行匹配，而不是用routeviews-rv2-20190301-1200.pfx2as_coalesced
            GetSibRel() #step 1, 3, 5，这步一开始错过了！在进行不一致分析的时候发现，后来补上
            GetBgpByPrefix(g_asn) #step 3, 4
            w_record = open(record_file_name, 'a')
            files = ['3_single_path', '4_multi_path_single', '4_multi_path_multi', '5_has_ixp_ip']
            #files = ['4_multi_path_single', '4_multi_path_multi']
            for filename in files:
                (count_ab, count_total) = CheckTraces(filename, False) #step 3
                w_record.write("In %s, ab num: %d, ab precent: %.2f\n" %(filename, count_ab, count_ab / count_total))
            w_record.close()
            #ClearBGP()
            ClearBGPByPrefix()
            ClearSibRel()
            ClearIp2AsDict()

        if steps.__contains__('step 4'):
            print('step 4')
            TagStepInRecordFile(record_file_name, 'step 4')
            GetPfx2ASByBgp('bgp_' + g_asn) #2021.3.2使用VP本身的bgp表进行匹配，而不是用routeviews-rv2-20190301-1200.pfx2as_coalesced
            GetSibRel() #step 1, 3, 5，这步一开始错过了！在进行不一致分析的时候发现，后来补上
            #GetBgp('bgp_' + g_asn) #step 3, 4
            GetBgpByPrefix(g_asn) #step 3, 4
            #PreOpenRouterGeoFiles()
            #PreOpenRouterIpFiles()
            #PreOpenAsRouterFiles()
            w_record = open(record_file_name, 'a')
            files = ['3_single_path_ab', '5_has_ixp_ip_ab']
            #files = ['3_single_path_ab']
            for filename in files:
                #freq_fst_seg = CalIpFreq(filename, 20, True)
                #pre_seg_list = []
                #for elem in freq_fst_seg:
                    #pre_seg_list.append(elem[0])
                #print(pre_seg_list)
                #PreConstrRouterIpDict(pre_seg_list)
                (count_ab, count_total) = CheckTraces(filename, True) #step 4
                w_record.write("In %s, ab num: %d, ab precent: %.2f\n" %(filename, count_ab, count_ab / count_total))
                #ClearRouterIpDict()
            #CloseGeoRouterFiles()
            #CloseRouterIpFiles()
            #CloseAsRouterFiles()
            w_record.close()
            #ClearBGP()
            ClearBGPByPrefix()
            ClearSibRel()
            ClearIp2AsDict()
        
        ##step 5和step 6后续不再使用-------------------------
        #step 5 再用bgp把step 4的不一致路径滤一遍，这步其实应该合并到step 4
        #PreOpenRouterGeoFiles()
        #CheckTraces(['res1_ab_single2_2', 'res1_ab_multi_2']) #step 3
        #CloseGeoRouterFiles()
        
        #step6 分析发现，有的时候rv mapping出的AS匹配bgp_path，有时候是mi mapping满足，原因不明
        # 这时，选择只要有一种能和bgp_path匹配，就认为正常
        #CheckTraces(['res1_ab_single', 'res1_ab_single2', 'res1_ab_multi']) #此时源文件是经过mi mapping过的
        #CheckTraces(['Test'])

        #step 5 对目的ip用Middar重新map AS，看是否有正常的路径，过滤（结果来看很少）
        #dirs = ['syd-au.20190301\\', 'hkg-cn.20190301\\', 'sjc2-us.20190301\\']
        #2021.3.2 在改为使用prefix进行匹配后，不做对目的ip用Middar重新map AS，改为看一看，匹配同一dst_as，不同dst_prefix的情况有多少
        if steps.__contains__('step 5'):
            print('step 5')
            TagStepInRecordFile(record_file_name, 'step 5')
            files = ['3_single_path_ab_ab', '4_multi_path_single_ab', '4_multi_path_multi_ab', '5_has_ixp_ip_ab_ab']
            #files = ['4_multi_path_single_ab', '4_multi_path_multi_ab', '5_has_ixp_ip_ab_ab']
            CombineFiles(files, 'ab_filter0')
            GetPathAsDict('bgp_' + g_asn)
            (count_ab, count_total) = CheckAbPathByDstAs('ab_filter0', 'ab_filter1')
            w_record = open(record_file_name, 'a')
            w_record.write("In ab_filter0, ab num: %d, ab precent: %.2f\n" %(count_ab, count_ab / count_total))
            w_record.close()
            ClearPathAsDict()

        #这一步是打补丁，实际中不用
        if steps.__contains__('delet step 5'):
            os.remove('ab_filter1')
            os.rename('ab_filter1_ab', 'ab_filter1')
            os.remove('ab_filter1_no_bgp')
            
        os.chdir('..\\')
        
    #2021.2.6 原来的GetDiffList有问题，没有考虑moas的情况，CompressAsPathToMin不会删掉moas情况下实际上重复的AS
    #这里修改了GetDiffList()，可以进一步减少ab的数量。再次运行前面的函数时，已经考虑了moas的情况，不需要再运行这一步
    if steps.__contains__('temp'):
        #dirs = ['syd-au.20190301\\', 'hkg-cn.20190301\\', 'sjc2-us.20190301\\']
        #for dir in dirs:
            #TmpReCheckTracesUseNewGetDiffList(dir)
        GetMiAsOfDstIpAndRecheck('temp', 'temp_res')
        
    CloseDb()

    #step 8 查找可替代的AS，再次过滤
    filename = 'res1.2.1_ab_single_nrt-jp.20190301_remain'
    #filename = 'test'
    freq_fst_seg = CalIpFreq(filename, 20)
    pre_seg_list = []
    for elem in freq_fst_seg:
        pre_seg_list.append(elem[0])
    #print(pre_seg_list)
    PreConstrRouterIpDict(pre_seg_list)
    PreOpenRouterGeoFiles()
    PreOpenRouterIpFiles()
    PreOpenAsRouterFiles()
    
    #filename = "res_not_last_hop_ab"
    #CheckTraces4(filename)
'''


def StatMatchEachTrace(ori_trace_path, ori_bgp_path, ip_path):
    info = dict()
    info['true'] = [set(), dict()]
    info['false'] = [set(), dict()]
    info['unknown'] = [set(), dict()]
    bgp_path_list = CompressAsPathToMin(ori_bgp_path).split(' ')
    ori_trace_path_list = ori_trace_path.split(' ')
    ip_path_list = ip_path.split(' ')
    normal = True
    bgp_prev_index = 0
    ip_prev_index = 0
    bgp_next_index = 0
    for i in range(0, len(ori_trace_path_list)):
        cur_hop = ori_trace_path_list[i]
        if cur_hop.__contains__('*') or cur_hop.__contains__('<'):
            continue
        if cur_hop.__contains__('?'):
            info['unknown'][0].add(ip_path_list[i])
            if ip_path_list[i] in info['unknown'][1].keys():
                info['unknown'][1][ip_path_list[i]] += 1
            else:
                info['unknown'][1][ip_path_list[i]] = 1
            continue
        if AsnInBgpPathList(cur_hop, bgp_path_list):
            bgp_prev_index = FindTraceAsInBgpPath(cur_hop, bgp_path_list)
            ip_prev_index = i
            bgp_next_index = bgp_prev_index + 1
            while bgp_next_index < len(bgp_path_list):
                if AsnInTracePathList(bgp_path_list[bgp_next_index], ori_trace_path_list):
                    break
                bgp_next_index += 1
            info['true'][0].add(ip_path_list[i])
            if ip_path_list[i] in info['true'][1].keys():
                info['true'][1][ip_path_list[i]] += 1
            else:
                info['true'][1][ip_path_list[i]] = 1
            continue  #正常，看下一跳
        ip_next_index = i + 1
        while ip_next_index < len(ip_path_list) and (ip_path_list[ip_next_index].__contains__('*') or ip_path_list[ip_next_index].__contains__('<')):
            ip_next_index += 1
        bgp_prev_hop = bgp_path_list[bgp_prev_index]
        if IsSib_2(bgp_prev_hop, cur_hop):  #和左边正常的hop是sibling关系
            info['true'][0].add(ip_path_list[i])
            if ip_path_list[i] in info['true'][1].keys():
                info['true'][1][ip_path_list[i]] += 1
            else:
                info['true'][1][ip_path_list[i]] = 1
            continue    #正常，看下一跳
        if bgp_next_index == 0: #abnormal
            print(ori_trace_path)
            print(ori_bgp_path)
        if bgp_next_index < len(bgp_path_list):
            bgp_next_hop = bgp_path_list[bgp_next_index]
            if IsSib_2(bgp_next_hop, cur_hop):   #和右边的hop是sibling关系或者离得特别近，可能是IXP引起的不一致
                info['true'][0].add(ip_path_list[i])
                if ip_path_list[i] in info['true'][1].keys():
                    info['true'][1][ip_path_list[i]] += 1
                else:
                    info['true'][1][ip_path_list[i]] = 1
                continue    #正常，看下一跳
        normal = False
        if bgp_prev_index + 1 < bgp_next_index:
            for j in range(bgp_prev_index + 1, bgp_next_index):
                bgp_hop = bgp_path_list[j]
                if IsSib_2(bgp_hop, cur_hop):   #和对应的bgp hop是sibling关系
                    normal = True    #正常，看下一跳
                    info['true'][0].add(ip_path_list[i])
                    if ip_path_list[i] in info['true'][1].keys():
                        info['true'][1][ip_path_list[i]] += 1
                    else:
                        info['true'][1][ip_path_list[i]] = 1
                    break
        if not normal:
            info['false'][0].add(ip_path_list[i])
            if ip_path_list[i] in info['false'][1].keys():
                info['false'][1][ip_path_list[i]] += 1
            else:
                info['false'][1][ip_path_list[i]] = 1
            for j in range(i + 1, len(ori_trace_path_list)):
            #if False:
                if ori_trace_path_list[j].__contains__('*') or ori_trace_path_list[j].__contains__('<'):
                    continue
                if ori_trace_path_list[j].__contains__('?'):
                    info['unknown'][0].add(ip_path_list[j])
                    if ip_path_list[j] in info['unknown'][1].keys():
                        info['unknown'][1][ip_path_list[j]] += 1
                    else:
                        info['unknown'][1][ip_path_list[j]] = 1
                    continue
                if AsnInBgpPathList(ori_trace_path_list[j], bgp_path_list):
                    info['true'][0].add(ip_path_list[j])
                    if ip_path_list[j] in info['true'][1].keys():
                        info['true'][1][ip_path_list[j]] += 1
                    else:
                        info['true'][1][ip_path_list[j]] = 1
                else:
                    info['false'][0].add(ip_path_list[j])
                    if ip_path_list[j] in info['false'][1].keys():
                        info['false'][1][ip_path_list[j]] += 1
                    else:
                        info['false'][1][ip_path_list[j]] = 1
            return (False, len(bgp_path_list) - bgp_prev_index, info)    #有一跳不正常，即为不正常，把有几处不一致的计算方法进行修改
    return (True, 0, info)

def StatMatchEachFile(filename, info): #info['true', 'false', 'unknown'], info['true']: [set(), dict()]
    rf = open(filename, 'r')
    curline = rf.readline()
    while curline:
        curline_bgp = ''
        curline_ip = rf.readline()
        elems = curline.strip('\n').strip(' ').split(']')
        if len(elems) < 2:
            curline = rf.readline()
            continue
        temp = curline_ip.strip('\n').strip(' ').split(']')
        if len(temp) < 2:
            curline = rf.readline()
            continue
        ip_path = temp[1].strip('\n').strip(' ')
        if not ip_path:
            curline = rf.readline()
            continue
        dst_key = elems[0][1:]
        dst_prefix = dst_key.split(' ')[0]
        ori_trace_path = elems[1].strip(' ')
        normal = False
        min_dif_size = 100
        similar_bgp_path = ''
        ori_trace_path_2 = ori_trace_path
        bgp_path_list = GetBgpPathFromBgpPrefixDict_2(dst_prefix)
        if not bgp_path_list:
            print("NOTICE: CheckTraces() prefix %s not found in bgp table" %dst_prefix)
            curline = rf.readline()
            continue
        sel_info = dict()
        for ori_bgp_path in bgp_path_list:
            (normal, dif_size, cur_info) = StatMatchEachTrace(ori_trace_path_2, ori_bgp_path, ip_path)
            if normal:
                sel_info = copy.deepcopy(cur_info)
                break
            if min_dif_size > dif_size:
                sel_info = copy.deepcopy(cur_info)
                min_dif_size = dif_size
        for cur_type in sel_info.keys():
            info[cur_type][0] |= sel_info[cur_type][0]
            for (ip, count) in sel_info[cur_type][1].items():
                if ip in info[cur_type][1].keys():
                    info[cur_type][1][ip] += count
                else:
                    info[cur_type][1][ip] = count
        curline = rf.readline()
    
    rf.close()
    return info

def CalStat_1(info): #[set(), dict()]
    num1 = len(info[0])
    num2 = len(info[1])
    freq_sum = 0
    for (ip, count) in info[1].items():
        freq_sum += count
    avg_ip_freq = freq_sum / num2
    return (num2, avg_ip_freq)
    
def StatMatchIp(vps, year, month, map_methods):
    steps = ['extra_step_match_statistic']
    year_str = str(year)
    month_str = str(month).zfill(2)
    date = year_str + month_str + '15'
    
    GetSibRel(year, month)
    GetIxpPfxDict(year, month)
    GetIxpAsSet()   #这条语句需要放在GetSibRel()之后，因为需要sib_dict，也就是as2org_dict
    ConnectToDb()
    SetCurMidarTableDate(year, month)

    for vp in vps:
        cur_dir = global_var.par_path +  global_var.out_my_anatrace_dir + '/' + vp + '_' + date + '/' #'nrt-jp.2019030115/'
        print(cur_dir)
        os.chdir(cur_dir)

        #os.system("copy ..\\%s %s" %(trace_file_name, trace_file_name))
        #os.system("copy ..\\bgp_%s bgp_%s" %(g_asn, g_asn))
        g_asn = global_var.trace_as_dict[vp]    
        bgp_filename = global_var.par_path + global_var.rib_dir + 'bgpdata/bgp_' + g_asn + '_' + date
        GetPfx2ASByBgp(bgp_filename) #2021.3.2使用VP本身的bgp表进行匹配，而不是用routeviews-rv2-20190301-1200.pfx2as_coalescedGetSibRel()
        GetBgpByPrefix(bgp_filename) #step 2 #get bgp_path to dst_prefix
        GetPathAsDict(bgp_filename) #step 3 #get bgp_path to dst_as
        
        global cur_map_method
        match_stat = dict()
        for tmp_map_method in map_methods:
            cur_map_method = tmp_map_method
            print(cur_map_method)
            #if cur_map_method.__contains__('ribs'):
            os.chdir(cur_map_method)
            record_file_name = 'record_' + vp + '.' + date + '_' + cur_map_method
            #os.system("pwd")
            #print(record_file_name)
            if not os.path.isfile(record_file_name):
                print(vp + '.' + date + '_' + cur_map_method + 'previous analyze not ready')
                os.chdir('..')
                continue
            record_file_name_2 = 'record2_' + vp + '.' + date + '_' + cur_map_method
            InitMidarCache()
            InitBdrCache()
            if cur_map_method.__contains__('bdrmapit'):
                if year == 2016 and month < 11: #2016年7-10月的bdrmapit没有跑出数据
                    os.chdir('..')
                    continue
                ConnectToBdrMapItDb(global_var.par_path + global_var.out_bdrmapit_dir + 'bdrmapit_' + vp + '_' + date + '.db')
                ConstrBdrCache()

            if steps.__contains__('extra_step_match_statistic'):
                print('extra_step_match_statistic')
                match_stat[cur_map_method] = dict()
                match_stat[cur_map_method]['true'] = [set(), dict()]
                match_stat[cur_map_method]['false'] = [set(), dict()]
                match_stat[cur_map_method]['unknown'] = [set(), dict()]
                files = ['3_single_path_fin', '4_multi_path_fin', '5_has_ixp_ip']
                for cur_file in files:
                    match_stat[cur_map_method] = StatMatchEachFile(cur_file, match_stat[cur_map_method])
                total_num = len(match_stat[cur_map_method]['true'][0] | match_stat[cur_map_method]['false'][0] | match_stat[cur_map_method]['unknown'][0])
                undecide_set = match_stat[cur_map_method]['true'][0] & match_stat[cur_map_method]['false'][0]
                if len(undecide_set) > 0:
                    print("true/false intersec: %s_%s_%s: %d" %(vp, date, cur_map_method, len(undecide_set)))
                    match_stat[cur_map_method]['true'][0] -= undecide_set
                    match_stat[cur_map_method]['false'][0] -= undecide_set
                if match_stat[cur_map_method]['true'][0] & match_stat[cur_map_method]['unknown'][0]: 
                    print("Warning. true/unknown intersec: %s_%s_%s: %d" %(vp, date, cur_map_method, len(match_stat[cur_map_method]['true'][0] & match_stat[cur_map_method]['unknown'][0])))
                if match_stat[cur_map_method]['false'][0] & match_stat[cur_map_method]['unknown'][0]:
                    print("Warning. false/unknown intersec: %s_%s_%s: %d" %(vp, date, cur_map_method, len(match_stat[cur_map_method]['false'][0] & match_stat[cur_map_method]['unknown'][0])))
                w_record = open(record_file_name_2, 'w')
                (true_num, avg_ip_freq_true) = CalStat_1(match_stat[cur_map_method]['true'])
                (false_num, avg_ip_freq_false) = CalStat_1(match_stat[cur_map_method]['false'])
                (unknown_num, avg_ip_freq_unknown) = CalStat_1(match_stat[cur_map_method]['unknown'])
                print("match rate: %.2f, unmatch rate: %.2f, unknown rate: %.2f, total ip num: %d" %(true_num / total_num, false_num / total_num, unknown_num / total_num, total_num))
                w_record.write("Match statics, match rate: %.2f, unmatch rate: %.2f, unknown rate: %.2f, total ip num: %d\n" %(true_num / total_num, false_num / total_num, unknown_num / total_num, total_num))
                print("avg_ip_freq_true: %.2f, avg_ip_freq_false: %.2f, avg_ip_freq_unknown: %.2f" %(avg_ip_freq_true, avg_ip_freq_false, avg_ip_freq_unknown))
                w_record.write("avg_ip_freq_true: %.2f, avg_ip_freq_false: %.2f, avg_ip_freq_unknown: %.2f\n" %(avg_ip_freq_true, avg_ip_freq_false, avg_ip_freq_unknown))
                w_record.close()
                
            if cur_map_method.__contains__('bdrmapit'):
                CloseBdrMapItDb()
            os.chdir('..')

        if steps.__contains__('extra_step_match_statistic'):
            fn = len(match_stat['ribs']['true'][0] &  match_stat['bdrmapit']['false'][0])
            tn = len(match_stat['ribs']['false'][0] & match_stat['bdrmapit']['true'][0])
            tp = len(match_stat['ribs']['true'][0] & match_stat['bdrmapit']['true'][0])
            fp = len(match_stat['ribs']['false'][0] & match_stat['bdrmapit']['false'][0])
            fnr = fn / (fn + tp)
            tnr = tn / (fp + tn)
            t_from_u_r = len(match_stat['ribs']['unknown'][0] & match_stat['bdrmapit']['true'][0]) / len(match_stat['ribs']['unknown'][0])
            f_from_u_r = len(match_stat['ribs']['unknown'][0] & match_stat['bdrmapit']['false'][0]) / len(match_stat['ribs']['unknown'][0])
            u_from_u_r = len(match_stat['ribs']['unknown'][0] & match_stat['bdrmapit']['unknown'][0]) / len(match_stat['ribs']['unknown'][0])
            wf = open('record2_' + vp + '.' + date, 'w')
            print("Match statistics: fn: %d, tn: %d, tp: %d, fp: %d, fnr: %.2f, tnr: %.2f" %(fn, tn, tp, fp, fnr, tnr))
            wf.write("Match statistics: fn: %d, tn: %d, tp: %d, fp: %d, fnr: %.2f, tnr: %.2f\n" %(fn, tn, tp, fp, fnr, tnr))
            print("Bdrmapit t_from_u_r: %.2f, f_from_u_r: %.2f, u_from_u_r: %.2f\n" %(t_from_u_r, f_from_u_r, u_from_u_r))
            wf.write("Bdrmapit t_from_u_r: %.2f, f_from_u_r: %.2f, u_from_u_r: %.2f\n" %(t_from_u_r, f_from_u_r, u_from_u_r))
            wf.close()
            match_stat.clear()

        ClearPathAsDict() #get bgp_path to dst_as
        ClearBGPByPrefix() #get bgp_path to dst_prefix
        ClearIp2AsDict()
        os.chdir('..')
    
    CloseDb()
    ClearIxpAsSet()
    ClearIxpPfxDict()
    ClearSibRel()

def FilterOneAbBetweenSameNormal(filename):
    rf = open(filename, 'r')
    wf_ab = open(filename + '_ab', 'w')
    wf_filter_oneab = open(filename + '_filter_oneab', 'w')
    wf_unmap = open(filename + '_unmap', 'w')
    count_ab = 0
    count_unmap = 0
    count_total = 0

    curline_trace = rf.readline()
    while curline_trace:
        count_total += 1
        curline_bgp = rf.readline()
        curline_ip = rf.readline()
        ori_trace_path = curline_trace.strip('\n').strip(' ').split(']')[1].strip(' ')
        ori_trace_path_list = ori_trace_path.split(' ')
        bgp_path_list = CompressAsPathToMin(curline_bgp.strip('\n').strip('\t')).split(' ')
        normal = True
        if CompressAsPathToMin(ori_trace_path) == '7660 2516 7922 13789 6461 <> 29791_13789':
            print(ori_trace_path)
            print(bgp_path_list)
        #print(bgp_path_list)
        for i in range(0, len(ori_trace_path_list)):
            cur_hop = ori_trace_path_list[i]
            if cur_hop.__contains__('*') or cur_hop.__contains__('?') or cur_hop.__contains__('<'):
                continue
            if not AsnInBgpPathList(cur_hop, bgp_path_list):
                bgp_prev = None
                bgp_next = None
                if i > 1: #看左边
                    for j in range(i - 1, -1, -1):
                        prev_hop = ori_trace_path_list[j]
                        if prev_hop.__contains__('*') or prev_hop.__contains__('?') or prev_hop.__contains__('<'):
                            continue
                        bgp_prev = FindTraceAsSetInBgpPath(prev_hop, bgp_path_list)
                        break
                if i < (len(ori_trace_path_list) - 1):
                    next_hop = None
                    for j in range(i + 1, len(ori_trace_path_list)):
                        next_hop = ori_trace_path_list[j]
                        if next_hop.__contains__('*') or next_hop.__contains__('?') or next_hop.__contains__('<'):
                            next_hop = None
                        else:
                            break
                    if next_hop:
                        bgp_next = FindTraceAsSetInBgpPath(next_hop, bgp_path_list)
                #print(cur_hop)
                #print(bgp_prev)
                #print(bgp_next)
                if bgp_prev and bgp_next and (bgp_prev & bgp_next):
                    pass #左右都map到了bgp_path中的同一跳，该异常跳可能是map出错
                else: #不正常，记录
                    normal = False
                    break
        if not normal:
            wf_ab.write(curline_trace)
            wf_ab.write(curline_bgp)
            wf_ab.write(curline_ip)
            count_ab += 1
        elif curline_trace.__contains__('?'): #2021.5.18 记录ip没有map的trace
            wf_unmap.write(curline_trace)
            wf_unmap.write(curline_bgp)
            wf_unmap.write(curline_ip)
            count_unmap += 1
        else:
            wf_filter_oneab.write(curline_trace)
            wf_filter_oneab.write(curline_bgp)
            wf_filter_oneab.write(curline_ip)
        curline_trace = rf.readline()
    
    wf_ab.close()
    wf_unmap.close()
    wf_filter_oneab.close()
    rf.close()
    return (count_ab, count_unmap, count_total)

def NormTraceAS(filename, w_filename): #traceline里有的hop '_'间有重复的asn，把多余的过滤掉    
    rf = open(filename, 'r')
    wf = open(w_filename, 'w')
    curline_trace= rf.readline()

    while curline_trace:
        curline_bgp = rf.readline()
        curline_ip = rf.readline()
        elems = curline_trace.strip('\n').split(']')
        dst_key = elems[0] + ']'
        trace_list = elems[1].split(' ')
        new_trace_list = []
        for cur_hop in trace_list:
            new_hop_list = []
            for asn in cur_hop.split('_'):
                if asn not in new_hop_list:
                    new_hop_list.append(asn)
            new_trace_list.append('_'.join(new_hop_list))
        wf.write(dst_key + ' ' + ' '.join(new_trace_list) + '\n')
        wf.write(curline_bgp)
        wf.write(curline_ip)
        curline_trace = rf.readline()
    rf.close()
    wf.close()

def FindSimilarestBgpPath(filename, w_filename):
    rf = open(filename, 'r')
    wf = open(w_filename, 'w')
    curline_trace= rf.readline()

    while curline_trace:
        curline_bgp = rf.readline()
        curline_ip = rf.readline()
        elems = curline_trace.strip('\n').split(']')
        trace_list = elems[1].split(' ')
        dst_as = elems[0].split(' ')[1]
        max_match_num = 0
        similarest_path = ''
        for asn in dst_as.split('_'):
            bgp_path_list = GetBgpPathByAs(asn)
            for cur_bgp_path in bgp_path_list:
                cur_match_num = 0
                for hop in cur_bgp_path.split(' '):
                    if AsnInTracePathList(hop, trace_list):
                        cur_match_num += 1
                if cur_match_num > max_match_num:
                    max_match_num = cur_match_num
                    similarest_path = cur_bgp_path
        wf.write(curline_trace)
        wf.write("\t%s\n" %similarest_path)
        wf.write(curline_ip)
        curline_trace = rf.readline()
    rf.close()
    wf.close()

def LooseCheck(vps, year, month):
    steps = ['norm_trace_asn', 'find_similarest_bgp', 'check_use_irr', 'check_use_newer_org_data', 'posb_ixp', 'oneab_betw_samenorm', 'same_dst_as']
    steps = ['check_use_irr', 'check_use_newer_org_data', 'posb_ixp', 'oneab_betw_samenorm', 'same_dst_as']
    year_str = str(year)
    month_str = str(month).zfill(2)
    date = year_str + month_str + '15'
    
    GetSibRel(year, month)
    GetIxpPfxDict(year, month)
    GetIxpAsSet()   #这条语句需要放在GetSibRel()之后，因为需要sib_dict，也就是as2org_dict
    ConnectToDb()
    SetCurMidarTableDate(year, month)
    if steps.__contains__('check_use_newer_org_data'):
        GetSibRelByMultiDataFiles(year, month)

    for vp in vps:
        cur_dir = global_var.par_path +  global_var.out_my_anatrace_dir + '/' + vp + '_' + date + '/' #'nrt-jp.2019030115/'
        print(cur_dir)
        if not os.path.isdir(cur_dir):
            print('NOTE!' + cur_dir + ': dir not exist!')
            continue
        os.chdir(cur_dir)

        g_asn = global_var.trace_as_dict[vp]    
        bgp_filename = global_var.par_path + global_var.rib_dir + 'bgpdata/bgp_' + g_asn + '_' + date
        GetPfx2ASByBgp(bgp_filename) #2021.3.2使用VP本身的bgp表进行匹配，而不是用routeviews-rv2-20190301-1200.pfx2as_coalescedGetSibRel()
        GetBgpByPrefix(bgp_filename) #step 2 #get bgp_path to dst_prefix
        GetPathAsDict(bgp_filename) #step 3 #get bgp_path to dst_as
        
        global cur_map_method
        cur_map_method = 'ribs_midar_bdrmapit'
        print(cur_map_method)
        #if cur_map_method.__contains__('ribs'):
        if not os.path.isdir(cur_map_method):
            print('NOTE!' + vp + '.' + date + '_' + cur_map_method + ': dir not exist!')
            os.chdir('..')
            continue
        os.chdir(cur_map_method)
        record_file_name = 'record_' + vp + '.' + date + '_' + cur_map_method
        if not os.path.isfile(record_file_name):
            print('NOTE! ' + record_file_name + ': file not exist!')
            os.chdir('../..')
            continue
        record_file_name3 = 'record3_' + vp + '.' + date + '_' + cur_map_method
        InitGeoCache()

        if steps.__contains__('norm_trace_asn'):
            print('norm_trace_asn')
            if os.path.exists(record_file_name3):
                os.remove(record_file_name3)     
            TagStepInRecordFile(record_file_name3, 'norm_trace_asn')  
            if not os.path.exists('final_ab'):
                print('NOTE! file not exist: ' + vp + '.' + date)
                with open('/home/slt/code/ana_c_d_incongruity/undo', 'a') as f:
                    f.write(vp + '.' + date + "\n")
                continue
            NormTraceAS('final_ab', 'final_ab0')
            
        if steps.__contains__('find_similarest_bgp'):
            print('find_similarest_bgp')
            TagStepInRecordFile(record_file_name3, 'find_similarest_bgp')  
            FindSimilarestBgpPath('final_ab0', 'final_ab1')

        if steps.__contains__('check_use_irr'): #严格来讲不算loose_check
            print('check_use_irr')
            TagStepInRecordFile(record_file_name3, 'check_use_irr') 
            w_record = open(record_file_name3, 'a')
            (count_ab, count_total) = CheckTracesByIRRData('final_ab1', True, False)
            w_record.write("In check_use_irr, ab num: %d, ab precent: %.2f\n" %(count_ab, count_ab / count_total))
            w_record.close()
            
        if steps.__contains__('check_use_newer_org_data'):
            print('check_use_newer_org_data')
            TagStepInRecordFile(record_file_name3, 'check_use_newer_org_data') 
            w_record = open(record_file_name3, 'a')
            (count_ab, count_total) = CheckTracesByIRRData('final_ab1_ab', False, True)
            w_record.write("In check_use_newer_org_data, ab num: %d, ab precent: %.2f\n" %(count_ab, count_ab / count_total))
            w_record.close()

        if steps.__contains__('posb_ixp'):
            print('posb_ixp')
            shutil.copyfile('final_ab1_ab_ab', 'final_ab2')
            TagStepInRecordFile(record_file_name3, 'posb_ixp')  
            w_record = open(record_file_name3, 'a')
            (count_ab, count_unmap, count_total) = CheckTraces('final_ab2', False, True) #检查可能的IXP
            w_record.write("In posb_ixp, ab num: %d, ab precent: %.2f\n" %(count_ab, count_ab / count_total))
            w_record.close()

        if steps.__contains__('oneab_betw_samenorm'):
            #2021.5.26 如果前后两跳都map到同一个正常bgp跳，不正常跳可能是因map引起
            print('oneab_betw_samenorm')
            #ClearTagInRecordFile(record_file_name3, 'same_dst_as')
            TagStepInRecordFile(record_file_name3, 'oneab_betw_samenorm')
            w_record = open(record_file_name3, 'a')
            (count_ab, count_unmap, count_total) = FilterOneAbBetweenSameNormal('final_ab2_ab')
            w_record.write("In oneab_betw_samenorm, ab num: %d, ab precent: %.2f\n" %(count_ab, count_ab / count_total))
            print("In oneab_betw_samenorm, ab num: %d, ab precent: %.2f" %(count_ab, count_ab / count_total))
            w_record.close()

        if steps.__contains__('same_dst_as'):
            #2021.3.2 在改为使用prefix进行匹配后，看一看，匹配同一dst_as，不同dst_prefix的情况有多少
            print('same_dst_as')
            TagStepInRecordFile(record_file_name3, 'same_dst_as')
            w_record = open(record_file_name3, 'a')
            (count_ab, count_total) = CheckAbPathByDstAs('final_ab2_ab_ab', 'final_ab2_ab_ab_ab')
            w_record.write("In same_dst_as, ab num: %d, ab precent: %.2f\n" %(count_ab, count_ab / count_total))
            print("In same_dst_as, ab num: %d, ab precent: %.2f" %(count_ab, count_ab / count_total))
            w_record.close()

        ClearPathAsDict() #get bgp_path to dst_as
        ClearBGPByPrefix() #get bgp_path to dst_prefix
        ClearIp2AsDict()
        os.chdir('../..')
    
    CloseDb()
    ClearIxpAsSet()
    ClearIxpPfxDict()
    ClearSibRel()
    ClearSibRelByMultiDataFiles()


def RunOneDate(vps, year, month, map_methods):
    steps = ['step 1']
    year_str = str(year)
    month_str = str(month).zfill(2)
    date = year_str + month_str + '15'
    
    GetSibRel(year, month)
    GetIxpPfxDict(year, month)
    GetIxpAsSet()   #这条语句需要放在GetSibRel()之后，因为需要sib_dict，也就是as2org_dict
    ConnectToDb()
    SetCurMidarTableDate(year, month)
    
    cur_dir = global_var.par_path +  'jzt/prefix-probing/20190101'
    print(cur_dir)
    os.chdir(cur_dir)

    for root,dirs,files in os.walk('trace'):
        for trace_file_name in files:
            print(trace_file_name)
            InitBdrCache()
            ConnectToBdrMapItDb('bdrmapit/bdrmapit_' + trace_file_name.replace('.', '_') + '.db')
            ConstrBdrCache()
                        
            #map ip2AS
            if steps.__contains__('step 1'):
                os.chdir('trace')
                ChgTrace2ASPath(trace_file_name) #step 1
                os.chdir('..')
            CloseBdrMapItDb()
            
    CloseDb()
    ClearIxpAsSet()
    ClearIxpPfxDict()
    ClearSibRel()
    

if __name__ == '__main__':
    #FilterOneAbBetweenSameNormal('test1')
    PreGetSrcFilesInDirs()
    #for year in range(2018,2021):
    for year in range(2019,2020):
        #for month in range(1,13):
        for month in range(1, 2):
            if (year == 2016 and month < 4) or (year == 2020 and month > 4): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
                continue
            RunOneDate(global_var.vps, year, month, global_var.map_methods)
            #StatMatchIp(global_var.vps, year, month, global_var.map_methods)
            #LooseCheck(global_var.vps, year, month)

