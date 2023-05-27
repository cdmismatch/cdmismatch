
import socket
import struct
import os
import glob
import datetime

from find_vp_v2 import CompressBGPPath

#return: (find_flag, pos)  当find_flag == True and pos == 255时，表明有mal_pos
def FindTraceHopInBGP(bgp_list, hop, pre_pos):
    find = False
    pos = 255
    for asn in hop.split('_'):
        if asn in bgp_list:
            find = True
            cur_pos = bgp_list.index(asn)
            if cur_pos >= pre_pos:  #cur_pos < pre_pos这个map按出错处理
                pos = min(cur_pos, pos)
    if not find:
        return (False, 255)
    return (True, pos)

def CheckAbHopCountAndMalPos(bgp, trace_list): #trace没有compress
    bgp_list = bgp.split(' ')
    pre_pos = 0
    ab_count = 0
    mal_pos_flag = False
    for trace_hop in trace_list:
        if trace_hop == '*' or trace_hop == '?' or trace_hop == '<>': #兼容后面修正IXP, IXP hop忽略
            continue
        (find, pos) = FindTraceHopInBGP(bgp_list, trace_hop, pre_pos)
        if pos < 255: #正常
            pre_pos = pos
        else:
            ab_count += 1
            if find:
                mal_pos_flag = True
    return (ab_count, mal_pos_flag)

def SelCloseBGP(bgps, trace_list):
    sel_bgp = ''
    min_ab_count = 255
    res_mal_pos_flag = False
    for bgp in bgps:
        (ab_count, mal_pos_flag) = CheckAbHopCountAndMalPos(bgp, trace_list)
        if (not mal_pos_flag) and (ab_count < min_ab_count):
            (sel_bgp, min_ab_count, res_mal_pos_flag) = (bgp, ab_count, mal_pos_flag)
    return (sel_bgp, min_ab_count, res_mal_pos_flag)

def CompressTrace(trace_list, ip_list): #如果是MOAS，要求不同AS之间按字母排序
    loop_flag = False
    new_list = []
    pre_hop = ''
    trace_to_ip_info = {}
    for i in range(0, len(trace_list)):
        hop = trace_list[i]
        if hop != pre_hop:
            if hop != '*' and hop != '?' and hop != '<>' and hop in new_list:
                for elem in new_list[new_list.index(hop) + 1:]:
                    if elem != '*' and elem != '?' and elem != '<>':
                        loop_flag = True
            new_list.append(hop)
            pre_hop = hop
        if hop not in trace_to_ip_info.keys():
            trace_to_ip_info[hop] = []
        if ip_list[i] not in trace_to_ip_info[hop]:
            trace_to_ip_info[hop].append(ip_list[i]) #ip要保持有序，后面miss bgp hop的时候用到
    return (new_list, trace_to_ip_info, loop_flag)

ip_accur_info = {}
mode_info = {'match': 0, 'mismatch': 1, 'partial_match': 2, 'unmap': 3}
def UpdateIPMapAccur(_ip, mode, pos):
    global ip_accur_info
    global mode_info
    if _ip not in ip_accur_info.keys():
        ip_accur_info[_ip] = [[0, 0.0], [0, 0.0], [0, 0.0], [0, 0.0]]
    ip_accur_info[_ip][mode_info[mode]][0] += 1
    ip_accur_info[_ip][mode_info[mode]][1] += round(pos, 2)

def CompareCD_PerTrace(bgp, trace_list, ip_list, trace_to_ip_info): #1.bgp要求已是最简状态; 2.mal_pos的情况先不处理，此函数不考虑; 3. trace AS_PATH loop先不处理，此函数不考虑
    bgp_list = bgp.split(' ')
    bgp_list.append('$')
    trace_list.append('$')
    segs = []
    pre_bgp_index = pre_trace_index = 0
    for trace_index in range(1, len(trace_list)): #默认BGP和traceroute第一跳相同
        trace_hop = trace_list[trace_index]
        if trace_hop == '*' or trace_hop == '?' or trace_hop == '<>': #兼容后面修正IXP, IXP hop忽略
            continue
        (find, bgp_index) = FindTraceHopInBGP(bgp_list, trace_hop, pre_bgp_index)
        if find:
            if bgp_index == 255: # 不应该出现这个情况，函数外已过滤
                print('Mal_pos should already be filtered')
                return
            segs.append([bgp_list[pre_bgp_index:bgp_index + 1], trace_list[pre_trace_index:trace_index + 1]])
            pre_bgp_index = bgp_index
            pre_trace_index = trace_index
    #print(segs)

    ip_list_len = len(ip_list)
    for _ip in trace_to_ip_info[trace_list[0]]: #trace_seg最左边的匹配hop记作match
        UpdateIPMapAccur(_ip, 'match', 0)
    for seg in segs:
        [bgp_seg, trace_seg] = seg
         #trace_seg最左边的匹配hop已在上一轮记了一遍match，这里不再重复记录
        for elem in trace_seg[1:-1]: #trace has extra elem
            if elem != '*' and elem != '?' and elem != '<>': #mismatch
                for _ip in trace_to_ip_info[elem]:
                    UpdateIPMapAccur(_ip, 'mismatch', (ip_list.index(_ip) + 1) / ip_list_len)
        if trace_seg[-1] == '$':
            break
        last_elem_partial_match = False
        if len(bgp_seg) > 2: #bgp has extra elem            
            last_elem_partial_match = True
            for elem in trace_seg[1:-1]:
                if elem == '*' or elem == '?': #trace中间有"*"或"?"，将trace_seg最右边的匹配hop置为match，否则将该hop的第一个IP置为partial_match
                    last_elem_partial_match = False
                    break
        _ip = trace_to_ip_info[trace_seg[-1]][0]
        if last_elem_partial_match:
            UpdateIPMapAccur(_ip, 'partial_match', (ip_list.index(_ip) + 1) / ip_list_len)
        else:
            UpdateIPMapAccur(_ip, 'match', 0) #match的不记录所在位置
        if len(trace_to_ip_info[trace_seg[-1]]) > 1:
            for _ip in trace_to_ip_info[trace_seg[-1]][1:]:                
                UpdateIPMapAccur(_ip, 'match', 0)
    if '?' in trace_to_ip_info.keys():
        for _ip in trace_to_ip_info['?']:
            UpdateIPMapAccur(_ip, 'unmap', (ip_list.index(_ip) + 1) / ip_list_len)

def InitBGPPathInfo(bgp_filename, bgp_path_info):
    with open(bgp_filename, 'r') as rf:
        curlines = rf.readlines(100000)
        while curlines:
            for curline in curlines:
                (prefix, path) = curline.strip('\n').split('|')
                if prefix not in bgp_path_info.keys():
                    bgp_path_info[prefix] = [[], set()]
                new_path = CompressBGPPath(path)
                if new_path not in bgp_path_info[prefix]:
                    bgp_path_info[prefix][0].append(new_path)
                    bgp_path_info[prefix][1].add(new_path.split(' ')[-1])
            curlines = rf.readlines(100000)

# def GetBGPPath_Or_OriASN(bgp_path_info, _ip, mode, ori_asn_cache):
#     #print('(' + _ip + ')')
#     if mode == 'get_orias':
#         if _ip in ori_asn_cache.keys():
#             return ori_asn_cache[_ip]
#     ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton(_ip))[0])
#     for mask_len in range(32, 7, -1):
#         # host4 = ipaddress.ip_interface(_ip + '/' + str(mask_len)) #这种方法慢到发指
#         # cur_prefix = host4.network.compressed        
#         if mask_len < 32:
#             mask = ~(1 << (31 - mask_len))
#             ip_int = ip_int & mask
#         cur_prefix = str(socket.inet_ntoa(struct.pack('I',socket.htonl(ip_int))))
#         cur_prefix = cur_prefix + '/' + str(mask_len)

#         if cur_prefix in bgp_path_info.keys():
#             if mode == 'get_path':
#                 return bgp_path_info[cur_prefix][0]
#             elif mode == 'get_orias':
#                 #res = '_'.join(sorted(list(bgp_path_info[cur_prefix][1])))
#                 res = bgp_path_info[cur_prefix]
#                 ori_asn_cache[_ip] = res
#                 return res
#     if mode == 'get_path':
#         return []
#     elif mode == 'get_orias':
#         ori_asn_cache[_ip] = ''
#         return ''
#     return None #错误的mode参数

def GetBGPPath_Or_OriASN(bgp_path_info, _ip, mode):
    asns = set()
    ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton(_ip))[0])
    for mask_len in range(32, 7, -1):
        # mask = 0xFFFFFFFF - (1 << (32 - mask_len)) + 1
        # cur_prefix_int = ip_int & mask
        # cur_prefix = str(socket.inet_ntoa(struct.pack('I',socket.htonl(cur_prefix_int))))
        if mask_len < 32:
            mask = ~(1 << (31 - mask_len))
            ip_int = ip_int & mask
        cur_prefix = str(socket.inet_ntoa(struct.pack('I',socket.htonl(ip_int))))
        cur_prefix = cur_prefix + '/' + str(mask_len)
        #print(cur_prefix)
        if cur_prefix in bgp_path_info.keys():
            return bgp_path_info[cur_prefix]

map_method = 'rib_based'

def MapTrace(bgp_path_info, ip_list, map_method, ori_asn_cache):
    trace_list = []
    for _ip in ip_list:
        if _ip == '*':
            continue
        if _ip in ori_asn_cache.keys():
            continue
        ori_asn_cache[_ip] = GetBGPPath_Or_OriASN(bgp_path_info, _ip, 'get_orias')
        # if _ip == '*':
        #     trace_list.append('*')
        #     continue
        # if map_method == 'rib_based':
        #     asns = GetBGPPath_Or_OriASN(info, _ip, 'get_orias', ori_asn_cache)
        #     if not asns:
        #         trace_list.append('?')
        #     else:
        #         trace_list.append(asns)
    return trace_list

def CompareCD(bgp_filename, trace_filename, ab_filename):
    global ip_accur_info
    global map_method
    bgp_path_info = {}
    wf_ab = open(ab_filename, 'w')
    start_time = datetime.datetime.now()
    InitBGPPathInfo(bgp_filename, bgp_path_info)
    end_time = datetime.datetime.now()
    print((end_time - start_time).seconds)
    
    start_time = datetime.datetime.now()
    ori_asn_cache = {}
    with open(trace_filename, 'r') as rf:
        curlines = rf.readlines(100000)
        while curlines:
            #print(len(ip_accur_info))
            for curline in curlines:
                (dst_ip, trace_ip_path) = curline.strip('\n').split(':')
                if not trace_ip_path: #有的dst_ip没有trace
                    continue
                ip_list = trace_ip_path.split(',')
                #bgps = GetBGPPath_Or_OriASN(bgp_path_info, dst_ip, 'get_path', None)
                ori_trace_list = MapTrace(bgp_path_info, ip_list, map_method, ori_asn_cache)
                # (trace_list, trace_to_ip_info, loop_flag) = CompressTrace(ori_trace_list, ip_list)
                # if loop_flag: #FIX-ME. AS PATH loop的情况暂不解决
                #     wf_ab.write(curline)
                #     continue
                # (sel_bgp, min_ab_count, mal_pos_flag) = SelCloseBGP(bgps, trace_list)
                # if mal_pos_flag: #FIX-ME. mal pos的情况暂不解决
                #     wf_ab.write(curline)
                #     continue
                #CompareCD_PerTrace(sel_bgp, trace_list, ip_list, trace_to_ip_info)
            curlines = rf.readlines(100000)
    wf_ab.close()

if __name__ == '__main__':
    os.chdir('/mountdisk1/ana_c_d_incongruity/out_my_anatrace/')
    # bgp = 'a b c d e f g h'
    # trace = 'a b * ? * * c c * e g g i k h l *'
    # ips = 'a b * ? * * c c * e g m i k h l *'
    # #print(CheckAbHopCountAndMalPos(bgp, trace))
    # CompareCD(bgp, trace, ips.split(' '))
    # print(ip_map_accur)
    tracevp_bgpvp_info = {'ams-nl': '80.249.208.34', 'jfk-us': '198.32.160.61', 'sjc2-us': '64.71.137.241', 'syd-au': '198.32.176.177', 'per-au': '198.32.176.177', 'zrh2-ch': '109.233.180.32', 'nrt-jp': '203.181.248.168'}
    bgp_dir = '/mountdisk1/ana_c_d_incongruity/rib_data/bgpdata/'
    trace_dir = '/mountdisk1/ana_c_d_incongruity/traceroute_data/result/'
    for vp in ['nrt-jp']:
        #os.chdir(vp + '/ribs/')
        trace_filenames = glob.glob(r'%strace_%s*' %(trace_dir, vp))
        for trace_filename in trace_filenames:
            date = trace_filename[trace_filename.rindex('.') + 1:]
            if date != '20190115':
                continue
            start_time = datetime.datetime.now()
            CompareCD(bgp_dir + 'bgp_' + tracevp_bgpvp_info[vp] + '_' + date, trace_filename, 'ab_' + vp + '_' + date)
            end_time = datetime.datetime.now()
            print((end_time - start_time).seconds)
            break
        os.chdir('../../')
