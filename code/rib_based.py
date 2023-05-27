
import socket
import struct
import json
import IPy
import datetime

def ReformBGP_PerFile(filename):
    info_1 = [None] * 33
    with open(filename, 'r') as rf:
        curlines = rf.readlines(100000)
        while curlines:
            for curline in curlines:
                [prefix, remain] = curline.split('/')
                [pref_len_str, remain] = remain.split('|')
                pref_len = int(pref_len_str)
                if pref_len == 32 and prefix == '1.0.16.0':
                    print(curline)
                ori_asn = remain.split(' ')[-1].strip('\n')
                if not info_1[pref_len]:
                    info_1[pref_len] = {}
                if prefix not in info_1[pref_len].keys():
                    info_1[pref_len][prefix] = set()
                info_1[pref_len][prefix].add(ori_asn)
                if pref_len < 8: #debug
                    print(curline)
            curlines = rf.readlines(100000)
    info_2 = [None] * 0x1000000 #2^24个prefix, val: [ori_asn_list, ori_pref_len, sub_prefix_dict, par_prefix_dict]
    for pref_len in range(32, 24, -1): #前缀长度>24的
        if not info_1[pref_len]:
            continue
        for (prefix, ori_asn_set) in info_1[pref_len].items():
            prefix_24 = prefix[:prefix.rindex('.')] + '.0'
            prefix_24_int = (socket.ntohl(struct.unpack("I",socket.inet_aton(prefix_24))[0])) >> 8
            prefix_w = prefix + '/' + str(pref_len)
            if not info_2[prefix_24_int]:
                info_2[prefix_24_int] = [[], 0, {}, {}]
            if prefix_w not in info_2[prefix_24_int][2].keys():
                info_2[prefix_24_int][2][prefix_w] = []
            for ori_asn in ori_asn_set:
                if ori_asn not in info_2[prefix_24_int][2][prefix_w]:
                    info_2[prefix_24_int][2][prefix_w].append(ori_asn)
    # for (prefix, ori_asn_set) in info_1[24].items(): #前缀长度为24的
    #     prefix_int = (socket.ntohl(struct.unpack("I",socket.inet_aton(prefix))[0])) >> 8
    #     prefix_w = prefix + '/24'
    #     if not info_2[prefix_int]:
    #         info_2[prefix_int] = [set(), 0, {}, {}]
    #     info_2[prefix_int][0] = info_2[prefix_int][0] | ori_asn_set
    #     info_2[prefix_int][1] = 24
    prefix_24_int = None #debug，为了防止后面变量写错
    for pref_len in range(24, 7, -1): #前缀长度<=24的
        if not info_1[pref_len]:
            continue
        sub_pref_num = 1 << (24 - pref_len)
        for (prefix, ori_asn_set) in info_1[pref_len].items():
            prefix_int = (socket.ntohl(struct.unpack("I",socket.inet_aton(prefix))[0])) >> 8 #起点
            for i in range(0, sub_pref_num):
                if not info_2[prefix_int]:
                    info_2[prefix_int] = [[], 0, {}, {}]
                if len(info_2[prefix_int][0]) == 0: #此时是最长前缀
                    info_2[prefix_int][0] = list(ori_asn_set)
                    info_2[prefix_int][1] = pref_len
                else:
                    if pref_len == info_2[prefix_int][1]: #MOAS
                        for ori_asn in ori_asn_set:
                            if ori_asn not in info_2[prefix_int][0]:
                                info_2[prefix_int][0].append(ori_asn)
                    else: #父前缀
                        prefix_w = prefix + '/' + str(pref_len)
                        if prefix_w not in info_2[prefix_int][3].keys():
                            info_2[prefix_int][3][prefix_w] = []
                            for ori_asn in ori_asn_set:
                                if ori_asn not in info_2[prefix_int][3][prefix_w]:
                                    info_2[prefix_int][3][prefix_w].append(ori_asn)
                prefix_int += 1
    with open(filename + '_slash24', 'w') as wf:
        num = 0
        for i in range(0, 0x1000000):
            if info_2[i]:
                temp = {i: info_2[i]}
                wf.write(json.dumps(temp) + '\n')
                num += 1
        print(num)

pref_asn_24_info = {}
def GetSlash24PrefixOriAS(filename):
    global pref_asn_24_info
    start_time = datetime.datetime.now()
    with open(filename, 'r') as rf:
        curlines = rf.readlines(100000)
        while curlines:
            for curline in curlines:
                temp = json.loads(curline.strip('\n'))
                for (key, val) in temp.items():
                    pref_asn_24_info[int(key)] = [val[0], val[2]]
            curlines = rf.readlines(100000)
    end_time = datetime.datetime.now()
    print((end_time - start_time).seconds)

pref_asn_info = {}
def GetNormPrefixOriAS(filename):
    global pref_asn_info
    start_time = datetime.datetime.now()
    with open(filename, 'r') as rf:
        curlines = rf.readlines(100000)
        while curlines:
            for curline in curlines:
                [prefix, remain] = curline.split('|')
                ori_asn = remain.split(' ')[-1].strip('\n')
                if prefix not in pref_asn_info.keys():
                    pref_asn_info[prefix] = set()
                pref_asn_info[prefix].add(ori_asn)
            curlines = rf.readlines(100000)
    end_time = datetime.datetime.now()
    print((end_time - start_time).seconds)

def GetASOfIP_Slash24(cur_ip):
    global pref_asn_24_info
    asns = set()
    prefix_24 = cur_ip[:cur_ip.rindex('.')] + '.0'
    prefix_24_int = (socket.ntohl(struct.unpack("I",socket.inet_aton(prefix_24))[0])) >> 8
    if prefix_24_int in pref_asn_24_info.keys():
        [ori_asn_list, sub_prefix_info] = pref_asn_24_info[prefix_24_int]
        max_pref_len = 24
        for (sub_prefix, sub_asn_list) in sub_prefix_info.items():
            #print('sub_prefix:' + sub_prefix)
            sub_pref_len = int(sub_prefix.split('/')[-1])
            if (sub_pref_len > max_pref_len) and (cur_ip in IPy.IP(sub_prefix)):
                asns = set(sub_asn_list)
                max_pref_len = sub_pref_len
        if not asns:
            asns = set(ori_asn_list)
    #print(asns)
    return asns
    
def GetASOfIP(cur_ip):
    global pref_asn_info
    asns = set()
    ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton(cur_ip))[0])
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
        if cur_prefix in pref_asn_info.keys():
            asns = pref_asn_info[cur_prefix]
            break
    return asns

def TestMapIP2AS(filename):
    global pref_asn_info
    global pref_asn_24_info
    cache1 = {}
    starttime = datetime.datetime.now()
    with open(filename, 'r') as rf:
        curlines = rf.readlines(100000)
        while curlines:
            for curline in curlines:
                for cur_ip in curline.split(':')[-1].strip('\n').split(','):
                    if cur_ip == '' or cur_ip == '*':
                        continue
                    if cur_ip in cache1.keys():
                        continue
                    cache1[cur_ip] = GetASOfIP_Slash24(cur_ip)
            curlines = rf.readlines(100000)
    endtime = datetime.datetime.now()
    print('slash24: ', end='')
    print((endtime - starttime).seconds)

    cache2 = {}
    starttime = datetime.datetime.now()
    with open(filename, 'r') as rf:
        curlines = rf.readlines(100000)
        while curlines:
            for curline in curlines:
                for cur_ip in curline.split(':')[-1].strip('\n').split(','):
                    if cur_ip == '' or cur_ip == '*':
                        continue
                    if cur_ip in cache2.keys():
                        continue
                    cache2[cur_ip] = GetASOfIP(cur_ip)
            curlines = rf.readlines(100000)
    endtime = datetime.datetime.now()
    print('normal: ', end='')
    print((endtime - starttime).seconds)

    ips1 = set(cache1.keys())
    ips2 = set(cache2.keys())
    for cur_ip in ips1.difference(ips2):
        print(cur_ip + ':' + ','.join(list(cache1[cur_ip])) + ';None')
    for cur_ip in ips2.difference(ips1):
        print(cur_ip + ':' + 'None;' + ','.join(list(cache2[cur_ip])))
    for cur_ip in (ips1 & ips2):
        if sorted(list(cache1[cur_ip])) != sorted(list(cache2[cur_ip])):
            print(cur_ip + ':' + ','.join(list(cache1[cur_ip])) + ';' + ','.join(list(cache2[cur_ip])))

if __name__ == '__main__':
    # for prefix in ['0.0.1.0', '0.1.1.0', '1.1.1.0', '255.255.255.0']:
    #     prefix_int = (socket.ntohl(struct.unpack("I",socket.inet_aton(prefix))[0])) >> 8
    #     print(prefix_int)
    # ReformBGP_PerFile('/mountdisk1/ana_c_d_incongruity/rib_data/bgpdata/bgp_203.181.248.168_20190115')
    # prefix = '1.0.168.0'
    # prefix_int = (socket.ntohl(struct.unpack("I",socket.inet_aton(prefix))[0])) >> 8
    # print(prefix_int)
    GetSlash24PrefixOriAS('/mountdisk1/ana_c_d_incongruity/rib_data/bgpdata/bgp_203.181.248.168_20190115_slash24')
    GetNormPrefixOriAS('/mountdisk1/ana_c_d_incongruity/rib_data/bgpdata/bgp_203.181.248.168_20190115')
    TestMapIP2AS('/mountdisk1/ana_c_d_incongruity/traceroute_data/result/trace_nrt-jp.20190115')
    #cur_ip = '98.188.121.214'
    #print(GetASOfIP_Slash24(cur_ip))
    #print(GetASOfIP(cur_ip))