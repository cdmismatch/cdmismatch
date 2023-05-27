
import os
import requests
import re
import json
import numpy as np
import math
import socket
import struct
import time
from urllib.request import urlopen

import global_var

full_ip_space = '0.0.0.0 - 255.255.255.255'
def GetIrrDictFromDefault():
    global irr_dict
    global irr_index_dict
    #rf = open(global_var.par_path + global_var.irr_dir + global_var.irr_filename_default, 'r')
    rf = open(global_var.par_path + global_var.irr_dir + 'irrdata', 'r')
    curline = rf.readline()
    num = 0
    tmp_set = set()

    while curline:
        num += 1
        key = curline[1:curline.index(':')].strip('"')
        if key == full_ip_space:
            curline = rf.readline()
            continue
        info = json.loads(curline.strip('\n'))
        data = info[key]
        (org_set, asn_set, src) = GetBelongedOrg(data)
        if org_set:
            tmp_set |= org_set
        # for elem in org_set:
        #     if elem.__contains__('Backbone') or elem.__contains__('backbone'):
        #         print(key)
        #         print(org_set)
        #         break
        irr_dict[key] = [org_set, asn_set, src]
        elems = key.split(' ')
        cur_ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton(elems[0]))[0])
        last_ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton(elems[-1]))[0])
        #print(last_ip_int)
        while cur_ip_int < last_ip_int:
            #print(cur_ip_int)
            cur_ip = str(socket.inet_ntoa(struct.pack('I',socket.htonl(cur_ip_int))))
            if cur_ip not in irr_index_dict.keys():
                irr_index_dict[cur_ip] = key
            cur_ip_int += (1 << 8)
        curline = rf.readline()
    rf.close()
   
def AddIrrDictFromOthers(filename, src):
    global irr_dict
    global irr_index_dict
    rf = open(filename, 'r')
    curline = rf.readline()
    num = 0

    while curline:
        num += 1
        key = curline[1:curline.index(':')].strip('"')
        if key == full_ip_space:
            curline = rf.readline()
            continue
        info = json.loads(curline.strip('\n'))
        data = info[key]
        org_set = set()
        asn_set = set()
        tmp_src = ''
        if src == 'afrinic':
            (org_set, asn_set, tmp_src) = GetBelongedOrgFromAfrinic(data)
        elif src == 'ripe':
            (org_set, asn_set, tmp_src) = GetBelongedOrgFromRipe(data)
        elif src == 'arin':
            (org_set, asn_set, tmp_src) = GetBelongedOrgFromArin(data)
        for elem in org_set:
            if elem.__contains__('Backbone') or elem.__contains__('backbone'):
                print(key)
                print(org_set)
                break
        if key not in irr_dict.keys(): #按道理不应该出现这种情况
            print('NOTE ip range not in apnic database')
            irr_dict[key] = [org_set, asn_set, tmp_src]
            elems = key.split(' ')
            cur_ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton(elems[0]))[0])
            last_ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton(elems[-1]))[0])
            #print(last_ip_int)
            while cur_ip_int < last_ip_int:
                #print(cur_ip_int)
                cur_ip = str(socket.inet_ntoa(struct.pack('I',socket.htonl(cur_ip_int))))
                if cur_ip not in irr_index_dict.keys():
                    irr_index_dict[cur_ip] = set()
                irr_index_dict[cur_ip].add(key)
                cur_ip_int += (1 << 8)
        else:
            irr_dict[key][0] |= org_set
            irr_dict[key][1] |= asn_set
        curline = rf.readline()
    rf.close()
    print("total lines: %d" %num)

def GetIrrDict():
    GetIrrDictFromDefault()
    for irr in global_var.irrs:
        if irr != 'apnic':
            AddIrrDictFromOthers(global_var.par_path + global_var.irr_dir + global_var.irr_filename_pre + irr, irr)
     
def GetIRRIndex(ip):
    global irr_index_dict
    slash24_prefix = ip[:ip.rindex('.')] + '.0'
    #print(slash24_prefix)
    if slash24_prefix in irr_index_dict.keys():
        return irr_index_dict[slash24_prefix]
    return None

irr_dict = dict()
irr_index_dict = dict()

def GetOrgAsnFromIRR(ip):
    slash24_prefix = ip[:ip.rindex('.')] + '.0'
    #print(slash24_prefix)
    if slash24_prefix not in irr_index_dict.keys():
        #print(1)
        return [None, None, None]
    key_set = irr_index_dict[slash24_prefix]
    #print(key)
    org_set = set()
    asn_set = set()
    src = ''
    for key in key_set:
        if key in irr_dict.keys():
            [tmp_org, tmp_asn, tmp_src] = irr_dict[key]
            org_set |= tmp_org
            asn_set |= tmp_asn
            src = tmp_src
    return [org_set, asn_set, src]


#重写irr_dict相关的函数
#IRR中有的prefix长度大于24，要考虑这种情况。且每个ip对应一个prefix
irr_dict_2 = dict()
def GetIRRIndex_2(ip):
    global irr_dict_2
    ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton(ip))[0])
    for mask_len in range(32, 0, -1):
        mask = 0xFFFFFFFF - (1 << (32 - mask_len)) + 1
        cur_prefix_int = ip_int & mask
        cur_prefix = str(socket.inet_ntoa(struct.pack('I',socket.htonl(cur_prefix_int))))
        cur_prefix = cur_prefix + '/' + str(mask_len)
        if cur_prefix in irr_dict_2.keys():
            return cur_prefix
    return None

def GetOrgAsnFromIRR_2(ip):
    prefix = GetIRRIndex_2(ip)
    if prefix:
        return irr_dict_2[prefix]
    return None

def GetSrcOfData(data):
    res = re.findall(r'APNIC found the following authoritative answer from: whois\.(.*?)\.net', data)
    if res:
        return res[0]
    else:
        res = re.findall(r'This is the (.*?) Whois server.', data)
        if res:
            return res[0].lower()
    return None
    
def PurifyOrgData(org_set):
    new_org_set = set()
    for elem in org_set:
        tmp = re.sub('\(<a.*?</a>\)', '', elem)
        new_org_set.add(tmp.strip(' '))
    return new_org_set

def PurifyAsnData(asn_set):
    new_asn_set = set()
    for elem in asn_set:
        tmp = re.sub('AS', '', elem)
        new_asn_set.add(tmp.strip(' '))
    return new_asn_set

#invalid_words = ['Backbone', 'backbone', 'Backbone Network', 'Internal Backbone', 'Backbone services', 'Backbone Servers', 'IP Backbone and infrastructure', \
#                'Backbone network', 'Backbone addressing', 'core backbone network', 'IPv4 Core Backbone', 'Backbone interfaces', 'backbone network', 'Backbone Links', \
#                'backbone segment', 'Backbone networks']
def GetBelongedOrg(data): #from apnic data, default
    org_set = set()
    asn_set = set()
    src = 'apnic' #default
    find_object = False
    for elem in data:
        if elem['type'] == 'object':
            find_object = True
            inetnum_part = False
            has_descr = False
            for sub_elem in elem['attributes']:
                if sub_elem['name'] == 'inetnum':
                    inetnum_part = True
                #if not inetnum_part: #一般都先有'inetnum'，这一组有'inetnum'再取值
                    #continue
                if sub_elem['name'] == 'org-name' or sub_elem['name'] == 'org' or sub_elem['name'] == 'organization' or sub_elem['name'] == 'owner':
                    for cur in sub_elem['values']:
                        org_set.add(cur)
                if sub_elem['name'] == 'origin' or sub_elem['name'] == 'aut-num':
                    for cur in sub_elem['values']:
                        if cur != 'N/A':
                            asn_set.add(cur[2:])
                '''
                if not has_descr and sub_elem['name'] == 'descr': #只取第一个descr，后面的可能是注释信息
                #if sub_elem['name'] == 'descr':                    
                    for cur in sub_elem['values']:
                        #if cur not in invalid_words:
                        org_set.add(cur)
                        has_descr = True
                '''
    ''' #不要这部分comments了，容易有歧义
    if not find_object:
        for elem in data:
            if elem['type'] == 'comments':
                if elem['comments'][0].startswith('%') or elem['comments'][0].startswith('#'):
                    continue
                for sub_elem in elem['comments']:
                    if not sub_elem.__contains__(' ('):
                        continue
                    tmp = sub_elem[:sub_elem.index(' (')]
                    tmplist = tmp.split(' ')
                    org = ' '.join(tmplist[:-1])
                    org_set.add(org)
    '''
    src = GetSrcOfData(json.dumps(data))
    return (PurifyOrgData(org_set), PurifyAsnData(asn_set), src)

def GetCountryFromIrrItem(data, src): #from apnic data, default
    country = None
    city = None
    if src == 'apnic':
        for elem in data:
            if elem['type'] == 'object':
                for sub_elem in elem['attributes']:
                    if (not country) and (sub_elem['name'] == 'country'):
                        country = sub_elem['values'][0]
                    if (not city) and (sub_elem['name'] == 'city'):
                        city = sub_elem['values'][0]
    elif src == 'afrinic':
        for elem in data:
            for (key, val) in elem.items():
                if (not country) and (key == 'country'):
                    country = val[0]
                if (not city) and (key == 'city'):
                    city = val[0]
    elif src == 'ripe':  
        for elem in data['objects']['object']:
            if 'attributes' in elem.keys():
                if 'attribute' in elem['attributes'].keys():
                    for sub_elem in elem['attributes']['attribute']:
                        if (not country) and (sub_elem['name'] == 'country'):
                            country = sub_elem['value']
                        if (not city) and (sub_elem['name'] == 'city'):
                            city = sub_elem['value']
    elif src == 'arin':
        for (key, val) in data.items():
            if key == 'Organization' or key == 'Customer':
                for elem in val:
                    if (not country) and ('Country' in elem.keys()):
                        country = elem['Country']
                    if (not city) and ('City' in elem.keys()):
                        city = elem['City']
    return (country, city)

def GetOneIpRangeOfIpint(ip_int, ip_ranges):
    left = 0
    right = len(ip_ranges) - 1
    #print(ip_int)
    while left <= right:
        mid = int((left + right) / 2)
        #print(ip_ranges[left][2] + '|' + ip_ranges[right][2])
        first_ip_int = ip_ranges[mid][0]
        last_ip_int = ip_ranges[mid][1]
        if first_ip_int > ip_int:
            right = mid - 1
        elif last_ip_int < ip_int:
            left = mid + 1
        else:
            #print('hit:%s' %ip_ranges[mid][2])
            return ip_ranges[mid][2]
    return None

non_overlay_ip_ranges_with_int_index_list = []
def PreGetNonOverlayIpRanges():
    global non_overlay_ip_ranges_with_int_index_list
    with open(global_var.par_path + global_var.irr_dir + global_var.ip_ranges_filename + '_dealed', 'r') as rf:
        data = rf.read()
    for elem1 in data.strip('\n').split('\n'):
        ip_ranges_with_int_index = []
        for elem2 in elem1.strip(';').split(';'):
            tmp_elems = elem2.split(',')
            ip_ranges_with_int_index.append([int(tmp_elems[0]), int(tmp_elems[1]), tmp_elems[2]])
        non_overlay_ip_ranges_with_int_index_list.append(ip_ranges_with_int_index)

def GetIpRangeOfIp(ip):
    global non_overlay_ip_ranges_with_int_index_list
    ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton(ip))[0])
    #print(ip_int)
    res = []
    for ip_ranges in non_overlay_ip_ranges_with_int_index_list:
        tmp = GetOneIpRangeOfIpint(ip_int, ip_ranges)
        if tmp:
            res.append(tmp)
    return res

prefix_country_cache = dict()
def GetCountryFromIrrFiles(ip):   #IrrFile有的有问题，暂时先用线上的
    dir = global_var.par_path + global_var.irr_dir
    ip_range_set = GetIpRangeOfIp(ip)
    country_set = set()
    city_set = set()
    for ip_range in ip_range_set:
        if ip_range in prefix_country_cache.keys():
            country_set.add(prefix_country_cache[ip_range][0])
            city_set.add(prefix_country_cache[ip_range][1])
            continue
        output = os.popen('grep \'{\\\"' + ip_range + '\\\"\' ' + dir + global_var.irr_filename_default)
        output_data_all = output.read()
        output_data = output_data_all.strip('\n').split('\n')[0]
        info = json.loads(output_data)
        data = info[ip_range]
        (country, city) = GetCountryFromIrrItem(data, 'apnic')
        if country:
            prefix_country_cache[ip_range] = [country, city]
            country_set.add(country)
            city_set.add(city)
            continue
        src = GetSrcOfData(json.dumps(data))
        if src != 'apnic':
            output = os.popen('grep \'{\\\"' + ip_range + '\\\"\' ' + dir + global_var.irr_filename_pre + src)
            output_data_all = output.read()
            if output_data_all:
                output_data = output_data_all.strip('\n').split('\n')[0]
                info = json.loads(output_data)
                data = info[ip_range]
                if data:
                    (country, city) = GetCountryFromIrrItem(data, src)
                    if country:
                        prefix_country_cache[ip_range] = [country, city]
                        country_set.add(country)
                        city_set.add(city)
                        continue
    return (country_set, city_set)

def GetCountryOrgAsnFromIrrOnline(ip):   #IrrFile有的有问题，暂时先用线上的
    ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton(ip))[0])
    for ip_range in prefix_country_cache.keys():
        [begin_ip, end_ip] = ip_range.split(' ')
        if ip_int >= int(begin_ip) and ip_int <= int(end_ip):
            return prefix_country_cache[ip_range]
    url = "https://wq.apnic.net/query?searchtext=" + ip #default: apnic
    #print(ip)
    req = requests.Session()
    headers = {"accept":"application/json"}
    resource = req.get(url, headers=headers) 
    if resource:                
        if resource.status_code == 200:     
            #print('here0')       
            ip_range = None
            data = json.loads(resource.text)
            src = GetSrcOfData(json.dumps(data))
            if src == 'afrinic':
                (ip_range, data) = GetIrrDataFromAfrinic(ip)
                (country, city) = GetCountryFromIrrItem(data, 'afrinic')
                (org_set, asn_set, src) = GetBelongedOrgFromAfrinic(data)
            elif src == 'arin':
                (ip_range, data) = GetIrrDataFromArin(ip)
                (country, city) = GetCountryFromIrrItem(data, 'arin')
                (org_set, asn_set, src) = GetBelongedOrgFromArin(data)
            elif src == 'ripe':
                (ip_range, data) = GetIrrDataFromRipe(ip)
                (country, city) = GetCountryFromIrrItem(data, 'ripe')
                (org_set, asn_set, src) = GetBelongedOrgFromRipe(data)
            else: #lactic and apnic
                ip_range = GetIpRangeFromApnicData(data)
                (country, city) = GetCountryFromIrrItem(data, 'apnic')
                (org_set, asn_set, src) = GetBelongedOrg(data)
            if ip_range:
                begin_ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton(ip_range.split(' ')[0]))[0])
                end_ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton(ip_range.split(' ')[-1]))[0])
                prefix_country_cache[str(begin_ip_int) + ' ' + str(end_ip_int)] = [asn_set, org_set, country, city]
            return [asn_set, org_set, country, city]

def GetBelongedOrgFromRipe(data):
    org_set = set()
    asn_set = set()    
    for elem in data['objects']['object']:
        #print(elem.keys())
        if 'attributes' in elem.keys():
            #print(elem['attributes'].keys())
            if 'attribute' in elem['attributes'].keys():
                for sub_elem in elem['attributes']['attribute']:
                    if sub_elem['name'] == 'descr':
                        org_set.add(sub_elem['value'])
                    if sub_elem['name'] == 'origin':
                        asn_set.add(sub_elem['value'])
        if 'resource-holder' in elem.keys():
            org_set.add(elem['resource-holder']['name'])
    return (PurifyOrgData(org_set), PurifyAsnData(asn_set), 'ripe')

def GetBelongedOrgFromArin(data):
    org_set = set()
    asn_set = set()
    #print(data)
    for (key, val) in data.items():
        if key == 'Organization':
            #print(key)
            #print(val)
            for elem in val:
                #print(elem.keys())
                if 'Name' in elem.keys():
                    #print(elem['Name'])
                    org_set.add(elem['Name'])
        if key == 'Network':
            for elem in val:
                #print(elem)
                if 'Organization' in elem.keys():
                    org_set.add(elem['Organization'])
                if 'Origin AS' in elem.keys():
                    asn = elem['Origin AS']
                    res = re.findall(r'AS(\d+)', asn)
                    #print(res)
                    for tmp in res:
                        asn_set.add(tmp)
            '''
            for (sub_key, sub_val) in elem.items():
                if sub_key == 'Organization':
                    org_set |= set(sub_val.split('\n')[0])
                if sub_key == 'Origin AS':
                    asn_set.add(sub_val[2:])
            '''
    return (PurifyOrgData(org_set), PurifyAsnData(asn_set), 'arin')

def GetBelongedOrgFromAfrinic(data):
    org_set = set()
    asn_set = set()
    for elem in data:
        for (key, val) in elem.items():
            if key == 'descr':
                org_set |= set(val)
            if key == 'origin':
                asn_set.add(val[0][2:])
    return (PurifyOrgData(org_set), PurifyAsnData(asn_set), 'afrinic')

def GetOrgAsnFromIRROnLine(ip):
    url = "https://wq.apnic.net/query?searchtext=" + ip #default: apnic
    #print(ip)
    req = requests.Session()
    headers = {"accept":"application/json"}
    resource = req.get(url, headers=headers) 
    if resource:                
        if resource.status_code == 200:     
            #print('here0')       
            data = json.loads(resource.text)
            #print(data)
            #print('here1')
            (org_set, asn_set, src) = GetBelongedOrg(data)
            #print('here2')
            #print(org_set)
            #print(asn_set)
            #print(src)
            #need_check_other_src = False
            if src != 'apnic': #2021.6.24 只要是其它src的都应该查源
                #if not org_set:
                #    need_check_other_src = True
                #else:
                #    for elem in org_set:
                #        if elem.__contains__('Backbone') or elem.__contains__('backbone'):
                #            need_check_other_src = True
                #            break
            #if need_check_other_src:
                tmp_org_set = set()
                tmp_asn_set = set()
                #print('here3')
                print(ip)
                if src == 'afrinic':
                    (ip_range, data) = GetIrrDataFromAfrinic(ip)
                    (tmp_org_set, tmp_asn_set, tmp_src) = GetBelongedOrgFromAfrinic(data)
                elif src == 'ripe':
                    (ip_range, data) = GetIrrDataFromRipe(ip)
                    (tmp_org_set, tmp_asn_set, tmp_src) = GetBelongedOrgFromRipe(data)
                elif src == 'arin':
                    #print(ip)
                    (ip_range, data) = GetIrrDataFromArin(ip)
                    (tmp_org_set, tmp_asn_set, tmp_src) = GetBelongedOrgFromArin(data)
                #print('here4')
                org_set |= tmp_org_set
                asn_set |= tmp_asn_set
        else:
            print('Failed to get response from apnic. ip: %s' %ip)
            return (None, None)
    else:
        print('resource empty from apnic. ip: %s' %ip) 
        return (None, None)
    return (org_set, asn_set)

def FormatIpRange(ip_range): #有的ip_range不是以.0开头或不是以.255结尾, 统一都扩展到/24
    elems = ip_range.split(' ')
    first_ip = elems[0][:elems[0].rindex('.')] + '.0'
    last_ip = elems[-1][:elems[-1].rindex('.')] + '.255'
    return (first_ip + ' - ' + last_ip)

def DelRemarks(data):
    key_word = '"name":"remarks"'
    key_word_len = len(key_word)
    first_index = data.find(key_word)
    while first_index != -1:
        next_index = data.find('"name":', first_index + key_word_len)
        if next_index == -1:
            data = data[:first_index]
        else:
            data = data[:first_index] + data[next_index:]
        first_index = data.find(key_word)
    return data

#这个函数可能有问题，下面把原来写好的irr文件改一遍，主要是改key(ip_range)
def GetPrefix(ori_data):
    ip_range_form = re.compile(r'[\" ](\d+\.\d+\.\d+\.\d+ - \d+\.\d+\.\d+\.\d+)')
    prefix_form = re.compile(r'(\"\d+\.\d+\.\d+\.\d+\/\d+)')
    #data = '[{"type":"comments","comments":["% APNIC found the following authoritative answer from: whois.afrinic.net"]},{"type":"comments","comments":["% This is the AfriNIC Whois server.","% The AFRINIC whois database "52.192.0.0 - 52.223.191.255"  the following terms of Use. See https://afrinic.net/whois/terms"]},{"type":"comments","comments":["% Note: this output has been filtered.","%       To receive output for a database update, use the \"-B\" flag."]},{"type":"comments","comments":["% Information related to \'41.204.160.0 - 41.204.163.255\'"]},{"type":"comments","comments":["% Abuse contact for \'41.204.160.0 - 41.204.163.255\' is \'noc@kenet.or.ke\'"]},{"type":"object","attributes":[{"name":"inetnum","values":["41.204.160.0 - 41.204.163.255"]},{"name":"netname","values":["KENET-KENETHQ"]},{"name":"descr","values":["KENET-KENET Headquaters"]},{"name":"country","values":["KE"]},{"name":"admin-c","links":[{"text":"KA29-afrinic","url":"search.html?query=KA29-afrinic"}]},{"name":"tech-c","links":[{"text":"KA29-afrinic","url":"search.html?query=KA29-afrinic"}]},{"name":"tech-c","links":[{"text":"KNT1-AFRINIC","url":"search.html?query=KNT1-AFRINIC"}]},{"name":"status","values":["ASSIGNED PA"]},{"name":"mnt-by","links":[{"text":"KENET","url":"search.html?query=KENET"}]},{"name":"source","values":["AFRINIC # Filtered"]},{"name":"parent","values":["41.204.160.0 - 41.204.191.255"]}],"objectType":"inetnum","primaryKey":"41.204.160.0 - 41.204.163.255"},{"type":"object","attributes":[{"name":"person","values":["Kennedy Aseda"]},{"name":"address","values":["Jomo Kenyatta Memorial Library"]},{"name":"address","values":["University of Nairobi"]},{"name":"address","values":["Nairobi"]},{"name":"address","values":["Kenya"]},{"name":"address","values":["Nairobi 30244 00100,"]},{"name":"address","values":["Kenya"]},{"name":"phone","values":["tel:+254-732-150500"]},{"name":"phone","values":["tel:+254-703-044500"]},{"name":"nic-hdl","values":["KA29-afrinic"]},{"name":"mnt-by","links":[{"text":"GENERATED-INI4TWSIEBFYI7EOP5T54JA5UNSVGMRT-MNT","url":"search.html?query=GENERATED-INI4TWSIEBFYI7EOP5T54JA5UNSVGMRT-MNT"}]},{"name":"source","values":["AFRINIC # Filtered"]}],"objectType":"person","primaryKey":"KA29-afrinic"},{"type":"object","attributes":[{"name":"person","values":["KENET Noc Team"]},{"name":"address","values":["P.O. Box 30244 00100,","Nairobi","Kenya"]},{"name":"phone","values":["tel:+254-732-150500"]},{"name":"nic-hdl","values":["KNT1-AFRINIC"]},{"name":"mnt-by","links":[{"text":"GENERATED-DSUQYE40I3IYGU6QGYI8QG5UM6MUMGXG-MNT","url":"search.html?query=GENERATED-DSUQYE40I3IYGU6QGYI8QG5UM6MUMGXG-MNT"}]},{"name":"source","values":["AFRINIC # Filtered"]}],"objectType":"person","primaryKey":"KNT1-AFRINIC"},{"type":"comments","comments":["% Information related to \'41.204.160.0/21AS36914\'"]},{"type":"object","attributes":[{"name":"route","values":["41.204.160.0/21"]},{"name":"descr","values":["KENET"]},{"name":"origin","values":["AS36914"]},{"name":"mnt-by","links":[{"text":"KENET","url":"search.html?query=KENET"}]},{"name":"source","values":["AFRINIC # Filtered"]}],"objectType":"route","primaryKey":"41.204.160.0/21AS36914"}]'
    data = DelRemarks(ori_data)
    res1 = re.findall(ip_range_form, data)
    res2 = re.findall(prefix_form, data)
    res_set = set()
    if res1:
        for elem in res1:
            res_set.add(FormatIpRange(elem.strip('"')))
    else:
        for elem in res2:
            #print(elem)
            res_set.add(elem.strip('"'))
    #print(res_set)
    return res_set

#41.204.160.0 - 41.204.191.255
def GetIpRangeSlashLen(ip_range):
    elems = ip_range.split(' ')
    ip1 = elems[0]
    ip2 = elems[2]
    #print(ip1)
    #print(ip2)
    ipseglist1 = ip1.split('.')
    ipseglist2 = ip2.split('.')
    diff_val = 0
    for i in range(0, 4):
        diff_val = (diff_val << 8) + int(ipseglist2[i]) - int(ipseglist1[i])
    return (32 - int(np.log2(diff_val + 1)))

def TransIpRangeToPrefix(ip_range):
    slashlen = GetIpRangeSlashLen(ip_range)
    return (ip_range.split(' ')[0] + '/' + str(slashlen))

def TransPrefixToIpRange(prefix):
    elems = prefix.split('/')
    ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton(elems[0]))[0])
    last_ip_int = ip_int + (1 << (32 - int(elems[1]))) - 1
    return (elems[0] + ' - ' + str(socket.inet_ntoa(struct.pack('I',socket.htonl(last_ip_int)))))

def GetLastIPInt(ip_range):
    elems = ip_range.split(' ')
    return socket.ntohl(struct.unpack("I",socket.inet_aton(elems[-1]))[0])

def GetNextRangeIP(ip_range):
    elems = ip_range.split(' ')
    #print(ip_range)
    ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton(elems[-1]))[0]) + 2 #*.*.*.1
    return str(socket.inet_ntoa(struct.pack('I',socket.htonl(ip_int))))

def GetNextRangeIP_2(ip_range):
    elems = ip_range.split(' ')
    #print(ip_range)
    ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton(elems[-1]))[0]) + 1 #因为有的不是/24子网，所以只增加1
    return str(socket.inet_ntoa(struct.pack('I',socket.htonl(ip_int))))

def GetSmallestIpRange(data, ref_ip):
    res_set = GetPrefix(data)
    if not res_set or full_ip_space in res_set:
        return TransPrefixToIpRange(ref_ip[:ref_ip.rindex('.')] + '.0/24')
    smallest_ip_int = 0
    smallest_ip_range = ''
    for elem in res_set:
        if elem.__contains__('/'):
            elem = TransPrefixToIpRange(elem)
        last_ip_int = GetLastIPInt(elem)
        if smallest_ip_int == 0 or smallest_ip_int > last_ip_int:
            smallest_ip_int = last_ip_int
            smallest_ip_range = elem
    #print(smallest_ip_range)
    return smallest_ip_range

def GetLargestIpRange(data, ref_ip):
    res_set = GetPrefix(data)
    if not res_set or full_ip_space in res_set:
        return TransPrefixToIpRange(ref_ip[:ref_ip.rindex('.')] + '.0/24')
    largest_ip_int = 0
    largest_ip_range = ''
    for elem in res_set:
        if elem.__contains__('/'):
            elem = TransPrefixToIpRange(elem)
        last_ip_int = GetLastIPInt(elem)
        if largest_ip_int < last_ip_int:
            largest_ip_int = last_ip_int
            largest_ip_range = elem
    return largest_ip_range
    
def GetLargestIpRange_2(range_list):
    largest_ip_int = 0
    largest_ip_range = ''
    for elem in range_list:
        if elem.__contains__('/'):
            elem = TransPrefixToIpRange(elem)
        last_ip_int = GetLastIPInt(elem)
        if largest_ip_int < last_ip_int:
            largest_ip_int = last_ip_int
            largest_ip_range = elem
    return largest_ip_range
    

def WriteData(ip_range, data, wf): #每次写一行
    tmp_dict = dict()
    tmp_dict[ip_range] = data
    json_str = json.dumps(tmp_dict)
    wf.write(json_str + '\n')

def CheckIp(ip):
    compile_ip = re.compile('^(1\d{2}|2[0-4]\d|25[0-5]|[1-9]\d|[1-9])\.(1\d{2}|2[0-4]\d|25[0-5]|[1-9]\d|\d)\.(1\d{2}|2[0-4]\d|25[0-5]|[1-9]\d|\d)\.(1\d{2}|2[0-4]\d|25[0-5]|[1-9]\d|\d)$')
    if compile_ip.match(ip):
        return True 
    else:  
        return False

#'0.0.0.0/8'
#'10.0.0.0/8', '127.0.0.0/8', '224.0.0.0/4', '240.0.0.0/4', '169.254.0.0/16', '192.168.0.0/16', '100.64.0.0/10', '172.16.0.0/12', '198.18.0.0/15'
def IsReservedIp(ip):
    elems = ip.split('.')
    seg1 = int(elems[0])
    if seg1 == 10 or seg1 == 127 or seg1 >= 224:
        return True
    seg2 = int(elems[1])
    if (seg1 == 169 and seg2 == 254) or (seg1 == 192 and seg2 == 168):
        return True
    if seg1 == 100 and (seg2 >= 64 and seg2 < 128):
        return True
    if seg1 == 172 and (seg2 >= 16 and seg2 < 32):
        return True
    if seg1 == 198 and (seg2 == 18 and seg2 == 19):
        return True
    return False

def NextPrefix24Ip(ip):
    ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton(ip))[0])
    if ip_int + 256 > 0xFFFFFFFF:
        print("%s exceed ip range" %ip)
        return None
    return str(socket.inet_ntoa(struct.pack('I',socket.htonl(ip_int + 256))))

def GetLatestIPRangeInSet(ip_range_set):
    largest_ip_int = 0
    largest_ip_range = ''
    for elem in ip_range_set:
        ip_range = FormatIpRange(elem)
        last_ip_int = GetLastIPInt(ip_range)
        if largest_ip_int < last_ip_int:
            largest_ip_int = last_ip_int
            largest_ip_range = ip_range
    return largest_ip_range

def GetDataInApnic():
    url_pre = "https://wq.apnic.net/query?searchtext="
    req = requests.Session()
    headers = {"accept":"application/json"}
    wf = open(global_var.par_path + 'test4', 'w')
    #ip = '1.0.0.1'
    ip = '1.0.0.1'
    num = 0
    time_exceed_times = 0

    while CheckIp(ip):# and num < 3:
        if IsReservedIp(ip):
            ip = NextPrefix24Ip(ip)
            continue
        ip_range_set = GetIRRIndex(ip)
        if ip_range_set:
            ip_range = GetLatestIPRangeInSet(ip_range_set)
            #print("here %s" %ip_range)
            #print(ip_range)
            ip = GetNextRangeIP(ip_range)
            continue
        #not found
        num += 1
        print(ip)
        url = url_pre + ip
        resource = req.get(url, headers=headers) 
        if resource:                
            if resource.status_code == 200:
                if resource.text.__contains__('Query rate limit exceeded'):
                    print('Query rate limit exceeded. ip: %s' %ip)
                    if time_exceed_times > 3:
                        print('Query rate limit exceeded. break')
                        break
                    time.sleep(5)
                    continue
                time_exceed_times = 0
                #print(resource.text)
                ip_range = GetLargestIpRange(resource.text, ip)
                #print(ip_range)
                data = json.loads(resource.text)
                WriteData(ip_range, data, wf)
                ip = GetNextRangeIP(ip_range)
            else:
                print('Failed to get response. ip: %s' %ip)
        else:
            print('resource empty. ip: %s' %ip) 
            break
    wf.close()
    print(num)

def Slash24IpRange(ip):
    pre = ip[:ip.rindex('.')]
    return (pre + '.0 - ' + pre + '.255')

def GetIpRangeFromApnicData(data, ref_ip=None):
    ip_range_list = []
    for elem in data:
        if elem['type'] == 'object':
            for sub_elem in elem['attributes']:
                if sub_elem['name'] == 'inetnum':
                    ip_range_list = sub_elem['values']
    if ip_range_list:
        return GetLargestIpRange_2(ip_range_list)
    src = GetSrcOfData(json.dumps(data))
    if src == 'arin': #arin的数据没有inetnum选项，在comment中
        return GetSmallestIpRange(json.dumps(data), ref_ip)
    return None

def GetDataInApnic_2():
    url_pre = "https://wq.apnic.net/query?searchtext="
    req = requests.Session()
    headers = {"accept":"application/json"}
    wf = open(global_var.par_path + global_var.irr_dir + 'tmp', 'w')
    #ip = '1.0.0.1'
    ip = '213.238.160.1'
    num = 0
    time_exceed_times = 0

    while CheckIp(ip):# and num < 3:
        if IsReservedIp(ip):
            ip = NextPrefix24Ip(ip)
            continue
        prefix = GetIRRIndex_2(ip)
        if prefix:
            ip_range = TransPrefixToIpRange(prefix)
            ip = GetNextRangeIP_2(ip_range)
            continue
        #not found
        num += 1
        print(ip)
        url = url_pre + ip
        resource = req.get(url, headers=headers) 
        if resource:                
            if resource.status_code == 200:
                if resource.text.__contains__('Query rate limit exceeded'):
                    print('Query rate limit exceeded. ip: %s' %ip)
                    if time_exceed_times > 3:
                        print('Query rate limit exceeded. break')
                        break
                    time.sleep(5)
                    continue
                time_exceed_times = 0
                data = json.loads(resource.text)
                ip_range = GetIpRangeFromApnicData(data, ip)
                if not ip_range or ip_range == full_ip_space:
                    if not ip_range:
                        print('NOTE ip none')
                    else:
                        print('NOTE ip: %s' %ip_range)
                    ip_range = Slash24IpRange(ip)
                print(ip_range)
                WriteData(ip_range, data, wf)
                ip = GetNextRangeIP_2(ip_range)
            else:
                print('Failed to get response. ip: %s' %ip)
        else:
            print('resource empty. ip: %s' %ip) 
            break
    wf.close()
    print(num)

def GetJsonInfoFromArinHtml(data):
    #data = ''
    #with open(filename, 'r') as rf:
        #data = rf.read()
    info_dict = dict()
    #print(data)
    #<td>Net Range</td><td>52.196.0.0 - 52.199.255.255</td>
    table_form = re.compile(r'<table>(.*?)</table>', re.S)
    res = re.findall(table_form, data)
    for table_info in res:
        table_head_form = re.compile(r'<th .*?>(.*?)</th>')
        res_1 = re.findall(table_head_form, table_info)
        head = 'None'
        if res_1:
            head = res_1[0]
        if head not in info_dict.keys():
            info_dict[head] = []
        tmp_dict = dict()
        info_form = re.compile(r'<td>(.*?)</td><td>(.*?)</td>', re.S)
        res_2 = re.findall(info_form, table_info)
        for elem in res_2:
            #val = elem[1].replace('<br>', '').replace('\n', '').replace(" .*", ' ')
            val = elem[1].replace('<br>', '').replace('\n', '')
            val = re.sub(" +", ' ', val)
            tmp_dict[elem[0]] = val
        info_dict[head].append(tmp_dict)
    json_data = json.dumps(info_dict)
    return json_data

def GetIrrDataFromArin(ip, wf=None, ip_range = None):
    #url = "https://whois.arin.net/ui/query.do"
    url = "https://whois.arin.net/ui/query.do?xslt=https%3A%2F%2Flocalhost%3A8080%2Fui%2Farin.xsl&flushCache=false&queryinput=" + ip + "&whoisSubmitButton=+"
    req = requests.Session()
    #data = {}
    #data['queryinput'] = ip
    #data['xslt'] = "https%3A%2F%2Flocalhost%3A8080%2Fui%2Farin.xsl"
    #data['flushCache'] = 'false'
    #data['whoisSubmitButton'] = '+'
    #resource = req.post(url, params=data)
    resource = req.get(url)
    location = ''
    if resource:                
        if resource.status_code == 200 and resource.history: #redirect
            location = resource.history[0].headers['Location']
            #print(location)
    headers = {'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'}
    resource_2 = req.get(location, headers=headers)
    if resource_2:                
        if resource_2.status_code == 200:
            #print(resource_2.text)
            data = GetJsonInfoFromArinHtml(resource_2.text)
            #print(data)
            if not ip_range:
                ip_range = GetLargestIpRange(data, ip)
            #print(ip_range)
            if wf:
                WriteData(ip_range, json.loads(data), wf)
            else: #直接返回数据
                return (ip_range, json.loads(data))
    return None

def GetIrrDataFromRipe(ip, wf=None, ip_range = None):
    url = "https://apps.db.ripe.net/db-web-ui/api/whois/search?abuse-contact=true&ignore404=true&managed-attributes=true&resource-holder=true&flags=r&offset=0&limit=20&query-string=" + ip
    req = requests.Session()
    headers = {"accept":"application/json"}   
    resource = req.get(url, headers=headers)     
    if resource:                
        if resource.status_code == 200:
            if not ip_range:
                ip_range = GetLargestIpRange(resource.text, ip)
            data = json.loads(resource.text)
            if wf:
                WriteData(ip_range, data, wf)
            else: #直接返回数据
                return (ip_range, data)
    return None

def GetJsonInfoFromAfrinicHtml(data):
    info_list = []
    #print(data)
    #<td>Net Range</td><td>52.196.0.0 - 52.199.255.255</td>
    table_form = re.compile(r'<pre class=\"plain-object\">(.*?)</pre>', re.S)
    res = re.findall(table_form, data)
    for table_info in res:
        tmp_dict = dict()
        for elem in table_info.split('\n'):
            info = elem.split(':')
            if len(info) < 2:
                continue
            if info[0] not in tmp_dict.keys():
                tmp_dict[info[0]] = []
            tmp_dict[info[0]].append(info[1].strip('\t').strip(' '))
        info_list.append(tmp_dict)
    json_data = json.dumps(info_list)
    #print(json_data)
    return json_data

def GetIrrDataFromAfrinic(ip, wf=None, ip_range = None):
    url = "https://www.afrinic.net/whois-web/public/?lang=en&key=" + ip + "&sourceDatabases=afrinic&tabs=on"
    req = requests.Session()
    headers = {'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'}
    resource = req.post(url, headers=headers)
    if resource:                
        if resource.status_code == 200:
            data = GetJsonInfoFromAfrinicHtml(resource.text)
            if not ip_range:
                ip_range = GetLargestIpRange(data, ip)
            #print(ip_range)
            if wf:
                WriteData(ip_range, json.loads(data), wf)
            else: #直接返回数据
                return (ip_range, json.loads(data))
    return None


def GetIrrDataFromIrrs(ip, src, wf_dict, ip_range = None):
    #print(ip)
    #print(src)
    if src not in global_var.irrs:
        print("src error: %s" %src)
        return
    if src == 'ripe':
        GetIrrDataFromRipe(ip, wf_dict[src], ip_range)
    elif src == 'arin':
        GetIrrDataFromArin(ip, wf_dict[src], ip_range)
    elif src == 'afrinic':
        GetIrrDataFromAfrinic(ip, wf_dict[src], ip_range)
    else:
        print("cannot download data from lacnic yet")

# def GetBackboneIpDataInOthers():
#     url_pre = "https://wq.apnic.net/query?searchtext="
#     req = requests.Session()
#     headers = {"accept":"application/json"}
#     wf = open(global_var.par_path + 'test4', 'w')
#     #ip = '1.0.0.1'
#     ip = '1.0.0.1'
#     num = 0
#     time_exceed_times = 0

#     while CheckIp(ip):# and num < 3:
#         if IsReservedIp(ip):
#             ip = NextPrefix24Ip(ip)
#             continue
#         ip_range_set = GetIRRIndex(ip)
#         #test_ip_int1 = socket.ntohl(struct.unpack("I",socket.inet_aton('212.74.0.1'))[0])
#         #test_ip_int2 = socket.ntohl(struct.unpack("I",socket.inet_aton('212.75.0.1'))[0])
#         #test_ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton(ip))[0])
#         #if test_ip_int > test_ip_int1 and test_ip_int < test_ip_int2:
#             #print(ip)
#             #print(ip_range_set)
#         if ip_range_set:
#             [org_set, asn_set, src] = GetOrgAsnFromIRR(ip)
#             if (not asn_set) and (src != 'apnic'):
#                 #print(1)
#                 for org in org_set:
#                     if org.__contains__('Backbone'):
#                         #print(ip)
#                         #print('Need reget backbone address')
#                         #需要重新从相应的irr上下org信息，因为apnic上的信息有时候对不上
#                         GetIrrDataFromIrrs(ip, src, wf)
#             ip_range = GetLatestIPRangeInSet(ip_range_set)
#             ip = GetNextRangeIP(ip_range)
#             continue
#         #not found
#         print("%s not found" %ip)
#         num += 1
#         #print(ip)
#         url = url_pre + ip
#         resource = req.get(url, headers=headers) 
#         if resource:                
#             if resource.status_code == 200:
#                 if resource.text.__contains__('Query rate limit exceeded'):
#                     print('Query rate limit exceeded. ip: %s' %ip)
#                     if time_exceed_times > 3:
#                         print('Query rate limit exceeded. break')
#                         break
#                     time.sleep(5)
#                     continue
#                 time_exceed_times = 0
#                 #print(resource.text)
#                 ip_range = GetLargestIpRange(resource.text, ip)
#                 #print(ip_range)
#                 data = json.loads(resource.text)
#                 WriteData(ip_range, data, wf)
#                 ip = GetNextRangeIP(ip_range)
#             else:
#                 print('Failed to get response. ip: %s' %ip)
#         else:
#             print('resource empty. ip: %s' %ip) 
#             break
#     wf.close()
#     print(num)

def GetOtherSrcData():
    wf_dict = dict()
    sel = 1
    for irr in global_var.irrs:
        if irr != 'apnic':
            if sel == 0:
                wf_dict[irr] = open(global_var.par_path + global_var.irr_dir + global_var.irr_filename_pre + irr, 'a')
            elif sel == 1:
                wf_dict[irr] = open(global_var.par_path + global_var.irr_dir + global_var.irr_filename_pre + irr + '_100', 'a')
            else:
                wf_dict[irr] = open(global_var.par_path + global_var.irr_dir + global_var.irr_filename_pre + irr + '_tmp', 'w')
    rf = open(global_var.par_path + global_var.irr_dir + 'irrdata', 'r')
    curline = rf.readline()
    num = 0
    tmp_set = set()
    if sel == 0:
        begin_ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton('96.20.33.0'))[0])
        end_ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton('99.255.255.255'))[0])
    elif sel == 1:
        begin_ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton('185.183.67.144'))[0])
        end_ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton('189.255.255.255'))[0])
    else:
        begin_ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton('192.234.9.0'))[0])
        end_ip_int = socket.ntohl(struct.unpack("I",socket.inet_aton('199.255.255.255'))[0])

    while curline:
        num += 1
        key = curline[1:curline.index(':')].strip('"')
        if key == full_ip_space or key == 'null':
            curline = rf.readline()
            continue
        tmp = socket.ntohl(struct.unpack("I",socket.inet_aton(key.split(' ')[-1]))[0])
        if tmp < begin_ip_int:
            curline = rf.readline()
            continue
        if tmp > end_ip_int:
            break
        info = json.loads(curline.strip('\n'))
        data = info[key]
        (org_set, asn_set, src) = GetBelongedOrg(data)
        if src != 'apnic':
            print(key)
            GetIrrDataFromIrrs(key.split(' ')[0], src, wf_dict, key)
        curline = rf.readline()
    rf.close()
    for irr in global_var.irrs:
        if irr != 'apnic':
            wf_dict[irr].close()    
    
def GetBackboneIpDataInOthers_2(): #每个/24ip都扫描一下
    url_pre = "https://wq.apnic.net/query?searchtext="
    req = requests.Session()
    headers = {"accept":"application/json"}
    wf_dict = dict()
    checked_ip_range_set = set()
    for irr in global_var.irrs:
        if irr != 'apnic':
            with open(global_var.par_path + global_var.irr_dir + global_var.irr_filename_pre + irr, 'r') as rf:
                curline = rf.readline()
                while curline:
                    key = curline[1:curline.index(':')].strip('"')
                    checked_ip_range_set.add(key)
                    curline = rf.readline()
            wf_dict[irr] = open(global_var.par_path + global_var.irr_dir + global_var.irr_filename_pre + irr, 'a')
    #ip = '1.0.0.1'
    ip = '1.0.0.1'
    num = 0
    time_exceed_times = 0
    print(checked_ip_range_set)

    while CheckIp(ip):# and num < 3:
        if not IsReservedIp(ip):
            ip_range_set = GetIRRIndex(ip)
            for ip_range in ip_range_set:
                if not ip_range in checked_ip_range_set:
                    #print(ip_range_set)
                    [org_set, asn_set, src] = GetOrgAsnFromIRR(ip)
                    if ((not org_set) or (not asn_set)) and (src != 'apnic'):
                        #print(1)
                        if not org_set:
                            GetIrrDataFromIrrs(ip, src, wf_dict, None)
                        else:
                            for org in org_set:
                                if org.__contains__('Backbone'):
                                    #print(ip)
                                    #print('Need reget backbone address')
                                    #需要重新从相应的irr上下org信息，因为apnic上的信息有时候对不上
                                    GetIrrDataFromIrrs(ip, src, wf_dict, None)
                    checked_ip_range_set |= ip_range_set
        ip = NextPrefix24Ip(ip)
        continue

    for irr in global_var.irrs:
        if irr != 'apnic':
            wf_dict[irr].close()
    print(num)

def ModiIrrFile():
    os.chdir(global_var.par_path + global_var.irr_dir)

    for irr in global_var.irrs:
        print(irr)
        rf = open(global_var.irr_filename_pre + irr + '_back', 'r')
        wf = open(global_var.irr_filename_pre + irr, 'w')
        wf_ab = open(global_var.irr_filename_pre + irr + '_ab', 'w')
        curline = rf.readline()
        while curline:
            key = curline[1:curline.index(':')].strip('"')
            info = json.loads(curline.strip('\n'))
            data = info[key]
            ip_range = GetIpRangeFromApnicData(data)
            if ip_range != key:
                print(ip_range + '   ' + key)
                tmp_dict = dict()
                tmp_dict[ip_range] = data
                wf.write(json.dumps(tmp_dict) + '\n')
            else:
                wf_ab.write(curline)
            curline = rf.readline()
        rf.close()
        wf.close()
        wf_ab.close()

        
if __name__ == '__main__':
    #GetIrrDictFromDefault()
    #GetDataInApnic_2()
    #GetOtherSrcData()
    #ModiIrrFile()
    pass
