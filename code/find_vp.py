
import os
from posixpath import join
import requests
import json
import re
from bs4 import BeautifulSoup

import global_var
from urllib.request import urlopen
from utils_v2 import CloseDb, GetPfx2ASByRv, ConnectToDb, SetCurMidarTableDate, GetAsListOfIpByRv, GetAsOfIpByMi, \
                    GetGeoOfIpByMi, GeoDistance
from get_ip2as_from_bdrmapit import ConnectToBdrMapItDb, ConstrBdrCache, GetIp2ASFromBdrMapItDb, InitBdrCache, \
                                    CloseBdrMapItDb
from download_irrdata import GetCountryFromIrrFiles, PreGetNonOverlayIpRanges, GetCountryOrgAsnFromIrrOnline
from constr_irr_db import ConstrIrrCache, ConnectToIrrDb, CloseIrrDb, GetIrrOrgFromDb
  
#http://routeviews.org/bgpdata/2019.01/UPDATES/updates.20190101.0000.bz2
g_req = requests.Session()
def GetOneRibFromRib():
    global g_req
    os.chdir(global_var.par_path + global_var.rib_dir + 'all_collectors_one_date_bgpdata/')
    url = 'http://routeviews.org/'
    r = g_req.get(url, stream=True)
    #print(r.content)
    #soup = BeautifulSoup(r.content.decode('utf-8').replace('\n', ' '), 'html.parser')    
    soup = BeautifulSoup(r.content.decode('utf-8'), 'html.parser')    
    #print(soup.a)
    sub_dir = '/2019.01/RIBS/'
    for link in soup.find_all('a', href=re.compile("bgpdata")):
        collector_addr = link['href'].strip('/')
        url2 = url + collector_addr + sub_dir
        print(url2)
        r2 = g_req.get(url2, stream=True, timeout=60) 
        soup2 = BeautifulSoup(r2.content.decode('utf-8'), 'html.parser')
        filenames = soup2.find_all('a', href=re.compile("bz2"))
        if len(filenames) < 3:
            continue
        #print(filenames)
        for i in range(0, 3):
            one_file = filenames[i]['href']
            url3 = url + collector_addr + sub_dir + one_file
            print(url3)
            r3 = g_req.get(url3, stream=True, timeout=60) 
            if r3:
                if r3.status_code == 200:
                    print(one_file)
                    w_filename = 'route-views2'
                    if collector_addr.__contains__('/'):
                        w_filename = collector_addr[:collector_addr.index('/')]
                    print(w_filename)
                    if os.path.exists(w_filename):
                        break
                    with open(w_filename, 'wb') as wf:
                        wf.write(r3.content)
                    print("%s filesize: %d" %(w_filename, os.path.getsize(w_filename)))
                    break

def GetOneRibFromRRC():
    global g_req
    os.chdir(global_var.par_path + global_var.rib_dir + 'all_collectors_one_date_bgpdata/')
    #url = "http://data.ris.ripe.net/" + collector + year_str + '.' + month_str + "/bview." + year_str + month_str + "15." + time_suffix
    #http://data.ris.ripe.net/rrc00/2019.03/bview.20190301.1600.gz
    
    for i in range(0,25):
        url = 'http://data.ris.ripe.net/rrc' + str(i).zfill(2) + '/2019.01/bview.20190101.1600.gz'
        r = g_req.get(url, stream=True)    
        if r:
            if r.status_code == 200:
                w_filename = 'rrc' + str(i).zfill(2)
                if os.path.exists(w_filename):
                    continue
                with open(w_filename, 'wb') as wf:
                    wf.write(r.content)
                print("%s filesize: %d" %(w_filename, os.path.getsize(w_filename)))

c2v_dict = dict()
v2c_dict = dict()
def GetAllVpsOfBgp():
    global c2v_dict
    global v2c_dict
    os.chdir(global_var.par_path + global_var.rib_dir + 'all_collectors_one_date_bgpdata/')
    c2v_filename = 'c2v_dict'
    if not os.path.exists(c2v_filename):
        for root,dirs,files in os.walk('.'):
            for filename in files:
                print(filename)
                collector = filename[:filename.rindex('.')]
                cmd = "bgpscanner -L %s > %s" %(filename, collector)
                print(cmd)
                os.system(cmd)    
                if collector not in c2v_dict.keys():
                    c2v_dict[collector] = set()
                with open(collector, 'r') as rf:
                    curline = rf.readline()
                    while curline:
                        elems = curline.split('|')
                        if len(elems) > 5 and not elems[1].__contains__(':'):
                            next_hop = elems[3]
                            c2v_dict[collector].add(next_hop)
                            if next_hop not in v2c_dict.keys():
                                v2c_dict[next_hop] = set()
                            v2c_dict[next_hop].add(collector)
                        curline = rf.readline()
        with open(c2v_filename, 'w') as wf:
            for (collector, vps) in c2v_dict.items():
                wf.write("%s:%s;" %(collector, ','.join(list(vps))))
    else:
        with open(c2v_filename, 'r') as rf:
            data_list = rf.read().strip(';').split(';')
        for elem in data_list:
            [collector, vps_str] = elem.split(':')
            vps_set = set(vps_str.strip(',').split(','))
            c2v_dict[collector] = vps_set
            for vp in vps_set:
                if vp not in v2c_dict.keys():
                    v2c_dict[vp] = set()
                v2c_dict[vp].add(collector)
        if '' in c2v_dict.keys():
            del c2v_dict['']
        if '' in v2c_dict.keys():
            del v2c_dict['']

trace_vp_dict = dict() #[ip:vp]
def GetAllVpsOfTraceroute():
    global trace_vp_dict
    trace_vp_filename = global_var.all_trace_par_path + global_var.all_trace_download_dir + 'trace_vp_dict'
    if not os.path.exists(trace_vp_filename):
        os.chdir(global_var.all_trace_par_path + global_var.all_trace_download_dir + '2019/01/')
        vps = set()
        for root,dirs,files in os.walk('.'):
            for filename in files:
                if (not filename.endswith('.warts')) and (not filename.endswith('.gz')) and (not filename.startswith('as_')):
                    # plain_filename = '.'.join(filename.split('.')[:2])
                    # if os.path.exists(plain_filename):
                    #     continue
                    # vp = filename[:filename.index('.')]
                    # if vp in vps:
                    #     continue
                    # cmd = "sc_analysis_dump %s > %s" %(filename, plain_filename)
                    # print(cmd)
                    # os.system(cmd)
                    plain_filename = filename
                    print(plain_filename)
                    vp = filename[:filename.index('.')]
                    with open(plain_filename, 'r') as rf:
                        curline = rf.readline()
                        while curline:
                            if not curline.startswith('T'):
                                curline = rf.readline()
                                continue
                            elems = curline.strip('\n').split('\t')
                            if len(elems) > 2:
                                trace_vp_dict[elems[1]] = vp
                                print(elems[1] + ':' + vp)
                                vps.add(vp)
                                break
                            curline = rf.readline()
        with open(trace_vp_filename, 'w') as wf:
            for (ip, vp) in trace_vp_dict.items():
                wf.write(ip + ':' + vp + ';')
    else:
        with open(trace_vp_filename, 'r') as rf:
            data_list = rf.read().strip(';').split(';')
            for elem in data_list:
                [ip, vp] = elem.split(':')
                trace_vp_dict[ip] = vp

rib_vp_info_dict = dict()
trace_vp_info_dict = dict()
def GetAsOfVps(vp_dict, filename, type):
    global rib_vp_info_dict
    global trace_vp_info_dict
    if type == 'rib':
        res_dict = rib_vp_info_dict
    elif type == 'trace':
        res_dict = trace_vp_info_dict
    if True:#not os.path.exists(filename):
        GetPfx2ASByRv()
        #ConnectToDb()
        #SetCurMidarTableDate(2019, 1)
        for vp in vp_dict:
            print(vp)
            res_dict[vp] = [set(), set(), '', ''] #asn_set, org_set, country, city
            as_set = set(GetAsListOfIpByRv(vp))
            print(as_set)   
            if None in as_set:
                as_set.remove(None)       
            res_dict[vp][0] = as_set
        for year in range(2018, 2021):
            print(year)
            year_str = str(year)
            ConnectToBdrMapItDb(global_var.par_path + global_var.out_bdrmapit_dir + 'bdrmapit_' + year_str + '.db')
            InitBdrCache()
            ConstrBdrCache()
            for vp in vp_dict:
                print(vp)
                tmp_as = GetIp2ASFromBdrMapItDb(vp)
                if tmp_as:
                    res_dict[vp][0].add(tmp_as)
            CloseBdrMapItDb()
        # with open(filename, 'w') as wf:
        #     for (asn, vps) in res_dict.items():
        #         wf.write("%s:%s;" %(asn, ','.join(list(vps))))
    else:
        with open(filename, 'r') as rf:
            data_list = rf.read().strip(';').split(';')
            for info in data_list:
                elems = info.split(':')
                res_dict[elems[0]] = set(elems[1].split(','))      

def GetAsOfRibVps():
    global v2c_dict
    global rib_vp_info_dict
    obj_filename = global_var.par_path + global_var.other_middle_data_dir + 'rib_vp_info_dict_from_irr'
    GetAllVpsOfBgp()
    if not os.path.exists(obj_filename):
        filename = global_var.par_path + global_var.other_middle_data_dir + 'rib_vp_as_dict' #暂时用不上
        GetAsOfVps(v2c_dict.keys(), filename, 'rib') #filename没有用
        #print(len(v2c_dict))
        for ip in v2c_dict.keys():
            print(ip)
            [asn_set, org_set, country, city] = GetCountryOrgAsnFromIrrOnline(ip) 
            if None in asn_set:
                asn_set.remove(None) 
            rib_vp_info_dict[ip][0] |= asn_set
            if None in org_set:
                org_set.remove(None) 
            rib_vp_info_dict[ip][1] = org_set
            rib_vp_info_dict[ip][2] = country
            rib_vp_info_dict[ip][3] = city
            # print(ip + ': ', end='')
            # if None in asn_set:
            #     asn_set.remove(None)  
            # print(','.join(asn_set) + '; ', end='')   
            # if None in org_set:
            #     org_set.remove(None)  
            # print(','.join(org_set) + '; ', end='')
            # if not country:
            #     country = ''
            # print(country + '; ', end='')
            # if not city:
            #     city = ''
            # print(city + ';')
        with open(obj_filename, 'w') as wf:
            for (ip, info) in rib_vp_info_dict.items():
                wf.write(ip + ':')
                [asn_set, org_set, country, city] = info
                if not country:
                    country = ''
                if not city:
                    city = ''
                wf.write(','.join(list(asn_set)) + ';' +  ','.join(list(org_set)) + ';' + country + ';' + city + '\n')
    else:
        with open(obj_filename, 'r') as rf:
            data_list = rf.read().strip('\n').split('\n')
            for data in data_list:
                index = data.index(':')
                ip = data[:index]
                info = data[index + 1:]
                [asn_str, org_str, country, city] = info.split(';')
                rib_vp_info_dict[ip] = [set(asn_str.split(',')), set(org_str.split(',')), country, city]
    
def GetAsOfTraceVps():
    global trace_vp_info_dict
    global trace_vp_dict
    obj_filename = global_var.par_path + global_var.other_middle_data_dir + 'trace_vp_info_dict_from_irr'
    GetAllVpsOfTraceroute()
    if not os.path.exists(obj_filename):
        filename = global_var.par_path + global_var.other_middle_data_dir + 'trace_vp_as_dict'
        GetAsOfVps(trace_vp_dict.keys(), filename, 'trace') #filename没有用
        for ip in trace_vp_dict.keys():
            print(ip)
            # (country_set, city_set) = GetCountryFromIrrFiles(ip)
            # (org_set, asn_set) = GetIrrOrgFromDb(ip)
            [asn_set, org_set, country, city] = GetCountryOrgAsnFromIrrOnline(ip)
            if None in asn_set:
                asn_set.remove(None) 
            trace_vp_info_dict[ip][0] |= asn_set
            if None in org_set:
                org_set.remove(None) 
            trace_vp_info_dict[ip][1] = org_set
            trace_vp_info_dict[ip][2] = country
            trace_vp_info_dict[ip][3] = city        
            # print(ip + ': ', end='')
            # if None in asn_set:
            #     asn_set.remove(None)  
            # print(','.join(asn_set) + '; ', end='')   
            # if None in org_set:
            #     org_set.remove(None)  
            # print(','.join(org_set) + '; ', end='')
            # if not country:
            #     country = ''
            # print(country + '; ', end='')
            # if not city:
            #     city = ''
            # print(city + ';')
        with open(obj_filename, 'w') as wf:
            for (ip, info) in trace_vp_info_dict.items():
                wf.write(ip + ':')
                [asn_set, org_set, country, city] = info
                if not country:
                    country = ''
                if not city:
                    city = ''
                wf.write(','.join(list(asn_set)) + ';' +  ','.join(list(org_set)) + ';' + country + ';' + city + '\n')
    else:
        with open(obj_filename, 'r') as rf:
            data_list = rf.read().strip('\n').split('\n')
            for data in data_list:
                index = data.index(':')
                ip = data[:index]
                info = data[index + 1:]
                [asn_str, org_str, country, city] = info.split(';')
                trace_vp_info_dict[ip] = [set(asn_str.split(',')), set(org_str.split(',')), country, city]

def GetMinDist(geo_set1, geo_set2):
    res = 1000000
    for geo1 in geo_set1:
        for geo2 in geo_set2:
            tmp = GeoDistance(geo1, geo2)
            if tmp < res:
                res = tmp
    return res

def FindCommonVp():  
    global rib_vp_info_dict
    global trace_vp_info_dict
    global v2c_dict
    global trace_vp_dict
    common_vp_dict = dict()
    for (rib_vp, rib_info) in rib_vp_info_dict.items():
        collector = v2c_dict[rib_vp]
        [rib_asn_set, rib_org_set, rib_country, rib_city] = rib_info
        for (trace_vp, trace_info) in trace_vp_info_dict.items():
            vp = trace_vp_dict[trace_vp]
            [trace_asn_set, trace_org_set, trace_country, trace_city] = trace_info
            if rib_asn_set & trace_asn_set:
                if rib_country == trace_country:
                    key = ','.join(list(collector)) + ' ' + vp
                    if key not in common_vp_dict.keys():
                        common_vp_dict[key] = dict()
                    key1 = ','.join(list(rib_asn_set & trace_asn_set)) + ' ' + rib_country
                    if key1 not in common_vp_dict[key].keys():
                        common_vp_dict[key][key1] = set()
                    common_vp_dict[key][key1].add(rib_vp + ' ' + trace_vp)
    for (key, info) in common_vp_dict.items():
        print(key)
        for (key1, info1) in info.items():
            print('\t' + key1)
            for elem in info1:
                print('\t\t' + elem)
    # join_as_set = set(rib_vp_info_dict.keys()) & set(trace_vp_info_dict.keys())
    # adj_ips = dict()
    # rib_ip_geo_dict = dict()
    # trace_ip_geo_dict = dict()
    # for date in [[2019, 1], [2019, 4], [2020, 1]]:
    #     SetCurMidarTableDate(date[0], date[1])
    #     #print(' '.join(list(rib_vp_info_dict.keys())))
    #     #print(' '.join(list(trace_vp_info_dict.keys())))
    #     for asn in join_as_set:
    #         #print(asn)
    #         # if asn != '7575':
    #         #     continue
    #         for rib_ip in rib_vp_info_dict[asn]:
    #             geo = GetGeoOfIpByMi(rib_ip)
    #             print('rib_ip: %s, geo: %s' %(rib_ip, geo))
    #             if rib_ip not in rib_ip_geo_dict.keys():
    #                 rib_ip_geo_dict[rib_ip] = set()
    #             rib_ip_geo_dict[rib_ip].add(geo)
    #         for trace_ip in trace_vp_info_dict[asn]:
    #             geo = GetGeoOfIpByMi(trace_ip)
    #             print('trace_ip: %s, vp: %s, geo: %s' %(trace_ip, trace_vp_dict[trace_ip], geo))
    #             if trace_ip not in trace_ip_geo_dict.keys():
    #                 trace_ip_geo_dict[trace_ip] = set()
    #             trace_ip_geo_dict[trace_ip].add(geo)
    # for asn in join_as_set:
    #     # if asn != '7575':
    #     #     continue
    #     for rib_ip in rib_vp_info_dict[asn]:
    #         for trace_ip in trace_vp_info_dict[asn]:
    #             # if rib_ip not in rib_ip_geo_dict.keys():
    #             #     print('h1')
    #             # if trace_ip not in trace_ip_geo_dict.keys():
    #             #     print('h2')
    #             dist = GetMinDist(rib_ip_geo_dict[rib_ip], trace_ip_geo_dict[trace_ip])
    #             if (dist < 10) or (rib_ip[:rib_ip.rindex('.')] == trace_ip[:trace_ip.rindex('.')]): #距离近或同一个/24子网
    #                 #print('here: %s %s' %(rib_ip, trace_ip))
    #                 #print(dist)
    #                 for c in v2c_dict[rib_ip]:
    #                     key = c + ' ' + trace_ip + ' ' + asn
    #                     if key not in adj_ips.keys():
    #                         adj_ips[key] = set()
    #                     adj_ips[key].add(rib_ip)
    #                     #adj_ips.add(rib_ip + ' ' + trace_ip + ' ' + asn)
    #                 #print(len(adj_ips))
    #             # print(dist)
    #             # print(v2c_dict[rib_ip])
    #             # print(rib_ip)
    #             # print(rib_ip_geo_dict[rib_ip])
    #             # print(trace_vp_dict[trace_ip])
    #             # print(trace_ip)
    #             # print(trace_ip_geo_dict[trace_ip])
    #             # print('')
    # for (key, val) in adj_ips.items():
    #     [c, trace_ip, asn] = key.split(' ')
    #     print(trace_vp_dict[trace_ip] + ' ' + trace_ip + ' ' + asn + ' ' + c)
    #     print('\t%s' %','.join(list(val)))
    # print(len(adj_ips))
    # CloseDb()

if __name__ == '__main__':
    #GetOneRibFromRib()
    #GetOneRibFromRRC()
    #GetAllVpsOfBgp()
    #GetAllVpsOfTraceroute()

    PreGetNonOverlayIpRanges()
    ConnectToIrrDb()
    ConstrIrrCache()
    #print(GetCountryFromIrrFiles('41.204.161.206'))
    GetAsOfRibVps() #不使用rib获取ip2as的mapping, 改为直接从irrdata中获取
    GetAsOfTraceVps() #不使用rib获取ip2as的mapping, 改为直接从irrdata中获取
    FindCommonVp()
    CloseIrrDb()

