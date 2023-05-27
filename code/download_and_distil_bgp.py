
import os
import requests
import json
import re

import global_var
from urllib.request import urlopen
#from get_as_path_of_spec_as_linux import get_update_as_path_of_spec_as
  
#http://routeviews.org/bgpdata/2019.01/UPDATES/updates.20190101.0000.bz2

wr_path = global_var.par_path + global_var.rib_dir
def check_collector_of_asn(asn): #, date_dir, date_name): #collector: "route-views.kixp"; date: "2019.01", filename: "rib.20190101.0000.bz2"
    #collectors = ["route-views.amsix/", "route-views.saopaulo/","route-views.sg/"]
    time_suffixes = ["0000.bz2", "0200.bz2", "0400.bz2", "0600.bz2", "0800.bz2", "1000.bz2", "1200.bz2", "1400.bz2", "1600.bz2", "1800.bz2", "2000.bz2", "2200.bz2"]
    failed_times = 0  
    req = requests.Session()
    year = 2019
    year_str = str(year)
    month = 4
    month_str = str(month).zfill(2)
    filename = 'test.bz2'
    collectors = ["", "route-views3/", "route-views4/", "route-views6/", "route-views.amsix/", "route-views.chicago/", "route-views.chile/", "route-views.eqix/", "route-views.flix/", "route-views.gorex/", "route-views.isc/", "route-views.kixp/", "route-views.jinx/", "route-views.linx/", "route-views.napafrica/", "route-views.nwax/", "route-views.phoix/", "route-views.telxatl/", "route-views.wide/", "route-views.sydney/", "route-views.saopaulo/", "route-views2.saopaulo/", "route-views.sg/", "route-views.perth/", "route-views.sfmix/", "route-views.soxrs/", "route-views.mwix/", "route-views.rio/", "route-views.fortaleza/", "route-views.gixa/"]    
    #collectors = ["route-views.sydney/"]
    for collector in collectors:
        for time_suffix in time_suffixes:
            url = 'http://routeviews.org/' + collector + 'bgpdata/' + year_str + '.' + month_str + '/RIBS/rib.' + year_str + month_str + '15.' + time_suffix
            #url: http://routeviews.org/route-views3/bgpdata/2019.03/RIBS/rib.20190301.0200.bz2
            print(url)
            resource = req.get(url, stream=True, timeout=60)        
            if resource:
                print("get ret: %d" %resource.status_code)
                if resource.status_code == 200:
                    wf = open(filename, "wb")
                    wf.write(resource.content)
                    wf.close()
                    cmd = "bgpscanner -a " + asn + " -L " + filename + " > temp_file2"
                    print(cmd)
                    os.system(cmd)                 
                    filesize = os.path.getsize('temp_file2')
                    if filesize > 0:
                        print("%s %d" %(collector, filesize))
                    break
            else:
                print("%s %s failed" %(collector, str(year) + month_str + '15.' + time_suffix))

def get_rib_from_rv(collector): #, date_dir, date_name): #collector: "route-views.kixp"; date: "2019.01", filename: "rib.20190101.0000.bz2"
    #collectors = ["route-views.amsix/", "route-views.saopaulo/","route-views.sg/"]
    time_suffixes = ["0000.bz2", "0200.bz2", "0400.bz2", "0600.bz2", "0800.bz2", "1000.bz2", "1200.bz2", "1400.bz2", "1600.bz2", "1800.bz2", "2000.bz2", "2200.bz2"]
    failed_times = 0  
    req = requests.Session()
    #wr_path += 'DataFromRv/'
    for year in range(2018, 2021):
    #for year in range(2018, 2019):
        year_str = str(year)
        for month in range(1, 13):
        #for month in range(4, 5):
            if (year == 2016 and month < 4) or (year == 2020 and month > 4): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
                continue
            month_str = str(month).zfill(2)
            for time_suffix in time_suffixes:
                url = 'http://routeviews.org/' + collector + 'bgpdata/' + str(year) + '.' + month_str + '/RIBS/rib.' + year_str + month_str + '15.' + time_suffix
                #url: http://routeviews.org/route-views3/bgpdata/2019.03/RIBS/rib.20190301.0200.bz2
                print(url)
                resource = req.get(url, stream=True, timeout=60)        
                if resource:
                    print("get ret: %d" %resource.status_code)
                    if resource.status_code == 200:
                        #for curline in resource.iter_lines():
                        #    get_as_path_of_as(asn, filename, wf)
                        if collector == '':
                            wf = open(wr_path + 'routeviews_' + str(year) + month_str + '15.' + time_suffix, "wb")
                        else:
                            wf = open(wr_path + collector.strip('/').replace('-', '') + '_' + str(year) + month_str + '15.' + time_suffix, "wb")
                        wf.write(resource.content)
                        wf.close()
                        break
                else:
                    print("%s %s failed" %(collector, str(year) + month_str + '15.' + time_suffix))
     
def get_rib_from_rrc(collector): #collector: "route-views.kixp"; date: "2019.01", filename: "rib.20190101.0000.bz2"
    #collectors = ["rrc00/", "route-views3/", "route-views4/", "route-views6/", "route-views.amsix/", "route-views.chicago/", "route-views.chile/", "route-views.eqix/", "route-views.flix/", "route-views.gorex/", "route-views.isc/", "route-views.kixp/", "route-views.jinx/", "route-views.linx/", "route-views.napafrica/", "route-views.nwax/", "route-views.phoix/", "route-views.telxatl/", "route-views.wide/", "route-views.sydney/", "route-views.saopaulo/", "route-views2.saopaulo/", "route-views.sg/", "route-views.perth/", "route-views.sfmix/", "route-views.soxrs/", "route-views.mwix/", "route-views.rio/", "route-views.fortaleza/", "route-views.gixa"]
    time_suffixes = ["0000.gz", "0800.gz", "1600.gz"]
    failed_times = 0  
    req = requests.Session()
    for year in range(2016, 2021):
    #for year in range(2018, 2019):
        year_str = str(year)
        for month in range(1, 13):
        #for month in range(4, 5):
            if (year == 2016 and month < 4) or (year == 2020 and month > 4): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
                continue
            month_str = str(month).zfill(2)
            for time_suffix in time_suffixes:
                url = "http://data.ris.ripe.net/" + collector + year_str + '.' + month_str + "/bview." + year_str + month_str + "15." + time_suffix
                #http://data.ris.ripe.net/rrc00/2019.03/bview.20190301.1600.gz
                print(url)
                requestOK = False
                try:
                    resource = req.get(url, stream=True, timeout=60)
                    if resource:
                        print("get ret: %d" %resource.status_code)
                        if resource.status_code == 200:
                            wf = open(wr_path + collector.strip('/') + '_' + str(year) + month_str + '15.' + time_suffix, "wb")
                            wf.write(resource.content)
                            wf.close()
                            break
                    else:
                        print("failed")
                        failed_times += 1
                except Exception as e:
                    print('Try to request again')
                    # 请求超时，再试几次
                    requestOK = False  # 标记
                    for i in range(2):
                        try:
                            resource = req.get(url, stream=True, timeout=60)
                            requestOK = True
                            if resource:
                                print("get ret: %d" %resource.status_code)
                                if resource.status_code == 200:
                                    wf = open(wr_path + collector.strip('/') + '_' + str(year) + month_str + '15.' + time_suffix, "wb")
                                    wf.write(resource.content)
                                    wf.close()
                                    break
                            else:
                                print("failed")
                                failed_times += 1
                            break
                        except Exception as e:
                            print('Try to request again')
                    # 请求三次还是不成功
                    if not requestOK:
                        print("************%s" %collector)   

def get_update_from_rv(collector): 
    ori_time_list = ["0000","0015","0030","0045","0100","0115","0130","0145","0200","0215","0230","0245","0300","0315","0330","0345","0400","0415","0430","0445","0500","0515","0530","0545","0600","0615","0630","0645","0700","0715","0730","0745","0800","0815","0830","0845","0900","0915","0930","0945","1000","1015","1030","1045","1100","1115","1130","1145","1200","1215","1230","1245","1300","1315","1330","1345","1400","1415","1430","1445","1500","1515","1530","1545","1600","1615","1630","1645","1700","1715","1730","1745","1800","1815","1830","1845","1900","1915","1930","1945","2000","2015","2030","2045","2100","2115","2130","2145","2200","2215","2230","2245","2300","2315","2330","2345"]
    failed_times = 0  
    fail_list = []
    #初始化fail_list
    #fail_list.append(ori_time_list[0])
    req = requests.Session()
    for year in range(2018, 2021):
    #for year in range(2019, 2020):
        year_str = str(year)
        for month in range(1, 13):
        #for month in range(4, 13):
            if (year == 2016 and month < 4) or (year == 2020 and month > 4): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
                continue
            month_str = str(month).zfill(2)
            #while len(fail_list) > 0:
            for cur_time in ori_time_list:
                #start_time = fail_list.pop(0)
                #time_list = ori_time_list[ori_time_list.index(start_time):]
                #if failed_times > 32:
                    #break
                #'http://routeviews.org/route-views.isc/bgpdata/2016.11/UPDATES/updates.20161101.1215.bz2'
                url = 'http://routeviews.org/' + collector + 'bgpdata/' + year_str + '.' + month_str + '/UPDATES/updates.' + year_str + month_str + '15.' + cur_time + ".bz2"
                print(url)
                requestOK = False
                for i in range(0, 3):
                    try:
                        resource = req.get(url, stream=True, timeout=60) 
                        if resource:
                            print("get ret: %d" %resource.status_code)
                            if resource.status_code == 200:
                                if collector == '':
                                    wf = open(wr_path + 'updates_routeviews_' + year_str + month_str + '15.' + cur_time + ".bz2", "wb")
                                else:
                                    wf = open(wr_path + 'updates_' + collector.strip('/').replace('-', '') + '_' + year_str + month_str + '15.'+ cur_time + ".bz2", "wb")
                                wf.write(resource.content)
                                wf.close()
                                requestOK = True
                                break
                    except Exception as e:
                        pass
                # 请求三次还是不成功
                if not requestOK:
                    print("************%s %s %s %s" %(collector, year_str, month_str, cur_time))     

def get_updates_from_rrc(collector):
    req = requests.Session()
    for year in range(2016, 2021):
    #for year in range(2018, 2019):
        year_str = str(year)
        for month in range(1, 13):
        #for month in range(4, 5):
            if (year == 2016 and month < 4) or (year == 2020 and month > 4): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
                continue
            month_str = str(month).zfill(2)
            for hour in range(0,24):
                str_hour = str(hour)
                if hour < 10:
                    str_hour = '0' + str_hour
                for minute in range (0, 60, 5):
                    str_minute = str(minute)
                    if minute < 10:
                        str_minute = '0' + str_minute
                    cur_time = str_hour + str_minute
                    url = "http://data.ris.ripe.net/" + collector + year_str + '.' + month_str + "/updates." + year_str + month_str + "15." + cur_time + ".gz"
                    #http://data.ris.ripe.net/rrc24/2019.03/updates.20190301.0240.gz
                    print(url)
                    requestOK = False
                    for i in range(0, 3):
                        resource = req.get(url, stream=True, timeout=60)
                        if resource:
                            print("get ret: %d" %resource.status_code)
                            if resource.status_code == 200:
                                wf = open(wr_path + 'updates_' + collector.strip('/') + '_' + str(year) + month_str + '15.' + cur_time + ".gz", 'wb')
                                wf.write(resource.content)
                                requestOK = True
                                break
                    # 请求三次还是不成功
                    if not requestOK:
                        print("************%s %s %s %s" %(collector, year_str, month_str, cur_time))     

def GetSpecificASDataFromLocal_Deleted(asn_list, path, collector):
    print(asn_list)
    #for root,dirs,files in os.walk(path):
        #for filename in files:
    for filename in os.listdir(path):
        #print(filename)
        if os.path.isfile(path + filename):
            if filename.__contains__(collector):
                print(filename) #'updates_rrc12_20180515.1250.gz', 'routeviews.isc_20170315.0000.bz2'
                for asn in asn_list:
                    cmd = "bgpscanner -a " + asn + " -L " + path + filename + " > temp_file"
                    #print(cmd)
                    os.system(cmd)                 
                    if os.path.getsize('temp_file') > 0:
                        tmp_name = filename[0:filename.rindex('.')]
                        w_filename = path + 'bgp_' + asn + '_' + tmp_name[tmp_name.rindex('_') + 1:tmp_name.rindex('.')]
                        cmd = "cat temp_file >> " + w_filename
                        print(cmd)
                        os.system(cmd) 
                        
def GetSpecificASDataFromLocal(asn, path, collector_list):
    #for root,dirs,files in os.walk(path):
        #for filename in files:
    wr_path = global_var.par_path + global_var.rib_dir + 'bgpdata/'
    for filename in os.listdir(path):
        #print(filename)
        if os.path.isfile(path + filename):
            contains_collector = False
            for collector in collector_list:
                if filename.__contains__(collector):
                    contains_collector = True
                    break
            if contains_collector:
                print(filename) #'updates_rrc12_20180515.1250.gz', 'routeviews.isc_20170315.0000.bz2'                
                cmd = "bgpscanner -a " + asn + " -L " + path + filename + " > temp_file"
                #print(cmd)
                os.system(cmd)                 
                if os.path.getsize('temp_file') > 0:
                    tmp_name = filename[0:filename.rindex('.')]
                    w_filename = wr_path + 'bgp_' + asn + '_' + tmp_name[tmp_name.rindex('_') + 1:tmp_name.rindex('.')]
                    cmd = "cat temp_file >> " + w_filename
                    print(cmd)
                    os.system(cmd) 

#这个函数是补丁函数，因为前期GetSpecificASDataFromLocal()命名方式不合理，后期不用这个函数
def CombineBgp(path):
    os.chdir(path)
    asn_list = ['7660', '7575', '34288']
    collector = dict()
    collector['7660'] = 'routeviews'
    collector['7575'] = 'routeviews.isc'
    collector['34288'] = 'rrc12'
    for asn in asn_list:
        #for year in range(2016,2022):
        for year in range(2016,2018):
            year_str = str(year)
            for month in range(1,13):
                month_str = str(month).zfill(2)
                cmd = 'cat ' + asn + '_' + collector[asn] + '_' + year_str + month_str + '* ' + \
                        asn + '_updates_' + collector[asn] + '_' + year_str + month_str + '* ' + \
                        '> bgp_' + asn + '_' + year_str + month_str + '15'
                print(cmd)
                os.system(cmd)

next_hop_dict = dict()
def GetAllNextHop(year_str, month_str, collector, src):   
    time_suffix = []
    url_pre = ''
    if src == 'rv':
        time_suffixes = ["0000.bz2", "0200.bz2", "0400.bz2", "0600.bz2", "0800.bz2", "1000.bz2", "1200.bz2", "1400.bz2", "1600.bz2", "1800.bz2", "2000.bz2", "2200.bz2"]
        url_pre = 'http://routeviews.org/' + collector + 'bgpdata/' + year_str + '.' + month_str + '/RIBS/rib.' + year_str + month_str + '15.'
    elif src == 'rrc':
        time_suffixes = ["0000.gz", "0800.gz", "1600.gz"]
        url_pre = "http://data.ris.ripe.net/" + collector + year_str + '.' + month_str + "/bview." + year_str + month_str + "15."
    failed_times = 0  
    req = requests.Session()
    for time_suffix in time_suffixes:
        url = url_pre + time_suffix
        #url: http://routeviews.org/route-views3/bgpdata/2019.03/RIBS/rib.20190301.0200.bz2
        print(url)
        resource = req.get(url, stream=True, timeout=60)        
        if resource:
            print("get ret: %d" %resource.status_code)
            if resource.status_code == 200:
                #for curline in resource.iter_lines():
                #    get_as_path_of_as(asn, filename, wf)
                filename = collector.strip('/') + '_' + year_str + month_str + '15.' + time_suffix
                print(filename)
                with open(filename, 'wb') as wf:
                    wf.write(resource.content)
                cmd = "bgpscanner -L %s > %s" %(filename, filename[:filename.rindex('.')])
                print(cmd)
                os.system(cmd)                 
                rf = open(filename[:filename.rindex('.')], 'r')
                curline = rf.readline()
                cur_set = set()
                while curline:
                    elems = curline.split('|')
                    if len(elems) > 5 and not elems[1].__contains__(':'):
                        cur = elems[3]
                        if cur in cur_set:
                            curline = rf.readline()
                            continue
                        cur_set.add(cur)
                        if cur not in next_hop_dict.keys():
                            next_hop_dict[cur] = []
                        next_hop_dict[cur].append(collector)
                    curline = rf.readline()
                rf.close()
                return
    print('download rib failed')

next_hop_as_dict = dict()
def GetAllNextHopAs(collector, src, req):   
    global next_hop_as_dict
    time_suffix = []
    url_pre = ''
    year_str = '2019'
    month_str = '01'
    if src == 'rv':
        time_suffixes = ["0000.bz2", "0200.bz2", "0400.bz2", "0600.bz2", "0800.bz2", "1000.bz2", "1200.bz2", "1400.bz2", "1600.bz2", "1800.bz2", "2000.bz2", "2200.bz2"]
        url_pre = 'http://routeviews.org/' + collector + 'bgpdata/' + year_str + '.' + month_str + '/RIBS/rib.' + year_str + month_str + '15.'
    elif src == 'rrc':
        time_suffixes = ["0000.gz", "0800.gz", "1600.gz"]
        url_pre = "http://data.ris.ripe.net/" + collector + year_str + '.' + month_str + "/bview." + year_str + month_str + "15."
    failed_times = 0
    for time_suffix in time_suffixes:
        url = url_pre + time_suffix
        #url: http://routeviews.org/route-views3/bgpdata/2019.03/RIBS/rib.20190301.0200.bz2
        print(url)
        resource = req.get(url, stream=True, timeout=60)        
        if resource:
            print("get ret: %d" %resource.status_code)
            if resource.status_code == 200:
                #for curline in resource.iter_lines():
                #    get_as_path_of_as(asn, filename, wf)
                #filename = collector.strip('/') + '_' + year_str + month_str + '15.' + time_suffix
                filename = 'tmp.' + time_suffix[time_suffix.index('.') + 1:]
                print(filename)
                with open(filename, 'wb') as wf:
                    wf.write(resource.content)
                cmd = "bgpscanner -L %s > %s" %(filename, filename[:filename.rindex('.')])
                print(cmd)
                os.system(cmd)                 
                rf = open(filename[:filename.rindex('.')], 'r')
                curline = rf.readline()
                while curline:
                    elems = curline.split('|')
                    if len(elems) > 5 and not elems[1].__contains__(':'):
                        as_path = elems[2]
                        nexthop_as = as_path.split(' ')[0]
                        if nexthop_as not in next_hop_as_dict.keys():
                            next_hop_as_dict[nexthop_as] = set()
                        next_hop_as_dict[nexthop_as].add(collector)
                    curline = rf.readline()
                rf.close()
                return
    print('download rib failed')

if __name__ == '__main__':
    cur_dir = global_var.par_path +  global_var.rib_dir + 'compress_files/'
    os.chdir(cur_dir)
    #print(cur_dir) 
    req = requests.Session()
    collectors = ["", "route-views3/", "route-views4/", "route-views6/", "route-views.amsix/", "route-views.chicago/", "route-views.chile/", "route-views.eqix/", "route-views.flix/", "route-views.gorex/", "route-views.isc/", "route-views.kixp/", "route-views.jinx/", "route-views.linx/", "route-views.napafrica/", "route-views.nwax/", "route-views.phoix/", "route-views.telxatl/", "route-views.wide/", "route-views.sydney/", "route-views.saopaulo/", "route-views2.saopaulo/", "route-views.sg/", "route-views.perth/", "route-views.sfmix/", "route-views.soxrs/", "route-views.mwix/", "route-views.rio/", "route-views.fortaleza/", "route-views.gixa/"]
    # collectors = ["route-views3/"]
    # collectors = ['', 'route-views.isc/']
    # collectors = ['rrc12/']
    for collector in collectors:
        #get_rib_from_rv(collector)
        #get_rib_from_rrc(collector)
        #get_update_from_rv(collector)
        #get_updates_from_rrc(collector)
        #GetAllNextHop('2018', '09', collector, 'rv')
        GetAllNextHopAs(collector, 'rv', req)
    for i in range(0,25):
        str_i = str(i)
        if i < 10:
            str_i = '0' + str_i
        collector = 'rrc' + str_i + '/'
        #GetAllNextHop('2018', '09', collector, 'rrc')
        GetAllNextHopAs(collector, 'rrc', req)
    with open(global_var.par_path + global_var.rib_dir + 'collector_next_hop_as', 'w') as wf:
        for (key, val) in next_hop_as_dict.items():
            wf.write("%s:%s\n" %(key, ','.join(list(val))))
            print("%s:%s" %(key, ','.join(list(val))))

    #path = wr_path
    #os.system("rm -f %s7660_*" %path)
    #GetSpecificASDataFromLocal(['7660'], path, 'routeviews_')
    #os.system("rm -f %sbgp_7575_*" %path)
    #GetSpecificASDataFromLocal(['7575'], path, 'routeviews.sydney_')
    #os.system("rm -f %sbgp_34288_*" %path)
    #GetSpecificASDataFromLocal(['34288'], path, 'rrc12_')    
    #path = global_var.par_path + global_var.rib_dir + 'compress_files/'
    #GetSpecificASDataFromLocal('6939', path, ['routeviews_', 'routeviews3_'])
    #GetSpecificASDataFromLocal('54728', path, ['routeviews_'])
    
    #CombineBgp(global_var.par_path + global_var.rib_dir)
    
    #check_collector_of_asn('20130')
