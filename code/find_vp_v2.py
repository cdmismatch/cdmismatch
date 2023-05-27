

from collections import defaultdict
import os
from posixpath import join
import requests
import json
import re
from bs4 import BeautifulSoup
from multiprocessing import Process, Queue, Pool
import numpy as np
import subprocess
import sqlite3
import dns.resolver, dns.reversename
import glob
import time

import global_var
from utils_v2 import GetPfx2ASByRv, GetAsListOfIpByRv, ConnectToDb, SetCurMidarTableDate, GetRouterOfIpByMi, GetGeoOfIpByMi, GeoDistance, IsIxpAs, \
                    ClearIxpAsSet, GetIxpAsSet

def CheckTraceFileNum(workdir):
    filenum_info = {}
    os.chdir(workdir)

    for root,dirs,files in os.walk('.'):
        for filename in files:
            if not filename.endswith('warts'):
                continue
            (vp, date, suffix) = filename.split('.')
            year_month = date[:6]
            if not year_month in filenum_info.keys():
                filenum_info[year_month] = set()
            filenum_info[year_month].add(vp)
    
    for year in range(2018, 2021):
        for month in range(1, 13):
            year_month = str(year) + str(month).zfill(2)
            if year_month not in filenum_info.keys():
                print(year_month + ':0')
            else:
                print(year_month + ':' + str(len(filenum_info[year_month])))

def ChgFilename(workdir):
    os.chdir(workdir)

    for root,dirs,files in os.walk('.'):
        for filename in files:
            if not filename.startswith('vpas_'):
                continue
            new_filename = ''
            if filename.endswith('route-views.s'): #修改一下错误
                new_filename = filename + 'g'
            if filename.endswith('route-views.napafric'):
                new_filename = filename + 'a'
            if filename.endswith('route-views.kix'):
                new_filename = filename + 'p'
            if filename.endswith('route-views.fortalez'):
                new_filename = filename + 'a'
            if filename.endswith('route-views.gix'):
                new_filename = filename + 'a'
            if new_filename:
                os.system('mv ' + filename + ' ' + new_filename)

def CheckBGPUndownload(workdir, collectors_filename):
    collectors = set()
    with open(collectors_filename, 'r') as rf:
        collectors = set(rf.read().split('\n'))

    filenum_info = {}
    os.chdir(workdir)
    for root,dirs,files in os.walk('.'):
        for filename in files:
            if not filename.startswith('vpas_'):
                continue
            (pref, date, collector) = filename.split('_')
            year_month = date[:6]
            if not collector in filenum_info.keys():
                filenum_info[collector] = set()
            # if vp == '':
            #     vp = 'rv2'
            filenum_info[collector].add(year_month)
    
    url_pre = 'http://routeviews.org/'
    req = requests.Session()
    #task_list = []
    paras = []
    for collector in collectors:
        url = ''    
        if collector == '':
            url = url_pre + 'bgpdata/'
        else:
            url = url_pre + collector + '/bgpdata/'
        try:
            r = req.get(url, stream=True)
            soup = BeautifulSoup(r.content.decode('utf-8'), 'html.parser')
            links = soup.find_all('a', href=True)            
            for link in links:
                content = link['href']
                if not (content.startswith('2018') or content.startswith('2019') or content.startswith('2020')):
                    continue
                date = content[:4] + content[5:7]
                if collector not in filenum_info.keys() or  date not in filenum_info[collector]:
                    #print(collector + ':' + date)                    
                    if len(task_list) >= 1:
                        for task in task_list:
                            task.join()
                        task_list.clear()
                    w_filename = date + '1500_' + collector + '.bz2'
                    if os.path.exists(w_filename) and os.path.getsize(w_filename):
                        continue
                    # task = Process(target=SubProc_Download, args=(url + date[:4] + '.' + date[4:6] + '/RIBS/rib.' + date + '15.0000.bz2', w_filename))
                    # task_list.append(task)
                    # task.start()
                    paras.append((url + date[:4] + '.' + date[4:6] + '/RIBS/rib.' + date + '15.0000.bz2', w_filename))
        except Exception as e:
            print(e)
            return
    pool = Pool(processes=80)
    pool.starmap(SubProc_Download, paras)
    pool.close()
    pool.join()

def GetVpsOfTraceroute():
    vp_info = {}
    os.chdir(global_var.all_trace_par_path + global_var.all_trace_download_dir + 'result/')
    
    for root,dirs,files in os.walk('.'):
        for filename in files:
            if (not filename.__contains__('20')) or filename.startswith('as_'):
                continue
            #print(filename)
            (vp, date) = filename.split('.')
            year_month = date[:6]
            day = date[6:]
            if (year_month in vp_info.keys()) and (vp in vp_info[year_month]) and \
                vp_info[year_month][vp][1] < int(day):
                continue
            with open(filename, 'r') as rf:
                data = rf.readline()
                while data:
                    if not data.startswith('T'):
                        data = rf.readline()
                        continue
                    elems = data.split('\t')
                    if year_month not in vp_info.keys():
                        vp_info[year_month] = {}
                    vp_info[year_month][vp] = [elems[1], int(day)]
                    break
    
    with open('traceroute_vps', 'w') as wf:
        for (year_month, info) in vp_info.items():
            wf.write(year_month + ':')
            for (vp, sub_info) in info.items():
                wf.write(vp + '_' + sub_info[0] + ';')
            wf.write('\n')
    print(len(vp_info.keys()))

def SubProc_Download(url, w_filename):
    # req = requests.Session()
    # print(url)
    # r = req.get(url, stream=True)    
    # if r and r.status_code == 200:
    #     with open(w_filename, 'wb') as wf:
    #         wf.write(r.content)
    try:
        print('wget -O ' + w_filename + ' ' + url)
        os.system('wget -O ' + w_filename + ' ' + url + ' --no-check-certificate')
    except Exception as e:
        print(url + 'error')
        print(e)

def MultiProc_Download_RRC(workdir):
    os.chdir(workdir)

    task_list = []
    for collector_id in range(0, 27):
        collector = 'rrc' + str(collector_id).zfill(2)
        for year in range(2018, 2021):
            year_str = str(year)
            for month in range(1, 13):
                month_str = str(month).zfill(2)
                url = 'https://data.ris.ripe.net/' + collector + '/' + year_str + '.' + month_str + '/bview.' + year_str + month_str + '15.0000.gz'
                if len(task_list) >= 128:
                    for task in task_list:
                        task.join()
                    task_list.clear()
                task = Process(target=SubProc_Download, args=(url, year_str + month_str + '1500_' + collector + '.gz'))
                task_list.append(task)
                task.start()

def MultiProc_Download_Isolario(workdir):
    os.chdir(workdir)

    task_list = []
    # for collector in ['Alderaan', 'Dagobah', 'Korriban', 'Naboo', 'Taris']:
    #     for year in range(2018, 2021):
    #         year_str = str(year)
    #         for month in range(1, 13):
    #             month_str = str(month).zfill(2)
    for elem in [['20190315', 'Dagobah'], ['20200315', 'Dagobah'], ['20200415', 'Dagobah'], ['20200515', 'Dagobah'], ['20200615', 'Dagobah'], ['20201215', 'Naboo'], ['20180415', 'Korriban'], ['20190815', 'Korriban'], ['20200315', 'Korriban'], ['20201115', 'Korriban'], ['20201215', 'Korriban'], ['20180115', 'Korriban'], ['20190715', 'Taris'], ['20190815', 'Taris'], ['20190915', 'Taris'], ['20191015', 'Taris'], ['20191115', 'Taris']]:
        [date, collector] = elem
        year_str = date[:4]
        month_str = date[4:6]
        if True:
            if True:
                url = 'https://www.isolario.it/Isolario_MRT_data/' + collector + '/' + year_str + '_' + month_str + '/rib.' + year_str + month_str + '15.0000.bz2'
                if len(task_list) >= 128:
                    for task in task_list:
                        task.join()
                    task_list.clear()
                task = Process(target=SubProc_Download, args=(url, year_str + month_str + '1500_' + collector + '.bz2'))
                task_list.append(task)
                task.start()

def MultiProc_Download_OneDate(collectors_filename, workdir, date):
    rv_collectors = None
    with open(collectors_filename, 'r') as rf:
        rv_collectors = set(rf.read().split('\n'))

    if not os.path.exists(workdir): os.mkdir(workdir)
    os.chdir(workdir)
    year = date[:4]
    month = date[4:6]
    task_list = []
    for collector_id in range(0, 27):
        collector = 'rrc' + str(collector_id).zfill(2)
        url = 'https://data.ris.ripe.net/' + collector + '/' + year + '.' + month + '/bview.' + date + '.0000.gz'
        task = Process(target=SubProc_Download, args=(url, date + '00_' + collector + '.gz'))
        if len(task_list) > 10:
            for t_task in task_list:
                t_task.join()
        task.start()
        task_list.append(task)
    # for collector in ['Alderaan', 'Dagobah', 'Korriban', 'Naboo', 'Taris']:
    #     url = 'https://www.isolario.it/Isolario_MRT_data/' + collector + '/' + year + '_' + month + '/rib.' + date + '.0000.bz2'
    #     task = Process(target=SubProc_Download, args=(url, date + '00_' + collector + '.bz2'))
    #     task_list.append(task)
    for collector in rv_collectors:
        url = ''    
        if collector == '':
            url = 'http://routeviews.org/' + 'bgpdata/' + year + '.' + month + '/RIBS/rib.' + date + '.0000.bz2'
        else:
            url = 'http://routeviews.org/' + collector + '/bgpdata/' + year + '.' + month + '/RIBS/rib.' + date + '.0000.bz2'
        task = Process(target=SubProc_Download, args=(url, date + '00_' + collector + '.bz2'))
        if len(task_list) > 10:
            for t_task in task_list:
                t_task.join()
        task.start()
        task_list.append(task)
    
    # for task in task_list:
    #     task.start()
    # for task in task_list:
    #     task.join()

        
# def MultiProc_Download_OneDate(collectors_filename, workdir, date):
#     rv_collectors = None
#     with open(collectors_filename, 'r') as rf:
#         rv_collectors = set(rf.read().split('\n'))

#     if not os.path.exists(workdir): os.mkdir(workdir)
#     os.chdir(workdir)
#     year = date[:4]
#     month = date[4:6]
#     day = date[6:]
#     task_list = []
#     for collector_id in range(0, 27):
#         collector = 'rrc' + str(collector_id).zfill(2)
#         for hour in range(0,24):
#             str_hour = str(hour).zfill(2)
#             for minute in range (0, 60, 5):
#                 str_minute = str(minute).zfill(2)
#                 cur_time = str_hour + str_minute
#                 url = 'http://data.ris.ripe.net/' + collector + '/' + year + '.' + month + "/updates." + year + month + day + '.' + cur_time + ".gz"
#                 w_filename = 'updates_' + collector + '_' + year + month + day + '.' + cur_time + ".gz"
#     # for collector in ['Alderaan', 'Dagobah', 'Korriban', 'Naboo', 'Taris']:
#     #     url = 'https://www.isolario.it/Isolario_MRT_data/' + collector + '/' + year + '_' + month + '/rib.' + date + '.0000.bz2'
#     #     task = Process(target=SubProc_Download, args=(url, date + '00_' + collector + '.bz2'))
#     #     task_list.append(task)
#     for collector in rv_collectors:
#         ori_time_list = ["0000","0015","0030","0045","0100","0115","0130","0145","0200","0215","0230","0245","0300","0315","0330","0345","0400","0415","0430","0445","0500","0515","0530","0545","0600","0615","0630","0645","0700","0715","0730","0745","0800","0815","0830","0845","0900","0915","0930","0945","1000","1015","1030","1045","1100","1115","1130","1145","1200","1215","1230","1245","1300","1315","1330","1345","1400","1415","1430","1445","1500","1515","1530","1545","1600","1615","1630","1645","1700","1715","1730","1745","1800","1815","1830","1845","1900","1915","1930","1945","2000","2015","2030","2045","2100","2115","2130","2145","2200","2215","2230","2245","2300","2315","2330","2345"]
#         for cur_time in ori_time_list:
#             url = ''
#             w_filename = ''
#             if collector == 'route-views2':
#                 url = 'http://routeviews.org/bgpdata/' + year + '.' + month + '/UPDATES/updates.' + year + month + day + '.' + cur_time + ".bz2"
#                 w_filename = 'updates_routeviews_' + year + month + day + '.' + cur_time + '.bz2'
#             else:
#                 url = 'http://routeviews.org/' + collector + '/bgpdata/' + year + '.' + month + '/UPDATES/updates.' + year + month + day + '.' + cur_time + ".bz2"
#                 w_filename = '/updates_' + collector.replace('-', '') + '_' + year + month + day + '.' + cur_time + '.bz2'
#             task = Process(target=SubProc_Download, args=(url, date + '00_' + collector + '.bz2'))
#             task_list.append(task)

def MultiProc_Scan_BGP(workdir, bgpdump_mode):
    os.chdir(workdir)
    #collector_vp_info = {'rrc03': ['80.249.208.34'], 'rrc11': ['198.32.160.61'], 'routeviews3': ['64.71.137.241'], 'routeviews.isc': ['198.32.176.177'], 'routeviews4': ['109.233.180.32'], 'routeviews': ['203.181.248.168']}
    collector_vp_info = {'rrc15': ['187.16.217.17'], 'rrc03': ['80.249.208.34'], 'rrc11': ['198.32.160.61'], 'routeviews3': ['64.71.137.241'], 'routeviews.sydney': ['45.127.172.46'], 'routeviews4': ['109.233.180.32'], 'routeviews': ['203.181.248.168'], 'routeviews.amsix': ['80.249.208.50']}
    task_list = []

    for root,dirs,files in os.walk('.'):
        for filename in files:
            if not filename.endswith('bz2') and not filename.endswith('gz'):
                continue
            #key_w = filename.split('.')[0].strip('updates_')
            # key_w = filename.split('.')[1]
            # print(key_w)
            # if key_w[:len('amsix_202202')] != 'amsix_202202':
            #     continue
            # if os.path.getsize(filename) == 0:
            #     os.remove(filename)
            #     continue
            if filename.startswith('updates_'):
                tmp_name = filename[len('updates_'):]
                collector = tmp_name[:tmp_name.index('_')]
            else:
                collector = filename[:filename.index('_')]
            for vp in collector_vp_info[collector]:
                #cmd = 'bgpscanner -s \"' + vp + '\" ' + filename + " > " + vp + date
                if bgpdump_mode == 'b':
                    cmd = "bgpdump -b " + filename + " " + vp
                elif bgpdump_mode == 'c':
                    w_filename = filename.replace(collector, vp)
                    # if os.path.exists(w_filename) and os.path.getsize(w_filename) > 0:
                    #     continue
                    cmd = "bgpdump -c " + filename + " " + vp + " > " + w_filename.strip('.bz2').strip('.gz')
                if len(task_list) >= 128:
                    for task in task_list:
                        task.join()
                    task_list.clear()
                task = Process(target=SubProc_SysCmd, args=(cmd, True))
                task_list.append(task)
                task.start()
    
    for task in task_list:
        task.join()

def SubProc_Scamper(taskQueue, placeholder):
    try:        
        while True:
            filename = taskQueue.get(True, 1) #任务完成后自己退出
            print("sc_analysis_dump %s" %filename)
            os.system("sc_analysis_dump %s > %s" %(filename, filename[:filename.rindex('.')]))
    except Exception as e:
        return

def MultiProc_Scamper(workdir):
    os.chdir(workdir)
    taskQueue = Queue()
    for root,dirs,files in os.walk('.'):
        for filename in files:
            taskQueue.put(filename)
    
    subprocs = []
    for i in range(0, 40):
        subprocs.append(Process(target=SubProc_Scamper, args=(taskQueue, True)))
    for subproc in subprocs:
        subproc.start()
    for subproc in subprocs:
        subproc.join()

def FindUndownloadedArkVP():
    url = 'https://publicdata.caida.org/datasets/topology/ark/ipv4/prefix-probing/'
    req = requests.Session()
    vp_info = {}
    # for year in range(2018, 2021):
    #     #print(year)
    #     for month in range(1, 13):
    #         #print(month)
    #         vp_info[str(year) + str(month).zfill(2)] = set()
    #         r = req.get(url + str(year) + '/' + str(month).zfill(2), stream=True)
    #         soup = BeautifulSoup(r.content.decode('utf-8'), 'html.parser')   
    #         for link in soup.find_all('a', href=True):
    #             filename = link['href'] 
    #             if not filename.__contains__('warts.gz'):
    #                 continue
    #             vp_info[str(year) + str(month).zfill(2)].add(filename.split('.')[0])
    with open('/home/slt/code/ana_c_d_incongruity/test2', 'r') as rf:
        for data in rf.readlines():
            (year_month, vps) = data.strip('\n').split(':')
            vp_info[year_month] = vps.split(';')
    
    os.chdir('/mountdisk3/traceroute_download_all/result/')
    for root,dirs,files in os.walk('.'):
        for filename in files:
            if filename.startswith('as_') or (not filename.__contains__('.20')):
                continue
            (vp, date) = filename.split('.')
            year_month = date[:6]
            if year_month == '201801' and vp == 'nrt-jp':
                print('debug')
            if (year_month in vp_info.keys()) and (vp in vp_info[year_month]):
                vp_info[year_month].remove(vp)
    
    # for (year_month, vps) in vp_info.items():
    #     if len(vps) == 0:
    #         continue
    #     print(year_month + ':' + ';'.join(list(vps)))
    
    task_list = []
    for (year_month, vps) in vp_info.items():
        if len(vps) == 0:
            continue
        year = year_month[:4]
        month = year_month[4:6]
        r = req.get(url + str(year) + '/' + str(month).zfill(2), stream=True)
        soup = BeautifulSoup(r.content.decode('utf-8'), 'html.parser')   
        for link in soup.find_all('a', href=True):
            filename = link['href']
            if not filename.__contains__('warts.gz'):
                continue
            (vp, date, nonce, suffix1, suffix2) = filename.split('.')
            if vp not in vps:
                continue
            day = int(date[6:8])
            if day >= 15 and day <= 20:
                w_filename = '/mountdisk3/traceroute_download_all/back/' + '.'.join([vp, date, suffix1, suffix2])
                if os.path.exists(w_filename) and os.path.getsize(w_filename) > 0:
                    continue
                if len(task_list) >= 40:
                    for task in task_list:
                        task.join()
                    task_list.clear()
                task = Process(target=SubProc_Download, args=(url + str(year) + '/' + str(month).zfill(2) + '/' + filename, w_filename))
                task_list.append(task)
                task.start()
                vps.remove(vp)

def DownloadedSpecArkVP():
    url = 'https://publicdata.caida.org/datasets/topology/ark/ipv4/prefix-probing/'
    req = requests.Session()
    
    #filenames = ['nrt-jp.20180215', 'nrt-jp.20180315', 'nrt-jp.20180615', 'nrt-jp.20180715', 'nrt-jp.20191215', 'nrt-jp.20200315', 'per-au.20200415', 'sjc2-us.20200515', 'sjc2-us.20200615', 'syd-au.20180615', 'syd-au.20180715', 'zrh2-ch.20190215', 'zrh2-ch.20190315', 'zrh2-ch.20190415', 'zrh2-ch.20200115', 'zrh2-ch.20200315']
    #filenames = ['ams-nl.20210109', 'nrt-jp.20210115', 'sjc2-us.20210116', 'syd-au.20210116', 'nrt-jp.20210216', 'sjc2-us.20210217', 'syd-au.20210217', 'nrt-jp.20210316', 'sjc2-us.20210322', 'syd-au.20210315', 'ams-nl.20210416', 'nrt-jp.20210415', 'syd-au.20210416']
    filenames = ['ams-nl.20210515','ams-nl.20210815','syd-au.20210515','syd-au.20210815','sao-br.20210115','sao-br.20210415','sao-br.20210515','sao-br.20210815','jfk-us.20200515']
    task_list = []
    for filename in filenames:
        (vp, date) = filename.split('.')
        year = date[:4]
        month = date[4:6]
        r = req.get(url + str(year) + '/' + str(month).zfill(2), stream=True)
        soup = BeautifulSoup(r.content.decode('utf-8'), 'html.parser')   
        for link in soup.find_all('a', href=True):
            url_filename = link['href']
            if not url_filename.__contains__(filename):
                continue
            w_filename = '/mountdisk3/traceroute_download_all/back/' + filename + '.warts.gz'
            task = Process(target=SubProc_Download, args=(url + str(year) + '/' + str(month).zfill(2) + '/' + url_filename, w_filename))
            task_list.append(task)
            task.start()
    for task in task_list:
        task.join()

def Temp_Rename(workdir):
    os.chdir(workdir)

    for root,dirs,files in os.walk('.'):
        for filename in files:
            if filename.endswith('.gz'):
                os.system('mv ' + filename + ' ' + filename[:filename.rindex('.')] + '.bz2')

def CheckUnknownVP(vp_filename, vp_as_filename):
    vps_info = {}
    with open(vp_filename, 'r') as rf:
        for data in rf.readlines():
            [date, vps] = data.split(':')
            for vp in vps.strip('\n').split(';'):
                if not vp:
                    continue
                n_vp = vp[:vp.index('_')]
                if n_vp not in vps_info.keys():
                    vps_info[n_vp] = []
                vps_info[n_vp].append(date)
    map_info = {}
    with open(vp_as_filename, 'r') as rf:
        for data in rf.readlines():
            [vp, asn] = data.strip('\n').split(':')
            map_info[vp] = asn
    unknown_vps_info = {}
    for (vp, dates) in vps_info.items():
        if vp not in map_info.keys():
            dates.sort()
            print(vp + ':' + ';'.join(dates))

def CheckASofUnknownVP(workdir, vp_filename):
    os.chdir(workdir)

    GetPfx2ASByRv(2019, 8)

    vps = []
    with open(vp_filename, 'r') as rf:
        vps = rf.read().strip('\n').split('\n')

    vp_info = {}
    for vp in vps:
        vp_info[vp] = set()
        for root,dirs,files in os.walk('.'):
            for filename in files:
                if not filename.startswith('as_' + vp):
                    continue
                #print(filename)
                with open(filename, 'r') as rf:
                    first_line = rf.readline()
                    if first_line:
                        ip = first_line.strip('\n').split('\t')[1]
                        asn = rf.readline().strip('\n').strip('\t').strip(' ').split(' ')[0]
                        info = ip + '_' + asn
                        if info not in vp_info[vp] and ('*' + info) not in vp_info[vp]:
                            if asn and asn != '*' and asn != '?':
                                vp_info[vp].add(info)
                            rv_asn = GetAsListOfIpByRv(ip)
                            if rv_asn and asn not in rv_asn:
                                new_info = ip + '_' + '_'.join(rv_asn)
                                if new_info not in vp_info[vp] and ('*' + new_info) not in vp_info[vp]:
                                    vp_info[vp].add('*' + new_info)
    
    for (vp, info) in vp_info.items():
        if len(info) == 0:
            print(vp + ': no traceroute files')
        elif len(info) > 1:
            print(vp + ':' + ';'.join(list(info)))

def GetASofVP(workdir, unmapped_vp_filename):
    os.chdir(workdir)
    discarded_vps = ['yyz-ca', 'arn-se']
        
    unmapped_vps = []
    with open(unmapped_vp_filename, 'r') as rf:
        unmapped_vps = rf.read().strip('\n').split('\n')

    vp_info = {}
    for vp in unmapped_vps:
        if vp in discarded_vps:
            continue
        done = False
        for root,dirs,files in os.walk('.'):
            for filename in files:
                if not filename.startswith('as_' + vp):
                    continue
                with open(filename, 'r') as rf:
                    first_line = rf.readline()
                    if first_line:
                        asn = rf.readline().strip('\n').strip('\t').strip(' ').split(' ')[0]
                        if asn and asn != '*' and asn != '?':
                            vp_info[vp] = asn
                            done = True
                if done:
                    break
    
    with open('rem_vp_as', 'w') as wf:
        for (vp, asn) in vp_info.items():
            print(vp + ':' + asn)
            wf.write(vp + ':' + asn + '\n')

def FindCommonASN(trace_vp_map_filename, trace_vp_filename, bgp_workdir):
    trace_vp_map = {}
    trace_asn_map = {}
    with open(trace_vp_map_filename, 'r') as rf:
        for data in rf.read().strip('\n').split('\n'):
            [vp, asn] = data.split(':')
            trace_vp_map[vp] = asn
            if asn not in trace_asn_map.keys():
                trace_asn_map[asn] = []
            trace_asn_map[asn].append(vp)
    
    trace_info = {}
    discarded_vps = ['yyz-ca', 'arn-se']
    with open(trace_vp_filename, 'r') as rf:
        for data in rf.read().strip('\n').split('\n'):
            [date, vps] = data.split(':')
            trace_info[date] = {}
            for vp in vps.split(';'):
                if not vp:
                    continue
                (t_vp, t_ip) = vp.split('_')
                if t_vp not in discarded_vps:
                    if t_vp not in trace_vp_map:
                        print('NOTE: %s not in trace vp!' %t_vp)
                    else:
                        asn = trace_vp_map[t_vp]
                        if asn not in trace_info[date].keys():
                            trace_info[date][asn] = set()
                        trace_info[date][asn].add(vp)
    
    bgp_info = {}
    os.chdir(bgp_workdir)
    for root,dirs,files in os.walk('.'):
        for filename in files:
            if not filename.startswith('vpas_'):
                continue
            [pref, p_date, collector] = filename.split('_')
            date = p_date[:6]
            if date not in bgp_info.keys():
                bgp_info[date] = {}
            if not collector:
                collector = 'route-views2'
            with open(filename, 'r') as rf:
                for data in rf.read().strip('\n').split('\n'):
                    if not data:
                        continue
                    ip = data[:data.rindex(':')]
                    asn = data[data.rindex(':') + 1:]
                    if ip.__contains__(':'): #ipv6暂时不分析
                        continue
                    if asn not in bgp_info[date].keys():
                        bgp_info[date][asn] = set()
                    bgp_info[date][asn].add(collector + '_' + ip)

    join_asn_info = {}
    for year in range(2018, 2021):
        for month in range(1, 13):
            if year == 2020 and month == 12:
                break
            date = str(year) + str(month).zfill(2)
    #         join_asns = set(trace_info[date].keys()) & set(bgp_info[date].keys())
    #         for asn in join_asns:
    #             if asn not in join_asn_info.keys():
    #                 join_asn_info[asn] = [set(), set(), set()]
    #             join_asn_info[asn][0].add(date)
    #             join_asn_info[asn][1].add(trace_info[date][asn])
    #             for elem in bgp_info[date][asn]:
    #                 join_asn_info[asn][2].add(elem)
    # with open('common_vp', 'w') as wf:
    # sort_list = sorted(join_asn_info.items(), key=lambda d:len(d[1][0]), reverse=True)
    #     for elem in sort_list:
    #         print('%s:%d:%s:%s' %(elem[0], len(elem[1][0]), ','.join(list(elem[1][1])), ','.join(list(elem[1][2]))))
    #         wf.write('%s:%d:%s:%s\n' %(elem[0], len(elem[1][0]), ','.join(list(elem[1][1])), ','.join(list(elem[1][2]))))
            join_asns = set(trace_info[date].keys()) & set(bgp_info[date].keys())
            if not join_asns:
                continue
            join_asn_info[date] = {}
            for asn in join_asns:
                join_asn_info[date][asn] = [dict(), dict()]
                for elem in trace_info[date][asn]:
                    (vp, ip) = elem.split('_')
                    join_asn_info[date][asn][0][ip] = vp
                for elem in bgp_info[date][asn]:
                    (collector, ip) = elem.split('_')
                    join_asn_info[date][asn][1][ip] = collector
    with open("common_asn", "w") as wf:
        json.dump(join_asn_info, wf, indent=1)  # 写为多行

def FindCommonVP(workdir):
    os.chdir(workdir)
    join_asn_info = None
    with open("common_asn", "r") as rf:
        join_asn_info = json.load(rf)
    join_vp_info = {}

    ConnectToDb()

    for (date, asn_info) in join_asn_info.items():
        SetCurMidarTableDate(int(date[:4]), int(date[4:6]))
        for (asn, vp_info_list) in asn_info.items():
            [trace_vp_info, bgp_vp_info] = vp_info_list
            trace_router_info = {}
            for ip in trace_vp_info.keys():
                router = GetRouterOfIpByMi(ip)
                if not router:
                    continue
                if router not in trace_router_info.keys():
                    trace_router_info[router] = set()
                trace_router_info[router].add(ip)
            bgp_router_info = {}
            for ip in bgp_vp_info.keys():
                router = GetRouterOfIpByMi(ip)
                if not router:
                    continue
                if router not in bgp_router_info.keys():
                    bgp_router_info[router] = set()
                bgp_router_info[router].add(ip)
            join_routers = set(trace_router_info.keys()) & set(bgp_router_info.keys())
            if join_routers:
                if date not in join_vp_info.keys():
                    join_vp_info[date] = {}
                join_vps = []
                for router in join_routers:
                    for ip in trace_router_info[router]:
                        join_vps.append(ip + '_' + trace_vp_info[ip])
                    for ip in bgp_router_info[router]:
                        join_vps.append(ip + '_' + bgp_vp_info[ip])
                    join_vp_info[date].append([asn, join_vps])
    
    for (date, info) in join_vp_info.items():
        print(date)
        for elem in info:
            [asn, join_vps] = elem
            print('\t' + asn + ':' + ','.join(join_vps))


def SubProc_SysCmd(cmd, placeholder=None):
# def SubProc_SysCmd(cmd):
    print(cmd)
    os.system(cmd)

def GetFullRibVPs(workdir):
    os.chdir(workdir)
    join_asn_info = None
    join_full_asn_info = {}
    with open("common_asn", "r") as rf:
        join_asn_info = json.load(rf)

    for (date, asn_info) in join_asn_info.items():
        year = date[:4]
        month = date[4:6]
        collectors = set()
        for (asn, vp_info_list) in asn_info.items():
            [trace_vp_info, bgp_vp_info] = vp_info_list
            for (ip, collector) in bgp_vp_info.items():
                collectors.add(collector)

        task_list = []
        for collector in collectors:
            url = None
            w_filename = None
            if collector.startswith('route-views'):
                if collector == 'route-views2':
                    url = 'http://routeviews.org/bgpdata/' + year + '.' + month + '/RIBS/rib.' + date + '15.0000.bz2'
                else:
                    url = 'http://routeviews.org/' + collector + '/bgpdata/' + year + '.' + month + '/RIBS/rib.' + date + '15.0000.bz2'
                w_filename = date + '1500_' + collector + '.bz2'
            elif collector.startswith('rrc'):
                url = 'https://data.ris.ripe.net/' + collector + '/' + year + '.' + month + '/bview.' + date + '15.0000.gz'
                w_filename = date + '1500_' + collector + '.gz'
            else: #isolario
                url = 'https://www.isolario.it/Isolario_MRT_data/' + collector + '/' + year + '_' + month + '/rib.' + date + '15.0000.bz2'
                w_filename = date + '1500_' + collector + '.bz2'
            if os.path.exists(w_filename) and os.path.getsize(w_filename) > 0:
                continue
            task = Process(target=SubProc_Download, args=(url, w_filename))
            task_list.append(task)
            task.start()
        for task in task_list:
            task.join()

        task_list = []
        for (asn, vp_info_list) in asn_info.items():
            [trace_vp_info, bgp_vp_info] = vp_info_list
            for (ip, collector) in bgp_vp_info.items():
                if collector.startswith('rrc'):
                    cmd = 'bgpscanner -i ' + ip + ' ' + date + '1500_' + collector + '.gz > bgp_' + ip + '_' + date
                else:
                    cmd = 'bgpscanner -i ' + ip + ' ' + date + '1500_' + collector + '.bz2 > bgp_' + ip + '_' + date
                task = Process(target=SubProc_SysCmd, args=(cmd, True))
                task_list.append(task)
                task.start()
        for task in task_list:
            task.join()
        
        for (asn, vp_info_list) in asn_info.items():
            [trace_vp_info, bgp_vp_info] = vp_info_list
            for (ip, collector) in bgp_vp_info.items():
                if os.path.exists('bgp_' + ip + '_' + date):
                    out = subprocess.getoutput('wc -l bgp_' + ip + '_' + date)
                    if int(out.split()[0]) > 250000:
                        if date not in join_full_asn_info.keys():
                            join_full_asn_info[date] = {}
                        if asn not in join_full_asn_info[date]:
                            join_full_asn_info[date][asn] = [trace_vp_info, {}]
                        join_full_asn_info[date][asn][1][ip] = collector

    with open("common_full_vp_asn", "w") as wf:
        json.dump(join_full_asn_info, wf, indent=1)  # 写为多行

def MultiProc_Download_SpecBGP():
    tracevp_collector_info = {'ams-nl':'rrc03', 'jfk-us':'rrc11', 'sjc2-us':'route-views3', 'syd-au':'route-views.isc', 'per-au':'route-views.isc', 'zrh2-ch':'route-views4', 'nrt-jp':'route-views2', 'sao-br':'rrc15', 'bcn-es':'rrc18', 'pna-es':'rrc18'}
    #tracevp_collector_info = {'nrt-jp':'routeviews2', 'ams-nl':'rrc03', 'jfk-us':'rrc11', 'sjc2-us':'routeviews3', 'syd-au':'routeviews.isc', 'per-au':'routeviews.isc', 'zrh2-ch':'routeviews4', 'sao-br':'rrc15', 'bcn-es':'rrc18', 'pna-es':'rrc18'}
    trace_dir = '/mountdisk1/ana_c_d_incongruity/traceroute_data/'
    bgp_dir = '/mountdisk1/ana_c_d_incongruity/rib_data/compress_files/'

    for (tracevp, collector) in tracevp_collector_info.items():
        task_list = []
        for year in range(2018, 2021):
            year_str = str(year)
            for month in range(1, 13):
                month_str = str(month).zfill(2)
                f = glob.glob(r'%s%s.%s%s*' %(trace_dir, tracevp, year_str, month_str))
                if not f:
                    continue
                day_str = f[0][-2:]
                if collector.startswith('route-views'):
                    if collector == 'route-views2':
                        url = 'http://routeviews.org/bgpdata/' + year_str + '.' + month_str + '/RIBS/rib.' + year_str + month_str + day_str + '.0000.bz2'
                        w_filename = bgp_dir + 'routeviews_' + year_str + month_str + day_str + '.0000' + '.bz2'
                    else:
                        url = 'http://routeviews.org/' + collector + '/bgpdata/' + year_str + '.' + month_str + '/RIBS/rib.' + year_str + month_str + day_str + '.0000.bz2'
                        w_filename = bgp_dir + collector.replace('-', '') + '_' + year_str + month_str + day_str + '.0000.bz2'
                elif collector.startswith('rrc'):
                    url = 'https://data.ris.ripe.net/' + collector + '/' + year_str + '.' + month_str + '/bview.' + year_str + month_str + day_str + '.0000.gz'
                    w_filename = bgp_dir + collector + '_' + year_str + month_str + day_str + '.0000.gz'
                if os.path.exists(w_filename) and os.path.getsize(w_filename) > 0:
                    continue
                task = Process(target=SubProc_Download, args=(url, w_filename))
                task_list.append(task)
                task.start()
        for task in task_list:
            task.join()
            
def MultiProc_Download_SpecBGP_For1103():
    dates = ['20190716', '20190815', '20190916', '20191015', '20191116', '20191216', '20200115', '20200216', '20200315', '20200416', '20200516', '20200615', '20200715', '20200816', '20200915', '20201015', '20201116', '20201216', '20210109', '20210416', '20210516', '20210615', '20210715', '20210816', '20210915', '20211115', '20220115', '20211015', '20211215', '20220215']
    bgp_dir = '/mountdisk1/ana_c_d_incongruity/rib_data/compress_files/temp_work6/dir_ribs/'
    task_list = []
    for date in dates:
        year_str = date[:4]
        month_str = date[4:6]
        day_str = date[6:8]
        url = 'http://routeviews.org/route-views.amsix/bgpdata/' + year_str + '.' + month_str + '/RIBS/rib.' + year_str + month_str + day_str + '.0000.bz2'
        w_filename = bgp_dir + 'routeviews.amsix' + '_' + year_str + month_str + day_str + '.0000.bz2'
        if os.path.exists(w_filename) and os.path.getsize(w_filename) > 0:
            continue
        task = Process(target=SubProc_Download, args=(url, w_filename))
        task_list.append(task)
        task.start()
    for task in task_list:
        task.join()

def MultiProc_Download_SpecBGP_2():
    tracevp_collector_info = {'ams-nl':'rrc03', 'jfk-us':'rrc11', 'sjc2-us':'route-views3', 'syd-au':'route-views.isc', 'per-au':'route-views.isc', 'zrh2-ch':'route-views4', 'nrt-jp':'route-views2', 'sao-br':'rrc15', 'bcn-es':'rrc18', 'pna-es':'rrc18'}
    trace_fns = glob.glob('/mountdisk1/ana_c_d_incongruity/traceroute_data/back/temp5/trace_*')
    filenames = []
    for trace_fn in trace_fns:
        tmp = trace_fn.split('_')[-1]
        vp, date = tmp.split('.')
        filenames.append(tracevp_collector_info[vp]+'_'+date)
    #filenames = ['rrc03_20210109', 'route-views2_20210115', 'route-views3_20210116', 'route-views.isc_20210116', 'route-views2_20210216', 'route-views3_20210217', 'route-views.isc_20210217', 'route-views2_20210316', 'route-views3_20210322', 'route-views.isc_20210315', 'rrc03_20210416', 'route-views2_20210415', 'route-views.isc_20210416']
    #filenames = ['route-views.sydney_20210116', 'route-views.sydney_20210217', 'route-views.sydney_20210315', 'route-views.sydney_20210416']
    bgp_dir = '/mountdisk1/ana_c_d_incongruity/rib_data/compress_files/temp_work5/dir_ribs/'
    
    task_list = []
    for filename in filenames:
        (collector, date) = filename.split('_')
        if collector != 'rrc03' or date[:6] != '202202':
            continue
        year_str = date[:4]
        month_str = date[4:6]
        day_str = date[6:8]
        if collector.startswith('route-views'):
            if collector == 'route-views2':
                url = 'http://routeviews.org/bgpdata/' + year_str + '.' + month_str + '/RIBS/rib.' + year_str + month_str + day_str + '.0000.bz2'
                w_filename = bgp_dir + 'routeviews_' + year_str + month_str + day_str + '.0000' + '.bz2'
            else:
                url = 'http://routeviews.org/' + collector + '/bgpdata/' + year_str + '.' + month_str + '/RIBS/rib.' + year_str + month_str + day_str + '.0000.bz2'
                w_filename = bgp_dir + collector.replace('-', '') + '_' + year_str + month_str + day_str + '.0000.bz2'
        elif collector.startswith('rrc'):
            url = 'https://data.ris.ripe.net/' + collector + '/' + year_str + '.' + month_str + '/bview.' + year_str + month_str + day_str + '.0000.gz'
            w_filename = bgp_dir + collector + '_' + year_str + month_str + day_str + '.0000.gz'
        task = Process(target=SubProc_Download, args=(url, w_filename))
        task_list.append(task)
        task.start()
    for task in task_list:
        task.join()

def SubProc_DownloadList(lists, placeholder):
    try:
        for elem in lists:
            [url, w_filename] = elem
            print('wget -O ' + w_filename + ' ' + url)
            os.system('wget -O ' + w_filename + ' ' + url + ' --no-check-certificate')
    except Exception as e:
        print(url + 'error')
        print(e)

def MultiProc_Download_SpecBGPUpdates():
    #tracevp_collector_info = {'ams-nl':'rrc03', 'jfk-us':'rrc11', 'sjc2-us':'route-views3', 'syd-au':'route-views.isc', 'per-au':'route-views.isc', 'zrh2-ch':'route-views4', 'nrt-jp':'route-views2', 'sao-br':'rrc15', 'bcn-es':'rrc18', 'pna-es':'rrc18'}
    tracevp_collector_info = {'syd-au':'route-views.isc'}
    trace_dir = '/mountdisk1/ana_c_d_incongruity/traceroute_data/'
    bgp_dir = '/mountdisk1/ana_c_d_incongruity/rib_data/compress_files/'

    for (tracevp, collector) in tracevp_collector_info.items():
        task_list = []
        #for year in range(2018, 2021):
        for year in range(2020, 2021):
            year_str = str(year)
            #for month in range(1, 13):
            for month in range(12, 13):
                month_str = str(month).zfill(2)
                f = glob.glob(r'%s%s.%s%s*' %(trace_dir, tracevp, year_str, month_str))
                if not f:
                    continue
                day_str = f[0][-2:]
                sub_task_list = []
                if collector.startswith('route-views'):
                    ori_time_list = ["0000","0015","0030","0045","0100","0115","0130","0145","0200","0215","0230","0245","0300","0315","0330","0345","0400","0415","0430","0445","0500","0515","0530","0545","0600","0615","0630","0645","0700","0715","0730","0745","0800","0815","0830","0845","0900","0915","0930","0945","1000","1015","1030","1045","1100","1115","1130","1145","1200","1215","1230","1245","1300","1315","1330","1345","1400","1415","1430","1445","1500","1515","1530","1545","1600","1615","1630","1645","1700","1715","1730","1745","1800","1815","1830","1845","1900","1915","1930","1945","2000","2015","2030","2045","2100","2115","2130","2145","2200","2215","2230","2245","2300","2315","2330","2345"]
                    for cur_time in ori_time_list:
                        url = ''
                        w_filename = ''
                        if collector == 'route-views2':
                            url = 'http://routeviews.org/bgpdata/' + year_str + '.' + month_str + '/UPDATES/updates.' + year_str + month_str + day_str + '.' + cur_time + ".bz2"
                            w_filename = bgp_dir + 'dir_updates_routeviews/updates_routeviews_' + year_str + month_str + day_str + '.' + cur_time + '.bz2'
                        else:
                            url = 'http://routeviews.org/' + collector + '/bgpdata/' + year_str + '.' + month_str + '/UPDATES/updates.' + year_str + month_str + day_str + '.' + cur_time + ".bz2"
                            w_filename = bgp_dir + 'dir_updates_' + collector.replace('-', '') + '/updates_' + collector.replace('-', '') + '_' + year_str + month_str + day_str + '.' + cur_time + '.bz2'
                        if os.path.exists(w_filename) and os.path.getsize(w_filename) > 0:
                            continue
                        sub_task_list.append([url, w_filename])
                elif collector.startswith('rrc'):
                    for hour in range(0,24):
                        str_hour = str(hour).zfill(2)
                        for minute in range (0, 60, 5):
                            str_minute = str(minute).zfill(2)
                            cur_time = str_hour + str_minute
                            url = 'http://data.ris.ripe.net/' + collector + '/' + year_str + '.' + month_str + "/updates." + year_str + month_str + day_str + '.' + cur_time + ".gz"
                            w_filename = bgp_dir + 'dir_updates_' + collector + '/updates_' + collector + '_' + year_str + month_str + day_str + '.' + cur_time + ".gz"
                            if os.path.exists(w_filename) and os.path.getsize(w_filename) > 0:
                                continue
                            sub_task_list.append([url, w_filename])
                task = Process(target=SubProc_DownloadList, args=(sub_task_list, True))
                task_list.append(task)
                task.start()
        for task in task_list:
            task.join()
            

def MultiProc_Download_SpecBGPUpdates_For1103(workdir):
    os.chdir(workdir)
    
    dates = [fn.split('_')[-1].split('.')[0] for fn in glob.glob('dir_ribs/*')] 
    paras = []       
    for date in dates:
        year_str = date[:4]
        month_str = date[4:6]
        day_str = date[6:8]
        task_list = []
        ori_time_list = ["0000","0015","0030","0045","0100","0115","0130","0145","0200","0215","0230","0245","0300","0315","0330","0345","0400","0415","0430","0445","0500","0515","0530","0545","0600","0615","0630","0645","0700","0715","0730","0745","0800","0815","0830","0845","0900","0915","0930","0945","1000","1015","1030","1045","1100","1115","1130","1145","1200","1215","1230","1245","1300","1315","1330","1345","1400","1415","1430","1445","1500","1515","1530","1545","1600","1615","1630","1645","1700","1715","1730","1745","1800","1815","1830","1845","1900","1915","1930","1945","2000","2015","2030","2045","2100","2115","2130","2145","2200","2215","2230","2245","2300","2315","2330","2345"]
        for cur_time in ori_time_list:
            url = 'http://routeviews.org/route-views.amsix/bgpdata/' + year_str + '.' + month_str + '/UPDATES/updates.' + year_str + month_str + day_str + '.' + cur_time + ".bz2"
            w_filename = 'dir_updates/updates_routeviews.amsix_' + year_str + month_str + day_str + '.' + cur_time + '.bz2'
            # if os.path.exists(w_filename) and os.path.getsize(w_filename) > 0:
            #     continue
            paras.append((url, w_filename))
        #     task = Process(target=SubProc_Download, args=(url, w_filename))
        #     task_list.append(task)
        #     task.start()
        # for task in task_list:
        #     task.join()
    pool = Pool(processes=80)
    pool.starmap(SubProc_Download, paras)
    pool.close()
    pool.join()

def MultiProc_Download_SpecBGPUpdates_2(workdir):
    os.chdir(workdir)
    
    filename_list = []
    for root,dirs,files in os.walk('dir_ribs/'):
        for filename in files:
            if filename.endswith('bz2') or filename.endswith('gz'):
            #if filename.__contains__('rrc'):
                filename_list.append(filename)
        
    for filename in filename_list:
        [collector, date] = filename.split('_')
        if collector != 'rrc03' or date[:6] != '202202':
            continue
        collector = collector.replace('routeviews', 'route-views')
        year_str = date[:4]
        month_str = date[4:6]
        day_str = date[6:8]
        #task_list = []
        paras = []
        if collector.startswith('route-views'):
            ori_time_list = ["0000","0015","0030","0045","0100","0115","0130","0145","0200","0215","0230","0245","0300","0315","0330","0345","0400","0415","0430","0445","0500","0515","0530","0545","0600","0615","0630","0645","0700","0715","0730","0745","0800","0815","0830","0845","0900","0915","0930","0945","1000","1015","1030","1045","1100","1115","1130","1145","1200","1215","1230","1245","1300","1315","1330","1345","1400","1415","1430","1445","1500","1515","1530","1545","1600","1615","1630","1645","1700","1715","1730","1745","1800","1815","1830","1845","1900","1915","1930","1945","2000","2015","2030","2045","2100","2115","2130","2145","2200","2215","2230","2245","2300","2315","2330","2345"]
            for cur_time in ori_time_list:
                url = ''
                w_filename = ''
                if collector == 'route-views':
                    url = 'http://routeviews.org/bgpdata/' + year_str + '.' + month_str + '/UPDATES/updates.' + year_str + month_str + day_str + '.' + cur_time + ".bz2"
                    w_filename = 'dir_updates_routeviews/updates_routeviews_' + year_str + month_str + day_str + '.' + cur_time + '.bz2'
                else:
                    url = 'http://routeviews.org/' + collector + '/bgpdata/' + year_str + '.' + month_str + '/UPDATES/updates.' + year_str + month_str + day_str + '.' + cur_time + ".bz2"
                    w_filename = 'dir_updates_' + collector.replace('-', '') + '/updates_' + collector.replace('-', '') + '_' + year_str + month_str + day_str + '.' + cur_time + '.bz2'
                if os.path.exists(w_filename) and os.path.getsize(w_filename) > 0:
                    continue
                # task = Process(target=SubProc_Download, args=(url, w_filename))
                # task_list.append(task)
                # task.start()
                paras.append((url, w_filename))
        elif collector.startswith('rrc'):
            for hour in range(0,24):
                str_hour = str(hour).zfill(2)
                for minute in range (0, 60, 5):
                    str_minute = str(minute).zfill(2)
                    cur_time = str_hour + str_minute
                    url = 'http://data.ris.ripe.net/' + collector + '/' + year_str + '.' + month_str + "/updates." + year_str + month_str + day_str + '.' + cur_time + ".gz"
                    w_filename = 'dir_updates_' + collector + '/updates_' + collector + '_' + year_str + month_str + day_str + '.' + cur_time + ".gz"
                    if os.path.exists(w_filename) and os.path.getsize(w_filename) > 0:
                        continue
                    # task = Process(target=SubProc_Download, args=(url, w_filename))
                    # task_list.append(task)
                    # task.start()
                    paras.append((url, w_filename))
        # for task in task_list:
        #     task.join()
        #     return
        
    pool = Pool(processes=80)
    pool.starmap(SubProc_Download, paras)
    pool.close()
    pool.join()

def GetVPGeo(workdir):
    os.chdir(workdir)
    join_asn_info = None
    with open("common_full_vp_asn", "r") as rf:
        join_asn_info = json.load(rf)

    ConnectToDb()

    for (date, asn_info) in join_asn_info.items():
        print(date)
        SetCurMidarTableDate(int(date[:4]), int(date[4:6]))
        for (asn, vp_info_list) in asn_info.items():
            print('\t' + asn + '\n\t\t', end ='')
            [trace_vp_info, bgp_vp_info] = vp_info_list
            for (ip, vp) in trace_vp_info.items():
                geo = GetGeoOfIpByMi(ip)
                if not geo:
                    geo = 'None'
                print(ip + ':' + vp + ':' + geo + ';', end = '')
                join_asn_info[date][asn][0][ip] = [vp, geo]
            print('\n\t\t', end ='')
            for (ip, collector) in bgp_vp_info.items():
                geo = GetGeoOfIpByMi(ip)
                if not geo:
                    geo = 'None'
                print(ip + ':' + collector + ':' + geo + ';', end = '')
                join_asn_info[date][asn][1][ip] = [collector, geo]
            print('')
    with open("vp_geo", "w") as wf:
        json.dump(join_asn_info, wf, indent=1)  # 写为多行

def GetNearVPs(workdir):
    os.chdir(workdir)
    join_asn_info = None
    with open("vp_geo", "r") as rf:
        join_asn_info = json.load(rf)

    near_vp_info = {}
    for (date, asn_info) in join_asn_info.items():
        for (asn, vp_info_list) in asn_info.items():
            [trace_vp_info, bgp_vp_info] = vp_info_list
            for (trace_ip, vp_geo) in trace_vp_info.items():
                [vp, trace_geo] = vp_geo
                for (bgp_ip, collector_geo) in bgp_vp_info.items():
                    [collector, bgp_geo] = collector_geo
                    if trace_geo != 'None' and bgp_geo != 'None' and GeoDistance(trace_geo, bgp_geo) < 100: #距离小于100公里
                        if date not in near_vp_info.keys():
                            near_vp_info[date] = {}
                        if asn not in near_vp_info[date].keys():
                            near_vp_info[date][asn] = {}
                        if trace_ip not in near_vp_info[date][asn].keys():
                            near_vp_info[date][asn][trace_ip] = [vp, trace_geo, []]
                        near_vp_info[date][asn][trace_ip][2].append([bgp_ip, collector, bgp_geo])
        
    with open("near_vp", "w") as wf:
        json.dump(near_vp_info, wf, indent=1)  # 写为多行
       
def StatNearVPs(workdir):
    os.chdir(workdir) 
    near_vp_info = None
    with open("near_vp", "r") as rf:
        near_vp_info = json.load(rf)
    
    stat_asn = {}
    for (date, asn_info) in near_vp_info.items():
        count = len(asn_info)
        for (asn, vp_info_list) in asn_info.items():
            if asn == '27678':
                count -= 1
                continue
            if asn not in stat_asn.keys():
                stat_asn[asn] = 0
            stat_asn[asn] += 1
        print(date + ':%d' %count)
    sort_list = sorted(stat_asn.items(), key=lambda d:d[1], reverse=True)
    for elem in sort_list:
        print(elem[0] + ':%d' %elem[1])

def StatPrefixInBGP(filename):
    prefixes = set()
    with open(filename, 'r') as rf:
        curline = rf.readline()
        while curline:
            elems = curline.split('|')
            if len(elems) > 2:
                prefixes.add(elems[1])
            curline = rf.readline()
    print(len(prefixes))

def FilterIXPAS(path_list):
    new_list = []
    for elem in path_list:
        if not IsIxpAs(elem):
            new_list.append(elem)
    return new_list    

def CompressBGPPath(path, ixp_asns = None): #在里面过滤掉IXP AS
    path_list = []
    pre_elem = ''
    for elem in path.split(' '):
        if elem != pre_elem:# and not IsIxpAs(elem): #20230405 不过滤IXP AS，先看看什么效果
            path_list.append(elem)
            pre_elem = elem
    if ixp_asns:
        return ' '.join(FilterIXPAS(path_list))
    else:
        return ' '.join(path_list)

def Tmp():
    prefix_info = {}
    with open('/mountdisk1/ana_c_d_incongruity/rib_data/all_collectors_one_date_bgpdata/2022010100_rrc18', 'r') as rf:
        lines = rf.readlines(100000000)
        while lines:
            for line in lines:
                elems = line.split('|')
                if len(elems) >= 7:
                    prefix = elems[5]
                    if not prefix.__contains__(':'): #not ipv6 prefix
                        path = elems[6]
                        if len(path.split(' ')) > 1:
                            if prefix not in prefix_info.keys():
                                prefix_info[prefix] = set()
                            new_path = CompressBGPPath(path)
                            prefix_info[prefix].add(new_path)
            lines = rf.readlines(100000000)
    
    cur_db = sqlite3.connect('/mountdisk4/bgpdata/bgp.db')
    if not cur_db:
        print("ConnectToDb bgp failed!")
    cur_db_cursor = cur_db.cursor()
    
    #创建表
    for i in range(0, 256):
        cur_db_cursor.execute('CREATE TABLE IF NOT EXISTS bgp_seg_info_%d(prefix TEXT, fst_asn TEXT, snd_asn TEXT, path_val TEXT)' %i)
        cur_db_cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS index_prefix_seg ON bgp_seg_info_%d (prefix, fst_asn, snd_asn)' %i)
    
    print('begin to insert table')
    count = 0
    for (prefix, paths) in prefix_info.items():
        elems = prefix.split('.')
        index = int(elems[1])
        for path in paths:
            elems = path.split(' ')
            pre_asn = elems[0]
            last_asn = elems[-1]
            for cur_asn in elems[1:]:
                select_sql = "SELECT path_val FROM bgp_seg_info_%d WHERE prefix='%s' AND fst_asn='%s' AND snd_asn='%s'" %(index, prefix, pre_asn, cur_asn)
                cur_db_cursor.execute(select_sql)
                result = cur_db_cursor.fetchall()
                if result:
                    cur_path = result[0][0]
                    if cur_path != 'Dup' and cur_path != path:
                        print('diff path: %s, %s' %(cur_path, path))
                        update_sql = "UPDATE bgp_seg_info_%d SET path_val='Dup' WHERE prefix='%s' AND fst_asn='%s' AND snd_asn='%s'" %(index, prefix, pre_asn, cur_asn)
                        cur_db_cursor.execute(update_sql)
                else:
                    insert_sql = "INSERT INTO bgp_seg_info_%d VALUES('%s','%s','%s','%s')" %(index, prefix, pre_asn, cur_asn, path)
                    cur_db_cursor.execute(insert_sql)
                pre_asn = cur_asn
                if cur_asn == last_asn:
                    break
                path = path[path.index(' ') + 1:]
            count += 1
            if count % 10000 == 0:
                print(count)
                cur_db.commit()    
    cur_db_cursor.close()
    cur_db.close()

def Tmp2(filename):
    wf = open(filename + '_', 'w')
    with open(filename, 'r') as rf:
        curline = rf.readline()
        while curline:
            elems = curline.split('|')
            wf.write(elems[-3] + '|' + elems[-2] + '\n')
            curline = rf.readline()
    wf.close()

def ReverseDns(ip):
    try:
        addrs = dns.reversename.from_address(ip)
        return str(dns.resolver.resolve(addrs,"PTR")[0])
    except Exception as e:
        return str(e)

def GetDnsofVP(workdir):
    os.chdir(workdir)
    join_asn_info = None
    with open("common_asn", "r") as rf:
        join_asn_info = json.load(rf)
    dns_info = {}
    for (date, asn_info) in join_asn_info.items():
        for (asn, vp_info_list) in asn_info.items():
            if asn not in dns_info.keys():
                dns_info[asn] = [{}, {}]
            [trace_vp_info, bgp_vp_info] = vp_info_list
            for (trace_ip, vp_name) in trace_vp_info.items():
                if trace_ip not in dns_info[asn][0].keys():
                    dns_info[asn][0][trace_ip] = [ReverseDns(trace_ip), vp_name]
            for (bgp_ip, collector_name) in bgp_vp_info.items():
                if bgp_ip not in dns_info[asn][1].keys():
                    dns_info[asn][1][bgp_ip] = [ReverseDns(bgp_ip), collector_name]
    with open('common_asn_dns', 'w') as wf:
        json.dump(dns_info, wf, indent=1)  # 写为多行

def Tmp_CopyFile():
    vps = ['jfk-us', 'sao-br', 'bcn-es', 'pna-es']
    dst_dir = '/mountdisk1/ana_c_d_incongruity/traceroute_data/'
    os.chdir('/mountdisk3/traceroute_download_all/result/')
    for vp in vps:
        for year in range(2018, 2021):
            for month in range(1, 13):            
                filename = vp + '.' + str(year) + str(month).zfill(2) + '15'
                if not os.path.exists(filename):
                    filename = vp + '.' + str(year) + str(month).zfill(2) + '16'
                    if not os.path.exists(filename):
                        filename = vp + '.' + str(year) + str(month).zfill(2) + '14'
                        if not os.path.exists(filename):
                            filename = vp + '.' + str(year) + str(month).zfill(2) + '17'
                            if not os.path.exists(filename):
                                filename = vp + '.' + str(year) + str(month).zfill(2) + '18'
                                if not os.path.exists(filename):
                                    #print(filename + ' not exist')
                                    continue
                os.system('cp ' + filename + ' ' + dst_dir)

def CombineUpdateFiles(workdir):
    os.chdir(workdir)
    #for collector in ['routeviews4/', 'rrc03/', 'rrc11/', 'routeviews3/', 'routeviews.isc/', 'routeviews/']:
    for collector in ['routeviews3/', 'routeviews4/', 'routeviews.isc/', 'routeviews/']:
        cur_dir = 'dir_updates_' + collector
        os.chdir(cur_dir)
        date_files_info = {}
        for root,dirs,files in os.walk('.'):
            for filename in files:
                if filename.endswith('bz2') or filename.endswith('gz'):
                    continue
                elems = filename.split('_')
                year_month = elems[2][:6]
                if year_month == '202012' and collector == 'routeviews.isc/':
                    continue
                if year_month not in date_files_info.keys():
                    date_files_info[year_month] = []
                date_files_info[year_month].append(filename)

        for year in range(2018, 2021):
            for month in range(1, 13):
                year_month = str(year) + str(month).zfill(2)
                combine_info = set()
                if year_month not in date_files_info.keys():
                    continue
                for filename in date_files_info[year_month]:
                    with open(filename, 'r') as rf:
                        print(filename)
                        curline = rf.readline()
                        if curline:
                            curline = rf.readline()
                            if curline:
                                curline = rf.readline() #前两行没用，bgpdump debug时的输出
                                while curline:
                                    elems = curline.split('|')
                                    if len(elems) < 5:
                                        curline = rf.readline()
                                        continue
                                    data = elems[-3] + '|' + elems[-2]
                                    combine_info.add(data)
                                    curline = rf.readline()
                w_filename = 'comb_' + filename[:filename.rindex('.')]
                with open(w_filename, 'w') as wf:
                    print(w_filename)
                    wf.write('\n'.join(list(combine_info)))
        os.chdir('../')

def CombineUpdateFiles_2(workdir): #routeviews.isc/中202012中有15,16两个日期，上一个函数CombineUpdateFiles()只处理一个月有一天的情况
    os.chdir(workdir)
    print(workdir)
    #for collector in ['routeviews4/', 'rrc03/', 'rrc11/', 'routeviews3/', 'routeviews.isc/', 'routeviews/']:
    #for collector in ['rrc03/', 'routeviews3/', 'routeviews.sydney/', 'routeviews/']:
    #for cur_dir in glob.glob('./dir_updates_*'):
    for cur_dir in ['./dir_updates/']: #for 1103 only
        #print(cur_dir)
        os.chdir(cur_dir)
        date_files_info = {}
        #year_month = '202012'
        for root,dirs,files in os.walk('.'):
            for filename in files:
                if filename.startswith('comb') or filename.endswith('bz2') or filename.endswith('gz'):
                    continue
                # if not filename.__contains__(year_month):
                #     continue
                elems = filename.split('_')
                date = elems[2][:8]
                if date not in date_files_info.keys():
                    date_files_info[date] = []
                date_files_info[date].append(filename)
        
        for date in date_files_info.keys():
            combine_info = set()
            for filename in date_files_info[date]:
                with open(filename, 'r') as rf:
                    print(filename)
                    curline = rf.readline()
                    if curline:
                        curline = rf.readline()
                        if curline:
                            curline = rf.readline() #前两行没用，bgpdump debug时的输出
                            while curline:
                                elems = curline.split('|')
                                if len(elems) < 5:
                                    curline = rf.readline()
                                    continue
                                data = elems[-3] + '|' + elems[-2]
                                combine_info.add(data)
                                curline = rf.readline()
            w_filename = 'comb_' + filename[:filename.rindex('.')]
            with open(w_filename, 'w') as wf:
                print(w_filename)
                wf.write('\n'.join(list(combine_info)))
        os.chdir('..')

def Tmp4():
    for collector in ['rrc03/', 'rrc11/']:
        cur_dir = '/mountdisk1/ana_c_d_incongruity/rib_data/compress_files/dir_updates_' + collector
        os.chdir(cur_dir)
        for root,dirs,files in os.walk('.'):
            for filename in files:
                if filename.endswith('g'):
                    os.rename(filename, filename[:filename.rindex('.')])

#if has loop, return None
def CompressBGPPathListAndTestLoop(path_list):
    comp_path_list = []
    prev_elem = ''
    for elem in path_list:
        if elem != prev_elem:
            if elem in comp_path_list: #loop
                return None
            comp_path_list.append(elem)
            prev_elem = elem
    return comp_path_list

def RefineBGP_Sub(filename, wf, wf_discard):
    set_num = 0
    priv_num = 0
    loop_num = 0
    total_num = 0
    print(filename)
    with open(filename, 'r') as rf:
        curlines = rf.readlines(100000)
        while curlines:
            print(total_num)
            total_num += len(curlines)
            ref_data = []
            for curline in curlines:
                if curline.__contains__('{'): #has set
                    wf_discard.write(curline)
                    set_num += 1
                    continue
                if curline.__contains__('103.213.240.0/24|7575 1221 4637 9498 58717 58689 135311'):
                    print('here')
                elems = curline.split('|')
                path_list = elems[1].strip('\n').split(' ')
                has_private_asn = False
                for private_asn in range(64512, 65536):
                    if str(private_asn) in path_list:
                        has_private_asn = True
                        break
                if has_private_asn:
                    wf_discard.write(curline)
                    priv_num += 1
                    continue
                comp_path_list = CompressBGPPathListAndTestLoop(path_list)
                if not comp_path_list: #has loop
                    wf_discard.write(curline)
                    loop_num += 1
                    continue
                ref_data.append(elems[0] + '|' + ' '.join(comp_path_list))
            wf.write('\n'.join(ref_data) + '\n')
            curlines = rf.readlines(100000)
    return (set_num, priv_num, loop_num, total_num)

def RefineBGP(rib_filename, update_filename):
    wf = open('bgp_' + rib_filename.split('/')[-1], 'w')
    wf_discard = open('discard/discard_' + rib_filename.split('/')[-1], 'w')
    (set_num1, priv_num1, loop_num1, total_num1) = RefineBGP_Sub(rib_filename, wf, wf_discard)
    set_num2, priv_num2, loop_num2, total_num2 = 0, 0, 0, 0
    if update_filename:
        (set_num2, priv_num2, loop_num2, total_num2) = RefineBGP_Sub(update_filename, wf, wf_discard)
    wf.close()
    wf_discard.close()
    total_num = total_num1 + total_num2
    # with open('/mountdisk1/ana_c_d_incongruity/rib_data/bgpdata/stat/stat_' + rib_filename.split('/')[-1], 'w') as wf_stat:
    #     wf_stat.write("%.2f, %.2f, %.2f" %((set_num1 + set_num2) / total_num, (priv_num1 + priv_num2) / total_num, (loop_num1 + loop_num2) / total_num))
    print("%.2f, %.2f, %.2f" %((set_num1 + set_num2) / total_num, (priv_num1 + priv_num2) / total_num, (loop_num1 + loop_num2) / total_num))

def RefineBGP_All(workdir):
    os.chdir(workdir)
    #vp_collector_info = {'80.249.208.34':'rrc03', '198.32.160.61':'rrc11', '64.71.137.241':'routeviews3', '198.32.176.177':'routeviews.isc', '109.233.180.32':'routeviews4', '203.181.248.168':'routeviews'}
    vp_collector_info = {'187.16.217.17': 'rrc15', '80.249.208.34':'rrc03', '80.249.208.50':'routeviews.amsix', '198.32.160.61':'rrc11', '64.71.137.241':'routeviews3', '45.127.172.46':'routeviews.sydney', '109.233.180.32':'routeviews4', '203.181.248.168':'routeviews'}
    
    rib_filename_info = {}
    for root,dirs,files in os.walk('dir_ribs/'):
        for filename in files:
            if filename.endswith('bz2') or filename.endswith('gz'):
                continue
            [vp, date] = filename.split('_')
            if vp != '80.249.208.34' or date[:6] != '202202':
                continue
            if vp not in rib_filename_info.keys():
                rib_filename_info[vp] = []
            rib_filename_info[vp].append(filename)
    
    task_list = []
    for (vp, rib_filenames) in rib_filename_info.items():
        update_dir = 'dir_updates_' + vp_collector_info[vp] + '/'
        #update_dir = 'dir_updates/' # for 1103 only
        for rib_filename in rib_filenames:
            task = Process(target=RefineBGP, args=('dir_ribs/' + rib_filename, update_dir + 'comb_updates_' + rib_filename))
            task_list.append(task)
            task.start()
    
    for task in task_list:
        task.join()

def TestLoopInTrace(ip_list):
    for i in range(1, len(ip_list)):
        if ip_list[i] == '*':
            continue
        if ip_list[i] in ip_list[:i - 1]:
            return i
    return -1

def DetectDuplicateAddrInTrace(ip_list):
    for i in range(1, len(ip_list)):
        if ip_list[i] == '*':
            continue
        if ip_list[i] == ip_list[i - 1]:
            return 1
    return 0

def RefineTrace(filename, w_filename):
    stat_info = {'U': 0, 'L': 0, 'L1': 0, 'G': 0, 'M': 0, 'C': 0, 'R': 0, 'V': 0, 'A': 0}
    #U: icmp_unreachable
    #L: loop_detected (discard trace)
    #L1: same ip in adjacent hop (discard trace)
    #G: gap_detected
    #M: multiple reply in one hop (discard trace)
    #C: complete path (with no '*')
    #R: dst reply path
    #V: usable path (except discarded traces)
    #print(filename)
    last_two_hop_same_num = 0
    abnormal_num = 0
    wf = open(w_filename, 'w')
    wf_multireply = open(w_filename + '_multireply', 'w')
    wf_sc_loop = open(w_filename + '_sc_loop', 'w')
    wf_my_loop = open(w_filename + '_my_loop', 'w')
    with open(filename, 'r', encoding='unicode_escape') as rf:
        curlines = rf.readlines(100000)  
        while curlines:
            #print(stat_info['A'])
            stat_info['A'] += len(curlines)
            for curline in curlines:
                if not curline.startswith('T'):
                    stat_info['A'] -= 1
                    continue                
                elems = curline.strip('\n').split('\t')
                (dst_ip, dst_reply_flag, halt_reason, path_complete_flag, hops) = (elems[2], elems[6], elems[10], elems[12], elems[13:])
                if halt_reason != 'S': #The reason, if any, why incremental probing stopped
                    stat_info[halt_reason] += 1
                    if halt_reason == 'L': #loop直接丢弃不用
                        wf_sc_loop.write(curline)
                        continue
                #print(dst_ip)
                #进一步检查
                multi_reply = False
                ip_list = []
                for hop in hops:
                    if hop.__contains__(';'): #multi replies on a hop
                        multi_reply = True
                        break
                    if hop == 'q':
                        ip_list.append('*')
                    else:
                        ip_list.append(hop.split(',')[0])
                if multi_reply:
                    stat_info['M'] += 1 #丢弃不用
                    wf_multireply.write(curline)
                    continue
                if dst_reply_flag == 'R': #dst replies, add dst to the last hop
                    ip_list.append(dst_ip)
                loop_pos = TestLoopInTrace(ip_list)
                if loop_pos > 0: #add loop case, discard
                    stat_info['L1'] += 1
                    # if loop_pos == len(ip_list) - 1: #最后出现循环
                    #     last_two_hop_same_num += 1
                    # if dst_reply_flag == 'R': #有这种情况吗?
                    #     #print(curline)
                    #     abnormal_num += 1
                    wf_my_loop.write(curline)
                    continue
                # if DetectDuplicateAddrInTrace(ip_list):
                #     last_two_hop_same_num += 1 #借用一下变量
                #     if dst_reply_flag == 'R':
                #         #print(curline)
                #         abnormal_num += 1
                #使用该trace，统计其它数据
                stat_info['V'] += 1
                if dst_reply_flag == 'R':
                    stat_info['R'] += 1
                if path_complete_flag == 'C':
                    stat_info['C'] += 1
                wf.write(dst_ip + ':' + ','.join(ip_list) + '\n')
            curlines = rf.readlines(100000)
    wf.close()
    wf_multireply.close()
    wf_sc_loop.close()
    wf_my_loop.close()
    # if stat_info['L1'] > 0:
    #     print(filename + ':' + str(stat_info['L1']) + ', ', end='')
    #     #print("%.2f" %(last_two_hop_same_num / stat_info['L1']))
    #     print(str(last_two_hop_same_num) + ', ', end='')
    #     print(abnormal_num)

    #print(str(abnormal_num) + ',' + str(last_two_hop_same_num))
    print(filename + ':' + str(stat_info))
    
    # if stat_info['A'] == 0:
    #     print('NOTE: no trace. %s' %filename)
    # else:
    #     print(stat_info['V'] / stat_info['A'])

def ResolveTrace_All(workdir):
    os.chdir(workdir)
    os.system('rm -f trace_*')
    filename_list = []
    for root,dirs,files in os.walk('.'):
        for filename in files:
            if filename == 'process_trace.sh' or filename.startswith('trace_') or filename.endswith('warts'):
                continue
            filename_list.append(filename)
    #RefineTrace('syd-au.20180115', 'test')
    task_list = []
    for filename in filename_list:
        task = Process(target=RefineTrace, args=(filename, 'trace_' + filename))
        task_list.append(task)
        task.start()   
    for task in task_list:
        task.join()

def TestBGPTraceDateConform():
    tracevp_bgpvp_info = {'ams-nl': '80.249.208.34', 'jfk-us': '198.32.160.61', 'sjc2-us': '64.71.137.241', 'syd-au': '198.32.176.177', 'per-au': '198.32.176.177', 'zrh2-ch': '109.233.180.32', 'nrt-jp': '203.181.248.168'}
    trace_dir = '/mountdisk1/ana_c_d_incongruity/traceroute_data/'
    bgp_dir = '/mountdisk1/ana_c_d_incongruity/rib_data/bgpdata/'

    trace_vp_date_info = {}
    for filename in os.listdir(trace_dir):
        if not filename.startswith('trace_'):
            continue
        [trace_vp, date] = filename[len('trace_'):].split('.')
        if trace_vp not in trace_vp_date_info.keys():
            trace_vp_date_info[trace_vp] = []
        trace_vp_date_info[trace_vp].append(date)
    bgp_vp_date_info = {}
    for filename in os.listdir(bgp_dir):
        if not filename.startswith('bgp_'):
            continue
        [bgp_vp, date] = filename[len('bgp_'):].split('_')
        if bgp_vp not in bgp_vp_date_info.keys():
            bgp_vp_date_info[bgp_vp] = []
        bgp_vp_date_info[bgp_vp].append(date)
    for elem in tracevp_bgpvp_info.items():
        [trace_vp, bgp_vp] = elem
        if trace_vp not in trace_vp_date_info.keys():
            print('Undone trace_vp: %s' %trace_vp)
            continue
        if bgp_vp not in bgp_vp_date_info.keys():
            print('Undone bgp_vp: %s' %bgp_vp)
            continue
        if trace_vp_date_info[trace_vp].sort() != bgp_vp_date_info[bgp_vp].sort():
            print('trace_vp: ' + ','.join(trace_vp_date_info[trace_vp].sort()))
            print('bgp_vp: ' + ','.join(bgp_vp_date_info[bgp_vp].sort()))

def DebugGrepInUpdates(vp, date, key):
    tracevp_collector_info = {'ams-nl':'rrc03', 'jfk-us':'rrc11', 'sjc2-us':'route-views3', 'syd-au':'route-views.isc', 'per-au':'route-views.isc', 'zrh2-ch':'route-views4', 'nrt-jp':'route-views2', 'sao-br':'rrc15', 'bcn-es':'rrc18', 'pna-es':'rrc18'}
    os.chdir('/mountdisk1/ana_c_d_incongruity/rib_data/compress_files/dir_updates_' + tracevp_collector_info[vp].replace('-', '').strip('2') + '/')
    for filename in os.listdir('.'):
        if filename.__contains__(date) and filename.endswith('bz2'):
            os.system('bgpdump -M ' + filename + ' > test')
            os.system('grep \"' + key + '\" test')

def Stat_BGP_Ab():
    os.chdir('/mountdisk1/ana_c_d_incongruity/rib_data/compress_files/dir_ribs/')
    info = defaultdict(lambda: defaultdict(list))
    for filename in os.listdir('.'):
        if filename.endswith('bz2') or filename.endswith('gz'):
            continue
        print(filename)
        (vp, date) = filename.split('_')
        GetIxpAsSet(date)
        set_num = 0
        priv_num = 0
        loop_num = 0
        prepend_num = 0
        total_num = 0
        ixp_num = 0
        with open(filename, 'r') as rf:
            curlines = rf.readlines()
            total_num += len(curlines)           
            for curline in curlines:
                if curline.__contains__('{'): #has set
                    set_num += 1
                    continue
                elems = curline.split('|')
                path_list = elems[1].strip('\n').split(' ')
                has_private_asn = False
                for private_asn in range(64512, 65536):
                    if str(private_asn) in path_list:
                        has_private_asn = True
                        break
                if has_private_asn:
                    priv_num += 1
                    continue
                comp_path_list = CompressBGPPathListAndTestLoop(path_list)
                if not comp_path_list: #has loop
                    loop_num += 1
                    continue
                if len(comp_path_list) < len(path_list):
                    prepend_num += 1
                for elem in comp_path_list:
                    if IsIxpAs(elem):
                        ixp_num += 1
        ClearIxpAsSet()
        info[vp][date] = [set_num / total_num, priv_num / total_num, loop_num / total_num, prepend_num / total_num, ixp_num / total_num]
        print('{}.{}:{}'.format(vp, date, info[vp][date]))

def FindUnsamperedWarts():
    taskQueue = Queue()
    vps = ['sjc2-us','pry-za','bcn-es','ams-nl','syd-au','sao-br','jfk-us']
    for vp in vps:
        filenames = glob.glob('/mountdisk3/traceroute_download_all/back/%s*.warts' %vp)
        for filename in filenames:
            key = filename.split('/')[-1][:-6]
            if not os.path.exists('/mountdisk3/traceroute_download_all/result/%s' %key):
                taskQueue.put(filename)
    subprocs = []
    for i in range(0, 40):
        subprocs.append(Process(target=SubProc_Scamper, args=(taskQueue, True)))
    for subproc in subprocs:
        subproc.start()
    for subproc in subprocs:
        subproc.join()

def union_for1103():
    fns = glob.glob('/mountdisk1/ana_c_d_incongruity/rib_data/compress_files/temp_work6/bgp_80.249.208.50_*')
    for fn in fns:
        date = fn.split('_')[-1]
        fn1 = '/mountdisk1/ana_c_d_incongruity/rib_data/bgpdata/bgp_80.249.208.34_%s' %date
        os.system('cp %s /mountdisk1/ana_c_d_incongruity/rib_data/bgpdata/back_bgp_80.249.208.34_%s' %(fn1, date))
        data = defaultdict(set)
        with open(fn1, 'r') as rf1:
            for line in rf1:
                pref, path = line.strip('\n').split('|')
                data[pref].add(path)
        with open(fn, 'r') as rf:
            for line in rf:
                pref, path = line.strip('\n').split('|')
                data[pref].add(path)
        with open(fn1, 'w') as wf:
            for pref, paths in data.items():
                for path in paths:
                    wf.write('%s|%s\n' %(pref, path))    

def union_for1103_2():
    fn1 = '/mountdisk1/ana_c_d_incongruity/rib_data/compress_files/temp_work6/bgp_80.249.208.50_20220215'
    fn2 = '/mountdisk1/ana_c_d_incongruity/rib_data/bgpdata/back_bgp_80.249.208.34_20220215'
    data = defaultdict(set)
    for fn in [fn1, fn2]:
        with open(fn, 'r') as rf:
            for line in rf:
                pref, path = line.strip('\n').split('|')
                data[pref].add(path)
    with open('/mountdisk1/ana_c_d_incongruity/rib_data/bgpdata/bgp_80.249.208.34_20220215', 'w') as wf:
        for pref, paths in data.items():
            for path in paths:
                wf.write('%s|%s\n' %(pref, path))
                    
if __name__ == '__main__':
    #union_for1103_2()
    #union_for1103()
    #Stat_BGP_Ab()
    #DebugGrepInUpdates('nrt-jp', '20180815', '220.67.153')
    os.chdir('/mountdisk1/ana_c_d_incongruity/rib_data/compress_files/dir_ribs/')
    fns = glob.glob('80.249.208.34_20*')
    paras = []
    for fn in fns:
        if fn < '80.249.208.34_20190715':
            continue
        paras.append((fn, ''))
    pool = Pool(processes=10)
    pool.starmap(RefineBGP, paras)
    pool.close()
    pool.join()
    # wf = open('test', 'w')
    # wf_discard = open('test1', 'w')
    # RefineBGP_Sub('/mountdisk1/ana_c_d_incongruity/rib_data/compress_files/dir_updates_routeviews.isc/comb_updates_198.32.176.177_20190215', wf, wf_discard)
    # os.chdir('/mountdisk1/ana_c_d_incongruity/traceroute_data/back/')
    # RefineTrace('syd-au.20180710', 'trace_syd-au.20180710')
    # RefineTrace('syd-au.20190112', 'trace_syd-au.20190112')
    #TestBGPTraceDateConform()
    # ResolveTrace_All('/mountdisk1/ana_c_d_incongruity/traceroute_data/back/temp2/')
    #ResolveTrace_All('/mountdisk3/traceroute_download_all/202204/')
    #RefineBGP_All('/mountdisk1/ana_c_d_incongruity/rib_data/compress_files/temp_work5/')
    #Tmp4()
    #CombineUpdateFiles('/mountdisk1/ana_c_d_incongruity/rib_data/compress_files/temp_work1/')
    #CombineUpdateFiles_2('/mountdisk1/ana_c_d_incongruity/rib_data/compress_files/temp_work6/')
    #MultiProc_Scan_BGP('/mountdisk1/ana_c_d_incongruity/rib_data/compress_files/dir_ribs/', 'b')
    #MultiProc_Scan_BGP('/mountdisk1/ana_c_d_incongruity/rib_data/compress_files/temp_work5/dir_ribs/', 'b')
    #for collector in ['rrc03/', 'rrc11/', 'routeviews3/', 'routeviews.isc/', 'routeviews4/', 'routeviews/']:
    # for fn in glob.glob('/mountdisk1/ana_c_d_incongruity/rib_data/compress_files/temp_work5/dir_updates_*'):
    # for fn in glob.glob('/mountdisk1/ana_c_d_incongruity/rib_data/compress_files/temp_work6/dir_updates/'):
    #     MultiProc_Scan_BGP(fn, 'c')
    #MultiProc_Download_SpecBGPUpdates()
    #MultiProc_Download_SpecBGPUpdates_2('/mountdisk1/ana_c_d_incongruity/rib_data/compress_files/temp_work5/')
    #MultiProc_Download_SpecBGPUpdates_For1103('/mountdisk1/ana_c_d_incongruity/rib_data/compress_files/temp_work6/')
    #MultiProc_Download_SpecBGP()
    #MultiProc_Download_SpecBGP_2()
    #MultiProc_Download_SpecBGP_For1103()
    #Tmp_CopyFile()
    #GetDnsofVP('/mountdisk1/ana_c_d_incongruity/rib_data/all_collectors_vp_as/')
    #Tmp2('/mountdisk1/ana_c_d_incongruity/rib_data/all_collectors_one_date_bgpdata/test2')
    #Tmp()
    #ChgFilename('/mountdisk1/ana_c_d_incongruity/rib_data/all_collectors_vp_as/temp/')
    #CheckBGPUndownload('/mountdisk1/ana_c_d_incongruity/rib_data/all_collectors_vp_as', '/home/slt/code/ana_c_d_incongruity/rv_collectors.dat')
    #GetVpsOfTraceroute()
    #CheckFileNum(global_var.all_trace_par_path + global_var.all_trace_download_dir + 'back/')
    #FindUndownloadedArkVP()
    #DownloadedSpecArkVP()
    #MultiProc_Scamper(global_var.all_trace_par_path + global_var.all_trace_download_dir + 'back/temp2/')
    #FindUnsamperedWarts() #类似MultiProc_Scamper()的功能
    #MultiProc_Download_RRC('/mountdisk1/ana_c_d_incongruity/rib_data/all_collectors_vp_as/')
    #MultiProc_Download_Isolario('/mountdisk1/ana_c_d_incongruity/rib_data/all_collectors_vp_as/')
    #Temp_Rename('/mountdisk1/ana_c_d_incongruity/rib_data/all_collectors_vp_as/')
    #CheckUnknownVP('/mountdisk3/traceroute_download_all/result/traceroute_vps', '/mountdisk3/traceroute_download_all/result/vp_as_map')
    #CheckASofUnknownVP('/mountdisk3/traceroute_download_all/result/', 'unmapped_vps')
    #GetASofVP('/mountdisk3/traceroute_download_all/result/', 'unmapped_vps')
    #FindCommonASN('/mountdisk3/traceroute_download_all/result/vp_as_map_whole', '/mountdisk3/traceroute_download_all/result/traceroute_vps', '/mountdisk1/ana_c_d_incongruity/rib_data/all_collectors_vp_as/')
    ##FindCommonVP('/mountdisk1/ana_c_d_incongruity/rib_data/all_collectors_vp_as/')
    #GetFullRibVPs('/mountdisk1/ana_c_d_incongruity/rib_data/all_collectors_vp_as/')
    #GetVPGeo('/mountdisk1/ana_c_d_incongruity/rib_data/all_collectors_vp_as/')
    #GetNearVPs('/mountdisk1/ana_c_d_incongruity/rib_data/all_collectors_vp_as/')
    #StatNearVPs('/mountdisk1/ana_c_d_incongruity/rib_data/all_collectors_vp_as/')
    #MultiProc_Download_OneDate('/home/slt/code/ana_c_d_incongruity/rv_collectors.dat', '/mountdisk1/ana_c_d_incongruity/rib_data/all_collectors_one_date_201902_bgpdata/', '2019', '02')
    
    #date = '20200714'
    #MultiProc_Download_OneDate('/home/slt/code/ana_c_d_incongruity/rv_collectors.dat', '/mountdisk1/ana_c_d_incongruity/rib_data/all_collectors_one_date_bgpdata/' + date + '/', date)
    #MultiProc_Scan_BGP('/mountdisk1/ana_c_d_incongruity/rib_data/all_collectors_one_date_bgpdata/')

    #StatPrefixInBGP('/mountdisk1/ana_c_d_incongruity/rib_data/all_collectors_one_date_bgpdata/route-views2')
    pass
