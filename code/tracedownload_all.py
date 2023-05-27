import os, sys, requests
from bs4 import BeautifulSoup
import multiprocessing as mp

import global_var

g_req = requests.Session()
def DownloadTracerouteOneMonth(year, month, record):
    global g_req
    url = 'http://data.caida.org/datasets/topology/ark/ipv4/prefix-probing/' + str(year) + '/' + str(month).zfill(2) + '/'
    try:
        if not os.path.exists(str(year) + '/' + str(month).zfill(2)):
            os.makedirs(str(year) + '/' + str(month).zfill(2) + '/')
        os.chdir(str(year) + '/' + str(month).zfill(2))
        r = g_req.get(url, stream=True)
        #print(r.content)
        soup = BeautifulSoup(r.content.decode('utf-8'), 'html.parser')
        for link in soup.find_all('a', href=True):
            filename = link['href']
            if not filename.__contains__('warts.gz'):
                continue
            elems = filename.split('.')
            del elems[2]
            w_filename = '.'.join(elems)
            tmp_filename = '.'.join(elems[:-1])
            if (os.path.exists(w_filename) and os.path.getsize(w_filename) > 0) or \
                (os.path.exists(tmp_filename) and os.path.getsize(tmp_filename) > 0):
                continue
            print(filename)
            urlf = url + filename
            succeed = False
            for i in range(0,3):
                resource_2 = g_req.get(urlf, stream=True, timeout=60) 
                if resource_2:
                    if resource_2.status_code == 200:
                        with open(w_filename, 'wb') as wf:
                            wf.write(resource_2.content)
                        succeed = True
                        print("%s filesize: %d" %(filename, os.path.getsize(w_filename)))
                        break
            if not succeed:
                print(filename + ' failed')
                record.write(filename + '\n')
        record.close()
        print(str(year) + ' ' + str(month) + ' done')
        os.chdir('../..')
    except Exception as e:
        os.chdir('../..')
        print(e)

def DownloadTracerouteOneMonth_2(year, month, vp):
    global g_req
    url = 'http://data.caida.org/datasets/topology/ark/ipv4/prefix-probing/' + str(year) + '/' + str(month).zfill(2) + '/'
    try:
        if not os.path.exists(str(year) + '/' + str(month).zfill(2)):
            os.makedirs(str(year) + '/' + str(month).zfill(2) + '/')
        os.chdir(str(year) + '/' + str(month).zfill(2))
        r = g_req.get(url, stream=True)
        #print(r.content)
        soup = BeautifulSoup(r.content.decode('utf-8'), 'html.parser')
        for link in soup.find_all('a', href=True):
            filename = link['href']
            if not filename.__contains__('warts.gz'):
                continue
            if not filename.startswith(vp):
                continue
            elems = filename.split('.')
            del elems[2]
            w_filename = '.'.join(elems)
            tmp_filename = '.'.join(elems[:-1])
            if (os.path.exists(w_filename) and os.path.getsize(w_filename) > 0) or \
                (os.path.exists(tmp_filename) and os.path.getsize(tmp_filename) > 0):
                continue
            print(filename)
            urlf = url + filename
            succeed = False
            for i in range(0,3):
                resource_2 = g_req.get(urlf, stream=True, timeout=60) 
                if resource_2:
                    if resource_2.status_code == 200:
                        with open(w_filename, 'wb') as wf:
                            wf.write(resource_2.content)
                        succeed = True
                        print("%s filesize: %d" %(filename, os.path.getsize(w_filename)))
                        break
            if not succeed:
                print(filename + ' failed')
                #record.write(filename + '\n')
        #record.close()
        print(str(year) + ' ' + str(month) + ' done')
        os.chdir('../..')
    except Exception as e:
        os.chdir('../..')
        print(e)

def DownloadTracerouteForSpecDays(year, month):
    global g_req
    url = 'http://data.caida.org/datasets/topology/ark/ipv4/prefix-probing/' + str(year) + '/' + str(month).zfill(2) + '/'
    try:
        r = g_req.get(url, stream=True)
        #print(r.content)
        soup = BeautifulSoup(r.content.decode('utf-8'), 'html.parser')
        filenames_list = []
        for link in soup.find_all('a', href=True):
            filename = link['href']
            if filename.__contains__('warts.gz'):
                filenames_list.append(filename)
        s_filenames_list = sorted(filenames_list)
        filename_dict = dict()
        for filename in s_filenames_list:
            #zrh-ch.20190102.1546394400.warts.gz
            elems = filename.split('.')
            vp = elems[0]
            date = elems[1] #20190102
            if vp not in filename_dict.keys():
                filename_dict[vp] = []
            if int(date[6:]) >= 15 and len(filename_dict[vp]) < 1: #3:
                filename_dict[vp].append(filename)
        #print(len(filename_dict.keys()))
        for (key, val) in filename_dict.items():
            for filename in val:                
                elems = filename.split('.')
                del elems[2]
                w_filename = '.'.join(elems)
                tmp_filename = '.'.join(elems[:-1])
                if (os.path.exists(w_filename) and os.path.getsize(w_filename) > 0) or \
                    (os.path.exists(tmp_filename) and os.path.getsize(tmp_filename) > 0):
                    continue
                print(filename)
                urlf = url + filename
                #succeed = False
                # for i in range(0,3):
                #     resource_2 = g_req.get(urlf, stream=True, timeout=60) 
                #     if resource_2:
                #         if resource_2.status_code == 200:
                #             with open(w_filename, 'wb') as wf:
                #                 wf.write(resource_2.content)
                #             succeed = True
                #             print("%s filesize: %d" %(filename, os.path.getsize(w_filename)))
                #             break
                # if not succeed:
                #     print(filename + ' failed')
                os.system('wget -O ' + w_filename + ' ' + urlf)
    except Exception as e:
        print(e)

def ResolveTrace(vp, flag):
    for root,dirs,files in os.walk('.'):
        for filename in files:
            if filename.startswith(vp) and filename.endswith('warts') and os.path.getsize(filename) > 0:
                plain_filename = filename[:filename.index('warts') - 1]
                print(plain_filename)
                if os.path.exists(plain_filename) and os.path.getsize(plain_filename) > 0:
                    continue
                cmd = "sc_analysis_dump %s > %s" %(filename, plain_filename)
                print(cmd)
                os.system(cmd)    

def DeleteNullFile():
    os.chdir('/mountdisk2/traceroute_download_all/')
    for year in range(2018,2021):
        os.chdir(str(year) + '/')
        for month in range(1, 13):
            #print(os.getcwd ())
            os.chdir(str(month).zfill(2) + '/')
            for root,dirs,files in os.walk('.'):
                for filename in files:                    
                    if os.path.getsize(filename) == 0:
                        print(filename)
                        os.remove(filename)
            os.chdir('../')
        os.chdir('../')

def ZipTraceFile(year, month):
    os.chdir(str(year) + '/' + str(month).zfill(2) + '/')
    for root,dirs,files in os.walk('.'):
        for filename in files:
            if filename.endswith('warts'):
                print(filename)
                os.system('gzip %s' %filename)
                #os.remove(filename)

def UnZipTraceFile(vp, flag):
    print(vp + ' begin')
    for root,dirs,files in os.walk('.'):
        for filename in files:
            if filename.startswith(vp) and filename.endswith('gz') and os.path.getsize(filename) > 0:
                print(filename)
                os.system('gunzip %s' %filename)
    print(vp + ' end')

if __name__ == '__main__':
    os.chdir(global_var.par_path + global_var.traceroute_dir + 'back/')
    if False:
        DownloadTracerouteForSpecDays(2019, 12)
        #DeleteNullFile()
        #ZipTraceFile(2019, 1)
        #DownloadTracerouteForSpecDays(2019, 1)
    else:
        proc_list = []        
        for year in range(2020,2022):
        #for year in [2019]:
            for month in range(1, 13):
            #for month in [1]:
                if (year == 2020 and month < 9): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
                    continue
                #if (year == 2019 and month == 1):
                #record = open('record_' + str(year) + str(month).zfill(2), 'a')
                #proc_list.append(mp.Process(target=DownloadTracerouteOneMonth,args=(year, month, record)))
                #proc_list.append(mp.Process(target=DownloadTracerouteForSpecDays,args=(year, month)))
        vps = set()
        for root,dirs,files in os.walk('.'):
            for filename in files:
                if filename.__contains__('warts'):
                    vps.add(filename.split('.')[0])
        for cur_vp in vps:
            proc_list.append(mp.Process(target=ResolveTrace,args=(cur_vp, True)))
            #proc_list.append(mp.Process(target=UnZipTraceFile,args=(cur_vp, True)))
            #UnZipTraceFile(vp)
        for proc in proc_list:
            proc.start()
        for proc in proc_list:
            proc.join()