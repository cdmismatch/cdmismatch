
import os
import requests
import re
import json
from urllib.request import urlopen
from bs4 import BeautifulSoup
import glob
import global_var

def GetExistKey(text_a, pre, cat=''):
    for i in range(0, 6):
        key = pre + cat + str(15 + i)
        if text_a.__contains__(key):
            return key
        key = pre + cat + str(15 - i)
        if text_a.__contains__(key):
            return key
    for i in range(6, 15):
        key = pre + cat + str(15 + i)
        if text_a.__contains__(key):
            return key
        key = pre + cat + '0' + str(15 - i)
        if text_a.__contains__(key):
            return key
    return ''

def DownloadTraceroute():
    req = requests.Session()
    os.chdir('/mountdisk3/traceroute_download_all/back/')
    years = ['2018', '2019', '2020', '2021', '2022']
    #undo: 201611, 201612
    #years = ['2022']
    months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
    #months = ['01', '02', '03']
    vps = ['ams-nl', 'nrt-jp', 'sao-br', 'sjc2-us', 'syd-au']
    #vps = ['sjc2-us', 'ord-us']
    existed_fns = glob.glob('/mountdisk1/ana_c_d_incongruity/traceroute_data/result/*warts')
    existed_keys = [fn.split('/')[-1][:-1*(len('.warts')+2)] for fn in existed_fns]
    existed_fns2 = glob.glob('/mountdisk3/traceroute_download_all/back/*.gz')
    existed_keys2 = [fn.split('/')[-1][:-1*(len('.warts.gz')+2)] for fn in existed_fns2]
    existed_keys = existed_keys + existed_keys2
    for year in years:
        for month in months:
            print(year + month)
            download_flag1 = False
            pre = 'https://publicdata.caida.org/datasets/topology/ark/ipv4/prefix-probing/' + year + '/' + month + '/'
            resource1 = req.get(pre, stream=True, timeout=60) 
            if resource1:
                print("get ret1: %d" %resource1.status_code)
                if resource1.status_code == 200:
                    download_flag1 = True
                    for vp in vps:
                        if vp+'.'+str(year)+str(month).zfill(2) in existed_keys:
                            print('{}, {}{} already exists.'.format(vp, year, month))
                            continue
                        print(vp)
                        download_flag2 = False
                        key = GetExistKey(resource1.text, vp + '.' + year + month)
                        if not key:
                            print('Failed to find ' + year + month + ' ' + vp)
                            continue
                        start_pos = resource1.text.index(key)
                        end_pos = resource1.text.index('.warts.gz', start_pos)
                        obj_name = resource1.text[start_pos:(end_pos + len('.warts.gz'))]
                        print(obj_name)
                        for i in range(0,3):
                            resource_2 = req.get(pre + obj_name, stream=True, timeout=60) 
                            if resource_2:
                                print("get ret2: %d" %resource_2.status_code)
                                if resource_2.status_code == 200:
                                    wf = open(vp + '.' + year + month + '15.warts.gz', "wb")
                                    wf.write(resource_2.content)
                                    wf.close()
                                    download_flag2 = True
                                    break
                        if not download_flag2:
                            print('Failed to download ' + obj_name)
                        else:
                            print('Download %s succeed' %obj_name)
            if not download_flag1:
                print('Failed to get response ' + year + month)

def DownloadMidarIff():
    req = requests.Session()
    dates = ['2016-03/', '2016-09/', '2017-02/', '2017-08/', '2018-03/', '2019-01/', '2019-04/', '2020-01/']
    files = ['midar-iff.ifaces.bz2', 'midar-iff.links.bz2', 'midar-iff.nodes.as.bz2', 'midar-iff.nodes.bz2', 'midar-iff.nodes.geo.bz2']
    for date in dates:
        for cur_file in files:
            print(date + cur_file)
            download_flag = False
            pre = 'https://publicdata.caida.org/datasets/topology/ark/ipv4/itdk/'
            for i in range(0,3):
                resource = req.get(pre + date + cur_file, stream=True, timeout=60) 
                if resource:
                    print("get ret: %d" %resource.status_code)
                    if resource.status_code == 200:
                        wf = open(date.strip('/') + '_' + cur_file, "wb")
                        wf.write(resource.content)
                        wf.close()
                        download_flag = True
                        break
            if not download_flag:
                print('Failed to download ' + date + cur_file)

def DownloadAsRelAndCC():
    wr_path = global_var.par_path + global_var.rel_cc_dir
    if not os.path.exists(wr_path):
        os.makedirs(wr_path)
    req = requests.Session()
    pre = 'https://publicdata.caida.org/datasets/as-relationships/serial-1/'
    suffixes = ['01.as-rel.txt.bz2', '01.ppdc-ases.txt.bz2']
    for year in range(2018, 2021):
    #for year in range(2020, 2022):
        year_str = str(year)
        for month in range(1, 13):
        #for month in range(4, 5):
            month_str = str(month).zfill(2)
            for suffix in suffixes:
                download_flag = False     
                for i in range(0,3):
                    #print(pre + year_str + month_str + suffix)
                    resource = req.get(pre + year_str + month_str + suffix, stream=True, timeout=60) 
                    if resource:
                        print("get ret: %d" %resource.status_code)
                        if resource.status_code == 200:
                            wf = open(wr_path + year_str + month_str + suffix, "wb")
                            wf.write(resource.content)
                            wf.close()
                            download_flag = True
                            break
                if not download_flag:
                    print('Failed to download %s' %(year_str + month_str + suffix))

def DownloadAsRelAndCC_2():
    os.chdir('/mountdisk1/ana_c_d_incongruity/as_rel_cc_data/')
    pre_asrel = 'https://publicdata.caida.org/datasets/as-relationships/serial-2/' #2022.1.31 原来as-rel也是用的serial-1的数据，查了一下，serial-2的数据应该更准确。但是serial-2没有cc数据
    for year in range(2018, 2021):
    #for year in range(2020, 2022):
        year_str = str(year)
        for month in range(1, 13):
        #for month in range(4, 5):
            month_str = str(month).zfill(2)
            filename_asrel = year_str + month_str + '01.as-rel2.txt.bz2'
            os.system('wget -O ' + filename_asrel + ' ' + pre_asrel + filename_asrel)

def DownloadAsOrg():
    wr_path = global_var.par_path + global_var.as2org_dir
    if not os.path.exists(wr_path):
        os.makedirs(wr_path)
    req = requests.Session()
    download_flag1 = False
    pre = 'https://publicdata.caida.org/datasets/as-organizations/'
    resource1 = req.get(pre, stream=True, timeout=60) 
    if resource1:
        print("get ret1: %d" %resource1.status_code)
        if resource1.status_code == 200:
            download_flag1 = True
            for year in range(2016, 2022):
                download_flag2 = False
                matches = re.findall("a href=\"" + str(year) + '(.*?)as-org2info.txt.gz', resource1.text)
                for elem in matches:
                    for i in range(0,3):
                        print(pre + str(year) + elem + 'as-org2info.txt.gz')
                        resource_2 = req.get(pre + str(year) + elem + 'as-org2info.txt.gz', stream=True, timeout=60) 
                        if resource_2:
                            print("get ret2: %d" %resource_2.status_code)
                            if resource_2.status_code == 200:
                                wf = open(wr_path + str(year) + elem + 'as-org2info.txt.gz', "wb")
                                wf.write(resource_2.content)
                                wf.close()
                                download_flag2 = True
                                break
                    if not download_flag2:
                        print('Failed to download ' + str(year) + elem)
    if not download_flag1:
        print('Failed to get response ' + str(year) + elem)

def DownloadPeeringDB():
    wr_path = global_var.par_path + global_var.peeringdb_dir
    if not os.path.exists(wr_path):
        os.makedirs(wr_path)
    req = requests.Session()
    #'https://publicdata.caida.org/datasets/peeringdb-v2/2016/01/peeringdb_2_dump_2016_01_15.sqlite
    #'https://publicdata.caida.org/datasets/peeringdb-v2/2016/01/peeringdb_dump_2016_01_16.sql'
    #for year in range(2016, 2022):
    for year in range(2016, 2017):
        year_str = str(year)
        #for month in range(1, 13):
        for month in range(1, 5):
            month_str = str(month).zfill(2)          
            pre = "https://publicdata.caida.org/datasets/peeringdb-v2/" + year_str + '/' + month_str + "/"#peeringdb_dump_" + year_str + '_' + month_str + "_15.sql"
            download_flag1 = False
            resource1 = req.get(pre, stream=True, timeout=60) 
            if resource1:
                print("get ret1: %d" %resource1.status_code)
                if resource1.status_code == 200:
                    download_flag1 = True
                    download_flag2 = False                    
                    key = GetExistKey(resource1.text, year_str + '_' + month_str, '_')
                    if not key:
                        print('Failed to find ' + year + month)
                        continue
                    matches = re.findall("<a href=\"(.*?)" + key + '(.*?)\">peeringdb', resource1.text)
                    elem = None
                    for tmp_elem in matches:
                        if tmp_elem[1].__contains__('json'): #优先下载json
                            elem = tmp_elem
                            break
                    if not elem:
                        for tmp_elem in matches:
                            if tmp_elem[1].__contains__('sqlite'): #次优先下载sqlite
                                elem = tmp_elem
                                break
                    if not elem:
                        elem = matches[0]
                    url = pre + elem[0] + key + elem[1]
                    print(url)
                    for i in range(0,3):                        
                        resource = req.get(url, stream=True, timeout=60)
                        if resource:
                            print("get ret: %d" %resource.status_code)
                            if resource.status_code == 200:
                                wf = open(wr_path + "peeringdb_" + key + elem[1], 'wb')
                                wf.write(resource.content)
                                download_flag2 = True
                                break
                    # 请求三次还是不成功
                    if not download_flag2:
                        print("************%s%s15%sfailed" %(year_str, month_str, elem[1])) 
            if not download_flag1:
                print('Failed to get response ' + year_str + month_str)

def DownloadPrefix2AS():
    wr_path = global_var.par_path + global_var.prefix2as_dir
    if not os.path.exists(wr_path):
        os.makedirs(wr_path)
    req = requests.Session()
    for year in range(2016, 2022):
    #for year in range(2016, 2017):
        year_str = str(year)
        #for month in range(1, 13):
        for month in range(5, 13):
            month_str = str(month).zfill(2)           
            #https://publicdata.caida.org/datasets/routing/routeviews-prefix2as/2016/01/routeviews-rv2-20160120-1200.pfx2as.gz
            pre = "https://publicdata.caida.org/datasets/routing/routeviews-prefix2as/" + year_str + '/' + month_str + "/"#routeviews-rv2-" + year_str + month_str + "15.sql"
            download_flag1 = False
            resource1 = req.get(pre, stream=True, timeout=60) 
            if resource1:
                print("get ret1: %d" %resource1.status_code)
                if resource1.status_code == 200:
                    download_flag1 = True
                    download_flag2 = False      
                    matches = re.findall("a href=\"(.*?)" + year_str + month_str + '15' + '(.*?).pfx2as.gz', resource1.text)
                    if matches:
                        elem = matches[0]
                        url = pre + elem[0] + year_str + month_str + '15' + elem[1] + '.pfx2as.gz'
                        print(url)
                        for i in range(0,3):                        
                            resource = req.get(url, stream=True, timeout=60)
                            if resource:
                                print("get ret: %d" %resource.status_code)
                                if resource.status_code == 200:
                                    wf = open(wr_path + year_str + month_str + '15' + '.pfx2as.gz', 'wb')
                                    wf.write(resource.content)
                                    download_flag2 = True
                                    break
                        # 请求三次还是不成功
                        if not download_flag2:
                            print("************%s%s15%sfailed" %(year_str, month_str, elem[1])) 
                    else:
                        print("%s%s cannot find files" %(year_str, month_str))
            if not download_flag1:
                print('Failed to get response ' + year_str + month_str)

def GetBelongedOrgInRipe_Delete(ip):
    url = "https://apps.db.ripe.net/db-web-ui/api/whois/search?abuse-contact=true&ignore404=true&managed-attributes=true&resource-holder=true&flags=r&offset=0&limit=20&query-string=" + ip
    req = requests.Session()
    headers = {"accept":"application/json"}
    resource = req.get(url, headers=headers) 
    if resource:                
        if resource.status_code == 200:
            data = json.loads(resource.text)            
            attr = data['objects']['object'][0]['attributes']['attribute']
            for elem in attr:
                if elem['name'] == 'netname':
                    if elem['value'] != 'NON-RIPE-NCC-MANAGED-ADDRESS-BLOCK':
                        res = data['objects']['object'][0]['resource-holder']['name']
                        print(res)
                        return res
                    #else: #没找到
                    break
        else:
            print('Failed to get response')
    else:
        print('resource empty')
    return None

def ScamperTrace(path):    
    os.chdir(path)
    for root,dirs,files in os.walk(path):
        for filename in files:
            if not filename.__contains__('.warts'):
                continue
            print(filename)
            next_pos = filename.index('.', filename.index('.') + 1)
            os.system('sc_analysis_dump %s > %s' %(filename, filename[:next_pos]))

def DownloadCoalescedData():
    os.chdir(global_var.par_path + global_var.rib_dir + 'coalesced/')
    req = requests.Session()
    for year in range(2018, 2020):
        year_str = str(year)
        for month in range(1, 13):
            month_str = str(month).zfill(2)
            url = 'https://publicdata.caida.org/datasets/routing/routeviews-prefix2as-coalesced/' + year_str + '/' + month_str + '/'
            try:
                r = req.get(url, stream=True)
                #print(r.content)
                soup = BeautifulSoup(r.content.decode('utf-8'), 'html.parser')
                find_res = soup.find_all('a', href=True)
                for link in find_res:
                    filename = link['href']
                    if not filename.__contains__(year_str + month_str + '15'):
                        continue
                    w_filename = filename[:filename.index(year_str)] + year_str + month_str + '15' + filename[filename.index('.'):]
                    if (os.path.exists(w_filename) and os.path.getsize(w_filename) > 0):
                        continue
                    print(w_filename)
                    urlf = url + filename
                    print(urlf)
                    succeed = False
                    for i in range(0,3):
                        resource_2 = req.get(urlf, stream=True, timeout=60) 
                        if resource_2:
                            if resource_2.status_code == 200:
                                with open(w_filename, 'wb') as wf:
                                    wf.write(resource_2.content)
                                succeed = True
                                print("%s filesize: %d" %(filename, os.path.getsize(w_filename)))
                                break
                    if not succeed:
                        print(filename + ' failed')
            except Exception as e:
                print(e)

def DownFile(url, local_filename):
    req = requests.Session()
    try:
        resource = req.get(url, stream=True, timeout=60) 
        if resource:
            print("get ret: %d" %resource.status_code)
            if resource.status_code == 200:
                wf = open(local_filename, "wb")
                wf.write(resource.content)
                wf.close()
    except Exception as e:
        print(e)

if __name__ == '__main__':
    #DownloadAsRelAndCC()

    #DownloadAsRelAndCC_2()
    #DownFile('https://publicdata.caida.org/datasets/routing/routeviews-prefix2as/2021/08/routeviews-rv2-20210815-1200.pfx2as.gz', '/mountdisk1/ana_c_d_incongruity/rib_data/coalesced/routeviews-rv2-20210815-1200.pfx2as.gz')
    #DownFile('https://publicdata.caida.org/datasets/as-relationships/serial-2/20210801.as-rel2.txt.bz2', '/mountdisk1/ana_c_d_incongruity/as_rel_cc_data/20210801.as-rel2.txt.bz2')
    #DownFile('https://publicdata.caida.org/datasets/as-relationships/serial-2/20180101.as-rel2.txt.bz2', '/mountdisk1/ana_c_d_incongruity/as_rel_cc_data/20180101.as-rel2.txt.bz2')
    #DownFile('ftp://ftp.radb.net/radb/dbase/archive/radb.db.210909.gz', '/mountdisk3/tmp.gz')
    
    DownloadTraceroute()
    #DownloadMidarIff()
    #DownloadAsRelAndCC()
    #DownloadAsOrg()
    #DownloadPeeringDB()
    #DownloadPrefix2AS()
    #DownloadCoalescedData()
    
    #GetBelongedOrgInRipe_Delete('212.36.135.22')
    #GetBelongedOrgInApnic('203.181.248.60') #APNIC
    #GetBelongedOrgInApnic('52.198.45.18') #ARIN
    #GetBelongedOrgInApnic('212.36.135.22') #RIPE
    #GetBelongedOrgInApnic('41.204.161.206') #AFRINIC
    #GetBelongedOrgInApnic('200.144.248.54') #LACNIC

    #ScamperTrace(global_var.par_path + 'jzt/prefix-probing/20190101')

