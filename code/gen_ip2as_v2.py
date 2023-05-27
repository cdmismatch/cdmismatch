
import os
import requests
from bs4 import BeautifulSoup
from multiprocessing import Process, Queue, Pool
import traceutils
import datetime
import json
import time
import glob

from find_vp_v2 import SubProc_SysCmd
g_parell_num = os.cpu_count()

def GetDates(traceroute_dir):
    dates = set()
    for filename in os.listdir(traceroute_dir):
        dates.add(filename.split('.')[-1])
    return dates

def Parallel_SysCmd(cmds):
    task_list = []
    for cmd in cmds:
        if len(task_list) > 10:
            for task in task_list:
                task.join()
            task_list.clear()
        task = Process(target=SubProc_SysCmd, args=(cmd, True)) #将true改掉
        task_list.append(task)
        task.start()
    for task in task_list:
        task.join()
    
def DelIncompleteFiles(threshold_size):
    for filename in os.listdir('.'):
        if os.path.getsize(filename) < threshold_size:
            os.remove(filename)

def DownloadData(workdir, url_format, w_filename_format, min_filesize, dates):
    #dates = GetDates('/mountdisk1/ana_c_d_incongruity/traceroute_data/result/')
    #dates = ['20210109', '20210115', '20210116', '20210217', '20210316', '20210322', '20210315', '20210416', '20210415']
    os.chdir(workdir)
    DelIncompleteFiles(min_filesize)
    cmds = []
    for date in dates:
        (year, month, day) = (date[:4], date[4:6], date[6:])
        w_filename = w_filename_format.replace('year', year).replace('month', month).replace('day', day)
        if os.path.exists(w_filename):
            continue
        url = url_format.replace('year', year).replace('month', month).replace('day', day)
        cmds.append('wget -O ' + w_filename + ' ' + url)
    Parallel_SysCmd(cmds)

def DownloadPeeringdb(dates):
    workdir = '/mountdisk1/ana_c_d_incongruity/peeringdb_data/'
    url_format = 'https://publicdata.caida.org/datasets/peeringdb/year/month/peeringdb_2_dump_year_month_day.json'
    w_filename_format = 'peeringdb_year_month_day.json'
    DownloadData(workdir, url_format, w_filename_format, 100000, dates)

def DownloadCoalescedRib(dates):
    req = requests.Session()
    #dates = GetDates('/mountdisk1/ana_c_d_incongruity/traceroute_data/result/')
    #dates = ['20210109', '20210115', '20210116', '20210217', '20210316', '20210322', '20210315', '20210416', '20210415']
    workdir = '/mountdisk1/ana_c_d_incongruity/rib_data/rv_prefix2as/'
    os.chdir(workdir)
    #DelIncompleteFiles(min_filesize)
    cmds = []
    for date in dates:
        (year, month, day) = (date[:4], date[4:6], date[6:])
        r = req.get('https://publicdata.caida.org/datasets/routing/routeviews-prefix2as/' + year + '/' + month + '/', stream=True)
        soup = BeautifulSoup(r.content.decode('utf-8'), 'html.parser')
        links = soup.find_all('a', href=True)            
        for link in links:
            content = link['href']
            if content.__contains__(date):
                url = 'https://publicdata.caida.org/datasets/routing/routeviews-prefix2as/' + year + '/' + month + '/' + content
                cmds.append('wget -O ' + date + '.pfx2as.gz' + ' ' + url)
                break
    Parallel_SysCmd(cmds)

def DownloadRIRDelegate(dates):
    rirs = {'afrinic': 'https://ftp.afrinic.net/pub/stats/afrinic/year/delegated-afrinic-extended-yearmonthday', \
            'apnic': 'https://ftp.apnic.net/stats/apnic/year/delegated-apnic-extended-yearmonthday.gz', \
            'arin': 'https://ftp.arin.net/pub/stats/arin/archive/year/delegated-arin-extended-yearmonthday.gz', \
            'lacnic': 'https://ftp.lacnic.net/pub/stats/lacnic/delegated-lacnic-extended-yearmonthday', \
            'ripe': 'https://ftp.ripe.net/pub/stats/ripencc/year/delegated-ripencc-extended-yearmonthday.bz2'}
    #rirs = {'arin': 'https://ftp.arin.net/pub/stats/arin/archive/year/delegated-arin-extended-yearmonthday.gz'}
    #dates = GetDates('/mountdisk1/ana_c_d_incongruity/traceroute_data/result/')        
    #dates = ['20210109', '20210115', '20210116', '20210217', '20210316', '20210322', '20210315', '20210416', '20210415']
    os.chdir('/mountdisk1/ana_c_d_incongruity/rir_data/')
    cmds = []
    for date in dates:
        (year, month, day) = (date[:4], date[4:6], date[6:])
        for (name, addr) in rirs.items():
            suffix = ''
            if addr.split('/')[-1].__contains__('.'):
                suffix = '.' + addr.split('/')[-1].split('.')[-1]
            url = addr.replace('year', year).replace('month', month).replace('day', day)
            cmds.append('wget -O ' + name + '.' + date + suffix + ' ' + url)
    Parallel_SysCmd(cmds)

def GenRIRPrefixes(dates):
    #dates = GetDates('/mountdisk1/ana_c_d_incongruity/traceroute_data/result/')
    rirs = {'afrinic': '', 'apnic': '.gz', 'arin': '.gz', 'lacnic': '', 'ripe': '.bz2'}
    cmds = []
    os.chdir('/mountdisk1/ana_c_d_incongruity/rir_data/')
    #for date in dates:
    for date in dates:
        with open(date + '.rir.files', 'w') as wf:
            for (rir, suffix) in rirs.items():
                wf.write(rir + '.' + date + suffix + '\n')
        cmds.append('rir2as -f ' + date + '.rir.files -r /mountdisk1/ana_c_d_incongruity/as_rel_cc_data/' + date + '.as-rel2.txt -c /mountdisk1/ana_c_d_incongruity/as_rel_cc_data/' + date + '.ppdc-ases.txt -o ' + date + '.rir.prefixes')
        #os.system('rir2as -f ' + date + '.rir.files -r /mountdisk1/ana_c_d_incongruity/as_rel_cc_data/' + date + '.as-rel2.txt -c /mountdisk1/ana_c_d_incongruity/as_rel_cc_data/' + date + '.ppdc-ases.txt -o ' + date + '.rir.prefixes')
    Parallel_SysCmd(cmds)

def GetClosestDate(given_date, date_list):
    min_val = 0xFFFFFFF
    closest_date = ''
    given_datetime = datetime.datetime(int(given_date[:4]), int(given_date[4:6]), int(given_date[6:]))
    for date in date_list:
        #print(date)
        diff = abs((datetime.datetime(int(date[:4]), int(date[4:6]), int(date[6:])) - given_datetime).days)
        if diff < min_val:
            min_val = diff
            closest_date = date
    return closest_date

def GenPrefixToAS(dates):
    #dates = GetDates('/mountdisk1/ana_c_d_incongruity/traceroute_data/result/')
    as_org_dates = set()
    for filename in os.listdir('/mountdisk1/ana_c_d_incongruity/as_org_data/'):
        as_org_dates.add(filename.split('.')[0])
    cmds = []
    #for date in dates:
    for date in dates:
        # if date[:6] != '202006':
        #     continue
        year_month = date[:6]
        date1 = date[:4] + '_' + date[4:6] + '_' + date[6:]
        peeringdb_suffix = '.json'
        if year_month == '201801' or year_month == '201802':
            peeringdb_suffix = '.sqlite'
        as_org_date = GetClosestDate(date, as_org_dates)
        if year_month == '202102':
            cmds.append('ip2as -p /mountdisk1/ana_c_d_incongruity/rib_data/rv_prefix2as/' + date + '.pfx2as -r /mountdisk1/ana_c_d_incongruity/rir_data/' + date + '.rir.prefixes -R /mountdisk1/ana_c_d_incongruity/as_rel_cc_data/20200101.as-rel3.txt -c /mountdisk1/ana_c_d_incongruity/as_rel_cc_data/20200101.ppdc-ases.txt -a /mountdisk1/ana_c_d_incongruity/as_org_data/' + as_org_date + '.as-org2info.txt -P /mountdisk1/ana_c_d_incongruity/peeringdb_data/peeringdb_' + date1 + peeringdb_suffix + ' -o /mountdisk1/ana_c_d_incongruity/out_ip2as_data/' + date + '.ip2as.prefixes')
        elif year_month == '202103':
            cmds.append('ip2as -p /mountdisk1/ana_c_d_incongruity/rib_data/rv_prefix2as/' + date + '.pfx2as -r /mountdisk1/ana_c_d_incongruity/rir_data/' + date + '.rir.prefixes -R /mountdisk1/ana_c_d_incongruity/as_rel_cc_data/20200401.as-rel3.txt -c /mountdisk1/ana_c_d_incongruity/as_rel_cc_data/20200401.ppdc-ases.txt -a /mountdisk1/ana_c_d_incongruity/as_org_data/' + as_org_date + '.as-org2info.txt -P /mountdisk1/ana_c_d_incongruity/peeringdb_data/peeringdb_' + date1 + peeringdb_suffix + ' -o /mountdisk1/ana_c_d_incongruity/out_ip2as_data/' + date + '.ip2as.prefixes')
        else:
            cmds.append('ip2as -p /mountdisk1/ana_c_d_incongruity/rib_data/rv_prefix2as/' + date + '.pfx2as -r /mountdisk1/ana_c_d_incongruity/rir_data/' + date + '.rir.prefixes -R /mountdisk1/ana_c_d_incongruity/as_rel_cc_data/' + year_month + '01.as-rel3.txt -c /mountdisk1/ana_c_d_incongruity/as_rel_cc_data/' + year_month + '01.ppdc-ases.txt -a /mountdisk1/ana_c_d_incongruity/as_org_data/' + as_org_date + '.as-org2info.txt -P /mountdisk1/ana_c_d_incongruity/peeringdb_data/peeringdb_' + date1 + peeringdb_suffix + ' -o /mountdisk1/ana_c_d_incongruity/out_ip2as_data/' + date + '.ip2as.prefixes')
            # cmds.append('ip2as -p /mountdisk1/ana_c_d_incongruity/rib_data/rv_prefix2as/' + date + '.pfx2as -r /mountdisk1/ana_c_d_incongruity/rir_data/' + date + '.rir.prefixes -R /mountdisk1/ana_c_d_incongruity/as_rel_cc_data/' + year_month + '01.as-rel3.txt -c /mountdisk1/ana_c_d_incongruity/as_rel_cc_data/' +  '20230301.ppdc-ases.txt -a /mountdisk1/ana_c_d_incongruity/as_org_data/' + as_org_date + '.as-org2info.txt -P /mountdisk1/ana_c_d_incongruity/peeringdb_data/peeringdb_' + date1 + peeringdb_suffix + ' -o /mountdisk1/ana_c_d_incongruity/out_ip2as_data/' + date + '.ip2as.prefixes')

    Parallel_SysCmd(cmds)

def ChgFileForm(date):
    # os.chdir('/mountdisk1/ana_c_d_incongruity/as_rel_cc_data/')
    # for filename in os.listdir('.'):
    #     if filename.__contains__('as-rel2'):
    #         if not filename.startswith('2021'):
    #             continue
    # filename = '/home/slt/code/el_git/bdrmapit_src/%s01.as-rel2.txt' %date[:6]
    filename = '/mountdisk1/ana_c_d_incongruity/as_rel_cc_data/%s01.as-rel2.txt' %date[:6]
    if True:
        if True:
            with open(filename[:filename.index('.')] + '.as-rel3.txt', 'w') as wf:
                with open(filename, 'r') as rf:
                    for line in rf.readlines():
                        if not line.startswith('#'):
                            line = line[:line.rindex('|')] + '\n'
                        wf.write(line)

bdrmapit_type = 'snmp_bdr'#'sxt_bdr'#'ori_bdr' #'ixp_modi_bdr'#'hoiho_s_bdr'

def RunBdrmapIt():
    as_org_dates = set()
    hoiho_dates = set()
    for filename in os.listdir('/mountdisk1/ana_c_d_incongruity/as_org_data/'):
        as_org_dates.add(filename.split('.')[0])
    if bdrmapit_type == 'hoiho_s_bdr' or bdrmapit_type == 'hoiho_l_bdr':
        for filename in os.listdir('/mountdisk1/ana_c_d_incongruity/hoiho/out/'):
            if filename.__contains__('hoiho_small'):
                hoiho_dates.add(filename.split('.')[0] + '01')
    #print(hoiho_dates)
    cmds = []
    for traceroute_filename in glob.glob('/mountdisk1/ana_c_d_incongruity/traceroute_data/result/*.warts'):
    #for traceroute_filename in glob.glob('/mountdisk1/ana_c_d_incongruity/traceroute_data/back/temp5/*.warts'):
    #for traceroute_filename in ['/mountdisk1/ana_c_d_incongruity/traceroute_data/back/temp5/ams-nl.20211015.warts']:
    #for traceroute_filename in glob.glob('/mountdisk3/traceroute_download_all/202204/*.warts'):
        # if not traceroute_filename.__contains__('2021'):
        #     continue
        # if not traceroute_filename.__contains__('nrt-jp'):
        #     continue
        # if not traceroute_filename.__contains__('.'):
        #     continue
        # (tmp, date) = traceroute_filename.split('.')
        # vp = tmp.split('_')[-1]
        # year_month = date[:6]
        #vp, date = traceroute_filename.split('/')[-1][len('trace_'):].split('.')
        vp, date, _ = traceroute_filename.split('/')[-1].split('.')
        if bdrmapit_type == 'snmp_bdr':
            if date[:4] != '2021': #snmp_bdrmapit专用
                continue
        db_filename = '/mountdisk1/ana_c_d_incongruity/out_bdrmapit/%s/bdrmapit_%s_%s.db' %(bdrmapit_type, vp, date)
        if os.path.exists(db_filename) and os.path.getsize(db_filename) > 0:
            # localtime = time.localtime(os.path.getctime(db_filename))
            # if localtime.tm_year == 2022 and localtime.tm_mon == 1 and localtime.tm_yday == 31:
            #     continue
            #continue
            #continue
            continue
        print(vp + '.' + date)
        with open('/home/slt/code/ana_c_d_incongruity/config.json', 'r') as rf:
            config_info = json.load(rf)
            config_info['ip2as'] = '/mountdisk1/ana_c_d_incongruity/out_ip2as_data/' + date + '.ip2as.prefixes'
            as_org_date = GetClosestDate(date, as_org_dates)
            config_info['as2org']['as2org'] = '/mountdisk1/ana_c_d_incongruity/as_org_data/' + as_org_date + '.as-org2info.txt'
            config_info['as-rels']['rels'] = '/mountdisk1/ana_c_d_incongruity/as_rel_cc_data/' + date[:6] + '01.as-rel3.txt'
            config_info['as-rels']['cone'] = '/mountdisk1/ana_c_d_incongruity/as_rel_cc_data/' + date[:6] + '01.ppdc-ases.txt'
            config_info['warts']['files'] = vp + '.' + date + '_warts.files'
            if bdrmapit_type == 'hoiho_s_bdr' or bdrmapit_type == 'hoiho_l_bdr':
                hoiho_date = GetClosestDate(date, hoiho_dates)
                config_info['hints'] = '/mountdisk1/ana_c_d_incongruity/hoiho/out/' + hoiho_date[:6] + '.hoiho_small.csv'
                #print('{}'.format(config_info['hints']))
            elif bdrmapit_type == 'snmp_bdr':
                config_info['aliases'] = '/mountdisk1/ana_c_d_incongruity/snmpv3/2021-04-alias-sets.csv'
            elif bdrmapit_type == 'midar':
                midar_fns = glob.glob('/mountdisk1/ana_c_d_incongruity/midar_data/*_midar-iff.nodes')
                mi_dates = [fn.split('/')[-1].split('_')[0].replace('-', '') + '01' for fn in midar_fns]
                closest_date = GetClosestDate(date, mi_dates)[:6]
                config_info['aliases'] = '/mountdisk1/ana_c_d_incongruity/midar_data/%s_midar-iff.nodes' %(closest_date)
            if date[:6] < '201803':
                config_info['peeringdb'] = '/mountdisk1/ana_c_d_incongruity/peeringdb_data/peeringdb_' + date[:4] + '_' + date[4:6] + '_15.sqlite'
            else:
                config_info['peeringdb'] = '/mountdisk1/ana_c_d_incongruity/peeringdb_data/peeringdb_' + date[:4] + '_' + date[4:6] + '_15.json'
            config_info['max_iterations'] = 100
            with open(vp + '.' + date + '_warts.files', 'w') as wf:    
                #wf.write('/mountdisk3/traceroute_download_all/back/' + vp + '.' + date + '.warts')
                wf.write(traceroute_filename)
            with open(vp + '.' + date + '_config.json', 'w') as wf:
                json.dump(config_info, wf, indent=1)  # 写为多行
            #cmds.append('bdrmapit -o %s -c %s.%s_config.json' %(db_filename, vp, date))
            cmds.append('bdrmapit json -c %s.%s_config.json -s %s' %(vp, date, db_filename))
        
    #Parallel_SysCmd(cmds)
    # for cmd in cmds:
    #     SubProc_SysCmd(cmd)
    pool = Pool(processes=10)#g_parell_num)
    pool.map(SubProc_SysCmd, cmds)
    pool.close()
    pool.join()

if __name__ == '__main__':
    # fns = glob.glob('/mountdisk1/ana_c_d_incongruity/out_ip2as_data/*.ip2as.prefixes')
    # dates = [fn.split('/')[-1].split('.')[0] for fn in fns]
    #dates = ['20210616', '20210802', '20210816']
    dates = ['20230514']
    # for month in range(10, 13):
    #     dates.append('2022%s15' %str(month).zfill(2))
    # for month in range(3, 4):
    #     dates.append('2023%s15' %str(month).zfill(2))
    #DownloadPeeringdb(dates)
    # DownloadCoalescedRib(dates)
    # DownloadRIRDelegate(dates)
    # GenRIRPrefixes(dates)
    # # for date in dates:
    # #     ChgFileForm(date)
    # GenPrefixToAS(dates)
    RunBdrmapIt()
    