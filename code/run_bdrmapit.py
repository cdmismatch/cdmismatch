
import os
import global_var
import threading
import json
import multiprocessing as mp

from gen_ip2as_command import PrepareBdrmapit, PreGetSrcFilesInDirs

def RunBdrmapit():        
    if not os.path.exists(global_var.par_path + global_var.out_bdrmapit_dir):
        os.makedirs(global_var.par_path + global_var.out_bdrmapit_dir)
    #vps = ['nrt-jp', 'per-au', 'syd-au', 'zrh2-ch']
    vps = ['sjc2-us', 'ord-us']
    #for year in range(2016,2021):
    for year in range(2018, 2021):
        year_str = str(year)
        #for month in range(1,13):
        for month in range(1, 13):
            if (year == 2016 and month < 4) or (year == 2020 and month > 4): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
                continue
            month_str = str(month).zfill(2)
            PrepareBdrmapit(year, month)
            for vp in vps:
                warts_filename = '%s%s%s.%s%s15.warts' %(global_var.par_path, global_var.traceroute_dir, vp, year_str, month_str)
                out_filename = '%s%sbdrmapit_%s_%s%s15.db' %(global_var.par_path, global_var.out_bdrmapit_dir, vp, year_str, month_str)
                if os.path.exists(out_filename) and os.path.getsize(out_filename):
                    continue
                if os.path.exists(warts_filename):
                    wf = open('warts.files', 'w')
                    wf.write(warts_filename + '\n')
                    wf.close()
                    print("%s%s %s" %(year_str, month_str, vp))
                    os.system("bdrmapit -o %s -c config.json" %out_filename)
                
def RunBdrmapitForJzt():
    os.chdir(global_var.par_path + 'jzt/prefix-probing/20190101/')
    PrepareBdrmapit(2019, 1)
    for root,dirs,files in os.walk('.'):
        for filename in files:
            if not filename.__contains__('bjl-gm.20190101.1546308000.warts'):
                continue
            print(filename)
            wf = open('warts.files', 'w')
            wf.write("%s\n" %filename)
            wf.close()
            os.system("bdrmapit -o bdrmapit_%s_20190101.db -c config.json" %filename[:filename.index('.')])
           
def RunBdrmapitForOneVp(vp):
    for root,dirs,files in os.walk('.'):
        for filename in files:
            if not filename.endswith('warts'):
                continue
            if not filename.startswith(vp):
                continue     
            t_filename = filename[:filename.rindex('.')]  
            db_filename = 'bdrmapit_%s.db' %t_filename
            if os.path.exists(db_filename) and os.path.getsize(db_filename) > 0:
                continue
            print(filename)
            # os.system('cp config.json config_%s.json' %t_filename)
            # data = None
            # with open('config_%s.json' % t_filename, 'r') as rf:
            #     data = rf.read()
            # data = data.replace(': \"warts', ': \"warts.%s' %t_filename)
            # with open('config_%s.json' % t_filename, 'w') as twf:
            #     twf.write(data)
            # wf = open('warts.%s.files' %t_filename, 'w')
            # wf.write("%s\n" %filename)
            # wf.close()
            # os.system("bdrmapit -o %s -c config_%s.json" %(db_filename, t_filename))
            wf = open('warts.files', 'w')
            wf.write("%s\n" %filename)
            wf.close()
            os.system("bdrmapit -o %s -c config.json" %db_filename)

#2019.1
def RunBdrmapitForAllTrace():
    os.chdir(global_var.all_trace_par_path + global_var.all_trace_download_dir + '2019/01/back')
    PrepareBdrmapit(2019, 1)
    thread_list = []
    for vp in ['ams2-nl', 'ams-nl', 'arn-se', 'bcn-es', 'bjl-gm', 'bwi-us', 'cbg-uk', 'cjj-kr', 'dub-ie', 'eug-us', 'fnl-us', 'hel-fi', 'hkg-cn', 'hlz-nz', 'mty-mx', 'nrt-jp', 'per-au', 'pna-es', 'pry-za', 'sao-br', 'scl-cl', 'sjc2-us', 'syd-au', 'wbu-us', 'yyz-ca', 'zrh2-ch', 'zrh-ch', 'ord-us', 'osl-no', 'rno-us', 'sea-us']:
        #thread_list.append(threading.Thread(target=RunBdrmapitForOneVp,args=(vp)))
        RunBdrmapitForOneVp(vp)
    # for thread in thread_list:
    #     thread.start()
           
def RunBdrmapitGroupOneyearPerVP():
    if not os.path.exists(global_var.par_path + global_var.out_bdrmapit_dir):
        os.makedirs(global_var.par_path + global_var.out_bdrmapit_dir)
    vps = ['nrt-jp', 'per-au', 'syd-au', 'zrh2-ch']
    #vps = ['zrh2-ch']
    for year in range(2016,2022):
    #for year in range(2016, 2017):
        year_str = str(year)
        month = 4
        PrepareBdrmapit(year, month)
        for vp in vps:
            wf = open('warts.files', 'w')
            end_month = 13
            if year == 2021:
                end_month = 5
            for month in range(1,end_month):
                month_str = str(month).zfill(2)
                wf.write("%s%s%s.%s%s15.warts\n" %(global_var.par_path, global_var.traceroute_dir, vp, year_str, month_str))
            wf.close()
            print("%s %s" %(year_str, vp))
            os.system("bdrmapit -o %s%sbdrmapit_%s_%s.db -c config.json" %(global_var.par_path, global_var.out_bdrmapit_dir, vp, year_str))

def RunBdrmapitGroupOneyearAllVP():
    if not os.path.exists(global_var.par_path + global_var.out_bdrmapit_dir):
        os.makedirs(global_var.par_path + global_var.out_bdrmapit_dir)
    vps = ['nrt-jp', 'per-au', 'syd-au', 'zrh2-ch']
    #vps = ['zrh2-ch']
    for year in range(2016,2022):
    #for year in range(2016, 2017):
        year_str = str(year)
        month = 4
        PrepareBdrmapit(year, month)
        wf = open('warts.files', 'w')
        for vp in vps:            
            end_month = 13
            if year == 2021:
                end_month = 5
            for month in range(1,end_month):
                month_str = str(month).zfill(2)
                wf.write("%s%s%s.%s%s15.warts\n" %(global_var.par_path, global_var.traceroute_dir, vp, year_str, month_str))
        wf.close()
        print(year_str)
        os.system("bdrmapit -o %s%sbdrmapit_%s.db -c config.json" %(global_var.par_path, global_var.out_bdrmapit_dir, year_str))

#3 days per month
def RunBdrmapitForAllTrace_2(year, month):
    date = str(year) + str(month).zfill(2)
    PrepareBdrmapit(year, month)
    with open('config_%s.json' %date, 'w') as wf:
        wf.write('{\n    "$schema": "schema.json",\n    "ip2as": "ip2as.prefixes_%s",\n    "as2org": {\n        "as2org": "as2org-file_%s"\n    },\n    "as-rels": {\n        "rels": "rels-file_%s",\n        "cone": "cone-file_%s"\n    },\n    "warts": {\n        "files": "warts_%s.files"\n    },\n    "processes": 3\n}\n' %(date, date, date, date, date))
    for root,dirs,files in os.walk('.'):
        for filename in files:
            if not filename.endswith('warts'): #例：cdg-fr.20180125.warts
                continue
            (cur_vp, cur_date, suffix) = filename.split('.')
            if date != cur_date[:6]:# or cur_vp != 'mty-mx':
                continue
            t_filename = filename[:filename.rindex('.')]  
            db_filename = 'bdrmapit_%s.db' %t_filename
            if os.path.exists(db_filename) and os.path.getsize(db_filename) > 0:
                continue
            print(filename)
            with open('warts_%s.files' %date, 'w') as wf:
                wf.write("%s\n" %filename)
            os.system("bdrmapit -o %s -c config_%s.json" %(db_filename, date))
            #return

def RunBdrmapitForSpecTraces(year, month, warts_files, db_name):
    date = str(year) + str(month).zfill(2)
    PrepareBdrmapit(year, month)
    with open('config_%s.json' %date, 'w') as wf:
        wf.write('{\n    "$schema": "schema.json",\n    "ip2as": "ip2as.prefixes_%s",\n    "as2org": {\n        "as2org": "as2org-file_%s"\n    },\n    "as-rels": {\n        "rels": "rels-file_%s",\n        "cone": "cone-file_%s"\n    },\n    "warts": {\n        "files": "%s"\n    },\n    "processes": 3\n}\n' %(date, date, date, date, warts_files))
    db_filename = 'bdrmapit_%s.db' %db_name
    os.system("bdrmapit -o %s -c config_%s.json" %(db_filename, date))
    #return

#3 days per month
def RunBdrmapitPermonthAllVps(year, month):
    date = str(year) + str(month).zfill(2)
    PrepareBdrmapit(year, month)
    with open('config_%s.json' %date, 'w') as wf:
        wf.write('{\n    "$schema": "schema.json",\n    "ip2as": "ip2as.prefixes_%s",\n    "as2org": {\n        "as2org": "as2org-file_%s"\n    },\n    "as-rels": {\n        "rels": "rels-file_%s",\n        "cone": "cone-file_%s"\n    },\n    "warts": {\n        "files": "warts_%s.files"\n    },\n    "processes": 3\n}\n' %(date, date, date, date, date))
    db_filename = 'bdrmapit_%s.db' %date
    # if os.path.exists(db_filename) and os.path.getsize(db_filename) > 0:
    #     return
    with open('warts_%s.files' %date, 'w') as wf:        
        output = os.popen('ls *%s*warts' %date)
        data = output.read()
        for filename in data.strip('\n').split('\n'):
            wf.write("%s\n" %filename)
    os.system("bdrmapit -o %s -c config_%s.json" %(db_filename, date))

if __name__ == '__main__':
    PreGetSrcFilesInDirs()
    RunBdrmapit()
    #RunBdrmapitGroupOneyearPerVP()
    #RunBdrmapitGroupOneyearAllVP()
    #RunBdrmapitForJzt()
    #RunBdrmapitForAllTrace()
    # thread_list = []
    # os.chdir(global_var.all_trace_par_path + global_var.all_trace_download_dir + 'back/')
    # #for year in range(2018,2021):
    # for year in range(2019,2020):
    #     for month in range(2, 3):
    #         if (year == 2020 and month > 4): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
    #             continue
    #         #thread_list.append(mp.Process(target=RunBdrmapitForAllTrace_2, args=(year, month)))
    #         thread_list.append(mp.Process(target=RunBdrmapitPermonthAllVps, args=(year, month)))
    #         #RunBdrmapitPermonthAllVps(year, month)
    # for thread in thread_list:
    #     thread.start()
    # for thread in thread_list:
    #     thread.join()
    #RunBdrmapitForAllTrace_2(2020, 2)
    #RunBdrmapitForSpecTraces(2020, 2, 'warts_mty-mx.files', 'mty-mx_2020')
    #RunBdrmapitPermonthAllVps(2020, 2)


