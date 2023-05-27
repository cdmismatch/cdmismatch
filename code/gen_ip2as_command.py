
import os
import global_var

prefix2as_files = []
as2org_files = []
rel_cc_files = []
peeringdb_files = []

def PreGetSrcFilesInDirs():
    global prefix2as_files
    global as2org_files
    global rel_cc_files
    global peeringdb_files

    for root,dirs,files in os.walk(global_var.par_path + global_var.prefix2as_dir):
        prefix2as_files = files[:]
    for root,dirs,files in os.walk(global_var.par_path + global_var.as2org_dir):
        as2org_files = files[:]
    for root,dirs,files in os.walk(global_var.par_path + global_var.rel_cc_dir):
        rel_cc_files = files[:]
    for root,dirs,files in os.walk(global_var.par_path + global_var.peeringdb_dir):
        peeringdb_files = files[:]

def FindFilename(year, month, day, filetype):
    global prefix2as_files
    global as2org_files
    global rel_cc_files
    global peeringdb_files

    month_str = str(month).zfill(2)
    day_str = str(day)
    if day < 10:
        day_str = '0' + day_str
    if filetype == global_var.prefix2as_flag:
        filename = str(year) + month_str + day_str + '.pfx2as'
        if filename in prefix2as_files:
            return filename
    elif filetype == global_var.as2org_flag:
        filename = str(year) + month_str + day_str + '.as-org2info.txt'
        #print(filename)
        if filename in as2org_files:
            return filename
    elif filetype == global_var.rel_flag:
        filename = str(year) + month_str + day_str + '.as-rel3.txt'
        if filename in rel_cc_files:
            return filename
    elif filetype == global_var.cone_flag:
        filename = str(year) + month_str + day_str + '.ppdc-ases.txt'
        if filename in rel_cc_files:
            return filename
    elif filetype == global_var.peeringdb_flag:
        filename = ''
        if year < 2018 or (year == 2018 and month < 3): #2018.3前peeringdb是sqlite格式
            filename = 'peeringdb_' + str(year) + '_' + month_str + '_' + day_str + '.sqlite'
        else: #2018.3后peeringdb是json格式
            filename = 'peeringdb_' + str(year) + '_' + month_str + '_' + day_str + '.json'
        if filename in peeringdb_files:
            return filename
    elif filetype == global_var.asrank_flag:
        filename = str(year) + month_str + day_str + '.asrank.txt'
        if filename in rel_cc_files:
            return filename
    else:
        print('file type error!')
    return None

def GetCloseDateFileSub(year, month, filetype):
    day = 15
    for i in range(0, 15):
        left_day = day - i
        if left_day > 0:
            filename = FindFilename(year, month, left_day, filetype)
            if filename:
                return filename
        right_day = day + i
        if right_day < 31:
            filename = FindFilename(year, month, right_day, filetype)
            if filename:
                return filename
    return None

def GetCloseDateFile(year, month, filetype):
    #print("filetype: %s" %filetype)
    for i in range(0, 12):
        #print("i: %d" %i)
        left_month = month - i
        if left_month > 0:
            #print("left_month: %d" %left_month)
            filename = GetCloseDateFileSub(year, left_month, filetype)
            if filename:
                return filename            
        right_month = month + i
        if right_month < 13:
            #print("right_month: %d" %right_month)
            filename = GetCloseDateFileSub(year, right_month, filetype)
            if filename:
                return filename    
    return None

#ip2as -p rib.prefixes -R rels-file -c cone-file \ -a as2org -file -P peeringdb.json -o ip2as.prefixes
def GenIp2AsCommand():
    PreGetSrcFilesInDirs()
    output_dir = global_var.par_path + global_var.out_ip2as_dir
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    wf = open('ip2as_commands.sh', 'w')
    wf.write('#!/bin/sh\n\n')
    for year in range(2016,2022):
        year_str = str(year)
        for month in range(1,13):
            if year == 2016 and month < 4:
                continue    #2016.4前的peeringdb数据不准，不做实验
            if year > 2018 or (year == 2018 and month > 3):
                continue #2021.5.8 之前做过了，不再做，以后重新做实验的时候可以解注释
            month_str = str(month).zfill(2)
            #print("year: %s, month: %s" %(year, month))
            prefix2as_filename = GetCloseDateFile(year, month, global_var.prefix2as_flag)
            as2org_filename = GetCloseDateFile(year, month, global_var.as2org_flag)
            rel_filename = GetCloseDateFile(year, month, global_var.rel_flag)
            cone_filename = GetCloseDateFile(year, month, global_var.cone_flag)
            peeringdb_filename = GetCloseDateFile(year, month, global_var.peeringdb_flag)
            if not prefix2as_filename or not as2org_filename or not rel_filename or not cone_filename or not peeringdb_filename:
                print("%s %s failed to get files %s %s %s %s %s" %(year, month, prefix2as_filename, as2org_filename, rel_filename, cone_filename, peeringdb_filename))
                continue #没有找到相应文件，不产生ip2as
            command = "ip2as -p %s -R %s -c %s -a %s -P %s -o %s15.ip2as.prefixes\n" \
                        %(global_var.par_path + global_var.prefix2as_dir + prefix2as_filename, \
                            global_var.par_path + global_var.rel_cc_dir + rel_filename, \
                            global_var.par_path + global_var.rel_cc_dir + cone_filename, \
                            global_var.par_path + global_var.as2org_dir + as2org_filename, \
                            global_var.par_path + global_var.peeringdb_dir + peeringdb_filename, output_dir + year_str + month_str)
            #print(command, endings='')
            wf.write("echo '%s'\n" %command)
            wf.write(command)
    wf.close()

def PrepareBdrmapit(year, month):
    year_str = str(year)
    month_str = str(month).zfill(2)
    ip2as_prefix_filename = global_var.par_path + global_var.out_ip2as_dir + year_str + month_str + '15.ip2as.prefixes'
    #ip2as_prefix_filename = global_var.par_path + global_var.out_ip2as_dir + '20190115.ip2as.prefixes'
    as2org_filename = global_var.par_path + global_var.as2org_dir + GetCloseDateFile(year, month, global_var.as2org_flag)
    rel_filename = global_var.par_path + global_var.rel_cc_dir + GetCloseDateFile(year, month, global_var.rel_flag)
    cone_filename = global_var.par_path + global_var.rel_cc_dir + GetCloseDateFile(year, month, global_var.cone_flag)
    # os.system("cp %s ip2as.prefixes" %ip2as_prefix_filename)
    # os.system("cp %s as2org-file" %as2org_filename)
    # os.system("cp %s rels-file" %rel_filename)
    # os.system("cp %s cone-file" %cone_filename)
    date = year_str + month_str
    os.system("cp %s ip2as.prefixes_%s" %(ip2as_prefix_filename, date))
    os.system("cp %s as2org-file_%s" %(as2org_filename, date))
    os.system("cp %s rels-file_%s" %(rel_filename, date))
    os.system("cp %s cone-file_%s" %(cone_filename, date))
                
if __name__ == '__main__':
    GenIp2AsCommand()
