from audioop import avg
from email.policy import default
import os
import re
#from graphql import Location
#from turtle import width
#from matplotlib.font_manager import _Weight
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.patches import Polygon
import matplotlib.ticker as mtick
import numpy as np
import sys
from scipy import stats
import statsmodels.api as sm
import glob
import json
from collections import Counter, defaultdict

import global_var
from get_ip2as_from_bdrmapit import ConnectToBdrMapItDb, GetIp2ASFromBdrMapItDb,CloseBdrMapItDb, InitBdrCache, \
                                    ConstrBdrCache
from debug import CmpLoopRate
from utils_v2 import GetIxpPfxDict_2, ClearIxpPfxDict, IsIxpIp

def CheckUndo():    
    path = global_var.par_path + global_var.out_my_anatrace_dir
    os.chdir(path)
    #syd-au_20160415
    for vp in global_var.vps:   #vp
        for year in range(2016,2021):
            for month in range(1,13):
                if (year == 2016 and month < 4) or (year == 2020 and month > 4): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
                    continue
                month_str = str(month).zfill(2)              
                date = str(year) + month_str + '15'   #date
                for method in global_var.map_methods:   #method
                    #syd-au_20160415/ribs/record_syd-au.20160415_ribs
                    filepath = vp + '_' + date + '/' + method + '/record_' + vp + '.' + date + '_' + method
                    #print(filepath)
                    if not os.path.isfile(filepath):
                        print('NOTE1!' + vp + date + method)
                        continue
                    with open(filepath, 'r') as f:
                        res = f.read()
                    re_res = re.findall('In ab_filter1, ab num: (\d.*), ab precent: (\d\.\d.*)', res)
                    if not re_res:
                        print('NOTE2!' + vp + date + method)
                    elif float(re_res[0][1]) == 0.0:
                        print('NOTE3!' + vp + date + method)

def CollectMatchIpStat():
    path = global_var.par_path + global_var.out_my_anatrace_dir
    os.chdir(path)
    
    for vp in global_var.vps:   #vp
        for method in global_var.map_methods:   #method
            os.system("cat %s*/%s/record2_* > collect_record2_%s_%s" %(vp, method, vp, method))

def StatNobgp():
    path = global_var.par_path + global_var.out_my_anatrace_dir
    os.chdir(path)
    wr_dir = 'statistics'
    if not os.path.isdir(wr_dir):
        os.makedirs(wr_dir)
    wf = open(wr_dir + '/no_bgp_stat', 'w')
    wf.write("#format: vp_date: no_bgp_percent total_num\n")
    for root,dirs,files in os.walk(path):
        for cur_dir in dirs:
            #if cur_dir == wr_dir:
            if cur_dir != 'nrt-jp_20180415':
                continue
            res = os.popen('wc -l ' + cur_dir + '/ribs/0_no_bgp').readline()
            #print(res) #'89029 nrt-jp_20180415/ribs/0_no_bgp'
            no_bgp_num = int(res.split(' ')[0])
            #print(no_bgp_num)
            res = os.popen("grep -r \"Total\" %s/ribs/record*" %cur_dir).readline()
            #print(res)
            total_num = int(res.split(':')[1].strip('\n').strip(' '))
            #print(total_num)
            wf.write("%s: %f %d\n" %(cur_dir, no_bgp_num / total_num, total_num))
    wf.close()

def StatIxp():
    path = global_var.par_path + global_var.out_my_anatrace_dir
    os.chdir(path)
    wr_dir = 'statistics'
    if not os.path.isdir(wr_dir):
        os.makedirs(wr_dir)
    wf = open(wr_dir + '/ixp', 'w')
    wf.write("#format: vp_date: ixp_percent total_num\n")
    for root,dirs,files in os.walk(path):
        for cur_dir in dirs:
            #if cur_dir == wr_dir:
            if cur_dir != 'nrt-jp_20180415':
                continue
            tmp = cur_dir.replace('_', '.')
            with open("%s/ribs/record_%s_ribs" %(cur_dir, tmp), 'r') as f:
                res = f.read()
            re_res = re.findall("Total valid trace num: (\d.*?)\n", res, re.DOTALL)
            total_num = int(re_res[0])
            re_res = re.findall("ixp num: (\d.*?)\n", res, re.DOTALL)
            ixp_num = int(re_res[0])
            wf.write("%s: %f %d\n" %(cur_dir, ixp_num / total_num, total_num))
            print("%s: %f %d" %(cur_dir, ixp_num / total_num, total_num))
    wf.close()

def StatMultipath():
    path = global_var.par_path + global_var.out_my_anatrace_dir
    os.chdir(path)
    wr_dir = 'statistics'
    if not os.path.isdir(wr_dir):
        os.makedirs(wr_dir)
    wf = open(wr_dir + '/multipath', 'w')
    wf.write("#format: vp_date: multipath_percent total_num\n")
    for root,dirs,files in os.walk('.'):
        for cur_dir in dirs:
            if cur_dir == wr_dir:
            #if cur_dir != 'nrt-jp_20180415':
                continue
            for sub_root,sub_dirs,sub_files in os.walk(cur_dir):
                for cur_method in sub_dirs:
                    tmp = cur_dir.replace('_', '.')
                    if not os.path.isfile("%s/%s/record_%s_%s" %(cur_dir, cur_method, tmp, cur_method)):
                        continue
                    with open("%s/%s/record_%s_%s" %(cur_dir, cur_method, tmp, cur_method), 'r') as f:
                        res = f.read()
                    re_res = re.findall("Total valid trace num: (\d.*?)\n", res, re.DOTALL)
                    total_num = 0
                    if re_res:
                        total_num = int(re_res[0])
                    multipath_num = 0
                    re_res = re.findall("still multi num: (\d.*?)\n", res, re.DOTALL)
                    if re_res:
                        multipath_num = int(re_res[0])
                    else:
                        re_res = re.findall("multi num: (\d.*?)\n", res, re.DOTALL)
                        if re_res:
                            multipath_num = int(re_res[0])
                    if total_num == 0:
                        wf.write("%s_%s: 0 0\n" %(cur_dir, cur_method))
                        print("%s_%s: 0 0" %(cur_dir, cur_method))
                    else:
                        wf.write("%s: %f %d\n" %(cur_dir, multipath_num / total_num, total_num))
                        #print("%s: %f %d" %(cur_dir, multipath_num / total_num, total_num))
    wf.close()

def StatAb():
    path = global_var.par_path + global_var.out_my_anatrace_dir
    os.chdir(path)
    wr_dir = 'statistics'
    if not os.path.isdir(wr_dir):
        os.makedirs(wr_dir)
    wf = open(wr_dir + '/ab', 'w')
    wf.write("#format: vp_date_method: ab_0_percent ab_1_percent total_num\n")
    for root,dirs,files in os.walk('.'):
        for cur_dir in dirs:
            if cur_dir == wr_dir:
            #if cur_dir != 'nrt-jp_20180415':
                continue
            for sub_root,sub_dirs,sub_files in os.walk(cur_dir):
                for cur_method in sub_dirs:
                    tmp = cur_dir.replace('_', '.')
                    if not os.path.isfile("%s/%s/record_%s_%s" %(cur_dir, cur_method, tmp, cur_method)):
                        continue
                    with open("%s/%s/record_%s_%s" %(cur_dir, cur_method, tmp, cur_method), 'r') as f:
                        res = f.read()
                    re_res = re.findall("Total valid trace num: (\d.*?)\n", res, re.DOTALL)
                    total_num = 0
                    if re_res:
                        total_num = int(re_res[0])
                    ab_1 = 0
                    ab_0 = 0
                    re_res = re.findall('In ab_filter1, ab num: (\d.*), ab precent: (0\.\d.*)', res)
                    if re_res:
                        ab_1 = int(re_res[0][0])
                        #print(re_res[0][0])
                        ab_0 = int(int(re_res[0][0]) / float(re_res[0][1]))
                        #print(re_res[0][1])
                    if total_num == 0:
                        wf.write("%s_%s: 0 0 0\n" %(cur_dir, cur_method))
                        print("%s_%s: 0 0 0" %(cur_dir, cur_method))
                    else:
                        wf.write("%s_%s: %f %f %d\n" %(cur_dir, cur_method, ab_0 / total_num, ab_1 / total_num, total_num))
                        print("%s_%s: %f %f %d" %(cur_dir, cur_method, ab_0 / total_num, ab_1 / total_num, total_num))
    wf.close()

def CalDateIndex(date):
    year = int(date[0:4])
    month = int(date[4:6])
    return ((year - 2016) * 12 + month - 4)

#vps = ['nrt-jp', 'per-au', 'syd-au', 'zrh2-ch']
def PlotAbStat():
    #2016.4~2020.4, four years + 1 month, 49 months, thus each vp has 49 time-plot res
    res = dict()
    time_plots = 49
    for vp in global_var.vps:
        res[vp] = dict()#[dict() for i in range(0, 2)] #ab_0, ab_1
        for method in global_var.map_methods:
            res[vp][method] = [] #每个method有ab_0, ab_1两种结果
            for i in range(0, 2): #每个ab_i有49个时间点结果
                res[vp][method].append([0.0 for j in range(0, time_plots)])
    rf = open(global_var.par_path + global_var.out_my_anatrace_dir + '/statistics/ab', 'r')
    curline = rf.readline()
    while curline:
        if curline.startswith('#'):
            curline = rf.readline()
            continue
        #format: vp_date_method: ab_0_percent ab_1_percent total_num
        elems = curline.split(' ')
        sub_elems = elems[0].strip(':').split('_')
        vp = sub_elems[0]
        index = CalDateIndex(sub_elems[1])
        method = sub_elems[2]
        ab_0 = float(elems[1])
        ab_1 = float(elems[2])
        #print(index)
        res[vp][method][0][index] = ab_0
        res[vp][method][1][index] = ab_1
        curline = rf.readline()
    
    color_list = ['#000000', '#0000FF', '#8A2BE2', '#A52A2A', '#DEB887', '#5F9EA0', '#7FFF00', '#D2691E', '#FF7F50', '#6495ED', '#FFF8DC', '#DC143C', '#00FFFF', '#00008B', '#008B8B', '#B8860B', '#A9A9A9', '#006400', '#BDB76B', '#8B008B', '#556B2F', '#FF8C00', '#9932CC', '#8B0000', '#E9967A', '#8FBC8F', '#483D8B', '#2F4F4F', '#00CED1', '#9400D3', '#FF1493', '#00BFFF', '#696969', '#1E90FF', '#B22222', '#FFFAF0', '#228B22', '#FF00FF', '#DCDCDC', '#F8F8FF', '#FFD700', '#DAA520', '#808080', '#008000', '#ADFF2F', '#F0FFF0', '#FF69B4', '#CD5C5C', '#4B0082', '#FFFFF0', '#F0E68C', '#E6E6FA', '#FFF0F5', '#7CFC00', '#FFFACD', '#ADD8E6', '#F08080', '#E0FFFF', '#FAFAD2', '#90EE90', '#D3D3D3', '#FFB6C1', '#FFA07A', '#20B2AA', '#87CEFA', '#778899', '#B0C4DE', '#FFFFE0', '#00FF00', '#32CD32', '#FAF0E6', '#FF00FF', '#800000', '#66CDAA', '#0000CD', '#BA55D3', '#9370DB', '#3CB371', '#7B68EE', '#00FA9A', '#48D1CC', '#C71585', '#191970', '#F5FFFA', '#FFE4E1', '#FFE4B5', '#FFDEAD', '#000080', '#FDF5E6', '#808000', '#6B8E23', '#FFA500', '#FF4500', '#DA70D6', '#EEE8AA', '#98FB98', '#AFEEEE', '#DB7093', '#FFEFD5', '#FFDAB9', '#CD853F', '#FFC0CB', '#DDA0DD', '#B0E0E6', '#800080', '#FF0000', '#BC8F8F', '#4169E1', '#8B4513', '#FA8072', '#FAA460', '#2E8B57', '#FFF5EE', '#A0522D', '#C0C0C0', '#87CEEB', '#6A5ACD', '#708090', '#FFFAFA', '#00FF7F', '#4682B4', '#D2B48C', '#008080', '#D8BFD8', '#FF6347', '#40E0D0', '#EE82EE', '#F5DEB3', '#FFFFFF', '#F5F5F5', '#FFFF00', '#9ACD32']
    marker_list = ['o', 'v', '^', '<', '>', '1', '2', '3', '4', 's', 'p', '*', 'h', 'H', '+', 'x', 'D', 'd']
    fig = plt.figure()    
    #设置X轴标签  
    plt.xlabel('date')  
    #设置Y轴标签  
    plt.ylabel('ab_percent') 
    ax = []
    for i in range(0, 4):
        ax.append(fig.add_subplot(221 + i))
    i = 0
    for vp in global_var.vps:
        j = 0
        for method in global_var.map_methods:
            for ab_i in range(0, 2):
                ax[i].scatter(range(0, time_plots), res[vp][method][0], c = color_list[j], marker= marker_list[ab_i], label = method + '_' + str(ab_i))
            j += 1
        i += 1  
    #plt.legend(loc='upper left')
    plt.show()

def CalMatchIpStat():
    path = global_var.par_path + global_var.out_my_anatrace_dir + '/statistics/matchip_stat/'
    os.chdir(path)
    for root,dirs,files in os.walk('.'):
        for filename in files:
            print(filename)
            with open(filename, 'r', encoding='unicode_escape') as f:
                res = f.read()
            re_res = re.findall(", match rate: (.*?), unmatch rate: (.*?), unknown rate: (.*?),", res)
            if not re_res:
                print('Format error 1!')
                continue
            sum_stat = [0 for i in range(3)]
            max_stat = [0 for i in range(3)]
            min_stat = [1 for i in range(3)]
            for elem in re_res:
                for i in range(0, 3):
                    tmp = float(elem[i])
                    sum_stat[i] += tmp
                    if max_stat[i] < tmp:
                        max_stat[i] = tmp
                    if min_stat[i] > tmp:
                        min_stat[i] = tmp
            num1 = len(re_res)
            re_res = re.findall("avg_ip_freq_true: (.*?), avg_ip_freq_false: (.*?), avg_ip_freq_unknown: (.*?)\n", res, re.DOTALL)
            if not re_res:
                print('Format error 2!')
                continue
            sum_freq_stat = [0 for i in range(3)]
            max_freq_stat = [0 for i in range(3)]
            min_freq_stat = [100 for i in range(3)]
            for elem in re_res:
                for i in range(0, 3):
                    tmp = float(elem[i])
                    sum_freq_stat[i] += tmp
                    if max_freq_stat[i] < tmp:
                        max_freq_stat[i] = tmp
                    if min_freq_stat[i] > tmp:
                        min_freq_stat[i] = tmp
            num2 = len(re_res)
            print("\tmatch: %.2f(%.2f, %.2f), %.2f(%.2f, %.2f)" %(sum_stat[0] / num1, max_stat[0], min_stat[0], sum_freq_stat[0] / num2, max_freq_stat[0], min_freq_stat[0]))
            print("\tunmatch: %.2f(%.2f, %.2f), %.2f(%.2f, %.2f)" %(sum_stat[1] / num1, max_stat[1], min_stat[1], sum_freq_stat[1] / num2, max_freq_stat[1], min_freq_stat[1]))
            print("\tunknown: %.2f(%.2f, %.2f), %.2f(%.2f, %.2f)" %(sum_stat[2] / num1, max_stat[2], min_stat[2], sum_freq_stat[2] / num2, max_freq_stat[2], min_freq_stat[2]))

def Tmp():
    record_file_name = '/mountdisk1/ana_c_d_incongruity/out_my_anatrace/nrt-jp_20181015/ribs_midar_bdrmapit/record_nrt-jp.20181015_ribs_midar_bdrmapit'
    with open(record_file_name, 'r') as f:
        res = f.read()
        re_res = re.findall('Total valid trace num: (\d.*)', res)
        if not re_res:
            print('NOTE2!' + vp + date + method)
        else:
            print(int(re_res[0]))

def StatClassify():    
    par_dir = global_var.par_path +  global_var.out_my_anatrace_dir
    os.chdir(par_dir)
    dir_list = os.listdir(par_dir)
    stat_dict = dict()
    classes = ['last_extra', 'first_hop_ab', 'detour', 'bifurc']
    for vp in global_var.vps:
        stat_dict[vp] = dict()
        for cur_class in classes:
            stat_dict[vp][cur_class] = []
        for cur_dir in dir_list:
            if os.path.isdir(os.path.join(par_dir, cur_dir)) and cur_dir.__contains__(vp) and \
            (cur_dir.__contains__('2018') or cur_dir.__contains__('2019')):
                filename = cur_dir + '/ribs_midar_bdrmapit/ana_record_' + cur_dir.replace('_', '.')
                with open(filename, 'r') as rf:
                    data = rf.read()
                re_res = re.findall('last_extra num: (\d.*), percent: (0\.\d.*)\n', data)
                stat_dict[vp]['last_extra'].append(float(re_res[0][1]))
                re_res = re.findall('first_hop_ab num: (\d.*), percent: (0\.\d.*)\n', data)
                stat_dict[vp]['first_hop_ab'].append(float(re_res[0][1]))
                re_res = re.findall('detour num: (\d.*), percent: (0\.\d.*)\n', data)
                stat_dict[vp]['detour'].append(float(re_res[0][1]))
                re_res = re.findall('bifurc num: (\d.*), percent: (0\.\d.*)\n', data)
                stat_dict[vp]['bifurc'].append(float(re_res[0][1]))
    for vp in global_var.vps:
        print(vp)
        for cur_class in classes:
            print("%.2f %.2f %.2f" %(np.mean(stat_dict[vp][cur_class]), np.min(stat_dict[vp][cur_class]), np.max(stat_dict[vp][cur_class])))
     
def StatLastExtraHopPerVP():    
    par_dir = global_var.par_path +  global_var.out_my_anatrace_dir
    os.chdir(par_dir)
    dir_list = os.listdir(par_dir)
    stat_dict = dict()
    classes = ['customer', 'provider', 'peer', 'unknown', 'multi']
    for vp in global_var.vps:
        stat_dict[vp] = dict()
        for cur_class in classes:
            stat_dict[vp][cur_class] = []
        for cur_dir in dir_list:
            if os.path.isdir(os.path.join(par_dir, cur_dir)) and cur_dir.__contains__(vp) and \
            (cur_dir.__contains__('2018') or cur_dir.__contains__('2019')):
                filename = cur_dir + '/ribs_midar_bdrmapit/ana_record_' + cur_dir.replace('_', '.')
                with open(filename, 'r') as rf:
                    data = rf.read()
                re_res = re.findall('reach_dst_last_extra, customer: (\d.*), percent: (0\.\d.*)\n', data)
                stat_dict[vp]['customer'].append(float(re_res[0][1]))
                re_res = re.findall('reach_dst_last_extra, provider: (\d.*), percent: (0\.\d.*)\n', data)
                stat_dict[vp]['provider'].append(float(re_res[0][1]))
                re_res = re.findall('reach_dst_last_extra, peer: (\d.*), percent: (0\.\d.*)\n', data)
                stat_dict[vp]['peer'].append(float(re_res[0][1]))
                re_res = re.findall('reach_dst_last_extra, unknown: (\d.*), percent: (0\.\d.*)\n', data)
                stat_dict[vp]['unknown'].append(float(re_res[0][1]))
                re_res = re.findall('reach_dst_last_extra, multi: (\d.*), percent: (0\.\d.*)\n', data)
                stat_dict[vp]['multi'].append(float(re_res[0][1]))
    for vp in global_var.vps:
        print(vp)
        for cur_class in classes:
            print("%.2f %.2f %.2f" %(np.mean(stat_dict[vp][cur_class]), np.min(stat_dict[vp][cur_class]), np.max(stat_dict[vp][cur_class])))
                   
def StatLastExtraHop():    
    par_dir = global_var.par_path +  global_var.out_my_anatrace_dir
    os.chdir(par_dir)
    dir_list = os.listdir(par_dir)
    stat_dict = dict()
    classes = ['customer', 'provider', 'peer', 'unknown', 'multi']
    for cur_class in classes:
        stat_dict[cur_class] = []
    for vp in global_var.vps:
        for cur_dir in dir_list:
            if os.path.isdir(os.path.join(par_dir, cur_dir)) and cur_dir.__contains__(vp) and \
            (cur_dir.__contains__('2018') or cur_dir.__contains__('2019')):
                filename = cur_dir + '/ribs_midar_bdrmapit/ana_record_' + cur_dir.replace('_', '.')
                with open(filename, 'r') as rf:
                    data = rf.read()
                re_res = re.findall('reach_dst_last_extra, customer: (\d.*), percent: (0\.\d.*)\n', data)
                stat_dict['customer'].append(float(re_res[0][1]))
                re_res = re.findall('reach_dst_last_extra, provider: (\d.*), percent: (0\.\d.*)\n', data)
                stat_dict['provider'].append(float(re_res[0][1]))
                re_res = re.findall('reach_dst_last_extra, peer: (\d.*), percent: (0\.\d.*)\n', data)
                stat_dict['peer'].append(float(re_res[0][1]))
                re_res = re.findall('reach_dst_last_extra, unknown: (\d.*), percent: (0\.\d.*)\n', data)
                stat_dict['unknown'].append(float(re_res[0][1]))
                re_res = re.findall('reach_dst_last_extra, multi: (\d.*), percent: (0\.\d.*)\n', data)
                stat_dict['multi'].append(float(re_res[0][1]))
    for cur_class in classes:
        print("%.2f %.2f %.2f" %(np.mean(stat_dict[cur_class]), np.min(stat_dict[cur_class]), np.max(stat_dict[cur_class])))
         
def StatAbPercent():    
    par_dir = global_var.par_path +  global_var.out_my_anatrace_dir
    os.chdir(par_dir)
    dir_list = os.listdir(par_dir)
    stat_dict = dict()
    classes = ['ab', 'unmap', 'ab_percent', 'unmap_percent']
    for vp in global_var.vps:
        stat_dict[vp] = dict()
        for cur_class in classes:
            stat_dict[vp][cur_class] = []
        for cur_dir in dir_list:
            if os.path.isdir(os.path.join(par_dir, cur_dir)) and cur_dir.__contains__(vp) and \
            (cur_dir.__contains__('2018') or cur_dir.__contains__('2019')):
                filename = cur_dir + '/ribs_midar_bdrmapit/record_' + cur_dir.replace('_', '.') + '_ribs_midar_bdrmapit'
                with open(filename, 'r') as rf:
                    data = rf.read()
                re_res = re.findall('In total, ab num: (\d.*), unmap num: (\d.*), ab precent: (0\.\d.*), unmap precent: (0\.\d.*)\n', data)
                stat_dict[vp]['ab'].append(float(re_res[0][0]))
                stat_dict[vp]['unmap'].append(float(re_res[0][1]))
                stat_dict[vp]['ab_percent'].append(float(re_res[0][2]))
                stat_dict[vp]['unmap_percent'].append(float(re_res[0][3]))
    for vp in global_var.vps:
        print(vp)
        for cur_class in classes:
            print("%.4f %.4f %.4f" %(np.mean(stat_dict[vp][cur_class]), np.min(stat_dict[vp][cur_class]), np.max(stat_dict[vp][cur_class])))

def StatNeigborIp(filename, ip):
    rf = open(filename, 'r')
    curline_trace = rf.readline()
    left_nei = dict()
    right_nei = dict()

    while curline_trace:
        curline_ip = rf.readline()
        #print(curline_ip)
        if curline_ip.__contains__(ip):
            elems = curline_ip.strip('\n').split(']')[1].strip(' ').split(' ')
            index = elems.index(ip)
            if index > 0:
                if elems[index - 1] not in left_nei.keys():
                    left_nei[elems[index - 1]] = [0, '']
                left_nei[elems[index - 1]][0] += 1
            while index < len(elems) and (elems[index] == ip or elems[index].__contains__('<')):
                index += 1
            if index < len(elems):
                if elems[index] not in right_nei.keys():
                    right_nei[elems[index]] = [0, '']
                right_nei[elems[index]][0] += 1
        curline_trace = rf.readline()
    for (key, val) in left_nei.items():
        val[1] = GetIp2ASFromBdrMapItDb(key)
    for (key, val) in right_nei.items():
        val[1] = GetIp2ASFromBdrMapItDb(key)
    print('left neighbor: ')
    sort_list = sorted(left_nei.items(), key=lambda d:d[1][0], reverse=True)
    print(sort_list)
    print('right neighbor: ')
    sort_list = sorted(right_nei.items(), key=lambda d:d[1], reverse=True)
    print(sort_list)

def StatIncompleteTraces():
    os.chdir(global_var.par_path + global_var.traceroute_dir)
    for root,dirs,files in os.walk('.'):
        for filename in files:
            if filename.endswith('warts') or filename == 'process_trace.sh':
                continue
            total_num = 0
            aest_num = 0
            partial_num = 0
            with open(filename, 'r') as rf:
                curline = rf.readline()
                while curline:
                    if not curline.startswith('T'):
                        curline = rf.readline()
                        continue
                    total_num += 1
                    if curline.__contains__('q'):
                        aest_num += 1
                    elems = curline.strip('\n').split('\t')
                    dst_ip = elems[2]
                    if not elems[-1].__contains__(dst_ip):
                        partial_num += 1
                    curline = rf.readline()
                print('Total: %d, aest perc: %.2f, partial_perc: %.2f' %(total_num, aest_num * 100 / total_num, partial_num * 100 / total_num))

#vps = ['nrt-jp', 'per-au', 'syd-au', 'zrh2-ch']
def PlotAbStat_v2():
    #2016.4~2020.4, four years + 1 month, 49 months, thus each vp has 49 time-plot res
    res = dict()
    time_plots = 30
    os.chdir(global_var.par_path + 'tmp_out_my_anatrace/')
    for vp in global_var.vps:
        res[vp] = dict()#[dict() for i in range(0, 2)] #ab_0, ab_1
        for method in global_var.map_methods:
            #res[vp][method] = [0.0 for j in range(0, time_plots)]
            res[vp][method] = []
            for year in range(2018,2021):
                for month in range(1,13):
                    if (year == 2020 and month > 4): #2016年4月前peeringdb数据不准，2020年5月后的数据不全
                        continue
                    date = str(year) + str(month).zfill(2)
                    #offset = (year - 2018) * 12 + month - 1
                    grep_cmd = 'grep \'In total\' ' + vp + '_' + date + '15/' + method + '/' + 'record4_*'
                    output = os.popen(grep_cmd).read()
                    if output:
                        #res[vp][method][offset] = float(re.findall('ab precent: (.+?),', output)[0]) * 100
                        res[vp][method].append(float(re.findall('ab precent: (.+?),', output)[0]) * 100)
    
    #color_list = ['#000000', '#0000FF', '#8A2BE2', '#A52A2A', '#DEB887', '#5F9EA0', '#7FFF00', '#D2691E', '#FF7F50', '#6495ED', '#FFF8DC', '#DC143C', '#00FFFF', '#00008B', '#008B8B', '#B8860B', '#A9A9A9', '#006400', '#BDB76B', '#8B008B', '#556B2F', '#FF8C00', '#9932CC', '#8B0000', '#E9967A', '#8FBC8F', '#483D8B', '#2F4F4F', '#00CED1', '#9400D3', '#FF1493', '#00BFFF', '#696969', '#1E90FF', '#B22222', '#FFFAF0', '#228B22', '#FF00FF', '#DCDCDC', '#F8F8FF', '#FFD700', '#DAA520', '#808080', '#008000', '#ADFF2F', '#F0FFF0', '#FF69B4', '#CD5C5C', '#4B0082', '#FFFFF0', '#F0E68C', '#E6E6FA', '#FFF0F5', '#7CFC00', '#FFFACD', '#ADD8E6', '#F08080', '#E0FFFF', '#FAFAD2', '#90EE90', '#D3D3D3', '#FFB6C1', '#FFA07A', '#20B2AA', '#87CEFA', '#778899', '#B0C4DE', '#FFFFE0', '#00FF00', '#32CD32', '#FAF0E6', '#FF00FF', '#800000', '#66CDAA', '#0000CD', '#BA55D3', '#9370DB', '#3CB371', '#7B68EE', '#00FA9A', '#48D1CC', '#C71585', '#191970', '#F5FFFA', '#FFE4E1', '#FFE4B5', '#FFDEAD', '#000080', '#FDF5E6', '#808000', '#6B8E23', '#FFA500', '#FF4500', '#DA70D6', '#EEE8AA', '#98FB98', '#AFEEEE', '#DB7093', '#FFEFD5', '#FFDAB9', '#CD853F', '#FFC0CB', '#DDA0DD', '#B0E0E6', '#800080', '#FF0000', '#BC8F8F', '#4169E1', '#8B4513', '#FA8072', '#FAA460', '#2E8B57', '#FFF5EE', '#A0522D', '#C0C0C0', '#87CEEB', '#6A5ACD', '#708090', '#FFFAFA', '#00FF7F', '#4682B4', '#D2B48C', '#008080', '#D8BFD8', '#FF6347', '#40E0D0', '#EE82EE', '#F5DEB3', '#FFFFFF', '#F5F5F5', '#FFFF00', '#9ACD32']
    color_list = ['red', 'brown', 'blue']
    marker_list = ['o', 'D', '+']
    #fig = plt.figure()  )
    i = 0
    plt.figure()
    for vp in global_var.vps:
        j = 0        
        plt.subplot(511 + i)
        plt.ylim((0, 50))
        plt.xlim((0, 28))
        y_ticks = np.arange(0, 50, 10)
        # plt.ylim((0, 20))
        # y_ticks = np.arange(0, 20, 5)
        plt.tick_params(labelsize=9) 
        plt.yticks(y_ticks)
        frame = plt.gca()
        if i == 2:
            plt.ylabel('Mismatch rate (%)', fontsize=10)
        if i == 4:
            frame.axes.get_xaxis().set_visible(True)
            plt.xticks([0, 6, 12, 18, 24],[r'$2018.01$', r'$2018.06$', r'$2019.01$', r'$2019.06$', r'$2020.01$'])
            plt.xlabel('Date', fontsize=10)
        else:
            frame.axes.get_xaxis().set_visible(False)
        if i == 0:
            for method in global_var.map_methods:
                    #ax[i].scatter(range(0, time_plots), res[vp][method], c = color_list[j], marker= marker_list[ab_i], label = method + '_' + str(ab_i))
                if method == 'bdrmapit':
                    plt.scatter(range(0, len(res[vp][method])), res[vp][method], c = color_list[j], marker= marker_list[j], s = 8., label='%s'%method)
                else:
                    plt.scatter(range(0, len(res[vp][method])), res[vp][method], c = '', edgecolors=color_list[j], marker= marker_list[j], s = 8., label='%s'%method)
                #plt.text(x, y , '%s' %vp, ha='center', va='center')
                j += 1
            plt.legend(loc='upper right', borderpad=0.5, labelspacing=1, prop={'size': 9}, ncol=3)
        else:
            for method in global_var.map_methods:
                    #ax[i].scatter(range(0, time_plots), res[vp][method], c = color_list[j], marker= marker_list[ab_i], label = method + '_' + str(ab_i))
                if method == 'bdrmapit':
                    plt.scatter(range(0, len(res[vp][method])), res[vp][method], c = color_list[j], marker= marker_list[j], s = 8.)
                else:
                    plt.scatter(range(0, len(res[vp][method])), res[vp][method], c = '', edgecolors=color_list[j], marker= marker_list[j], s = 8.)
                j += 1
        ax2 = plt.twinx()  # this is the important function
        #ax2.set_ylim((0, 50))
        ax2.set_yticks([])
        ax2.set_yticklabels([])
        #ax2.set_visible(False)
        ax2.set_ylabel('            %s' %vp, rotation=0, fontsize=10)
        i += 1
    #plt.rcParams['figure.figsize'] = (1.0, 4.0)
    #plt(figsize=(8, 6), dpi=80)
    j = 0
    #'nrt-jp', 'per-au', 'syd-au', 'zrh2-ch', 'sjc2-us'
    plt.tight_layout()
    #plt.subplots_adjust(left=0, bottom=0, right=1, top=1, wspace=0, hspace=0)
    eps_fig = plt.gcf() # 'get current figure'
    eps_fig.savefig('comparison.eps', format='eps')
    plt.show()

def PlotCdf():
    arr = []
    with open(global_var.par_path +  global_var.out_my_anatrace_dir + '/last_as_in_last_extra', 'r') as rf:
        curline = rf.readline()
        i = 0
        while curline:
            #arr.append(float(curline[curline.index(',')+1:-1]))
            count = int(curline.strip('\n').split(': ')[-1])
            for k in range(0, count):
                arr.append(i)
            i += 1
            curline = rf.readline()
            # if i > 100:
            #     break
    ecdf = sm.distributions.ECDF(arr)
    x = np.linspace(min(arr), max(arr), len(arr))
    y = ecdf(x)
    #print(y)
    y_dot5 = np.linspace(0.5, 0.5, len(arr))
    y_dot9 = np.linspace(0.9, 0.9, len(arr))
    idx_dot5 = np.argwhere(np.diff(np.sign(y - y_dot5))).flatten()
    idx_dot9 = np.argwhere(np.diff(np.sign(y - y_dot9))).flatten()
    plt.axhline(y=0.5, xmin = 0.0, xmax = x[idx_dot5[0]]/x[-1], color="red", linestyle="--")
    plt.axhline(y=0.9, xmin = 0.0, xmax = x[idx_dot9[0]]/x[-1], color="red", linestyle="--")
    plt.axvline(x=x[idx_dot5[0]], ymin = 0.0, ymax = 0.5, color="red", linestyle="--")
    plt.axvline(x=x[idx_dot9[0]], ymin = 0.0, ymax = 0.9, color="red", linestyle="--")
    plt.text(x[idx_dot5[0]]+5, 0.48, '(%d,0.5)'%x[idx_dot5[0]], color="red")
    plt.text(x[idx_dot9[0]]+5, 0.86, '(%d,0.9)'%x[idx_dot9[0]], color="red")
    plt.xlim((0, x[-1]))
    plt.ylim((0,1))
    #plt.axvline(x=22, ymin=0.0, ymax=0.33, color="red", linestyle="--")
    plt.plot(x, y, color='black')
    # cdf = stats.cumfreq(arr)
    # plt.plot(cdf[0])
    plt.tick_params(labelsize=12)
    plt.tight_layout()
    eps_fig = plt.gcf() # 'get current figure'
    eps_fig.savefig('last_as_in_last_extra_cdf.eps', format='eps')
    plt.show()

def PlotCdf_2():
    color_list = ['yellow', 'black', 'blue', 'green', 'pink']
    max_x = 0
    j = 0
    for vp in global_var.vps:
        arr = []        
        with open(global_var.par_path + global_var.out_my_anatrace_dir + '/tmp_bifurc_' + vp, 'r') as rf:
            curline = rf.readline()
            curline = rf.readline()
            i = 0
            while curline:
                #arr.append(float(curline[curline.index(',')+1:-1]))
                count = int(curline[curline.index(':')+1:curline.index(',')])
                for k in range(0, count):
                    arr.append(i)
                i += 1
                curline = rf.readline()
                # if i > 100:
                #     break
        ecdf = sm.distributions.ECDF(arr)
        x = np.linspace(min(arr), max(arr), len(arr))
        y = ecdf(x)
        if x[-1] > max_x:
            max_x = x[-1]
        #print(y)
        # if vp == 'nrt-jp' or vp == 'sjc2-us':
        #     y_dot5 = np.linspace(0.5, 0.5, len(arr))
        #     idx_dot5 = np.argwhere(np.diff(np.sign(y - y_dot5))).flatten()
        #     plt.axhline(y=0.5, xmin = 0.0, xmax = x[idx_dot5[0]]/x[-1], color="red", linestyle="--")
        #     plt.axvline(x=x[idx_dot5[0]], ymin = 0.0, ymax = 0.5, color="red", linestyle="--")
        #     plt.text(x[idx_dot5[0]]+50, 0.48, '(%d,0.5)'%x[idx_dot5[0]], color="red")
        plt.plot(x, y, color=color_list[j], label='%s'%vp)
        j += 1
    plt.xlim((0,max_x))
    plt.ylim((0,1))
    plt.tick_params(labelsize=12)
    plt.legend(loc='lower right', borderpad=0.5, labelspacing=1, prop={'size': 12}, ncol=3)
    plt.tight_layout()
    eps_fig = plt.gcf() # 'get current figure'
    eps_fig.savefig('last_ab_cdf.eps', format='eps')
    plt.show()

def PlotArray(data, xlabel, ylabel, ylim, vp_divs, xticks2, xticklabels2, save_filename):
    #color_list = ['green', 'lightblue', 'blue', 'red', 'brown', 'black', 'gold']
    #marker_list = ['o', '*', '*', '^', 'v', 'p', 's']
    color_list = ['green', 'blue', 'red', 'brown', 'cyan', 'black']
    marker_list = ['*', '+', '^', '_', 'x', '-']
    
    #fig, ax = plt.subplots(figsize=(14, 4))
    fig, ax = plt.subplots(figsize=(8, 8))
    #fig, ax = plt.subplots()
    #plt.ylim((0, 0.1))
    ax.set_ylim((ylim[0], ylim[1]))
    ax.set_xlim(0, len(data['ori_bdr']))
    dates_str = []
    cur_date = '20220228'
    for year in range(2018, 2023):
        dates_str.append(str(year) + '.01')
        if year == 2021:
            dates_str.append('2021.10')
            dates_str.append('2022.4')
            break
        dates_str.append(str(year) + '.07')
    dates_str = dates_str[:-1]
    dates_tick = [i*6 for i in range(len(dates_str))]
    ax.set_xticks(dates_tick)
    ax.set_xticklabels(dates_str)
    # ax.set_xticks(xticks2)
    # ax.tick_params(color='w', labelsize=10)
    # ax.set_xticklabels(xticklabels2)
    #ax.set_xlabel(xlabel, fontsize=8)
    ax.set_ylabel('mismatch rate', fontsize=10)
    ax.yaxis.set_major_formatter(mtick.FormatStrFormatter('%.2f'))
    i = 0
    #for map_method in ['coa_rib_based', 'midar_approx', 'midar', 'ori_bdr', 'hoiho_l_bdr', 'snmp_bdr', 'sxt_bdr']:
    #for map_method in ['coa_rib_based', 'midar', 'ori_bdr', 'hoiho_s_bdr', 'snmp_bdr', 'sxt_bdr']:
    #for map_method in ['sxt_bdr_before_rect', 'sxt_bdr']:
    for map_method in ['ori_bdr']:
        vals = data[map_method]
        label = None
        if map_method == 'coa_rib_based':
            label = 'Rib-based'
        elif map_method == 'midar':
            label = 'Midar+BdrmapIt'
        elif map_method == 'midar_approx':
            label = 'Midar+BdrmapIt-approximate'
        elif map_method == 'ori_bdr':
            label = 'BdrmapIt'
        elif map_method == 'hoiho_s_bdr':
            label = 'Hoiho+BdrmapIt'
        elif map_method == 'sxt_bdr':
            label = 'BdrmapIt_improved'
        elif map_method == 'snmp_bdr':
            label = 'SNMPv3+BdrmapIt'
        print(color_list[i])
        if map_method == 'midar' or map_method == 'snmp_bdr' or map_method == 'hoiho_s_bdr':
            ax.scatter(range(0, len(vals)), vals, marker= marker_list[i], color=color_list[i], s = 40., label=label)
        elif map_method == 'sxt_bdr':
            #ax.plot(range(0, len(vals)), vals, color=color_list[i],alpha=0.3,linestyle='--',marker=marker_list[i],markeredgecolor=color_list[i],linewidth=2,markersize='5', label=label, markerfacecolor='none')
            ax.plot(range(0, len(vals)), vals, color=color_list[i],alpha=0.3,linestyle='--',marker='',linewidth=1,label=label)
        else:
            ax.scatter(range(0, len(vals)), vals, facecolors = 'none', marker= marker_list[i], edgecolors=color_list[i], s = 20., label=label)
        #plt.plot(range(0, len(vals)), vals, c = color_list[i], marker= marker_list[i], label='%s'%map_method)
        i += 1
    #ax.legend(loc='upper right', borderpad=1, labelspacing=1, prop={'size': 10}, ncol=3, framealpha = 1, markerscale = 1)
    #ax.text(23, data['sxt_bdr'][22]-0.05, '2019.11')
    #ax.text(6, 0.64, '2018.11')
    ax.text(7.5, 0.77, '2018.11')
    ax.text(16, 0.94, '2019.07')
    ax.text(4, 0.86, 'ams-nl-1')
    ax.annotate("", xy=(0, 0.85), xycoords='data',
            xytext=(18.6, 0.85), textcoords='data',
            arrowprops=dict(arrowstyle="<->", connectionstyle="arc3"),)
    ax.text(29, 0.86, 'ams-nl-2')
    ax.annotate("", xy=(17.5, 0.85), xycoords='data',
            xytext=(48, 0.85), textcoords='data',
            arrowprops=dict(arrowstyle="<->", connectionstyle="arc3"),)
    # ax.text(11.5, 0.5, 'part1')
    # ax.text(25, 0.5, 'part2')
    # ax.text(2.5, 0.62, 'ams-nl-1-low')
    # ax.text(11, 0.62, 'ams-nl-1-high')
    #ax.text(25, 0.45, 'part2')

    # ax.annotate("", xy=(13, 0.16), xycoords='data',
    #         xytext=(13.8, 0.10), textcoords='data',
    #         arrowprops=dict(arrowstyle="->", connectionstyle="arc3"), )
    # ax.text(10.5, 0.01, '2019.02\n2019.03')    
    # ax.annotate("", xy=(22.5, 0.14), xycoords='data',
    #         xytext=(24, 0.08), textcoords='data',
    #         arrowprops=dict(arrowstyle="->", connectionstyle="arc3"), )
    # ax.text(22, 0.04, '2019.11')

    ax.vlines(vp_divs, 0, 1, linestyles='solid', colors='black', linewidth=0.6)
    ax.vlines([10], 0, 0.75, linestyles='dashed', colors='black', linewidth=0.6)
    #ax.vlines([10], 0, 0.62, linestyles='dashed', colors='black', linewidth=0.6)
    ax.vlines([18], 0, 0.92, linestyles='dashed', colors='black', linewidth=0.6)
    plt.tight_layout()
    #plt.sca(ax)
    ax.plot()
    plt.show()
    #eps_fig = plt.gcf() # 'get current figure'
    #eps_fig.savefig(save_filename, format='eps')
    fig.savefig(save_filename)
    
    
def PlotArray_v2(data, xlabel, ylabel, ylim, vp_divs, xticks2, xticklabels2, save_filename):
    #color_list = ['green', 'lightblue', 'blue', 'red', 'brown', 'black', 'gold']
    #marker_list = ['o', '*', '*', '^', 'v', 'p', 's']
    color_list = ['green', 'blue', 'red', 'brown', 'cyan', 'black']
    marker_list = ['*', 'o', '^', '*', 'x', '-']
    
    #fig, ax = plt.subplots(figsize=(14, 4))
    fig, ax = plt.subplots(figsize=(8, 8))
    #fig, ax = plt.subplots()
    #plt.ylim((0, 0.1))
    ax.set_xticks(xticks2)
    ax.set_xticklabels(xticklabels2)
    ax.set_xlabel(xlabel, fontsize=8)
    ax.set_ylim((ylim[0], ylim[1]))
    ax.set_xlim(0, len(data['ori_bdr']))
    dates_str = []
    ax.set_ylabel('mismatch ratio', fontsize=10)
    ax.yaxis.set_major_formatter(mtick.FormatStrFormatter('%.2f'))
    i = 0
    labels = {"coa_rib_based":'RIB-based', "rib_peeringdb":'PeeringDB-RIB', "ori_bdr":'BdrmapIt', "ml_map": 'LearnToCorrect'}  
    for map_method in data.keys():
        vals = data[map_method]
        print(color_list[i])
        if marker_list[i] != '*':
            ax.scatter(range(0, len(vals)), vals, facecolors = 'none', marker= marker_list[i], edgecolors=color_list[i], s = 20., label=labels[map_method]) #
        else:
            ax.scatter(range(0, len(vals)), vals, marker= marker_list[i], edgecolors=color_list[i], s = 20., label=labels[map_method])
        i += 1
    ax.vlines(vp_divs, 0, 1, linestyles='solid', colors='black', linewidth=0.6)
    ax.legend(bbox_to_anchor=(0, 1.02, 1, 0.2), loc='lower left', mode='expand', ncol=2, prop={'size': 20})    
    plt.tight_layout()
    #plt.sca(ax)
    ax.plot()
    plt.show()
    #eps_fig = plt.gcf() # 'get current figure'
    #eps_fig.savefig(save_filename, format='eps')
    fig.savefig(save_filename)

def PlotLoopRate():
    loop_info = CmpLoopRate()
    data_info = {}
    vp_divs = []
    for map_method in loop_info.keys():
        data_info[map_method] = []
        for vp in sorted(loop_info[map_method].keys()):
            for date in sorted(loop_info[map_method][vp].keys()):
                data_info[map_method].append(loop_info[map_method][vp][date])
    PlotArray(data_info, 'vp\n(date)', 'loop rate', [0, 0.1], vp_divs)

def DateAdjInMonth(date1, date2):
    (year1, month1) = (int(date1[:4]), int(date1[4:6]))
    (year2, month2) = (int(date2[:4]), int(date2[4:6]))
    if year1 == year2 and month2 == month1 + 1:
        return True
    if year2 == year1 + 1 and month1 == 12 and month2 == 1:
        return True
    return False

def GetVPDivs():
    vp_divs = []
    sum = 0
    for vp in ['ams-nl', 'jfk-us', 'nrt-jp', 'sjc2-us', 'syd-au', 'zrh2-ch']:
        filenames = glob.glob(r'/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/trace_stat_%s*' %(vp, vp))
        sum += len(filenames)
        vp_divs.append(sum)
        sum += 1
    xticks2 = [vp_divs[0] / 2]
    #xticks2 = [9, vp_divs[0] / 2 + 7]
    for i in range(1, len(vp_divs)):
        xticks2.append((vp_divs[i] + vp_divs[i - 1]) / 2)
    xticklabels2 = ['ams-nl\n2018.01-2021.01,\n2021.04', 'jfk-us\n2018.01-2018.11,\n2020.06-2020.10', 'nrt-jp\n2018.01-2021.04', 'sjc2-us\n2018.01-2019.03,\n2020.05-2021.03', 'syd-au\n2018.01,\n2018.04-2020.04,\n2020.12-2021.04', 'zrh2-ch\n2018.01-2020.07']
    #xticklabels2 = ['ams-nl-1\n\n2018.01-2019.06', 'ams-nl-2\n2019.07-2021.01,\n2021.04', 'jfk-us\n2018.01-2018.11,\n2020.06-2020.10', 'nrt-jp\n2018.01-2021.04', 'sjc2-us\n2018.01-2019.03,\n2020.05-2021.03', 'syd-au\n2018.01,\n2018.04-2020.04,\n2020.12-2021.04', 'zrh2-ch\n2018.01-2020.07']
    #xticklabels2 = ['ams-nl-1               ams-nl-2\n2018.01-2021.01,\n2021.04', 'jfk-us\n2018.01-2018.11,\n2020.06-2020.10', 'nrt-jp\n2018.01-2021.04', 'sjc2-us\n2018.01-2019.03,\n2020.05-2021.03', 'syd-au\n2018.01,\n2018.04-2020.04,\n2020.12-2021.04', 'zrh2-ch\n2018.01-2020.07']
    
    return (vp_divs, xticks2, xticklabels2)


def GetVPDivs_v2():
    vp_divs = []
    sum1 = 0
    vps = ['ams-nl', 'nrt-jp', 'sjc2-us', 'syd-au'] #'sao-br', 
    for vp in vps:
        filenames = glob.glob(r'/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/trace_stat_%s*' %(vp, vp))
        sum1 += len(filenames)
        vp_divs.append(sum1)
        sum1 += 1
    with open('/mountdisk2/common_vps/cmp_trace_match_atlas.json', 'r') as rf:
        data = json.load(rf)
        sum1 += len(data['ml_map'])
        vp_divs.append(sum1)
    xticks2 = [vp_divs[0] / 2]
    #xticks2 = [9, vp_divs[0] / 2 + 7]
    for i in range(1, len(vp_divs)):
        xticks2.append((vp_divs[i] + vp_divs[i - 1]) / 2)
    #xticklabels2 = ['ams-nl\n2018.01-2022.04,\n2021.04', 'jfk-us\n2018.01-2018.11,\n2020.06-2020.10', 'nrt-jp\n2018.01-2021.04', 'sjc2-us\n2018.01-2019.03,\n2020.05-2021.03', 'syd-au\n2018.01,\n2018.04-2020.04,\n2020.12-2021.04', 'zrh2-ch\n2018.01-2020.07']
    xticklabels2 = vps + ['Atlas']
    #xticklabels2 = ['ams-nl-1               ams-nl-2\n2018.01-2021.01,\n2021.04', 'jfk-us\n2018.01-2018.11,\n2020.06-2020.10', 'nrt-jp\n2018.01-2021.04', 'sjc2-us\n2018.01-2019.03,\n2020.05-2021.03', 'syd-au\n2018.01,\n2018.04-2020.04,\n2020.12-2021.04', 'zrh2-ch\n2018.01-2020.07']
    
    return (vp_divs, xticks2, xticklabels2)

def PlotTraceMatchRate():
    data_info = {}
    (vp_divs, xticks2, xticklabels2) = GetVPDivs()
    midar_dates = set()
    for filename in os.listdir('/mountdisk1/ana_c_d_incongruity/midar_data/'):
        midar_dates.add(filename.split('_')[0].replace('-', ''))
    # xticks = []
    # xlabels = []
    #pre_date = '00000000'
    #data_info['midar_approx'] = []
    #for map_method in ['coa_rib_based', 'midar', 'ori_bdr', 'hoiho_s_bdr', 'snmp_bdr', 'sxt_bdr']:#, 'snmp_bdrmapit']:
    for map_method in ['ori_bdr']:
        data_info[map_method] = []
        if map_method == 'snmp_bdr':
            print('')
        i = 0
        #for vp in ['ams-nl', 'jfk-us', 'nrt-jp', 'sjc2-us', 'syd-au', 'zrh2-ch']:
        for vp in ['ams-nl']:
            filenames = glob.glob(r'/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/%s/trace_stat_%s*' %(vp, map_method, vp))
            if map_method != 'snmp_bdr':
                filenames.sort()
                for filename in filenames:
                    # date = filename.split('.')[-1]
                    # if map_method == 'rib_based':
                    #     if not DateAdjInMonth(pre_date, date):
                    #         xticks.append(len(data_info[map_method]))
                    #         xlabels.append(date[2:4] + '.' + date[4:6])
                    #     pre_date = date
                    with open(filename, 'r') as rf:
                        data = rf.read()
                        find_res = re.findall(r'match:(.*?)\n', data)
                        rate = 1 - float(find_res[0])
                        #if map_method == 'midar':
                        if False:
                            date = filename.split('.')[-1]
                            if date[:6] in midar_dates:
                                data_info['midar'].append(rate)
                                data_info['midar_approx'].append(np.nan)
                            else:
                                data_info['midar'].append(np.nan)
                                data_info['midar_approx'].append(rate)
                        else:
                            data_info[map_method].append(rate)
                # if map_method == 'rib_based':
                #     vp_divs.append(len(data_info[map_method]))
                    # xticks.append(len(data_info[map_method]) - 1)
                    # xlabels.append(pre_date[2:4] + '.' + pre_date[4:6])
            else:
                while len(data_info['snmp_bdr']) < vp_divs[i] - 1:
                    data_info['snmp_bdr'].append(np.nan)
                if filenames:
                    with open(filenames[0], 'r') as rf:
                        data = rf.read()
                        find_res = re.findall(r'match:(.*?)\n', data)
                        rate = 1 - float(find_res[0])
                        data_info['snmp_bdr'].append(rate)
                else:
                    data_info['snmp_bdr'].append(np.nan)
            data_info[map_method].append(np.nan)
            if False:#map_method == 'midar':
                data_info['midar_approx'].append(2)
            i += 1
        data_info[map_method] = data_info[map_method][:-1]
        # if map_method == 'midar':
        #     data_info['midar_approx'].append(rate)
    #PlotArray(data_info, 'vp\n(date)', 'Trace-level mismatch rate', [0, 1], vp_divs, xticks2, xticklabels2, 'mismatch_trace_rate_no_midar_approx.eps')
    PlotArray(data_info, 'vp\n(date)', 'Trace-level mismatch rate', [0, 1], vp_divs[0:1], xticks2[0:1], xticklabels2[0:1], 'mismatch_trace_rate_no_midar_approx.eps')



def PlotTraceMatchRate_v2():
    data_info = {}
    (vp_divs, xticks2, xticklabels2) = GetVPDivs_v2()
    mappings = ['coa_rib_based', 'rib_peeringdb', 'ori_bdr', 'ml_map']
    with open('/mountdisk2/common_vps/cmp_trace_match_atlas.json', 'r') as rf:
        atlas_data = json.load(rf) 
    for map_method in mappings:
        data_info[map_method] = []
        for vp in ['ams-nl', 'nrt-jp', 'sjc2-us', 'syd-au']: #, 'sao-br'
        #for vp in ['ams-nl']:
            filenames = glob.glob(r'/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/%s_filtersndmm/trace_stat_*' %(vp, map_method))
            filenames.sort()
            for filename in filenames:
                with open(filename, 'r') as rf:
                    data = rf.read()
                    find_res = re.findall(r'match:(.*?)\n', data)
                    rate = float(find_res[0])
                    data_info[map_method].append(rate)
            data_info[map_method].append(np.nan)
        data_info[map_method] = data_info[map_method] + atlas_data[map_method]
        data_info[map_method] = data_info[map_method][:-1]
        # if map_method == 'midar':
        #     data_info['midar_approx'].append(rate)
    PlotArray_v2(data_info, 'vp\n(date)', 'Trace-level match rate', [0, 1], vp_divs, xticks2, xticklabels2, 'mismatch_trace_ratio.eps')
    #PlotArray(data_info, 'vp\n(date)', 'Trace-level mismatch rate', [0, 1], vp_divs[0:1], xticks2[0:1], xticklabels2[0:1], 'mismatch_trace_rate_no_midar_approx.eps')


def PlotArray_List(data_info, xlabel, ylabel_list, ylim_list, vp_divs, xticks2, xticklabels2, save_filename):
    color_list = ['green', 'blue', 'red', 'brown', 'cyan', 'black']
    marker_list = ['o', '+', '^', '_', 'x', 's']
    list_len = len(data_info.keys())
    
    #fig, ax = plt.subplots(figsize=(14, 4))
    fig, axs = plt.subplots(list_len, 1, sharex=True, figsize=(14, 8))
    #plt.ylim((0, 0.1))    
    types = []
    for i in range(0, list_len):
        axs[i].set_ylim((ylim_list[i][0], ylim_list[i][1]))
        if 'succ' in data_info.keys():
            method_1 = list(data_info['succ'].keys())[0]
            axs[i].set_xlim(0, len(data_info['succ'][method_1]) - 1)   
            types = ['unmap', 'succ', 'fail', 'other']     
        elif 'tp' in data_info.keys():
            method_1 = list(data_info['tp'].keys())[0]
            axs[i].set_xlim(0, len(data_info['tp'][method_1]) - 1)  
            types = ['tp', 'fn', 'fp']      
        axs[i].set_ylabel(ylabel_list[i], fontsize=10)
        axs[i].set_xticks(xticks2)
        axs[i].tick_params(color='w', labelsize=10)
        axs[i].yaxis.set_major_formatter(mtick.FormatStrFormatter('%.2f'))
    axs[-1].set_xticklabels(xticklabels2)
    #axs[-1].set_xlabel(xlabel, fontsize=8)
    i = 0
    for _type in types:        
        j = 0
        for (map_method, vals) in data_info[_type].items():
            label = None
            if map_method == 'coa_rib_based':
                label = 'Rib-based'
            elif map_method == 'midar':
                label = 'Midar+BdrmapIt'
            elif map_method == 'midar_approx':
                label = 'Midar+BdrmapIt-approximate'
            elif map_method == 'ori_bdr':
                label = 'BdrmapIt'
            elif map_method == 'hoiho_s_bdr':
                label = 'Hoiho+BdrmapIt'
            elif map_method == 'sxt_bdr':
                label = 'BdrmapIt-improved'
            elif map_method == 'snmp_bdr':
                label = 'SNMPv3+BdrmapIt'
            if map_method == 'midar' or map_method == 'snmp_bdr' or map_method == 'hoiho_s_bdr':
                axs[i].scatter(range(0, len(vals)), vals, marker= marker_list[j], color=color_list[j], s = 40., label=label)
            elif map_method == 'sxt_bdr':
                #axs[i].plot(range(0, len(vals)), vals, color=color_list[j],alpha=0.3,linestyle='--',marker=marker_list[j],markeredgecolor=color_list[j],linewidth=2,markersize='5', label=label, markerfacecolor='none')
                axs[i].plot(range(0, len(vals)), vals, color=color_list[i],alpha=0.3,linestyle='--',marker='',linewidth=1,label=label)
            else:
                axs[i].scatter(range(0, len(vals)), vals, facecolors = 'none', marker= marker_list[j], edgecolors=color_list[j], s = 20., label=label)
            j += 1
        axs[i].vlines(vp_divs, ylim_list[i][0], ylim_list[i][1], linestyles='solid', colors='black', linewidth=0.6)
        # if i == 2 or i == 3:
        #     axs[i].vlines([18], 0, 0.1, linestyles='dashed', colors='brown', linewidth=0.6)
        #     axs[i].text(16, 0.12, '2019.7')
        i += 1
    axs[0].legend(loc='upper right', borderpad=0.5, labelspacing=1, prop={'size': 10}, ncol=3, framealpha = 1, markerscale = 1)
    
    
    # # Make the shaded region
    # ix = np.linspace(a, b)
    # iy = 1
    # verts = [(a, 0), *zip(ix, iy), (b, 0)]
    # poly = Polygon(verts, facecolor ='green',
    #             edgecolor ='0.5', alpha = 0.4)
    # ax.add_patch(poly)

    #plt.sca(ax)
    for i in range(0, list_len): 
        axs[i].plot()
    plt.subplots_adjust(wspace=0, hspace=0.1)
    plt.tight_layout()
    plt.show()
    #eps_fig = plt.gcf() # 'get current figure'
    #eps_fig.savefig(save_filename, format='eps')
    fig.savefig(save_filename)

def PlotIpRate():
    #data_info = {'succ': {}, 'fail': {}, 'other': {}, 'unmap': {}, 'ixp_as': {}}
    data_info = {'succ': {}, 'fail': {}, 'other': {}, 'unmap': {}}
    (vp_divs, xticks2, xticklabels2) = GetVPDivs()
    midar_dates = set()
    for filename in os.listdir('/mountdisk1/ana_c_d_incongruity/midar_data/'):
        midar_dates.add(filename.split('_')[0].replace('-', ''))
    for _type in data_info.keys():
        #data_info[_type]['midar_approx'] = []
        for map_method in ['coa_rib_based', 'midar', 'ori_bdr', 'hoiho_s_bdr', 'snmp_bdr', 'sxt_bdr']:
        #for map_method in ['sxt_bdr_before_rect', 'sxt_bdr']:
            data_info[_type][map_method] = []     
            i = 0       
            for vp in ['ams-nl', 'jfk-us', 'nrt-jp', 'sjc2-us', 'syd-au', 'zrh2-ch']:
                filenames = glob.glob(r'/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/%s/ip_stat_nodstip_%s*' %(vp, map_method, vp))
                if map_method != 'snmp_bdr':
                    filenames.sort()
                    for filename in filenames:
                        with open(filename, 'r') as rf:
                            data = rf.read()
                            find_res = re.findall(r'%s: (.*?)\(' %_type, data)
                            if find_res:
                                if False:#map_method == 'midar':
                                    date = filename.split('.')[-1]
                                    if date[:6] in midar_dates:
                                        data_info[_type]['midar'].append(float(find_res[0]))
                                        data_info[_type]['midar_approx'].append(np.nan)
                                    else:
                                        data_info[_type]['midar'].append(np.nan)
                                        data_info[_type]['midar_approx'].append(float(find_res[0]))
                                else:
                                    data_info[_type][map_method].append(float(find_res[0]))
                            else:
                                data_info[_type][map_method].append(0.0)
                else:
                    while len(data_info[_type]['snmp_bdr']) < vp_divs[i] - 1:
                        data_info[_type]['snmp_bdr'].append(np.nan)
                    if filenames:
                        with open(filenames[0], 'r') as rf:
                            data = rf.read()
                            find_res = re.findall(r'%s: (.*?)\(' %_type, data)
                            data_info[_type]['snmp_bdr'].append(float(find_res[0]))
                    else:
                        data_info[_type]['snmp_bdr'].append(np.nan)
                data_info[_type][map_method].append(np.nan)
                if False: #map_method == 'midar':
                    data_info[_type]['midar_approx'].append(np.nan)
                i += 1
            data_info[_type][map_method] = data_info[_type][map_method][:-1]
            if False: #map_method == 'midar':
                data_info[_type]['midar_approx'] = data_info[_type]['midar_approx'][:-1]
    PlotArray_List(data_info, 'vp\n(date)', ['unmap ratio', 'succeed ratio', 'fail ratio', 'unknown ratio'], [[0, 0.5], [0.5, 1], [0, 0.2], [0, 0.2]], vp_divs, xticks2, xticklabels2, 'ip_rate_per_vp.eps')

def PlotArray_Union(data_info, xlabel, ylabel_list, ylim_list, save_filename):
    # color_list = ['green', 'lightblue', 'blue', 'red', 'brown', 'black', 'gold']
    # marker_list = ['o', '*', '*', '^', 'v', 'p', 's']
    color_list = ['red', 'gold', 'blue', 'green', 'brown', 'black', 'gold']
    marker_list = ['o', '^', '+', '*', 'v', 'p', 's']
    list_len = len(data_info.keys())
    
    #xticks2 = list(range(0, 50, 6))
    xticks2 = list(range(0, 38, 6))
    xticklabels2 = ['2018.01', '2018.06', '2019.01', '2019.06', '2020.01', '2020.06', '2021.01']#, '2021.06', '2022.01']
    fig, axs = plt.subplots(list_len, 1, sharex=True, figsize=(8, 4))
    #fig, axs = plt.subplots(list_len, 1, sharex=True)
    #plt.ylim((0, 0.1))    
    types = []
    for i in range(0, list_len):
        axs[i].set_ylim((ylim_list[i][0], ylim_list[i][1]))
        if 'succ' in data_info.keys():
            method_1 = list(data_info['succ'].keys())[0]
            axs[i].set_xlim(0, len(data_info['succ'][method_1]) - 1)   
            #types = ['succ', 'fail', 'other', 'unmap']     
            types = list(data_info.keys())
        elif 'tp' in data_info.keys():
            method_1 = list(data_info['tp'].keys())[0]
            axs[i].set_xlim(0, len(data_info['tp'][method_1]) - 1)  
            types = ['tp', 'fn', 'fp']      
        axs[i].set_ylabel(ylabel_list[i], fontsize=16)
        axs[i].set_xticks(xticks2)
        axs[i].set_xticklabels(xticklabels2)
        #axs[i].tick_params(color='w', labelsize=8)
        axs[i].tick_params(labelsize=12)
    i = 0
    for _type in types:       
        j = 0 
        for (map_method, vals) in data_info[_type].items():
            label = None
            if map_method == 'coa_rib_based':
                label = 'Rib-based'
            elif map_method == 'midar':
                label = 'Midar+BdrmapIt'
            elif map_method == 'midar_approx':
                label = 'Midar+BdrmapIt-approximate'
            elif map_method == 'ori_bdr':
                label = 'BdrmapIt'
            elif map_method == 'hoiho_s_bdr':
                label = 'Hoiho+BdrmapIt'
            elif map_method == 'sxt_bdr':
                label = 'BdrmapIt-improved'
            elif map_method == 'snmp_bdr':
                label = 'SNMPv3+BdrmapIt'
            if map_method == 'sxt_bdr':
                axs[i].scatter(range(0, len(vals)), vals, facecolors='none', marker= marker_list[j], edgecolors=color_list[j], s = 3., label=label)
                #axs[i].plot(range(0, len(vals)), vals, marker= marker_list[j], markersize=4, c=color_list[j], label=label, markeredgecolor=color_list[j], linewidth=1, markerfacecolor='none')
            else:
                axs[i].scatter(range(0, len(vals)), vals, c=color_list[j], marker= marker_list[j], edgecolors=color_list[j], s = 5., label=label)
                #axs[i].plot(range(0, len(vals)), vals, marker= marker_list[j], markersize=4, c=color_list[j], label=label, markeredgecolor=color_list[j], linewidth=1, markerfacecolor='none')
            j += 1
        #axs[i].vlines(vp_divs, ylim_list[i][0], ylim_list[i][1], linestyles='solid', colors='black', linewidth=0.6)
        i += 1
    #axs[1].legend(loc='upper right', borderpad=0.5, labelspacing=1, prop={'size': 9}, ncol=3, framealpha = 1)
    #axs[1].legend(loc='upper right', borderpad=0.5, labelspacing=1, prop={'size': 10}, ncol=3, framealpha = 1)
    axs[0].legend(bbox_to_anchor=(0, 1.02, 1, 0.2), loc='lower left', mode='expand', ncol=3, prop={'size': 12})    
    #plt.sca(ax)
    for i in range(0, list_len): 
        axs[i].plot()
    plt.subplots_adjust(wspace=0, hspace=0.1)
    plt.tight_layout()
    plt.show()
    #eps_fig = plt.gcf() # 'get current figure'
    #eps_fig.savefig(save_filename, format='eps')
    fig.savefig(save_filename)

def PlotIpRate_2():
    data_info = {'succ': {}, 'fail': {}, 'other': {}, 'unmap': {}}
    midar_dates = set()
    for filename in os.listdir('/mountdisk1/ana_c_d_incongruity/midar_data/'):
        midar_dates.add(filename.split('_')[0].replace('-', ''))
    for _type in data_info.keys():
        #data_info[_type]['midar_approx'] = []
        for map_method in ['coa_rib_based', 'midar', 'ori_bdr', 'hoiho_s_bdr', 'snmp_bdr', 'sxt_bdr']:
            data_info[_type][map_method] = []            
            filenames = glob.glob(r'/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/cmp_method/ip_stat.%s*' %(map_method))
            filenames.sort()
            for filename in filenames:
                with open(filename, 'r') as rf:
                    data = rf.read()
                    find_res = re.findall(r'%s: (.*?)\(' %_type, data)
                    if find_res:
                        if False:#map_method == 'midar':
                            date = filename.split('.')[-1]
                            if date[:6] in midar_dates:
                                data_info[_type]['midar'].append(float(find_res[0]))
                                data_info[_type]['midar_approx'].append(np.nan)
                            else:
                                data_info[_type]['midar'].append(np.nan)
                                data_info[_type]['midar_approx'].append(float(find_res[0]))
                        else:
                            data_info[_type][map_method].append(float(find_res[0]))
                    else:
                        data_info[_type][map_method].append(np.nan)
    # for _type in data_info.keys():
    #     print('{}'.format(data_info[_type]['midar']))
    #     print('{}'.format(data_info[_type]['midar_approx']))
    #PlotArray_List(data_info, 'vp\n(date)', ['succ rate', 'fail rate', 'other rate', 'unmap rate', 'ixp_as rate'], [[0.5, 1], [0, 0.2], [0, 0.2], [0, 0.5], [0, 0.5]], vp_divs, xticks2, xticklabels2, 'ip_rate_bdr_hoiho_s.eps')
    PlotArray_Union(data_info, 'vp\n(date)', ['succ rate', 'fail rate', 'other rate', 'unmap rate'], [[0.5, 1], [0, 0.2], [0, 0.2], [0, 0.5]], 'ip_rate_bdr_hoiho_s.eps')
    
def PlotIpRate_midar():
    data_info = {'succ': {}, 'fail': {}, 'unmap': {}}
    midar_dates = set()
    for filename in os.listdir('/mountdisk1/ana_c_d_incongruity/midar_data/'):
        midar_dates.add(filename.split('_')[0].replace('-', ''))
    for _type in data_info.keys():
        with open('/mountdisk2/common_vps/cmp_ip_%s_midar_date.json' %_type) as rf:
            data = json.load(rf)
            for m, t in data.items():
                s = sorted(t.items(), key=lambda x:x[0])
                data_info[_type][m] = [temp[1] for temp in s]
    PlotArray_Union(data_info, 'vp\n(date)', ['succ rate', 'fail rate', 'unmap rate'], [[0.5, 1], [0, 0.2], [0, 0.2], [0, 0.5]], 'ip_rate_bdr_hoiho_s.eps')
    
def PlotIpSpan():
    data_info = {'succ': {}, 'fail': {}, 'other': {}, 'unmap': {}}#, 'ixp_as': {}}
    (vp_divs, xticks2, xticklabels2) = GetVPDivs()
    for _type in data_info.keys():
        #for map_method in ['rib_based', 'bdrmapit', 'midar']:
        for map_method in ['coa_rib_based', 'midar', 'ori_bdr']:
            data_info[_type][map_method] = []
            for vp in ['ams-nl', 'jfk-us', 'nrt-jp', 'sjc2-us', 'syd-au', 'zrh2-ch']:
                filenames = glob.glob(r'/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/%s/stat_nodstip_stat1_ipaccur_%s*' %(vp, map_method, vp))
                filenames.sort()
                for filename in filenames:
                    with open(filename, 'r') as rf:
                        data = rf.read()
                        find_res = re.findall(r'%s: .*?span: (.*?)\n' %_type, data)
                        data_info[_type][map_method].append(int(find_res[0]))
                data_info[_type][map_method].append(2)
            data_info[_type][map_method] = data_info[_type][map_method][:-1]
    #print(data_info)
    PlotArray_List(data_info, 'vp\n(date)', ['succ span', 'fail span', 'other span', 'unmap span', 'ixp_as span'], [[0, 60], [0, 150], [0, 300], [0, 600], [0, 600]], vp_divs, xticks2, xticklabels2, 'ip_span.eps')

    
def StatIpSpan():
    data_info = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    avg_info = defaultdict(lambda: defaultdict(defaultdict))
    for map_method in ['coa_rib_based', 'midar', 'ori_bdr']:
        for vp in ['ams-nl', 'jfk-us', 'nrt-jp', 'sjc2-us', 'syd-au', 'zrh2-ch']:
            filenames = glob.glob(r'/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/%s/ip_stat_nodstip_%s*' %(vp, map_method, vp))
            filenames.sort()
            for filename in filenames:
                tmp_vp = vp
                if vp == 'ams-nl':
                    if filename.split('.')[-1][:6] < '201811':
                        tmp_vp = 'ams-nl 1'
                    else:
                        tmp_vp = 'ams-nl 2'
                with open(filename, 'r') as rf:
                    data = rf.read()
                    find_res = re.findall(r'(.*?): .*?span: (.*?)\n', data)
                    for elem in find_res:
                        data_info[tmp_vp][elem[0]][map_method].append(int(elem[1]))
    for vp, val in data_info.items():
        for t_type, subval in val.items():
            for t_map_method, subsubval in subval.items():
                avg_info[vp][t_type][t_map_method] = sum(subsubval) / len(subsubval)
    for vp, val in avg_info.items():
        print('%s&'%vp, end='')
        for t_type in ['succ', 'fail', 'other', 'unmap']:
            for t_map_method in ['coa_rib_based', 'midar', 'ori_bdr']:
                print('%d&'%val[t_type][t_map_method], end='')
        print('')
    
def PlotLinkAccr():
    data_info = {'tp': {}, 'fp': {}, 'fn': {}}
    (vp_divs, xticks2, xticklabels2) = GetVPDivs()
    midar_dates = set()
    for filename in os.listdir('/mountdisk1/ana_c_d_incongruity/midar_data/'):
        midar_dates.add(filename.split('_')[0].replace('-', ''))
    for map_method in ['coa_rib_based', 'midar', 'ori_bdrmapit', 'hoiho_s_bdrmapit']:
        for _type in data_info.keys():
            data_info[_type][map_method] = []
            if map_method == 'midar':
                data_info[_type]['midar_approx'] = []
        for vp in ['ams-nl', 'jfk-us', 'sjc2-us', 'syd-au', 'zrh2-ch', 'nrt-jp']:
            filenames = glob.glob(r'/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/%s/linkaccur_%s*' %(vp, map_method, vp))
            filenames.sort()
            for filename in filenames:
                with open(filename, 'r') as rf:
                    data = rf.read()
                    find_res = re.findall(r'tp: (.*?), fp: (.*?), fn: (.*)', data)
                    tmp = list(find_res[0])
                    if map_method == 'midar':
                        date = filename.split('.')[-1]
                        if date[:6] in midar_dates:
                            data_info['tp']['midar'].append(float(tmp[0]))
                            data_info['fp']['midar'].append(float(tmp[1]))
                            data_info['fn']['midar'].append(float(tmp[2]))
                            data_info['tp']['midar_approx'].append(np.nan)
                            data_info['fp']['midar_approx'].append(np.nan)
                            data_info['fn']['midar_approx'].append(np.nan)
                        else:
                            data_info['tp']['midar'].append(np.nan)
                            data_info['fp']['midar'].append(np.nan)
                            data_info['fn']['midar'].append(np.nan)
                            data_info['tp']['midar_approx'].append(float(tmp[0]))
                            data_info['fp']['midar_approx'].append(float(tmp[1]))
                            data_info['fn']['midar_approx'].append(float(tmp[2]))
                    else:
                        data_info['tp'][map_method].append(float(tmp[0]))
                        data_info['fp'][map_method].append(float(tmp[1]))
                        data_info['fn'][map_method].append(float(tmp[2]))
            data_info[_type][map_method].append(np.nan)
            if map_method == 'midar':
                data_info[_type]['midar_approx'].append(np.nan)
        data_info[_type][map_method] = data_info[_type][map_method][:-1]
        if map_method == 'midar':
            data_info[_type]['midar_approx'] = data_info[_type]['midar_approx'][:-1]
    PlotArray_List(data_info, 'vp\n(date)', ['tp', 'fn', 'fp'], [[0, 1], [0, 1], [0, 1]], vp_divs, xticks2, xticklabels2, 'link_accur.eps')

def stat_discrete_continuous_mm(vps = None):
    if not vps:
        vps = ['ams-nl', 'jfk-us', 'sjc2-us', 'syd-au', 'zrh2-ch', 'nrt-jp']
    info = defaultdict(defaultdict)
    types = ['continuous_mm', 'discrete_mm']
    stat = defaultdict(lambda: defaultdict(defaultdict))
    for vp in vps:
        os.chdir('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/ana_compare_res/' %vp)
        filenames = glob.glob(r'stat_classify*')
        for filename in filenames:
            date = filename.split('.')[-1]
            with open(filename, 'r') as rf:
                data = json.load(rf)
                for _type in types:
                    info[(vp, date)][_type] = float(data[_type][1])
        for _type in types:
            if vp == 'ams-nl':
                stat[vp + '1'][_type]['max'] = max([info[key][_type] for key in info.keys() if key[0] == vp and key[1][:6] < '201811'])
                stat[vp + '1'][_type]['min'] = min([info[key][_type] for key in info.keys() if key[0] == vp and key[1][:6] < '201811'])            
                stat[vp + '1'][_type]['avg'] = sum([info[key][_type] for key in info.keys() if key[0] == vp and key[1][:6] < '201811']) / 10
                stat[vp + '2'][_type]['max'] = max([info[key][_type] for key in info.keys() if key[0] == vp and key[1][:6] >= '201811' and key[1][:6] < '201907'])
                stat[vp + '2'][_type]['min'] = min([info[key][_type] for key in info.keys() if key[0] == vp and key[1][:6] >= '201811' and key[1][:6] < '201907'])            
                stat[vp + '2'][_type]['avg'] = sum([info[key][_type] for key in info.keys() if key[0] == vp and key[1][:6] >= '201811' and key[1][:6] < '201907']) / 8
                stat[vp + '3'][_type]['max'] = max([info[key][_type] for key in info.keys() if key[0] == vp and key[1][:6] >= '201907'])
                stat[vp + '3'][_type]['min'] = min([info[key][_type] for key in info.keys() if key[0] == vp and key[1][:6] >= '201907'])            
                stat[vp + '3'][_type]['avg'] = sum([info[key][_type] for key in info.keys() if key[0] == vp and key[1][:6] >= '201907']) / 18
            else:
                stat[vp][_type]['max'] = max([info[key][_type] for key in info.keys() if key[0] == vp])
                stat[vp][_type]['min'] = min([info[key][_type] for key in info.keys() if key[0] == vp])            
                stat[vp][_type]['avg'] = sum([info[key][_type] for key in info.keys() if key[0] == vp]) / len(filenames)
    # s = sorted(info.items(), key=lambda x: x[0])
    # for key, val in s:
    #     print('{}: '.format(key), end='')
    #     for _type, subval in val.items():
    #         print('{}: {}, '.format(_type, subval), end='')
    #     print('')
    mm = {}
    mm_stat = {}
    for vp in vps:
        os.chdir('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/' %vp)
        filenames = glob.glob(r'trace_stat_*')
        for filename in filenames:
            date = filename.split('.')[-1]
            with open(filename, 'r') as rf:
                data = rf.readlines()
                val = 1 - float(data[-1].split(':')[-1].strip('\n'))
                mm[(vp, date)] = val
        if vp == 'ams-nl':
            mm_stat[vp + '1'] = sum([mm[key] for key in info.keys() if key[0] == vp and key[1][:6] < '201811']) / 10
            mm_stat[vp + '2'] = sum([mm[key] for key in info.keys() if key[0] == vp and key[1][:6] >= '201811' and key[1][:6] < '201907']) / 8
            mm_stat[vp + '3'] = sum([mm[key] for key in info.keys() if key[0] == vp and key[1][:6] >= '201907']) / 18
        else:
            mm_stat[vp] = sum([mm[key] for key in info.keys() if key[0] == vp]) / len(filenames)
    for vp, val in stat.items():
        print('{}: '.format(vp))
        for _type, subval in val.items():
            #print('\t{}: max: {}, min: {}, avg: {}'.format(_type, subval['max'], subval['min'], subval['avg']))
            print('{}: avg: {:.2%}'.format(_type, subval['avg'] * mm_stat[vp]))

def stat_three_mm_kinds():
    vps = ['ams-nl', 'jfk-us', 'sjc2-us', 'syd-au', 'zrh2-ch', 'nrt-jp']
    info = defaultdict(defaultdict)
    types = ['ixp', 'extra', 'succ']
    stat = defaultdict(lambda: defaultdict(defaultdict))
    ixp1 = []
    ixp2 = []
    for vp in vps:
        os.chdir('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/' %vp)
        filenames = glob.glob(r'discrimin_stat*')
        for filename in filenames:
            date = filename.split('.')[-1]
            with open(filename, 'r') as rf:
                for line in rf.readlines():
                    _type = list({_type for _type in types if line.__contains__(_type)})[0]
                    val = line.split(':')[-1].strip(' ').strip('\n')
                    info[(vp, date)][_type] = float(val)
                    if _type == 'ixp' and vp == 'ams-nl':
                        if date[:6] < '201907':
                            ixp1.append(float(val))
                        else:
                            ixp2.append(float(val))
        for _type in types:
            stat[vp][_type]['max'] = max([info[key][_type] for key in info.keys() if key[0] == vp])
            stat[vp][_type]['min'] = min([info[key][_type] for key in info.keys() if key[0] == vp])            
            stat[vp][_type]['avg'] = sum([info[key][_type] for key in info.keys() if key[0] == vp]) / len(filenames)
    # for vp, val in stat.items():
    #     print('{}'.format(vp), end='')
    #     for _type, subval in val.items():
    #         print('&{:.2%}({:.2%}, {:.2%})'.format(subval['avg'], subval['min'], subval['max']), end='')
    #     print('\\\\')
    for vp, val in stat.items():
        print('{}'.format(vp), end='')
        for _type, subval in val.items():
            print('&{:.2%}'.format(subval['avg']), end='')
        print('\\\\')
    print('{}'.format(sum(ixp1) / len(ixp1)))
    print('{}'.format(sum(ixp2) / len(ixp2)))

def stat_succ_ip():
    vps = ['ams-nl', 'jfk-us', 'sjc2-us', 'syd-au', 'zrh2-ch', 'nrt-jp']
    #vps = ['nrt-jp']
    info = defaultdict(Counter)
    stat = defaultdict(defaultdict)
    res = []
    for vp in vps:
        os.chdir('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/' %vp)
        filenames = glob.glob(r'2_succip_failed*')
        for filename in filenames:
            date = filename.split('.')[-1]
            #if date[:6] != '202008': continue
            with open(filename, 'r') as rf:
                lines = [rf.readline() for _ in range(3)]
                while lines[0]:
                    (mm_ips, pm_ips, _) = lines[2].split(']')
                    if len(mm_ips[1:].split(',')) == 1:
                        #print('')
                        pass
                    res.append(len(mm_ips[1:].split(',')))
                    lines = [rf.readline() for _ in range(3)]
    x = sorted(res)
    # print(x.index(2))
    # print(len(x))
    # return
    ecdf = sm.distributions.ECDF(x)
    y = ecdf(x)
        
    fig, ax = plt.subplots()
    #ax.scatter(x, y)
    ax.plot(x, y)
    # ax.set_xticks(range(0, x[-1] + 1), minor = True)
    # ax.set_xticks(range(0, x[-1] + 1, 5), minor = False)
    # ax.set_yticks(np.arange(0, 1.1, 0.2))
    #ax.set_xticks(range(0, x[-1] + 1, 5), fontsize=18)
    #ax.set_yticks(np.arange(0, 1.1, 0.2), fontsize=18)
    ax.set_xticklabels(range(0, x[-1] + 1, 5), fontsize= 16)
    #ax.set_yticklabels(np.arange(0, 1.1, 0.2), fontsize= 18)
    ax.set_yticklabels([0, 0.2, 0.4, 0.6, 0.8, 1.0], fontsize= 16)
    ax.set_xlim(0, x[-1] + 1)
    ax.set_ylim(0, 1)
    ax.set_xlabel('number of mismatched hops', fontsize=16)
    # ax.grid(b=True, which='major')
    # ax.grid(b=True, which='minor', alpha=0.2)

    plt.tight_layout()
    #eps_fig = plt.gcf() # 'get current figure'
    #eps_fig.savefig('last_as_in_last_extra_cdf_nogrid.eps', format='eps')
    plt.show()
    #                 info[(vp, date)][len(mm_ips[1:].split(','))] += 1
    #                 lines = [rf.readline() for _ in range(3)]        
    # s = 0
    # t = 0
    # for key, val in info.items():
    #     multi = sum([val[i] for i in val.keys() if i >= 3])
    #     if multi / sum(val.values()) > 0.5:
    #         s += 1
    #     t += 1
    #     #print('{}:{}'.format(key, multi / sum(val.values())))
    # print(s / t)
    #eps_fig = plt.gcf() # 'get current figure'
    #eps_fig.savefig(save_filename, format='eps')
    fig.savefig('mm_hop_num_in_succ_ip_mm.eps')

def PlotCdfBase(arrs):
    # ecdf = []
    # x = []
    # y = []
    # for arr in arrs:
    #     ecdf.append(sm.distributions.ECDF(arr))
    #     x.append(np.linspace(min(arr), max(arr), len(arr)))
    #     y.append(ecdf(x))
    #ecdf = sm.distributions.ECDF(arrs[0])
    #x = np.linspace(min(arrs[0]), max(arrs[0]), len(arrs[0]))
    #y = ecdf(x)
    x = arrs[0]
    ecdf = sm.distributions.ECDF(x)
    y = ecdf(x)
    #y = np.cumsum(x)
    # y_dot5 = np.linspace(0.5, 0.5, len(arr))
    # y_dot9 = np.linspace(0.9, 0.9, len(arr))
    # idx_dot5 = np.argwhere(np.diff(np.sign(y - y_dot5))).flatten()
    # idx_dot9 = np.argwhere(np.diff(np.sign(y - y_dot9))).flatten()
    # plt.axhline(y=0.5, xmin = 0.0, xmax = x[idx_dot5[0]]/x[-1], color="red", linestyle="--")
    # plt.axhline(y=0.9, xmin = 0.0, xmax = x[idx_dot9[0]]/x[-1], color="red", linestyle="--")
    # plt.axvline(x=x[idx_dot5[0]], ymin = 0.0, ymax = 0.5, color="red", linestyle="--")
    # plt.axvline(x=x[idx_dot9[0]], ymin = 0.0, ymax = 0.9, color="red", linestyle="--")
    # plt.text(x[idx_dot5[0]]+5, 0.48, '(%d,0.5)'%x[idx_dot5[0]], color="red")
    # plt.text(x[idx_dot9[0]]+5, 0.86, '(%d,0.9)'%x[idx_dot9[0]], color="red")
    
    #classes = 
    #fig, ax = plt.subplots(figsize=(6, 3))
    fig, ax = plt.subplots()
    ax.set_xscale('symlog')
    ax.set_xlim((0.5, x[-1]))
    ax.set_ylim((0,1))
    #ax.scatter(x, y, label='provider AS')
    #ax.plot(range(0, len(vals)), vals, color=color_list[i],alpha=0.3,linestyle='--',marker='',linewidth=1,label=label)
    ax.plot(x, y, label='provider AS')

    t_size = 20
    x = arrs[1]
    ecdf = sm.distributions.ECDF(x)
    y = ecdf(x)
    #ax.scatter(x, y, label='(provider AS, customer AS)')
    ax.plot(x, y, label='(provider AS, customer AS)')
    #ax.legend(handles=scatter.legend_elements()[0], labels=classes)
    ax.legend(loc='lower right', prop={'size': 16})
    plt.tick_params(labelsize=16)
    ax.set_xlabel('Frequency of AS/AS pair', fontsize=16)
    
    plt.tight_layout()
    eps_fig = plt.gcf() # 'get current figure'
    eps_fig.savefig('last_as_in_last_extra_cdf1.eps', format='eps')
    plt.show()

def cmp_discr():
    data = defaultdict(lambda: defaultdict(list))
    for vp in ['ams-nl', 'sjc2-us', 'syd-au', 'nrt-jp', 'zrh2-ch']:#, 'sao-br']:
        for discr_method in ['neighAs_classify', 'classify', 'jsac_2_classify']:
            with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/cmp_%s' %(vp, discr_method)) as rf:
            #with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ori_bdr_filtersndmm/cmp_%s' %(vp, discr_method)) as rf:
                f_data = rf.readlines()  
                for line in f_data:   
                    for _type in ['ixp', 'succeed', 'extra']:
                        find_res = re.findall(r'%s.%s.20.*?: (.*?)\n' %(_type, vp), line)
                        if find_res:
                            #print('{}, {}, {}'.format(_type, discr_method, find_res[0]))
                            if discr_method == 'classify' and float(find_res[0]) < 0.5:
                                continue
                            data[_type][discr_method].append(float(find_res[0]))
    for _type in ['ixp', 'succeed', 'extra']:
        print('{}:{}'.format(_type, sum(data[_type]['classify']) / len(data[_type]['classify'])))
    color_list = ['green', 'blue', 'red']
    marker_list = ['o', 'x', 's', '_', 's']
    
    fig, ax = plt.subplots(figsize=(6, 3))
    #fig, ax = plt.subplots()
    #plt.ylim((0, 0.1))
    ax.set_ylim(0, 1)
    ax.set_xlim(0.5, 3.5)
    ax.set_xticks([1, 2, 3])
    ax.set_yticks([0, 0.2, 0.4, 0.6, 0.8, 1.0])
    ax.set_yticklabels(['0%', '20%', '40%', '60%', '80%', '100%'])
    ax.tick_params(color='w', labelsize=12)
    #ax.set_xticklabels(['IXP-\nmapping', 'Succeed-IP-\nidentify', 'Extra-\ntail'])
    #ax.set_xticklabels(['IXPMapping\n(15.7%)', 'ExtraTail\n(3.8%)', 'SucceedMapping\n(1.7%)'])
    ax.set_xticklabels(['IXPMapping', 'ExtraTail', 'SuccessMapping'])
    #ax.set_xlabel(xlabel, fontsize=8)
    ax.set_ylabel('coverage', fontsize=12)
    i = 1
    for _type in ['ixp', 'extra', 'succeed']:
        val = data[_type]
        j = 0
        for discr_method in ['neighAs_classify', 'jsac_2_classify', 'classify']:
            subvals = val[discr_method]
            print('{}, {}: {}'.format(_type, discr_method, sum(subvals) / len(subvals)))
            label = None
            if discr_method == 'neighAs_classify':
                label = 'RelatedCandidate'
            elif discr_method == 'classify':
                label = 'IASC'
            elif discr_method.startswith('jsac'):
                label = 'DiffCount'    
            if marker_list[j] != 'o' and marker_list[j] != 's':
                if i == 1:
                    ax.scatter([i + (j - 1) * 0.1 for k in range(len(subvals))], subvals, marker= marker_list[j], color=color_list[j], s = 60., label=label)
                else:
                    ax.scatter([i + (j - 1) * 0.1 for k in range(len(subvals))], subvals, marker= marker_list[j], color=color_list[j], s = 60.)
            else:
                if i == 1:
                    ax.scatter([i + (j - 1) * 0.1 for k in range(len(subvals))], subvals, facecolors = 'none', marker= marker_list[j], edgecolors=color_list[j], s = 60., label=label)
                else:
                    ax.scatter([i + (j - 1) * 0.1 for k in range(len(subvals))], subvals, facecolors = 'none', marker= marker_list[j], edgecolors=color_list[j], s = 60.)
            #plt.plot(range(0, len(vals)), vals, c = color_list[i], marker= marker_list[i], label='%s'%map_method)
            j += 1
        i += 1
    ax.legend(loc='lower left', borderpad=0.8, labelspacing=0.6, prop={'size': 9.5}, ncol=3, framealpha = 0.6, markerscale = 1, bbox_to_anchor=(0.04, 1.02))
    plt.tight_layout()
    #plt.sca(ax)
    ax.plot()
    plt.show()
    # eps_fig = plt.gcf() # 'get current figure'
    # eps_fig.savefig('cmp_discr.eps', format='eps')
    fig.savefig('cmp_discr.eps')

def stat_aggr_ases():
    vps = ['ams-nl', 'jfk-us', 'sjc2-us', 'syd-au', 'zrh2-ch', 'nrt-jp']
    #vps = ['nrt-jp']
    info = Counter()
    info1 = Counter()
    t = 0
    for vp in vps:
        os.chdir('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/' %vp)
        filenames = glob.glob(r'3_extra_moas_trail*')
        for filename in filenames:
            date = filename.split('.')[-1]
            #if date[:6] != '202008': continue
            with open(filename, 'r') as rf:
                lines = [rf.readline() for _ in range(3)]
                while lines[0]:
                    t += 1
                    (dst_ip, trace) = lines[0][1:].strip('\n').split(']')
                    last_bgp = lines[1].strip('\n').strip('\t').split(' ')[-1]
                    last_trace = None
                    for elem in reversed(trace.split(' ')):
                        if elem != '*' and elem != '?' and not elem.startswith('<'):
                            last_trace = elem
                            break
                    info[last_bgp] += 1
                    info1[(last_bgp, last_trace)] += 1
                    lines = [rf.readline() for _ in range(3)]
    sort = sorted(info.items(), key=lambda x: x[1], reverse = False)
    sort1 = sorted(info1.items(), key=lambda x: x[1], reverse = False)
    # print(len(sort1))
    # print(sort[0][1] / t)
    # print(sort1[0][1] / t)
    # sum = 0
    # for i in range(0, len(sort)):
    #     sum += sort[i][1]
    #     if sum > 0.9 * t:
    #         print(i + 1)
    #         break
    # sum = 0
    # for i in range(0, len(sort)):
    #     sum += sort1[i][1]
    #     if sum > 0.9 * t:
    #         print(i + 1)
    #         break
    # print(sort1[0][0])
    # print(sort1[1][0])
    p = [elem[1] for elem in sort]
    p1 = [elem[1] for elem in sort1]
    PlotCdfBase([p, p1])
            
def distr_hori_bar_chart(results, category_names):
    #labels = [k + '%' for k in results.keys()]
    labels = list(results.keys())
    data = np.array(list(results.values()))
    data_cum = data.cumsum(axis=1)
    #category_colors = plt.colormaps['RdYlGn'](np.linspace(0.15, 0.85, data.shape[1]))
    category_colors = ['aqua', 'orange', 'gold', 'red', 'green']

    fig, ax = plt.subplots()
    #fig, ax = plt.subplots()
    ax.invert_yaxis()
    ax.xaxis.set_visible(False)
    ax.set_xlim(0, np.sum(data, axis=1).max())
    #ax.set_xlim(0, 1)

    for i, (colname, color) in enumerate(zip(category_names, category_colors)):
        widths = data[:, i]
        starts = data_cum[:, i] - widths
        #rects = ax.barh(labels, widths, left=starts, height=0.2, label=colname, color=color)
        rects = ax.barh(labels, widths, left=starts, label=colname, color=color)

        # r, g, b, _ = color
        # text_color = 'white' if r * g * b < 0.5 else 'darkgrey'
        text_color = 'black'
        ax.bar_label(rects, label_type='center', color=text_color, size = 10)
    ax.legend(ncol=len(category_names), bbox_to_anchor=(0, 1),
              loc='lower left', fontsize=10)

    #survey(results, category_names)
    plt.tight_layout()
    eps_fig = plt.gcf() # 'get current figure'
    eps_fig.savefig('discrete_continuous_mm.eps', format='eps')
    plt.show()

def stat_discrete_mm_num():    
    vps = ['ams-nl', 'sjc2-us', 'syd-au', 'nrt-jp', 'sao-br']
    #vps = ['nrt-jp']
    r1 = []
    r2 = []
    r3 = []
    for vp in vps:
        val = [[], [], []]
        os.chdir('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/ana_compare_res/' %vp)
        filenames = glob.glob(r'stat_classify*')
        for filename in filenames:
            date = filename.split('.')[-1]
            #if date[:6] != '202008': continue
            with open(filename, 'r') as rf:
                data = json.load(rf)
                val[0].append(data['discrete_mm'][2]['1_mm'][0] / data['discrete_mm'][0])
                val[1].append(data['discrete_mm'][2]['2_mm'][0] / data['discrete_mm'][0])
                val[2].append(1 - (data['discrete_mm'][2]['1_mm'][0] + data['discrete_mm'][2]['2_mm'][0]) / data['discrete_mm'][0])
        r1.append(sum(val[0]) / len(val[0]))
        r2.append(sum(val[1]) / len(val[1]))
        r3.append(sum(val[2]) / len(val[2]))
    
    labels = vps
    width = 0.5       # the width of the bars: can also be len(x) sequence    
    #fig, ax = plt.subplots(figsize=(6, 6))
    fig, ax = plt.subplots(figsize=(6, 1.5))
    t_r = [er1 + er2 for (er1, er2) in zip(r1, r2)]
    ax.bar(labels, r1, width, label='1 hop')
    ax.bar(labels, r2, width, bottom=r1, label='2 hops')
    ax.bar(labels, r3, width, bottom=t_r, label='>2 hops')

    #ax.bar('<2019.7', [0.01169], 0.35, label='real-mismatch')
    #ax.set_yticks([0.8, 0.85, 0.9, 0.95, 1.0])
    ax.set_yticks([0.8, 0.9, 1.0])
    ax.grid(axis='y',linestyle=':',color='grey',alpha=0.6)
    ax.set_ylabel('percentage', fontsize=12)
    ax.set_yticklabels(['80%', '90%', '100%'])
    #ax.set_title('')
    #ax.legend(bbox_to_anchor=(0, 1.02, 1, 0.2), loc='lower left', mode='expand', ncol=1, prop={'size': 15})
    ax.legend(bbox_to_anchor=(0, 1.02, 1, 0.2), loc='lower left', mode='expand', ncol=3, prop={'size': 12})
    ax.set_ylim(0, 1)

    plt.tick_params(labelsize=12)
    
    plt.tight_layout()
    eps_fig = plt.gcf() # 'get current figure'
    eps_fig.savefig('discrete_mm_num.eps', format='eps')
    plt.show()

def stat_mm_ip_rel():
    vps = ['ams-nl', 'jfk-us', 'sjc2-us', 'syd-au', 'zrh2-ch', 'nrt-jp']
    #vps = ['nrt-jp']
    #info = {}
    info = defaultdict(list)
    t = 0
    for vp in vps:
        os.chdir('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/ana_compare_res/' %vp)
        filenames = glob.glob(r'discrete_mm_ips_stat*')
        for filename in filenames:
            date = filename.split('.')[-1]
            GetIxpPfxDict_2(int(date[:4]), int(date[4:6]))
            #if date[:6] != '202008': continue
            with open(filename, 'r') as rf:
                total = int(rf.readline().split(':')[-1].strip('\n').strip(' '))
                ixp = float(rf.readline().split(':')[-1].strip('\n').strip(' ')) * total
                sibling = float(rf.readline().split(':')[-1].strip('\n').strip(' ')) * total
                neighbor = float(rf.readline().split(':')[-1].strip('\n').strip(' ')) * total
                other = float(rf.readline().split(':')[-1].strip('\n').strip(' ')) * total
                ips = set()
                extra_ixp = 0
                with open('../1_ixpip_failed.%s.%s' %(vp, date), 'r') as rf1:                    
                    lines = [rf.readline() for _ in range(3)]
                    while lines[0]:                        
                        (mm_ips, pm_ips, _) = lines[2].split(']')
                        ips = ips | set(mm_ips[1:].split(','))
                        ips = ips | set(pm_ips[1:].split(','))
                        lines = [rf.readline() for _ in range(3)]
                for _ip in ips:
                    if IsIxpIp(_ip):
                        extra_ixp += 1
                ixp += extra_ixp
                total += extra_ixp
                info['ixp'].append(ixp / total)
                info['sibling'].append(sibling / total)
                info['neighbor'].append(neighbor / total)
                info['other'].append(other / total)
            ClearIxpPfxDict()
        print('{}:{:.1%}, {:.1%}, {:.1%}, {:.1%}'.format(vp, sum(info['ixp']) / len(info['ixp']), \
                                sum(info['sibling']) / len(info['sibling']), \
                                sum(info['neighbor']) / len(info['neighbor']), \
                                sum(info['other']) / len(info['other'])))
    # for key, val in info.items():
    #     print('{}:{}'.format(key, val))

def get_mean_trace_mm_rate():
    info = defaultdict(list)
    for vp in ['ams-nl', 'jfk-us', 'sjc2-us', 'syd-au', 'zrh2-ch', 'nrt-jp']:
        os.chdir('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/' %vp)
        for filename in glob.glob(r'trace_stat_*'):
            date = filename.split('.')[-1]
            val = None
            with open(filename, 'r') as rf:
                data = rf.readlines()
                val = 1 - float(data[-1].split(':')[-1].strip('\n'))
            if vp == 'ams-nl':
                if date[:6] < '201811':
                    info[vp + '1'].append(val)
                else:
                    info[vp + '2'].append(val)
            else:
                info[vp].append(val)
    for key, val in info.items():
        print('{}:{}'.format(key, sum(val) / len(val)))

def get_ark_discrete_continous():
    rms = {}
    mes = {}
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/real_mm_rates.json', 'r') as rf:
        data = json.load(rf)
        for vp, val in data.items():
            if vp == 'ams-nl':
                rms[vp+'1'] = []
                rms[vp+'2'] = []
                mes[vp+'1'] = []
                mes[vp+'2'] = []
            else:
                rms[vp] = []
                mes[vp] = []
            for date, subval in val.items():
                fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/ana_compare_res/stat_classify.%s.%s*' %(vp, vp, date[:6]))
                with open(fns[0], 'r') as rf1:
                    tmp = json.load(rf1)
                    if vp == 'ams-nl':
                        if date < '20190701':
                            rms[vp+'1'].append(subval)
                            mes[vp+'1'].append(subval * tmp["discrete_mm"][1] / tmp["continuous_mm"][1])
                        else:
                            rms[vp+'2'].append(subval)
                            mes[vp+'2'].append(subval * tmp["discrete_mm"][1] / tmp["continuous_mm"][1])
                    else:
                        rms[vp].append(subval)
                        mes[vp].append(subval * tmp["discrete_mm"][1] / tmp["continuous_mm"][1])
    for vp, val in rms.items():
        print('{}:{}'.format(vp, np.mean(val)))
    for vp, val in mes.items():
        print('{}:{}'.format(vp, np.mean(val)))

def plot_discrete_continous_2():
    labels = ['         ams-nl', ' ', '   ', 'sjc2-us', '      ', 'syd-au', '    ', 'nrt-jp', '       ', 'sao-br']
    discrete = [0.217,0.153, 0, 0.074, 0, 0.070, 0, 0.074, 0, 0.079]
    continous = [0.028, 0.181, 0, 0.022, 0, 0.022, 0, 0.049, 0, 0.037]
    a = 0.75
    b = 0.15
    width = [a, a, b, a, b, a, b, a, b, a]       # the width of the bars: can also be len(x) sequence
    
    # labels = ['<19.7\nams-nl', '  >19.6\nams-nl', 'jfk-us', 'sjc2-us', 'syd-au', 'zrh2-ch', 'nrt-jp']
    # discrete = [0.15531, 0.218676923, 0.08348125, 0.105182609, 0.112444444, 0.118283871, 0.106661111]
    # continous = [0.01169,0.098246154, 0.14214375, 0.020034783, 0.015333333, 0.014619355, 0.03945]
    # width = 0.35       # the width of the bars: can also be len(x) sequence
    
    fig, ax = plt.subplots(figsize=(6, 4))

    ax.bar(labels, continous, width, label='real-mismatch')
    ax.bar(labels, discrete, width, bottom=continous, label='mapping error')
    ax.text(0, 1.02*(continous[0] + discrete[0]), 'ams-nl-1',
                ha='center', va='bottom', rotation=0)
    ax.text(3* a / 2 - 0.1, 1.02*(continous[1] + discrete[1]), 'ams-nl-2',
                ha='center', va='bottom', rotation=0)

    #ax.bar('<2019.7', [0.01169], 0.35, label='real-mismatch')
    ax.grid(axis='y',linestyle=':',color='grey',alpha=0.6)
    ax.set_ylabel('rate', fontsize=12)
    #ax.set_title('')    
    ax.tick_params(labelsize=12)
    ax.set_ylim([0, 0.4])
    ax.legend(loc='upper right', prop={'size': 12})

    plt.tight_layout()
    #eps_fig = plt.gcf() # 'get current figure'
    #eps_fig.savefig('ark_discrete_continuous_mm.eps', format='eps')
    plt.show()
    fig.savefig('ark_discrete_continuous_mm.eps', bbox_inches='tight')
    
def plot_discrete_continous():
    #labels被我改坏了，还需要再改
    labels = [' ', 'ams-nl', '  ', '   ', 'sjc2-us', '      ', 'syd-au', '    ', 'nrt-jp', '       ', 'sao-br']
    #discrete = [0.1573808, 0.260693936, 0.14296141, 0, 0.08348125, 0, 0.106661111, 0, 0.105182609, 0, 0.112444444, 0, 0.118283871]
    discrete = [0.1249, 0.1906, 0.1377, 0, 0.1249,        0, 0.1063,     0, 0.0947,      0, 0.1052,      0, 0.1102]
    #continous = [0.009,0.031806064, 0.18203859, 0, 0.14214375, 0, 0.03945, 0, 0.020034783, 0, 0.015333333, 0, 0.014619355]
    continous = [0.0083, 0.0292, 0.1524,         0, 0.0851,      0, 0.0357,     0, 0.0131, 0, 0.0130,       0, 0.0111]
    a = 0.75
    b = 0.15
    width = [a, a, a, b, a, b, a, b, a, b, a, b, a]       # the width of the bars: can also be len(x) sequence
    
    # labels = ['<19.7\nams-nl', '  >19.6\nams-nl', 'jfk-us', 'sjc2-us', 'syd-au', 'zrh2-ch', 'nrt-jp']
    # discrete = [0.15531, 0.218676923, 0.08348125, 0.105182609, 0.112444444, 0.118283871, 0.106661111]
    # continous = [0.01169,0.098246154, 0.14214375, 0.020034783, 0.015333333, 0.014619355, 0.03945]
    # width = 0.35       # the width of the bars: can also be len(x) sequence
    
    fig, ax = plt.subplots(figsize=(6, 3))

    ax.bar(labels, continous, width, label='real mismatch')
    ax.bar(labels, discrete, width, bottom=continous, label='mapping error')
    ax.text(-0.1, 1.02*(continous[0] + discrete[0]), 'ams-nl-\n1-low',
                ha='center', va='bottom', rotation=0)
    ax.text((2* a + b) / 2, 1.02*(continous[1] + discrete[1]), 'ams-nl-\n1-high',
                ha='center', va='bottom', rotation=0)
    ax.text(2.55 * a, 1.02*(continous[2] + discrete[2]), 'ams-nl-2',
                ha='center', va='bottom', rotation=0)

    #ax.bar('<2019.7', [0.01169], 0.35, label='real-mismatch')
    ax.grid(axis='y',linestyle=':',color='grey',alpha=0.6)
    ax.set_ylabel('percentage', fontsize=12)
    #ax.set_title('')    
    plt.tick_params(labelsize=12)
    ax.set_ylim([0, 0.34])
    ax.legend(loc='upper right', prop={'size': 12})

    plt.tight_layout()
    eps_fig = plt.gcf() # 'get current figure'
    eps_fig.savefig('discrete_continuous_mm.eps', format='eps')
    plt.show()

def f():
    fig = plt.figure()
    
    x = [1,2,3,4,5,6,7]
    y = [1,3,4,2,5,8,6]
    
    
    left, bottom, width, height = 0.1,0.1,0.8,0.8
    ax1 = fig.add_axes([left,bottom,width,height])
    ax1.plot(x,y,'r')
    ax1.set_xlabel("x")
    ax1.set_ylabel("y")
    ax1.set_title('title')
    
    left, bottom, width, height = 0.2,0.6,0.25,0.25
    ax2 = fig.add_axes([left,bottom,width,height])
    ax2.plot(x,y,'g')
    ax2.set_xlabel('x')
    ax2.set_ylabel('y')
    ax2.set_title('title inside1')
    
    left, bottom, width, height = 0.6,0.2,0.25,0.25
    plt.axes([left,bottom,width,height])
    plt.plot(y[::-1],x,'b')
    plt.xlabel('x')
    plt.ylabel('y')
    plt.title('tile inside2')
    
    plt.show()

def stat_traceroute_ab():
    os.chdir('/mountdisk1/ana_c_d_incongruity/traceroute_data/stat/')
    info = defaultdict(lambda: defaultdict(list))
    for vp in ['ams-nl', 'jfk-us', 'sjc2-us', 'syd-au', 'zrh2-ch', 'nrt-jp']:
        for filename in glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/trace_stat_%s*' %(vp, vp)):
            date = filename.split('.')[-1]
            with open(filename, 'r') as rf:
                remain = int(rf.readline().split(':')[-1].strip('\n'))
                nobgp = float(rf.readline().split(':')[-1].strip('\n'))                
                nobgp = int(remain * nobgp)
                remain -= nobgp
                v = Counter()
                for filename1 in glob.glob('trace_%s.%s_*' %(vp, date)):
                    _type = filename1.split('_')[-1]
                    with open(filename1, 'r') as rf:
                        v[_type] += len(rf.readlines())
                total = remain + nobgp + sum(v.values())
                info[vp]['nobgp'].append(nobgp / total)
                info[vp]['loop'].append(v['loop'] / total)
                info[vp]['multireply'].append(v['multireply'] / total)
        info[vp]['nobgp'] = sum(info[vp]['nobgp']) / len(info[vp]['nobgp'])
        info[vp]['loop'] = sum(info[vp]['loop']) / len(info[vp]['loop'])
        info[vp]['multireply'] = sum(info[vp]['multireply']) / len(info[vp]['multireply'])

    for vp, val in info.items():
        print('{}'.format(vp), end='')
        for _type, subval in val.items():
            print('&{}:{}'.format(_type, subval), end='')
        print('')

def stat_traceroute_ab_2():
    os.chdir('/mountdisk1/ana_c_d_incongruity/traceroute_data/back/')
    info1 = defaultdict(list)
    info2 = defaultdict(list)
    for filename in os.listdir('.'):
        print(filename)
        (vp, date) = filename.split('.')
        n1 = 0
        n2 = 0
        n = 0
        with open(filename, 'r') as rf:
            for line in rf.readlines():
                if line.startswith('#'):
                    continue
                if line.__contains__('I'):
                    n1 += 1
                if line.__contains__('N'):
                    n2 += 1
                n += 1
            info1[vp].append(n1 / n)
            info2[vp].append(n2 / n)
    for vp, val in info1.items():
        print('{}:{}'.format(vp, sum(val) / len(val)))
    for vp, val in info2.items():
        print('{}:{}'.format(vp, sum(val) / len(val)))

def StatIpCrossing_0():
    for vp in ['ams-nl', 'jfk-us', 'sjc2-us', 'syd-au', 'zrh2-ch', 'nrt-jp']:
        for filename in glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/ipaccur_nodstip_%s.202012*' %(vp, vp)):
            with open(filename, 'r') as rf:
                data = json.load(rf)
                print(len(data))
    print('')
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/cmp_method/union_ip_accur_info.sxt_bdr.202012', 'r') as rf:
        data = json.load(rf)
        print(len(data))

def StatIpCrossing(filename):
    data = []
    with open(filename, 'r') as rf:
        ip_accur_info = json.load(rf)
        #ip_accur_info[_ip] = [[0, 0.0, set()], [0, 0.0, set()], [0, 0.0, set()], [0, 0.0, set()], [0, 0.0, set()]]
        for _ip, val in ip_accur_info.items():
            data.append(sum([len(elem[2]) for elem in val]))
    
    data1 = []
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/cmp_method/union_ip_accur_info.sxt_bdr.201903', 'r') as rf:
        g_ip_accur_info = json.load(rf)
        for _ip, val in g_ip_accur_info.items():
            data1.append(sum([len(elem[2]) for elem in val]))
    
    data = sorted(data)
    print(len(data))
    print(data.index(2))
    print(data.index(3))
    print(data.index(4))
    print(data.index(5))
    print(data.index(6))
    print('')
    data1 = sorted(data1)
    print(len(data1))
    print(data1.index(2))

    fig, ax = plt.subplots()
    ecdf = sm.distributions.ECDF(data)
    x = np.linspace(min(data), max(data), len(data))
    y = ecdf(x)
    ax.set_xscale('symlog')
    ax.plot(x, y, color='b', label='test')

    ecdf = sm.distributions.ECDF(data1)
    x = np.linspace(min(data1), max(data1), len(data1))
    y = ecdf(x)
    ax.plot(x, y, color='r', label='g')

    ax.set_xlim((0,max(data1)))
    ax.set_ylim((0,1))
    ax.tick_params(labelsize=12)
    ax.legend(loc='lower right', borderpad=0.5, labelspacing=1, prop={'size': 12}, ncol=3)
    plt.tight_layout()
    eps_fig = plt.gcf() # 'get current figure'
    eps_fig.savefig('last_ab_cdf.eps', format='eps')
    plt.show()

def cal_grouped_ips():
    span = []
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/ams-nl/sxt_bdr/ipaccur_nodstip_ams-nl.20201216.json', 'r') as rf:
    #with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/cmp_method/union_ip_accur_info.sxt_bdr.202012', 'r') as rf:
        data = json.load(rf)
        for _ip, val in data.items():
            span.append(sum([elem[0] for elem in val]))
    span = sorted(span)
    total = len(span)

    for i in range(2, 15):
        print(span.index(i) / total)
    ecdf = sm.distributions.ECDF(span)
    x = np.linspace(min(span), max(span), len(span))
    y = ecdf(x)        
    fig, ax = plt.subplots()
    ax.scatter(x, y)
    ax.set_xscale('symlog')    
    ax.set_xlim(0, x[-1] + 1)
    ax.set_ylim(0, 1)
    ax.set_xlabel('Number of traces an IP exists', fontsize=24)
    # ax.grid(b=True, which='major')
    # ax.grid(b=True, which='minor', alpha=0.2)

    plt.tight_layout()
    plt.show()
    #eps_fig = plt.gcf() # 'get current figure'
    #eps_fig.savefig(save_filename, format='eps')
    fig.savefig('ip_span.eps')

def check_diff_mm():
    prev_ips = set()
    prev_total = -82918
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/ams-nl/sxt_bdr/mm_ams-nl.20190615', 'r') as rf:
        lines = [rf.readline() for _ in range(3)]
        while lines[0]:
            prev_total += 1
            (mm_ips, pm_ips, _) = lines[2].split(']')
            for _ip in mm_ips[1:].split(','):
                prev_ips.add(_ip)
            for _ip in pm_ips[1:].split(','):
                prev_ips.add(_ip)
            lines = [rf.readline() for _ in range(3)]
    #for year in range(2020, 2021):
    for year in range(2019, 2020):
        for month in range(7, 13):
            if month == 11: continue
            for filename in glob.glob(r'/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/ams-nl/sxt_bdr/mm_ams-nl.%s%s*'%(str(year), str(month).zfill(2))):
                print(filename)
                with open(filename, 'r') as rf:
                    total = 0
                    ip_lines = defaultdict(set)
                    lines = [rf.readline() for _ in range(3)]
                    while lines[0]:
                        total += 1
                        (mm_ips, pm_ips, _) = lines[2].split(']')
                        for _ip in mm_ips[1:].split(','):
                            if _ip not in prev_ips:
                                #cur_ip_info[_ip] = cur_ip_info[_ip] + lines
                                ip_lines[_ip].add(total)
                        for _ip in pm_ips[1:].split(','):
                            if _ip not in prev_ips:
                                #cur_ip_info[_ip] = cur_ip_info[_ip] + lines
                                ip_lines[_ip].add(total)
                        lines = [rf.readline() for _ in range(3)]
                    s = sorted(ip_lines.items(), key=lambda x: len(x[1]), reverse=True)
                    print(total - prev_total)
                    total_set = set()
                    for i in range(0, 5):
                        print('{}: {}'.format(s[i][0], len(s[i][1].difference(total_set)) / total))
                        total_set = total_set | s[i][1]

def cal_286_in_3257():
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/ams-nl/sxt_bdr/mm_ams-nl.20190716', 'r') as rf:
        total = 0
        t = 0        
        lines = [rf.readline() for _ in range(3)]
        while lines[0]:
            (mm_ips, pm_ips, ip_path) = lines[2].strip('\n').split(']')
            if mm_ips.__contains__('77.67.76.34'):
                total += 1
                if lines[1].__contains__(' 286 '):
                    t += 1
            lines = [rf.readline() for _ in range(3)]
        print('{},{}'.format(t, total))

def cal_286_and_3257():
    os.chdir('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/ams-nl/sxt_bdr/')
    #for year in range(2019, 2020):
    for year in range(2019, 2021):
        for month in range(1, 13):
            date = str(year) + str(month).zfill(2)
            total_mm = 0
            t_mm = 0
            total_match = 0
            t_match = 0
            print(date)
            for filename in glob.glob(r'mm_ams-nl.%s*'%date):
                with open(filename, 'r') as rf:                            
                    lines = [rf.readline() for _ in range(3)]
                    while lines[0]:
                        total_mm += 1
                        (mm_ips, pm_ips, ip_path) = lines[2].strip('\n').split(']')
                        #if mm_ips.__contains__('77.67.76.34') or mm_ips.__contains__('134.222.155.83'):
                        bgp_list = lines[1].strip('\n').strip('\t').split(' ')
                        if len(bgp_list) > 1:
                            if bgp_list[1] == '286' or bgp_list[1] == '3257':
                                t_mm += 1
                        lines = [rf.readline() for _ in range(3)]
            for filename in glob.glob(r'match_ams-nl.%s*'%date):
                with open(filename, 'r') as rf:                            
                    lines = [rf.readline() for _ in range(3)]
                    while lines[0]:
                        total_match += 1
                        #if lines[2].__contains__('77.67.76.34') or lines[2].__contains__('134.222.155.83'):
                        bgp_list = lines[1].strip('\n').strip('\t').split(' ')
                        if len(bgp_list) > 1:
                            if bgp_list[1] == '286' or bgp_list[1] == '3257':
                                t_match += 1
                        lines = [rf.readline() for _ in range(3)]
            print('(t_mm + t_match)/(total_mm + total_match):{}'.format((t_mm + t_match)/(total_mm + total_match)))
            print('t_mm/(t_mm + t_match):{}'.format(t_mm / (t_mm + t_match)))
            print('t_mm/total_mm:{}'.format(t_mm / total_mm))

def temp():    
    #data = [0.089536365, 0.098677187, 0.087227212, 0.097831961, 0.109612634, 0.102819207, 0.107647962, 0.127290014, 0.101807275, 0.109803068, 0.112596719, 0.116241582, 0.176130552, 0.170040419, 0.169243954, 0.163861934, 0.167583779, 0.177808521, 0.188528905, 0.175350971, 0.210822344, 0.201037783, 0.243931174, 0.184692328, 0.206833257, 0.20676356, 0.206159009, 0.20750225, 0.220889359, 0.268920963, 0.274108521, 0.27160006, 0.279076165, 0.266934704, 0.287496862, 0.294249781, 0.281548863, 0.318865136, 0.347158012, 0.264939213]
    data1 = [0.014779874213836478,0.16235693090292497,0.13676869430453312,0.10465116279069768,0.017360071834780007,0.016090104585679808,0.01619644723092999,0.022785999530185577,0.025565388397246803,0.026766846677623857,0.026397838287258366,0.03485318691811888,0.03613777526821005,0.04080090668681526,0.03154459753444525,0.03775280898876404,0.02534775888717156,0.036627505183137524,0.02274530547474213,0.03052464228934817,0.025938189845474614,0.5340729001584786,0.0341796875,0.029682398337785694,0.041743970315398886,0.03875968992248062,0.05948252867431315,0.05880376344086022,0.04068202213580616,0.03528468323977546,0.028398058252427184,0.019738528582414764,0.02707527521570961,0.039272535582498685,0.038780245276765,0.048624098316858134,0.047417218543046355,0.032153179190751446,0.025421387123514782,0.032501989917750065]
    data2 = [0.2748427672955975,0.24077999152183127,0.22433165439752034,0.20333123821495916,0.23885064351990423,0.279431482971306,0.3484848484848485,0.31759455015268967,0.39503441494591934,0.35078656961728105,0.35439617543130325,0.3110527572212939,0.36589497459062675,0.3921420476010578,0.35786802030456855,0.36764044943820223,0.362596599690881,0.4250172771250864,0.4623115577889447,0.47980922098569156,0.48951434878587197,0.14833597464342313,0.5126953125,0.5520926090828139,0.5473098330241187,0.5252713178294574,0.5148039477193919,0.571236559139785,0.6389470535447203,0.6634589681903235,0.6361650485436893,0.688797744168162,0.7396608152335614,0.6918819188191881,0.7063307921776599,0.6134117018434411,0.6304635761589404,0.7315751445086706,0.6827852998065764,0.7281772353409393]
    i = range(0, len(data1))
    #fig, ax = plt.subplots(figsize=(6, 6))
    fig, ax = plt.subplots()
    #ax.bar(i, data,color='gold')
    
    ax.bar(i, data1)
    ax.bar(i, data2, bottom=data1)
    plt.tick_params(labelsize=12)
    
    plt.tight_layout()
    eps_fig = plt.gcf() # 'get current figure'
    plt.show()

def temp2():
    ana_res_filename = '/mountdisk2/ana_roa_res.json'
    stat_info = defaultdict(list)#{'valid': 0, 'subpref': 0, 'moas': 0, 'uni-invalid': 0}
    with open(ana_res_filename, 'r') as rf:
        stat_info = json.load(rf)
    fig, ax = plt.subplots()
    x_range = range(0, len(stat_info['valid']))
    types = ['uni-invalid', 'moas', 'subpref']
    colors = ['green', 'blue', 'red', 'brown', 'cyan', 'black']
    markers = ['o', '+', '^', '_', 'x', 's']
    for i in range(0, len(types)):
        if i == 1:
            ax.scatter(x_range, stat_info[types[i]], alpha=0.3,marker=markers[i],c=colors[i],linewidth=1,label=types[i])
        else:
            ax.scatter(x_range, stat_info[types[i]], alpha=0.3,marker=markers[i],edgecolors=colors[i],facecolors='none',linewidth=1,label=types[i])
    #ax.set_ylim([0, 6000])

    ax.legend(bbox_to_anchor=(0, 1.02, 1, 0.2), loc='lower left', mode='expand', ncol=3, prop={'size': 12})
    ax.grid(axis='y',linestyle=':',color='grey',alpha=0.6)
    xticks = range(0, 49, 6)
    ax.set_xticks(xticks)
    xticklabels = ['2018.1', '2018.6', '2019.1', '2019.6', '2020.1', '2020.6', '2021.1', '2021.6', '2022.1']
    ax.set_xticklabels(xticklabels)
    plt.tight_layout()
    eps_fig = plt.gcf() # 'get current figure'
    plt.show()

def PlotHist_tmp():
    arr1 = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0064516129032258064, 0.01904761904761905, 0.0196078431372549, 0.020618556701030927, 0.023255813953488372, 0.025, 0.026785714285714284, 0.029940119760479042, 0.03, 0.030612244897959183, 0.030864197530864196, 0.03333333333333333, 0.03389830508474576, 0.03571428571428571, 0.036585365853658534, 0.03723404255319149, 0.037383177570093455, 0.0390625, 0.04132231404958678, 0.041666666666666664, 0.04201680672268908, 0.047058823529411764, 0.05, 0.050314465408805034, 0.05333333333333334, 0.056179775280898875, 0.058333333333333334, 0.059322033898305086, 0.06097560975609756, 0.06164383561643835, 0.06164383561643835, 0.06666666666666667, 0.06918238993710692, 0.06962025316455696, 0.07006369426751592, 0.07534246575342465, 0.07766990291262135, 0.07857142857142857, 0.07874015748031496, 0.08064516129032258, 0.08235294117647059, 0.08602150537634409, 0.08737864077669903, 0.09278350515463918, 0.0962566844919786, 0.0963855421686747, 0.0967741935483871, 0.0975609756097561, 0.1, 0.10215053763440861, 0.10218978102189781, 0.10416666666666667, 0.10576923076923077, 0.10869565217391304, 0.10891089108910891, 0.12, 0.125, 0.12972972972972974, 0.14093959731543623, 0.1411764705882353, 0.14285714285714285, 0.14285714285714285, 0.14432989690721648, 0.1464968152866242, 0.15337423312883436, 0.15483870967741936, 0.15894039735099338, 0.16666666666666666, 0.17699115044247787, 0.1782178217821782, 0.18478260869565216, 0.1875, 0.18888888888888888, 0.18888888888888888, 0.19148936170212766, 0.1919191919191919, 0.1935483870967742, 0.2, 0.2, 0.2, 0.20224719101123595, 0.2037037037037037, 0.2111111111111111, 0.21296296296296297, 0.21505376344086022, 0.2336448598130841, 0.25, 0.26136363636363635, 0.2625, 0.2857142857142857, 0.2903225806451613, 0.2988505747126437, 0.3333333333333333, 0.34146341463414637, 0.36507936507936506, 0.4105263157894737, 0.42857142857142855, 0.5444444444444444, 0.5818181818181818, 0.6666666666666666, 0.6666666666666666, 0.6818181818181818, 0.7090909090909091, 0.75, 0.7623762376237624, 0.7736842105263158, 0.8333333333333334, 0.8360655737704918, 0.8888888888888888, 0.8943661971830986, 0.8991596638655462, 0.9090909090909091, 0.93, 0.9338842975206612, 0.9351851851851852, 0.9411764705882353, 0.9523809523809523, 0.9625, 0.9655172413793104, 0.9705882352941176, 0.978021978021978, 0.9787234042553191, 0.9803921568627451, 0.9805825242718447, 0.9819819819819819, 0.9819819819819819, 0.9877300613496932, 0.9880952380952381, 0.9885057471264368, 0.9893617021276596, 0.9895833333333334, 0.9906542056074766, 0.9914529914529915, 0.9915254237288136, 0.9924812030075187, 0.9927007299270073, 0.9939024390243902, 0.9945054945054945, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]
    arr2 = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0064516129032258064, 0.01904761904761905, 0.0196078431372549, 0.020618556701030927, 0.023255813953488372, 0.025, 0.026785714285714284, 0.029940119760479042, 0.03, 0.030612244897959183, 0.030864197530864196, 0.03333333333333333, 0.03389830508474576, 0.03571428571428571, 0.036585365853658534, 0.03723404255319149, 0.037383177570093455, 0.0390625, 0.04132231404958678, 0.041666666666666664, 0.04201680672268908, 0.047058823529411764, 0.05, 0.050314465408805034, 0.05333333333333334, 0.056179775280898875, 0.058333333333333334, 0.059322033898305086, 0.06097560975609756, 0.06164383561643835, 0.06164383561643835, 0.06666666666666667, 0.06918238993710692, 0.06962025316455696, 0.07006369426751592, 0.07534246575342465, 0.07766990291262135, 0.07857142857142857, 0.07874015748031496, 0.08064516129032258, 0.08235294117647059, 0.08602150537634409, 0.08737864077669903, 0.09278350515463918, 0.0962566844919786, 0.0963855421686747, 0.0967741935483871, 0.0975609756097561, 0.1, 0.10215053763440861, 0.10218978102189781, 0.10416666666666667, 0.10576923076923077, 0.10869565217391304, 0.10891089108910891, 0.12, 0.125, 0.12972972972972974, 0.14093959731543623, 0.1411764705882353, 0.14285714285714285, 0.14285714285714285, 0.14432989690721648, 0.1464968152866242, 0.15337423312883436, 0.15483870967741936, 0.15894039735099338, 0.16666666666666666, 0.17699115044247787, 0.1782178217821782, 0.18478260869565216, 0.1875, 0.18888888888888888, 0.18888888888888888, 0.19148936170212766, 0.1919191919191919, 0.1935483870967742, 0.2, 0.2, 0.2, 0.20224719101123595, 0.2037037037037037, 0.2111111111111111, 0.21296296296296297, 0.21505376344086022, 0.2336448598130841, 0.25, 0.26136363636363635, 0.2625, 0.2857142857142857, 0.2903225806451613, 0.2988505747126437, 0.3333333333333333, 0.34146341463414637, 0.36507936507936506, 0.4105263157894737, 0.42857142857142855, 0.5444444444444444, 0.5818181818181818, 0.6666666666666666, 0.6666666666666666, 0.6818181818181818, 0.7090909090909091, 0.75, 0.7623762376237624, 0.7736842105263158, 0.8333333333333334, 0.8360655737704918, 0.8888888888888888, 0.8943661971830986, 0.8991596638655462, 0.9090909090909091, 0.93, 0.9338842975206612, 0.9351851851851852, 0.9411764705882353, 0.9523809523809523, 0.9625, 0.9655172413793104, 0.9705882352941176, 0.978021978021978, 0.9787234042553191, 0.9803921568627451, 0.9805825242718447, 0.9819819819819819, 0.9819819819819819, 0.9877300613496932, 0.9880952380952381, 0.9885057471264368, 0.9893617021276596, 0.9895833333333334, 0.9906542056074766, 0.9914529914529915, 0.9915254237288136, 0.9924812030075187, 0.9927007299270073, 0.9939024390243902, 0.9945054945054945, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]
    # stat = Counter()
    # for elem in arr:
    #     stat[elem] += 1
    # val = sorted(stat.items(), key=lambda x:x[0])
    # val1 = [v[1] / len(arr) for v in val]
    #sns.distplot(arr1,hist=False,kde_kws={"color":"red","linestyle":"-"},norm_hist=False,label="hijack")
    #sns.distplot(arr2,hist=False,kde_kws={"color":"blue","linestyle":"--"},norm_hist=False,label="not-hijack")
    fig, ax = plt.subplots()
    ax.hist(arr1)
    plt.xlim((0, 1))
    plt.tight_layout()
    plt.show()

def PlotIpRate_3():
    data_info = {'succ': {}, 'unmap': {}, 'fail': {}}
    midar_dates = set()
    for filename in os.listdir('/mountdisk1/ana_c_d_incongruity/midar_data/'):
        midar_dates.add(filename.split('_')[0].replace('-', ''))
    for _type in data_info.keys():
        #data_info[_type]['midar_approx'] = []
        for map_method in ['hoiho_s_bdr', 'midar', 'ori_bdr']:
            data_info[_type][map_method] = []            
            filenames = glob.glob(r'/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/cmp_method/ip_stat.%s*' %(map_method))
            filenames.sort()
            for filename in filenames:
                with open(filename, 'r') as rf:
                    data = rf.read()
                    find_res = re.findall(r'%s: (.*?)\(' %_type, data)
                    if find_res:
                        if False:#map_method == 'midar':
                            date = filename.split('.')[-1]
                            if date[:6] in midar_dates:
                                data_info[_type]['midar'].append(float(find_res[0]))
                                data_info[_type]['midar_approx'].append(np.nan)
                            else:
                                data_info[_type]['midar'].append(np.nan)
                                data_info[_type]['midar_approx'].append(float(find_res[0]))
                        else:
                            data_info[_type][map_method].append(float(find_res[0]))
                    else:
                        data_info[_type][map_method].append(np.nan)
    # for _type in data_info.keys():
    #     print('{}'.format(data_info[_type]['midar']))
    #     print('{}'.format(data_info[_type]['midar_approx']))
    #PlotArray_List(data_info, 'vp\n(date)', ['succ rate', 'fail rate', 'other rate', 'unmap rate', 'ixp_as rate'], [[0.5, 1], [0, 0.2], [0, 0.2], [0, 0.5], [0, 0.5]], vp_divs, xticks2, xticklabels2, 'ip_rate_bdr_hoiho_s.eps')
    PlotArray_Union(data_info, 'vp\n(date)', ['succ rate', 'unmap rate', 'fail rate'], [[0.6, 1], [0, 0.3], [0, 0.1]], 'ip_rate_bdr_hoiho_s.eps')
    
if __name__ == '__main__':
    #PlotIpRate_3()
    #get_ark_discrete_continous()
    #plot_discrete_continous_2()
    #PlotHist_tmp()
    #temp2()
    cmp_discr()
    #cal_286_in_3257()
    #cal_286_and_3257()
    #check_diff_mm()
    #StatIpCrossing()
    #cal_grouped_ips()
    #StatIpCrossing('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/nrt-jp/sxt_bdr/ipaccur_nodstip_nrt-jp.20200215.json')
    #stat_traceroute_ab()
    #stat_traceroute_ab_2()
    #stat_discrete_mm_num()
    #plot_discrete_continous()
    #get_mean_trace_mm_rate()
    #stat_mm_ip_rel()
    # category_names = ['IXP', 'Extra-tail', 'Succeed IP']
    # results = {'ams-nl': [0.12, 0.03, 0.03], 'jfk-us': [0.18, 0.02, 0.01], 'sjc2-us': [0.26, 0.05, 0.01], 'syd-au': [0.13, 0.04, 0.01], 'zrh2-ch': [0.17, 0.03, 0.01], 'nrt-jp': [0.04, 0.07, 0.02]}
    #category_names = ['continuous-mismatch', 'discrete-mismatch']
    #results = {'ams-nl-1': [0.07, 0.93], 'ams-nl-2': [0.31, 0.69], 'jfk-us': [0.63, 0.37], 'sjc2-us': [0.16, 0.84], 'syd-au': [0.12, 0.88], 'zrh2-ch': [0.11, 0.89], 'nrt-jp': [0.27, 0.73]}
    #distr_hori_bar_chart(results, category_names)
    #stat_aggr_ases()
    #stat_three_mm_kinds()
    #stat_succ_ip()
    #stat_discrete_continuous_mm([sys.argv[1]])
    #stat_discrete_continuous_mm(None)

    #PlotLinkAccr()
    #PlotIpSpan()
    #StatIpSpan()
    #PlotIpRate()
    #PlotIpRate_2()
    #PlotIpRate_midar()
    #PlotTraceMatchRate()
    #PlotTraceMatchRate_v2()
    #PlotLoopRate()

    #PlotAbStat_v2()
    #PlotCdf()
    #PlotCdf_2()
    #StatIncompleteTraces()
    #StatNobgp()
    #StatIxp()
    #StatAb()
    #StatMultipath()

    #PlotAbStat()

    #CheckUndo()

    #CollectMatchIpStat()
    #CalMatchIpStat()

    #StatClassify()

    #StatLastExtraHop()

    #StatAbPercent()

    # date = sys.argv[1] + '15'
    # vp = 'zrh2-ch'
    # ip = '212.36.135.22'
    # print(date)
    # os.chdir('/mountdisk1/ana_c_d_incongruity/out_my_anatrace/' + vp + '_' + date + '/ribs_midar_bdrmapit/')
    # os.system('cat 1_has_set 2_has_loop 3_single_path 4_multi_path 5_has_ixp_ip > test')
    # ConnectToBdrMapItDb(global_var.par_path + global_var.out_bdrmapit_dir + 'bdrmapit_' + vp + '_' + date + '.db')
    # ConstrBdrCache()
    # print(GetIp2ASFromBdrMapItDb(ip))
    # StatNeigborIp('test', ip)
    #StatNeigborIp('/mountdisk1/ana_c_d_incongruity/traceroute_data/zrh2-ch.' + date, '212.36.135.22')
