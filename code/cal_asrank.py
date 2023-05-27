
import os

import global_var

def CalOneDateAsRank(filename):
    date = filename.split('.')[0]
    rf = open(filename, 'r')
    wf = open(date + '.asrank.txt', 'w')
    curline = rf.readline()
    info_list = dict()

    while curline:
        if curline.startswith('#'):
            curline = rf.readline()
            continue
        elems = curline.strip("\n").split(' ')
        asn = elems[0]
        cone_size = len(elems) - 1
        info_list[asn] = cone_size
        curline = rf.readline()
    rf.close()

    sort_list = sorted(info_list.items(), key=lambda d:d[1], reverse=True)
    for i in range(0, len(sort_list)):
        wf.write(sort_list[i][0] + ' ')
    wf.close()

if __name__ == '__main__':    
    par_dir = global_var.par_path +  global_var.rel_cc_dir
    print(par_dir)
    os.chdir(par_dir)
    for root,dirs,files in os.walk('.'):
        for filename in files:
            if filename.__contains__('ppdc-ases'):
                print(filename)
                CalOneDateAsRank(filename)
