
import os
import requests
import sys

resp_plain = {0: 'connect succeed', 1: 'Failed to get response', 2: 'resource empty', 3: ''}

def TryConnLg(url, req):
    req = requests.Session()
    res = 100
    #error_text = ''
    for i in range(0,2):
        try:
            resource = req.get(url, timeout=10) 
            if resource:                
                if resource.status_code == 200:
                    return 0
                else:
                    res = 1 #'Failed to get response'
            else:
                if res > 2:
                    res = 2 #'resource empty'
        except Exception as e:
            if res > 3:
                res = 3 #'Error: %s' %e
                resp_plain[3] = 'Error: %s' %e
    return res

def LgHasTraceroute(url, req):
    for i in range(0,3):
        try:
            resource = req.get(url, timeout=10) 
            if resource:                
                if resource.status_code == 200:
                    if resource.text.__contains__('traceroute') or resource.text.__contains__('Traceroute'):
                        return True
                    else:
                        return False
        except Exception as e:
            pass
    return False

def TryEachLg(lg_filename):
    req = requests.Session()
    done_url_set = set()
    os.chdir('/home/slt/code/ana_c_d_incongruity/')
    with open(lg_filename + '_conn_res', 'r') as rf:
        data = rf.read()
        for elem in data.strip('\n').split('\n'):
            url = elem.split(':\t')[0]
            done_url_set.add(url)
    with open(lg_filename + '_conn_res', 'a') as wf:
        with open(lg_filename, 'r') as rf:
            data = rf.read()
            for elem in data.strip('\n').split('\n'):
                url = elem.split('\t')[1].strip('\n')
                if url in done_url_set:
                    continue            
                conn_res = TryConnLg(url, req)
                print(url + ':\t' + resp_plain[conn_res])
                wf.write(url + ':\t' + resp_plain[conn_res] + '\n')
                done_url_set.add(url)

def FindLgWithTraceroute(lg_filename):
    req = requests.Session()
    #done_url_set = set()
    os.chdir('/home/slt/code/ana_c_d_incongruity/')
    # with open(lg_filename + '_conn_res', 'r') as rf:
    #     data = rf.read()
    #     for elem in data.strip('\n').split('\n'):
    #         url = elem.split(':\t')[0]
    #         done_url_set.add(url)
    with open(lg_filename + '_with_traceroute', 'w') as wf:
        with open(lg_filename, 'r') as rf:
            data = rf.read()
            for elem in data.strip('\n').split('\n'):
                url = elem.split(':\t')[0]
                # if url in done_url_set:
                #     continue            
                if LgHasTraceroute(url, req):
                    print(url)
                    wf.write(url + '\n')
                #done_url_set.add(url)

def FindRecaptcha(lg_filename):
    pass

if __name__ == '__main__':
    #print(os.system('pwd'))
    #TryEachLg('lg')
    FindLgWithTraceroute('lg_conn_res_succeed')
