
from turtle import done
import requests
import re
import os
from bs4 import BeautifulSoup
import multiprocessing as mp
import json
import time

from requests.models import Request
#from requests.packages.urllib3.util import timeout
from gen_ip2as_command import PreGetSrcFilesInDirs
from utils_v2 import GetAsRankDict, GetAsRankFromDict, GetAsNeighs, GetAsNeighsFromDict, ClearAsNeighs

#ip_form = re.compile('(1\d{2}|2[0-4]\d|25[0-5]|[1-9]\d|[1-9])\.(1\d{2}|2[0-4]\d|25[0-5]|[1-9]\d|\d)\.(1\d{2}|2[0-4]\d|25[0-5]|[1-9]\d|\d)\.(1\d{2}|2[0-4]\d|25[0-5]|[1-9]\d|\d)')
ip_form = re.compile('\d+\.\d+\.\d+\.\d+')
ipv6_form = re.compile('[0-9a-fA-F]*:[0-9a-fA-F]*:[0-9a-fA-F]*:[0-9a-fA-F]*:[0-9a-fA-F]*:[0-9a-fA-F]*')

def FindIpInData(data):
    global ip_form
    find_res = re.findall(ip_form, data)    
    if not find_res:
        find_res = re.findall(ipv6_form, data)
        if not find_res:
            if data.__contains__('*') or data.__contains__('???'):
                return '*'
            else:
                return None
    res_set = set()
    for elem in find_res:
        res_set.add(elem)
    return ','.join(list(res_set))

def GetTrFromData(data):
    trace_str = ''
    start_flag = False
    i = 0
    related_data = None
    if data.__contains__('</pre>') or data.__contains__('</PRE>'):
        related_data = re.findall('<pre.*?>(.+)</pre>', data, re.DOTALL | re.IGNORECASE)
    else:
        related_data = [data]
    if not related_data:
        return ''
    related_data[0] = related_data[0].replace('<br>', '\n') #padding
    related_data[0] = related_data[0].replace('<BR>', '\n') #padding
    related_data[0] = related_data[0].replace('\\n', '\n') #padding
    related_data[0] = related_data[0].replace('&nbsp;', ' ') #padding
    related_data[0] = related_data[0].replace('<!--', ' ') #padding
    related_data[0] = related_data[0].replace('-->', ' ') #padding
    related_data[0] = related_data[0].replace('<br />', '\n') #padding
    related_data[0] = related_data[0].replace('\"', ' ') #padding
    related_data[0] = related_data[0].replace('|--', ' ') #padding
    related_data[0] = related_data[0].replace('\t', ' ') #padding
    related_data[0] = related_data[0].replace('&amp;nbsp;', ' ') #padding
    if related_data[0].__contains__('no route') or related_data[0].__contains__('No route'):
        return ''
    #print(related_data[0])
    for cur_line in related_data[0].strip('\n').split('\n'):
        elems = cur_line.strip('\t').strip(' ').split(' ')
        concat_ch = ''
        if not elems[0].strip('.').isdigit():
            if i <= 0: #还没开始
                continue
            else: #{}
                concat_ch = ','
        elif int(elems[0].strip('.')) != i + 1:
            continue
        else:
            i = int(elems[0].strip('.'))
            concat_ch = ' '
        ip = FindIpInData(cur_line)
        if not ip:
            #print('Error type: %s' %cur_line)        
            pass
        else:
            trace_str += concat_ch + ip
    return trace_str.strip(' ')

def GetTrFromUrlGet(url, req, eye = '', headers = None, timeout = 30):
    try:
        resource = None
        if headers:
            resource = req.get(url, headers=headers, timeout=timeout) 
        else:
            resource = req.get(url, timeout=timeout) 
        if resource:                
            if resource.status_code == 200:
                return GetTrFromData(resource.text)
            else:
                print(url + ': ' + resource.status_code)
    except Exception as e:
        print('Connect to %s eye %s error: %s' %(url, eye, e))
        pass
    return None

def GetTrFromUrlPost(url, req, data, eye = '', headers = None, timeout = 30):
    try:
        resource = None
        if headers:
            resource = req.post(url, data=data, headers=headers, timeout=timeout) 
        else:
            resource = req.post(url, data=data, timeout=timeout)
        if resource:                
            if resource.status_code == 200:
                return GetTrFromData(resource.text)
                # else:
                #     print(resource.text)
    except Exception as e:
        #print('Connect to %s eye %s error: %s' %(url, eye, e))
        pass
    return None

def JustGetRespFromUrl(url, req, headers = None):
    try:
        resource = None
        if headers:
            resource = req.get(url, headers=headers, timeout=30) 
        else:
            resource = req.get(url, timeout=30) 
        if resource:                
            if resource.status_code == 200:
                return resource.content
    except Exception as e:
        print('Connect to %s error: %s' %(url, e))
    return None    

def method_tr_post_base(url, req, data, headers = None, timeout = 30):
    trace_list = []
    trace = GetTrFromUrlPost(url, req, data, None, headers, timeout)
    if trace:
        trace_list.append(trace)
    return trace_list

def method_tr_get_base(url, req, headers = None, timeout = 30):
    trace_list = []
    trace = GetTrFromUrlGet(url, req, None, headers, timeout)
    if trace:
        trace_list.append(trace)
    return trace_list

def GetEyes(url, req):
    data = JustGetRespFromUrl(url, req)
    if not data:
        return []
    eyes = []
    try:
        soup = BeautifulSoup(data.decode('utf-8'), 'html.parser')
        find_res = soup.find_all('option')
        for link in find_res:
            eyes.append(link['value'])
    except Exception as e:
            pass
    return eyes
    
def GetTraceListFromEyesGet(eyes, url, req, headers = None, timeout = 30):
    trace_list = []
    for eye in eyes:
        print('eye: %s' %eye)
        url_1 = url.replace('eye', eye)
        trace = GetTrFromUrlGet(url_1, req, eye, headers, timeout)
        if trace:
            trace_list.append(trace)
    return trace_list
    
def GetTraceListFromEyesPost(eyes, url, req, data, key_of_eye, headers = None, timeout=30):
    trace_list = []
    for eye in eyes:
        print('eye: %s' %eye)
        trace = None
        data[key_of_eye] = eye
        trace = GetTrFromUrlPost(url, req, data, eye, headers, timeout)
        if trace:
            trace_list.append(trace)
        else:
            print('%s not get trace' %eye)
    return trace_list

def tr_as59(dst_ip, req): #https://www.net.wisc.edu/cgi-bin/public/lg-as59.pl
    eyes = ['rx-cssc-b380-1-core', 'rx-animal-226-2-core', 'r-wa222-12-1-radial', 'r-wa222-12-2-radial', 'r-csscplat-b380-3-core', 's-cssc-b380-3-core']
    url = 'https://www.net.wisc.edu/cgi-bin/public/lg-as59.pl?router=eye&query=traceroute&arg=%s' %dst_ip
    return GetTraceListFromEyesGet(eyes, url, req)

def tr_priceton(dst_ip, req): #https://www.net.princeton.edu/traceroute.html
    data = {'target': '8.8.8.8', 'cmd': '+Go+'}
    url = 'https://www.net.princeton.edu/cgi-bin/traceroute.pl'
    return method_tr_post_base(url, req, data)

def tr_garr(dst_ip, req): #https://gins.garr.it/LG/
    url = 'https://gins.garr.it/LG/'
    eyes = GetEyes(url, req)
    
    os.environ['NO_PROXY'] = 'gins.garr.it'
    headers = {"referer":"https://gins.garr.it/LG/"} #没有这一句会响应'forbidden'
    url = 'https://gins.garr.it/LG/lg_ajax.php?command=traceroute%%20ADDRESS&router=eye&ip=%s' %dst_ip
    return GetTraceListFromEyesGet(eyes, url, req, headers)

def tr_belwue(dst_ip, req): #http://route-server.belwue.net/summary/route-server/ipv4
    url = 'http://route-server.belwue.net/traceroute/route-server/ipv4?q=%s' %dst_ip
    return method_tr_get_base(url, req)

def tr_switch_ch_v6(dst_ip, req): #https://www.switch.ch/network/tools/ipv6lookingglass/
    eyes = ['swiBA3.switch.ch', 'swiCE1.switch.ch', 'swiCE2.switch.ch', 'swiCE3.switch.ch', 'swiEZ3.switch.ch', 'swiKR2.switch.ch', 'swiZH1.switch.ch']
    url = 'https://www.switch.ch/network/tools/ipv6lookingglass/'
    data = {'router': '', 'query': 'trace', 'arg': dst_ip} 
    trace_list = GetTraceListFromEyesPost(eyes, url, req, data, 'router')
    return trace_list

def tr_switch_ch(dst_ip, req): #https://www.switch.ch/network/tools/lookingglass/
    eyes = ['swiBA3.switch.ch', 'swiCE1.switch.ch', 'swiCE2.switch.ch', 'swiCE3.switch.ch', 'swiEZ3.switch.ch', 'swiKR2.switch.ch', 'swiZH1.switch.ch']
    url = 'https://www.switch.ch/network/tools/lookingglass/'
    data = {'router': '', 'query': 'trace', 'arg': dst_ip} 
    trace_list = GetTraceListFromEyesPost(eyes, url, req, data, 'router')
    return trace_list

def tr_dfn(dst_ip, req): #https://www.noc.dfn.de/lg
    eyes = ['Garching']
    url = 'https://www.noc.dfn.de/lg/'
    data = {'router': '', 'query': 'trace', 'protocol': 'IPv4', 'addr': dst_ip}
    trace_list = GetTraceListFromEyesPost(eyes, url, req, data, 'router')
    return trace_list

def tr_rediris(dst_ip, req): #http://www.rediris.es/red/lg/
    headers = {"referer":"https://www.rediris.es/red/lg/"}
    eyes = ['130.206.194.254', '130.206.212.254', '130.206.198.125', '130.206.206.250', '130.206.197.254', '130.206.198.254', '130.206.195.254', '130.206.211.254', '130.206.201.254']
    url = 'https://www.rediris.es/red/lg/lg.pl'
    data = {'rtr': '', 'query': 'traceroute', 'family': 'inet', 'addr': dst_ip}
    trace_list = GetTraceListFromEyesPost(eyes, url, req, data, 'rtr', headers)
    return trace_list

def tr_telus(dst_ip, req): #http://aurora.on.tac.net/
    url = 'http://aurora.on.tac.net/'
    eyes = GetEyes(url, req)
    
    #headers = {"referer":"http://aurora.on.tac.net/"} #没有这一句会响应'forbidden'
    url = 'http://aurora.on.tac.net/'    
    data = {'router': '', 'query': 'trace', 'protocol': 'IPv4', 'addr': dst_ip}
    trace_list = GetTraceListFromEyesPost(eyes, url, req, data, 'router')
    return trace_list

def tr_telstra(dst_ip, req): #https://www.telstra.net/cgi-bin/trace
    url = 'https://www.telstra.net/cgi-bin/trace'
    data = {'destination': dst_ip}
    return method_tr_post_base(url, req, data)

def tr_han_de(dst_ip, req): #http://www.han.de/cgi-bin/nph-trace.cgi
    url = 'https://www.han.de/cgi-bin/nph-trace.cgi'
    data = {'addr': dst_ip, 'start': ' Start ', 'method': 'traceroute', '.cgifields': 'ipv6'}
    return method_tr_post_base(url, req, data)
    
def tr_hopus(dst_ip, req): #http://lg.hopus.net/
    url = 'http://lg.hopus.net/'
    eyes = GetEyes(url, req)
    
    #headers = {"referer":"http://aurora.on.tac.net/"} #没有这一句会响应'forbidden'
    url = 'http://lg.hopus.net/'
    data = {'router': '', 'query': 'trace', 'protocol': 'IPv4', 'addr': dst_ip}
    trace_list = GetTraceListFromEyesPost(eyes, url, req, data, 'router')
    return trace_list

def tr_as1403(dst_ip, req): #http://lg.as1403.net
    url = 'http://lg.as1403.net/raw/traceroute4?host=%s' %dst_ip
    return method_tr_get_base(url, req)

def tr_sunset_se(dst_ip, req): #https://lookingglass.sunet.se/lg.cgi
    url = 'https://lookingglass.sunet.se/lg.cgi'
    eyes = GetEyes(url, req)
    
    #headers = {"referer":"http://aurora.on.tac.net/"} #没有这一句会响应'forbidden'
    url = 'https://lookingglass.sunet.se/lg.cgi'    
    data = {'router': '', 'query': 'trace', 'protocol': 'IPv4', 'addr': dst_ip}
    trace_list = GetTraceListFromEyesPost(eyes, url, req, data, 'router')
    return trace_list

def tr_rnp_br(dst_ip, req): #http://memoria.rnp.br/ip/lg.php
    eyes = ['Rio de Janeiro, RJ', 'Sao Paulo, SP', 'Brasilia, DF', 'Minas Gerais, MG']
    #headers = {"referer":"http://aurora.on.tac.net/"} #没有这一句会响应'forbidden'
    url = 'https://memoria.rnp.br/ip/lg.php'
    data = {'router': '', 'query': 'trace', 'protocol': 'IPv4', 'addr': dst_ip}
    trace_list = GetTraceListFromEyesPost(eyes, url, req, data, 'router')
    return trace_list

def tr_telenor_se(dst_ip, req): #http://lg.telenor.net/mod-perl/lg
    url = 'https://lg.telenor.net/mod-perl/lg'
    eyes = GetEyes(url, req)

    url = 'https://lg.telenor.net/mod-perl/lg?ip_version=IPv4&type=Traceroute&router=eye&parameters=%s&submit=Submit&.cgifields=ip_version&.cgifields=type' %dst_ip
    return GetTraceListFromEyesGet(eyes, url, req)

def tr_wiscnet(dst_ip, req): #https://lg.wiscnet.net/lg/
    url = 'https://lg.wiscnet.net/lg/'
    eyes = GetEyes(url, req)
    
    url = 'https://lg.wiscnet.net/lg/?router=eye&query=traceroute&arg=%s' %dst_ip
    return GetTraceListFromEyesGet(eyes, url, req)
    
def tr_nordu(dst_ip, req): #http://lg.nordu.net/lg.cgi
    url = 'http://lg.nordu.net/lg.cgi'
    eyes = GetEyes(url, req)
    
    url = 'http://lg.nordu.net/lg.cgi'
    data = {'router': '', 'query': 'trace', 'protocol': 'IPv4', 'addr': dst_ip}
    trace_list = GetTraceListFromEyesPost(eyes, url, req, data, 'router')
    return trace_list

def tr_tpg_appt(dst_ip, req): #http://looking-glass.connect.com.au/lg
    url = 'http://looking-glass.connect.com.au/lg'
    eyes = GetEyes(url, req)
    
    url = 'http://looking-glass.connect.com.au/lg'
    data = {'router': '', 'query': 'trace', 'protocol': 'IPv4', 'addr': dst_ip}
    trace_list = GetTraceListFromEyesPost(eyes, url, req, data, 'router')
    return trace_list

def tr_cesnet(dst_ip, req): #https://lg.cesnet.cz/
    url = 'https://lg.cesnet.cz/'
    eyes = GetEyes(url, req)
    
    url = 'https://lg.cesnet.cz/execute.php'
    data = {'routers': '', 'query': 'traceroute', 'dontlook': '', 'parameter': dst_ip}
    trace_list = GetTraceListFromEyesPost(eyes, url, req, data, 'routers')        
    return trace_list

def tr_beeline(dst_ip, req): #http://lg.gldn.net/
    eyes = ['3216;Amsterdam', '3216;Stockholm', '3216;Frankfurt', '3216;HongKong', '3216;Erevan', '3216;Erevan2', '3216;Moscow', '3216;Piter', '3216;NNovgorod', '3216;Yaroslavl', '3216;Rostov', '3216;Ekaterinburg', '3216;Saratov', '3216;Ufa', '3216;Nsk', '3216;Khabarovsk', '3216;Vladivostok', '8402;Amsterdam', '8402;Stockholm', '8402;Frankfurt', '8402;Moscow', '8402;Piter']
    url = 'http://lg.gldn.net/result/'
    data = {'action': 'trace', 'ipversion': 'IPv4', 'param': dst_ip, 'node': ''}
    trace_list = GetTraceListFromEyesPost(eyes, url, req, data, 'node')        
    return trace_list    

def tr_uar(dst_ip, req): #http://lg.uar.net/
    url = 'http://lg.uar.net/'
    eyes = GetEyes(url, req)
    
    url = 'http://lg.uar.net/'
    data = {'query': 'trace', 'addr': dst_ip, 'router': ''}
    trace_list = GetTraceListFromEyesPost(eyes, url, req, data, 'router')        
    return trace_list

def tr_as3326(dst_ip, req): #http://lg.as3326.net
    url = 'http://lg.as3326.net'
    eyes = GetEyes(url, req)
    
    url = 'http://lg.as3326.net/execute.php'    
    data = {'routers': '', 'query': 'traceroute', 'dontlook': '', 'parameter': dst_ip}
    trace_list = GetTraceListFromEyesPost(eyes, url, req, data, 'routers')        
    return trace_list

def tr_centurylink(dst_ip, req): #https://lookingglass.centurylink.com/
    url = 'https://lookingglass.centurylink.com/'
    eyes = GetEyes(url, req)
    
    url = 'https://lookingglass.centurylink.com/'    
    data = {'category': 'traceroute', 'site_name': '', 'ip': dst_ip, 'mask': '', 'packet_size': '64', 'packet_count': '1'}
    trace_list = GetTraceListFromEyesPost(eyes, url, req, data, 'site_name')        
    return trace_list

def tr_globalcrossing(dst_ip, req): #http://ipstats.globalcrossing.net/dotcom/link7r.shtml?mode=r2h&type=trace
    url = 'http://ipstats.globalcrossing.net/dotcom/link7r.shtml?mode=r2h&type=trace'
    eyes = GetEyes(url, req)
    
    url = 'http://ipstats.globalcrossing.net/dotcom/link7r.shtml?src=eye&mode=r2h&dst=%s&type=trace' %dst_ip
    return GetTraceListFromEyesGet(eyes, url, req)
    
def tr_slac_stanford(dst_ip, req): #http://www.slac.stanford.edu/cgi-bin/nph-traceroute.pl?choice=yes
    url = 'https://www.slac.stanford.edu/cgi-bin/nph-traceroute.pl?target=%s&function=traceroute' %dst_ip
    return method_tr_get_base(url, req)

def tr_iinet(dst_ip, req): #http://looking-glass.iinet.net.au/
    url = 'http://looking-glass.iinet.net.au/'
    eyes = GetEyes(url, req)
    
    url = 'http://looking-glass.iinet.net.au/lg.cgi'    
    data = {'query': 'trace', 'protocol': 'IPv4', 'addr': dst_ip, 'router': ''}
    trace_list = GetTraceListFromEyesPost(eyes, url, req, data, 'router')        
    return trace_list

def tr_idola(dst_ip, req): #http://lg.idola.net.id/
    url = 'http://lg.idola.net.id/'
    eyes = GetEyes(url, req)
    
    url = 'http://lg.idola.net.id/?command=trace&protocol=ipv4&query=%s&router=eye' %dst_ip
    return GetTraceListFromEyesGet(eyes, url, req)

def tr_vocus(dst_ip, req): #http://tools.vocus.com.au/lg/
    url = 'http://tools.vocus.com.au/lg/'    
    data = {'query': 'trace', 'protocol': 'IPv4', 'addr': dst_ip, 'router': ''}
    return GetTraceListFromEyesPost(['Sydney'], url, req, data, 'router')

def tr_rr(dst_ip, req): #http://rr.netins.net/lg/
    url = 'http://rr.netins.net/lg/lg.cgi'    
    data = {'type': 'Traceroute', 'Count': '5', 'ipaddr': dst_ip}
    return method_tr_post_base(url, req, data)

def tr_grnet(dst_ip, req): #https://mon.grnet.gr/lg/
    eyes = ['1', '2', '3']
    url = 'https://mon.grnet.gr/lg/index.cgi?fname=getResponse&args=traceroute&cmd=traceroute&args=%s&args=%s&args=eye&device=eye' %(dst_ip, dst_ip)
    return GetTraceListFromEyesGet(eyes, url, req)

def tr_olsson(dst_ip, req): #http://coyote.olsson.net/lg/
    url = 'http://coyote.olsson.net/lg/?router=eye&query=traceroute&addr=%s' %dst_ip
    eyes = ['UNI*C_Core1%2C+DTU%2C+Lyngby', 'UNI*C_Core3%2C+DTU%2C+Lyngby', 'OLSSON_Core1%2C+Vallensbaek']
    return GetTraceListFromEyesGet(eyes, url, req)
    
def use_get_eye_post_base(url, req, data, key_of_eye):
    eyes = GetEyes(url, req)
    for eye in eyes:
        data[key_of_eye] = eye
        trace = GetTrFromUrlPost(url, req, data, eye)
        if trace:
            return True
    return False

def use_get_eye_get_base(url, req, get_param):
    eyes = GetEyes(url, req)
    for eye in eyes:
        param_new = get_param.replace('eye', eye, 1)
        trace = GetTrFromUrlGet(url + param_new, req, eye)
        if trace:
            return True
    return False

def CheckTrMethod(filename):
    req = requests.session()
    os.chdir('/home/slt/code/ana_c_d_incongruity/')
    dst_ip = '8.8.8.8'
    methods = ['others']
    post_data_list = [[{'query': 'trace', 'protocol': 'IPv4', 'addr': dst_ip, 'router': ''}, 'router'], \
                    [{'routers': '', 'query': 'traceroute', 'dontlook': '', 'parameter': dst_ip}, 'routers'], \
                    [{'router': '', 'query': 'trace', 'arg': dst_ip} , 'router'], \
                    [{'rtr': '', 'query': 'traceroute', 'family': 'inet', 'addr': dst_ip}, 'rtr'], \
                    [{'action': 'trace', 'ipversion': 'IPv4', 'param': dst_ip, 'node': ''}, 'node']]
    get_param_list = ['?router=eye&query=traceroute&arg=%s' %dst_ip, \
                    '?router=eye&query=traceroute&addr=%s' %dst_ip, \
                    '?ip_version=IPv4&type=Traceroute&router=eye&parameters=%s&submit=Submit&.cgifields=ip_version&.cgifields=type' %dst_ip, \
                    '?command=trace&protocol=ipv4&router=eye&query=%s' %dst_ip]
    for i in range(0, len(post_data_list)):
        methods.append('get_eye_post_%d' %i)
    for i in range(0, len(get_param_list)):
        methods.append('get_eye_get_%d' %i)
    done_url = set()
    for method in methods:
        if os.path.exists(filename + '_' + method) and os.path.getsize(filename + '_' + method):
            with open(filename + '_' + method, 'r') as rf:
                for url in rf.read().strip('\n').split('\n'):
                    done_url.add(done_url)
    wf = dict()
    for method in methods:
        wf[method] = open(filename + '_' + method, 'a')
    with open(filename, 'r') as rf:
        for url in rf.read().strip('\n').split('\n'):
            if url in done_url:
                continue
            print(url)
            match = False
            for i in range(0, len(post_data_list)):
                data_info = post_data_list[i]
                if use_get_eye_post_base(url, req, data_info[0], data_info[1]): #method match
                    print('\tmatch get_eye_post_%d' %i)
                    wf['get_eye_post_%d' %i].write(url + '\n')
                    match = True
                    break
            if match:
                continue
            for i in range(0, len(get_param_list)):
                if use_get_eye_get_base(url, req, get_param_list[i]): #method match
                    print('\tmatch get_eye_get_%d' %i)
                    wf['get_eye_get_%d' %i].write(url + '\n')
                    match = True
                    break
            if match:
                continue
            print('\tnot match')
            wf['others'].write(url + '\n')
    for method in methods:
        wf[method].close()

def tr_rcn(dst_ip, req): #http://lg.rcn.net/lgform.cgi
    url = 'http://lg.rcn.net/lgform.cgi'
    eyes = GetEyes(url, req)

    url = 'http://lg.rcn.net/lg.cgi'
    data = {'query': 'trace', 'args': dst_ip, 'router': '', 'submit': 'Submit'}
    trace_list = GetTraceListFromEyesPost(eyes, url, req, data, 'router')    
    return trace_list

def tr_fifi(dst_ip, req): #http://www.fifi.org/services/traceroute
    trace_list = []
    url = 'http://www.fifi.org/services/traceroute?hostname=%s&nprobes=1&resolved=no&submit=Traceroute' %dst_ip
    try:
        resource = req.get(url, timeout=30) 
        if resource:                
            if resource.status_code == 200:
                trace = ''
                related_data = re.findall('.*<TABLE ALIGN=center>(.+?)</TABLE>', resource.text, re.DOTALL)
                for cur_line in related_data[0].strip('\n').split('\n'):
                    find_res = re.findall(ip_form, cur_line)
                    if find_res:
                        trace += ' ' + find_res[0]
                    elif cur_line.__contains__('???'):
                        trace += ' *'
    except Exception as e:
        pass
    if trace:
        trace_list.append(trace.strip(' '))
    return trace_list

def tr_ipartners(dst_ip, req): #http://cgi.ipartners.pl/cgi-bin/traceroute.cgi
    trace_list = []
    url = 'http://cgi.ipartners.pl/cgi-bin/traceroute.cgi'
    data = {'hostname': dst_ip, 'bgcolor': 'ffffff', 'text': '000000', 'check': 'check'}
    return method_tr_post_base(url, req, data)

def tr_he(dst_ip, req): #https://lg.he.net/
    trace_list = []
    try:
        url = 'https://lg.he.net/'
        resource = req.get(url, timeout=30) 
        if resource:                
            if resource.status_code == 200:
                data = resource.text
                find_res = re.findall('name=\"token\" value=\"(.+?)\"', data)
                if not find_res:
                    return []
                token = find_res[0]
                #eyes = re.findall('name=\"routers\[\]\" .+? value=\"(.+?)\" checked=\"checked\" traceroute=\"y\"', data)
                eyes = re.findall('name=\"routers\[\]\".+?value=\"(.+?)\"', data, re.DOTALL)
                for eye in eyes:
                    trace = ''
                    data = {'token': token, 'routers[]': eye, 'command': 'traceroute', 'ip': dst_ip, 'raw': '1', 'afPref': 'preferV6'}
                    headers = {'referer': 'https://lg.he.net/'}
                    trace = GetTrFromUrlPost(url, req, data, eye, headers)
                    if trace:
                        trace_list.append(trace)
    except Exception as e:
        pass
    return trace_list   

def tr_aarnet(dst_ip, req): #http://lg.aarnet.edu.au
    url = 'https://lg.aarnet.edu.au/traceroute_results?ipaddress=%s&onoffnet=off&traceroute=on' %dst_ip
    return method_tr_get_base(url, req)

def tr_zettagrid(dst_ip, req): #http://lg.zettagrid.com/lg/
    url = 'https://lg.zettagrid.com/lg/'
    eyes = GetEyes(url, req)

    url = 'https://lg.zettagrid.com/lg/'
    data = {'router': '', 'query': 'trace', 'protocol': 'IPv4', 'addr': dst_ip}
    trace_list = GetTraceListFromEyesPost(eyes, url, req, data, 'router')
    return trace_list

def tr_as8218(dst_ip, req): #http://lg.as8218.eu
    url = 'http://lg.as8218.eu/'
    eyes = GetEyes(url, req)

    url = 'http://lg.as8218.eu/'
    data = {'router': '', 'query': 'trace', 'protocol': 'IPv4', 'addr': dst_ip}
    trace_list = GetTraceListFromEyesPost(eyes, url, req, data, 'router')
    return trace_list

def tr_atom86(dst_ip, req): #https://lg.atom86.net/
    url = 'https://lg.atom86.net/'
    eyes = GetEyes(url, req)
    
    url = 'https://lg.atom86.net/execute.php'    
    data = {'routers': '', 'query': 'traceroute', 'dontlook': '', 'parameter': dst_ip}
    headers = {"referer":"https://lg.atom86.net/"}
    trace_list = GetTraceListFromEyesPost(eyes, url, req, data, 'routers', headers)
        
    return trace_list

def tr_evolink(dst_ip, req): #http://lg.evolink.net
    eyes = ['Core Router', 'Border Router']    
    url = 'http://lg.evolink.net'    
    data = {'router': '', 'iptype4': 'IPv4', 'iptype': 'IPv4', 'searchfor': dst_ip, 'options': 'traceroute', 'Execute': 'Execute', '.cgifields': 'iptype4', '.cgifields': 'options', '.cgifields': 'iptype'}
    return GetTraceListFromEyesPost(eyes, url, req, data, 'router')

def tr_macomnet(dst_ip, req): #http://www.macomnet.net/testlab/cgi-bin/nph-trace?
    url = 'http://www.macomnet.net/testlab/cgi-bin/nph-trace?%s' %dst_ip
    return method_tr_get_base(url, req)

def tr_kamp(dst_ip, req): #http://www.kamp-lookingglass.de/
    url = 'http://www.kamp-lookingglass.de/'
    data = {'cmd': 'traceroute4', 'arg': dst_ip}
    return method_tr_post_base(url, req, data)

def tr_cprm(dst_ip, req): #http://glass.cprm.net/bgp.html
    url = 'http://glass.cprm.net/output.php'
    data = {'query': 'trace', 'ip': 'ipv4', 'addr': dst_ip}
    return method_tr_post_base(url, req, data)

def tr_zyx(dst_ip, req): #http://traceroute.zyx.ro/
    url = 'http://traceroute.zyx.ro/index.php?host=%s&submit=Traceroute%%21' %dst_ip
    return method_tr_get_base(url, req)

def tr_sdv(dst_ip, req): #http://lg.sdv.fr/   
    trace_list = [] 
    try:
        trace = ''
        url = 'http://traceroute.sdv.fr/index.php?v6=0&host=%s' %dst_ip
        resource = req.get(url, timeout=30) 
        if resource:                
            if resource.status_code == 200:
                find_res = re.findall('<td>(.+?)</td>', resource.text, re.DOTALL)
                for elem in find_res:
                    ip = FindIpInData(elem)
                    if ip:
                        trace += ' ' + ip
        if trace:
            trace_list.append(trace)
    except Exception as e:
        #print('Connect to %s eye %s error: %s' %(url, eye, e))
        pass
    return trace_list    


def tr_ucom(dst_ip, req): #https://lg.ucom.am/
    url = 'https://lg.ucom.am/'
    eyes = GetEyes(url, req)
    data = {'router': '', 'query': 'trace', 'protocol': 'IPv4', 'addr': dst_ip}
    return GetTraceListFromEyesPost(eyes, url, req, data, 'router')

def tr_webpartner(dst_ip, req): #http://tools.webpartner.dk/traceroute.html
    url = 'http://tools.webpartner.dk/cgi-bin/traceroute.pl?HIP=%s' %dst_ip
    return method_tr_get_base(url, req)

def tr_as9370(dst_ip, req): #http://as9370.bgp4.jp/
    url = 'http://as9370.bgp4.jp/lg.cgi?query=32&arg=%s' %dst_ip
    return method_tr_get_base(url, req)

def tr_as9371(dst_ip, req): #http://as9371.bgp4.jp/
    url = 'http://as9371.bgp4.jp/lg.cgi?query=34&arg=%s' %dst_ip
    return method_tr_get_base(url, req)

def tr_bbtower(dst_ip, req): #http://lg01.colo01.bbtower.ad.jp/
    url = 'https://lg01.colo01.bbtower.ad.jp/cgi-bin/lg.cgi'
    data = {'CMD': 'traceroute', 'ARG': dst_ip}
    return method_tr_post_base(url, req, data)

def tr_hafey(dst_ip, req): #http://www.hafey.org/cgi-bin/trace.sh
    url = 'http://www.hafey.org/cgi-bin/bgplg?cmd=traceroute&req=%s' %dst_ip
    return method_tr_get_base(url, req)

def tr_uecomm(dst_ip, req): #http://looking-glass.uecomm.net.au/
    url = 'http://looking-glass.uecomm.net.au/'
    eyes = ['1', '2']
    data = {'router': '', 'query': '5', 'arg': dst_ip}
    return GetTraceListFromEyesPost(eyes, url, req, data, 'router')

def tr_bluemoon(dst_ip, req): #http://www.bluemoon.net/trace.html
    url = 'http://www.bluemoon.net/cgi-bin/tc.cgi'
    data = {'host': dst_ip}
    return method_tr_post_base(url, req, data)

def tr_as10929(dst_ip, req): #http://lg.as10929.net
    url = 'http://lg.as10929.net/ajax.php?cmd=traceroute&host=%s' %dst_ip
    return method_tr_get_base(url, req)

def tr_davespeed(dst_ip, req): #http://davespeed.com/cgi-bin/traceroute.cgi
    url = 'http://davespeed.com/cgi-bin/traceroute.cgi'
    data = {'target': dst_ip, 'js': 'yes'}
    return method_tr_post_base(url, req, data)

def tr_unlimitednet(dst_ip, req): #http://lg.unlimitednet.us/
    url = 'http://lg.unlimitednet.us/ajax.php?cmd=traceroute&host=%s' %dst_ip
    return method_tr_get_base(url, req)

# def tr_internet2_per_eye(eye, dst_ip, queue):
#     req = requests.session()
#     print('begin ' + eye)
#     url = 'https://routerproxy.net.internet2.edu/routerproxy/?method=device&device=%s' %eye
#     resource = req.get(url)
#     url = 'https://routerproxy.net.internet2.edu/routerproxy/?method=submit&device=%s&command=traceroute&menu=0&arguments=%s' %(eye, dst_ip)
#     trace = GetTrFromUrlGet(url, req, eye, None, 60)
#     if trace:
#         queue.put(trace)

def tr_internet2(dst_ip, req): #https://routerproxy.grnoc.iu.edu/internet2/    
    trace_list = [] 
    try:
        url = 'https://routerproxy.net.internet2.edu/routerproxy/'
        resource = req.get(url, timeout=30) 
        if resource:                
            if resource.status_code == 200:
                eyes = re.findall('name=\"host_radio\" id=\"(.+?)\"', resource.text, re.DOTALL)
                queue = mp.Queue()
                proc_list = []                
                for eye in eyes:
                #for eye in ['162.252.70.252']:
                    print('begin ' + eye)
                    url = 'https://routerproxy.net.internet2.edu/routerproxy/?method=device&device=%s' %eye
                    resource = req.get(url, timeout=30)
                    url = 'https://routerproxy.net.internet2.edu/routerproxy/?method=submit&device=%s&command=traceroute&menu=0&arguments=%s' %(eye, dst_ip)
                    trace = GetTrFromUrlGet(url, req, eye, None, 60)
                    print(trace)
                    if trace:
                        trace_list.append(trace)
                #     proc_list.append(mp.Process(target=tr_internet2_per_eye, args=(eye, dst_ip, queue)))
                # for elem in proc_list:
                #     elem.start()
                # for elem in proc_list:
                #     elem.join()
                # while not queue.empty():
                #     trace_list.append(queue.get())                    
    except Exception as e:
        #print('Connect to %s eye %s error: %s' %(url, eye, e))
        pass
    return trace_list   

def tr_globedom(dst_ip, req): #http://www.globedom.com/cgi-bin/trace
    trace_list = []
    url = 'http://www.globedom.com/cgi-bin/traceit'
    data = {'HOSTNAME': dst_ip, 'RESOLVE': 'TRUE'}
    return method_tr_post_base(url, req, data)

def tr_seeweb(dst_ip, req): #https://www.seeweb.it/data-center/traceroute-diretto
    trace_list = []
    url = 'https://www.seeweb.it/get_traceroute?host=' + dst_ip
    try:
        resource = req.get(url) 
        if resource:                
            if resource.status_code == 200:
                trace = ''
                find_res = re.findall('<li>(.+?)</li>', resource.text)
                for elem in find_res:
                    if elem[:elem.index(' ')].isdigit():
                        ip = FindIpInData(elem)
                    if ip:
                        trace += ' ' + ip
                if trace:
                    trace_list.append(trace)
    except Exception as e:
        #print('Connect to %s eye %s error: %s' %(url, eye, e))
        pass
    return trace_list

def tr_as13030(dst_ip, req): #https://www.as13030.net/looking-glass.php
    url = 'https://www.as13030.net/traceroute.func.php?traceroute=' + dst_ip + '&proto=4'
    return method_tr_get_base(url, req)

def tr_thunderworx(dst_ip, req): #http://noc.thunderworx.net/cgi-bin/public/traceroute.pl
    url = 'http://noc.thunderworx.net/cgi-bin/public/traceroute.pl?target=' + dst_ip
    return method_tr_get_base(url, req)

def tr_fmc(dst_ip, req): #http://traceroute.fmc.lu/
    url = 'http://traceroute.fmc.lu/cgi-bin/traceping.pl'
    data = {'program': 'traceroute', 'address': dst_ip}
    return method_tr_post_base(url, req, data)

def tr_bite(dst_ip, req): #http://lg.bite.lt
    url = 'http://lg.bite.lt/'
    eyes = GetEyes(url, req)
    
    trace_list = []
    for eye in eyes:
        print(eye)
        url = 'http://lg.bite.lt/ajax.php?cmd=traceroute&host=' + dst_ip + '&server=' + eye
        trace = GetTrFromUrlGet(url, req)
        if trace:
            trace_list.append(trace)
    return trace_list

def tr_as14442(dst_ip, req): #http://lg.as14442.net
    url = 'http://lg.as14442.net/ajax.php?cmd=traceroute&host=%s' %dst_ip
    return method_tr_get_base(url, req)

def tr_customers_datapipe(dst_ip, req): #http://customers.datapipe.net/trace.pl
    url = 'http://customers.datapipe.net/trace.pl?%s' %dst_ip
    return method_tr_get_base(url, req)

def tr_rhnet(dst_ip, req): #https://www.rhnet.is/cgi-bin/nlg/lg.cgi
    url = 'https://www.rhnet.is/cgi-bin/nlg/lg.cgi'
    eyes = ['ndn-gw1.rhnet.is', 'ndn-gw2.rhnet.is']
    data = {'router': '', 'query': 'trace', 'arg': dst_ip}
    return GetTraceListFromEyesPost(eyes, url, req, data, 'router')

def tr_rhnet_2(dst_ip, req): #https://www.rhnet.is/cgi-bin/rh-traceroute
    url = 'https://www.rhnet.is/cgi-bin/rh-traceroute?ip=%s' %dst_ip
    return method_tr_get_base(url, req)

def tr_ix(dst_ip, req): #http://lg.ix.net.ua/
    url = 'https://lg.ix.net.ua/'
    eyes = ['RS-1', 'RS-2']
    data = {'routername': '', 'service': 'traceroute', 'address': dst_ip} #, '.submit': '&#25552;&#20132;'}
    return GetTraceListFromEyesPost(eyes, url, req, data, 'routername')

def tr_bix(dst_ip, req): #http://lg.bix.bg/
    url = 'http://lg.bix.bg/'
    eyes = GetEyes(url, req)

    url = 'http://lg.bix.bg/?query=trace&addr=' + dst_ip + '&router=eye'
    return GetTraceListFromEyesGet(eyes, url, req)
    
def tr_telecoms_bg(dst_ip, req): #http://lg.telecoms.bg/
    url = 'http://lg.telecoms.bg/ajax.php?cmd=traceroute&host=' + dst_ip
    return method_tr_get_base(url, req)

def tr_ovh(dst_ip, req): #https://lg.ovh.net
    eyes = ['sgp', 'vin', 'sbg', 'bhs', 'hil', 'rbx', 'lim', 'gra', 'waw', 'syd1', 'eri']
    url = 'https://lg.ovh.net/traceroute/eye/ipv4?q=' + dst_ip
    return GetTraceListFromEyesGet(eyes, url, req)
    
def tr_mbix(dst_ip, req): #http://lg.mbix.ca/cgi-bin/bgplg
    url = 'http://lg.mbix.ca/cgi-bin/bgplg?cmd=traceroute&req=' + dst_ip
    return method_tr_get_base(url, req)

def tr_alog(dst_ip, req): #http://lg.alog.com.br/
    url = 'http://lg.alog.com.br/cgi-bin/lg.cgi'
    trace_list = []
    data = {'query': 'traceroute', 'parameters': dst_ip}
    try:
        resource = req.post(url, data=data, timeout=30)
        if resource:                
            if resource.status_code == 200:
                trace = ''
                find_res = re.findall('<a>(.+?)</a>', resource.text)
                for elem in find_res:
                    ip = FindIpInData(elem)
                    if ip:
                        trace += ' ' + ip
                if trace:
                    trace_list.append(trace)
    except Exception as e:
        #print('Connect to %s eye %s error: %s' %(url, eye, e))
        pass
    return trace_list

def tr_opt_nc(dst_ip, req): #http://lookingglass.opt.nc
    trace_list = []
    url = 'https://lookingglass.opt.nc/api/query/'
    data = {'query_location': "opt_noumea_nc", 'query_target': dst_ip, 'query_type': "traceroute", 'query_vrf': "global"}
    try:
        resource = req.post(url, json=data, timeout=30)
        if resource:                
            if resource.status_code == 200:
                trace = GetTrFromData(resource.text)
                if trace:
                    trace_list.append(trace) 
    except Exception as e:
        #print('Connect to %s eye %s error: %s' %(url, eye, e))
        pass    
    return trace_list
    
def tr_les(dst_ip, req): #http://lg.les.net/cgi-bin/bgplg
    url = 'http://lg.les.net/cgi-bin/bgplg?cmd=traceroute&req=' + dst_ip
    return method_tr_get_base(url, req)
    
def tr_isomedia(dst_ip, req): #http://lg.isomedia.com/
    url = 'http://lg.isomedia.com/perl/LookingGlass.pl'
    data = {'arg': dst_ip, 'qt': 'trace'}
    return method_tr_post_base(url, req, data)
    
def tr_egihosting(dst_ip, req): #http://lg.egihosting.com
    url = 'http://lg.egihosting.com/ajax.php?cmd=traceroute&host=' + dst_ip
    return method_tr_get_base(url, req)
    
def tr_as19531(dst_ip, req): #http://lg.as19531.net/
    url = 'https://lg.as19531.net/ajax.php?cmd=traceroute&host=' + dst_ip
    return method_tr_get_base(url, req)
    
def tr_joesdatacenter(dst_ip, req): #http://lg.joesdatacenter.com
    url = 'http://lg.joesdatacenter.com/ajax.php?cmd=traceroute&host=' + dst_ip
    return method_tr_get_base(url, req)
    
def tr_buf1_as20278(dst_ip, req): #http://lg.buf1.as20278.net/
    url = 'http://lg.buf1.as20278.net/ajax.php?cmd=traceroute&host=' + dst_ip
    return method_tr_get_base(url, req)
    
def tr_chi1_as20278(dst_ip, req): #http://lg.chi1.as20278.net/
    url = 'http://lg.chi1.as20278.net/ajax.php?cmd=traceroute&host=' + dst_ip
    return method_tr_get_base(url, req)
    
def tr_speedtest(dst_ip, req): #http://speedtest.choopa.net/
    url = 'http://speedtest.choopa.net/ajax.php?cmd=traceroute&host=' + dst_ip
    return method_tr_get_base(url, req)
    
def tr_gitoyen(dst_ip, req): #https://lg.gitoyen.net/summary/whiskey+vodka+x-ray/ipv4
    eyes = ['whiskey', 'vodka', 'x-ray']
    url = 'https://lg.gitoyen.net/traceroute/eye/ipv4?q=' + dst_ip
    return GetTraceListFromEyesGet(eyes, url, req, None, 60)

#感觉这个eye没啥用，ping 8.8.8.8全是*
def tr_core_heg(dst_ip, req): #https://lg.core.heg.com
    url = 'https://lg.core.heg.com/traceroute.cgi?addr=%s&mode=trace' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_topnet(dst_ip, req): #http://lg.topnet.ua
    url = 'http://lg.topnet.ua/'
    eyes = GetEyes(url, req)

    url = 'http://lg.topnet.ua/execute.php'
    data = {'routers': '', 'query': 'traceroute', 'parameter': dst_ip, 'dontlook': ''}
    return GetTraceListFromEyesPost(eyes, url, req, data, 'routers')
    
def tr_itandtel(dst_ip, req): #https://lg.itandtel.at/
    trace_list = []
    url = 'https://lg.itandtel.at/api/query/'
    data = {'query_location': "core_router_wels", 'query_target': dst_ip, 'query_type': "traceroute", 'query_vrf': "global"}
    try:
        resource = req.post(url, json=data, timeout=30)
        if resource:                
            if resource.status_code == 200:
                trace = GetTrFromData(resource.text)
                if trace:
                    trace_list.append(trace) 
    except Exception as e:
        #print('Connect to %s eye %s error: %s' %(url, eye, e))
        pass    
    return trace_list
    
def tr_ggamaur(dst_ip, req): #https://lg.ggamaur.net/
    url = 'https://lg.ggamaur.net/'
    eyes = GetEyes(url, req)

    url = 'https://lg.ggamaur.net/lg.pl'
    data = {'query': 'trace', 'protocol': 'ip', 'addr': dst_ip, 'router': ''}
    return GetTraceListFromEyesPost(eyes, url, req, data, 'router')
    
def tr_tnib_de(dst_ip, req): #https://noc.tnib.de/lg/
    url = 'https://noc.tnib.de/lg/index.cgi'
    data = {'query': '21', 'arg': dst_ip}
    return method_tr_post_base(url, req, data)
    
def tr_flex(dst_ip, req): #http://lg.flex.ru/
    url = 'http://lg.flex.ru/'
    eyes = GetEyes(url, req)

    url = 'http://lg.flex.ru/'
    data = {'query': 'trace', 'protocol': 'IPv4', 'addr': dst_ip, 'router': ''}
    return GetTraceListFromEyesPost(eyes, url, req, data, 'router')
    
#没搞定
def tr_registro_br(dst_ip, req): #https://registro.br/cgi-bin/nicbr/trt
    trace_list = []
    url = 'https://registro.br/v2/ajax/traceroute?query=%s&version=4' %dst_ip
    try:
        trace = ''
        url = 'https://registro.br/tecnologia/ferramentas/traceroute/'
        resource = req.get(url, timeout=30)
        headers = {'referer': 'https://registro.br/tecnologia/ferramentas/traceroute/', 'X-XSRF-TOKEN': '8E8F07C26673E1EA1843AC3318ED7514417FEA3F'}
        resource = req.get(url, timeout=30, headers=headers) 
        if resource:                
            if resource.status_code == 200:
                find_res = re.findall('\"(.+?)\"', resource.text, re.DOTALL)
                for elem in find_res:
                    if elem.strip(' ').split(' ')[0].isdigit():
                        ip = FindIpInData(elem)
                        if ip:
                            trace += ' ' + ip
        if trace:
            trace_list.append(trace)
    except Exception as e:
        #print('Connect to %s eye %s error: %s' %(url, eye, e))
        pass
    return trace_list    
    
def tr_channel_11(dst_ip, req): #https://lg.channel-11.net/
    url = 'https://lg.channel-11.net/index.php?host=%s&submit=Traceroute%%21' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_sg_gs(dst_ip, req): #http://network.sg.gs/lg/
    url = 'http://network.sg.gs/lg/'
    eyes = GetEyes(url, req)

    url = 'http://network.sg.gs/lg/'
    data = {'query': 'trace', 'protocol': 'IPv4', 'addr': dst_ip, 'router': ''}
    return GetTraceListFromEyesPost(eyes, url, req, data, 'router')
    
def tr_virtutel(dst_ip, req): #http://tools.virtutel.com.au/lg/lg.cgi
    url = 'http://tools.virtutel.com.au/lg/lg.cgi'
    eyes = GetEyes(url, req)

    url = 'http://tools.virtutel.com.au/lg/lg.cgi'
    data = {'query': 'trace', 'protocol': 'IPv4', 'addr': dst_ip, 'router': ''}
    return GetTraceListFromEyesPost(eyes, url, req, data, 'router')
    
def tr_atman(dst_ip, req): #http://lg.atman.pl
    url = 'http://lg.atman.pl/'
    eyes = GetEyes(url, req)

    url = 'http://lg.atman.pl/?query=trace&protocol=IPv4&addr=%s&router=eye' %dst_ip
    return GetTraceListFromEyesGet(eyes, url, req)
    
def tr_kloth(dst_ip, req): #http://www.kloth.net/services/traceroute.php
    url = 'http://www.kloth.net/services/traceroute.php'
    data = {'d6m': dst_ip}
    return method_tr_post_base(url, req, data)
    
def tr_gaertner(dst_ip, req): #https://noc.gaertner.de/cgi-bin/looking-glass.cgi
    url = 'https://noc.gaertner.de/cgi-bin/looking-glass.cgi'
    data = {'type': 'traceroute', 'dest': dst_ip, 'Action': 'Go!'}
    return method_tr_post_base(url, req, data)
    
def tr_as24961(dst_ip, req): #https://lg.as24961.net/
    url = 'https://lg.as24961.net/traceroute/lg/ipv4?q=%s' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_masterinter(dst_ip, req): #https://lg.masterinter.net/
    url = 'https://lg.masterinter.net/traceroute/rc.4d.prg/ipv4?q=%s' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_as25369(dst_ip, req): #https://www.as25369.net/
    url = 'https://www.as25369.net/'
    eyes = GetEyes(url, req)

    url = 'https://www.as25369.net/execute.php'
    data = {'routers': '', 'query': 'traceroute', 'dontlook': '', 'parameter': dst_ip}
    return GetTraceListFromEyesPost(eyes, url, req, data, 'routers')
    
def tr_arpnetworks(dst_ip, req): #http://lg.arpnetworks.com/
    url = 'http://lg.arpnetworks.com/cgi-bin/bgplg?cmd=traceroute&req=%s' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_active24(dst_ip, req): #https://www.active24.cz/lg/lg.cgi
    url = 'https://www.active24.cz/lg/lg.cgi'
    eyes = GetEyes(url, req)

    url = 'https://www.active24.cz/lg/lg.cgi'
    data = {'query': 'trace', 'protocol': 'IPv4', 'addr': dst_ip, 'router': ''}
    return GetTraceListFromEyesPost(eyes, url, req, data, 'router')
    
def tr_cyfra(dst_ip, req): #https://cyfra.ua/ru/lg    
    trace_list = []
    try:
        url = 'https://cyfra.ua/ru/lg'
        resource = req.get(url) 
        if resource:                
            if resource.status_code == 200:
                csrf_token = re.findall('meta name=\"_csrf\" content=\"(.+?)\"', resource.text)
                url = 'https://cyfra.ua/rest/netinfo/lookinglass'
                headers = {'x-csrf-token': csrf_token[0]}
                data = {'type': 'traceroute', 'address': dst_ip}
                resource = req.post(url, json=data, headers=headers, timeout=30)
                if resource:                
                    if resource.status_code == 200:
                        trace = GetTrFromData(resource.text)
                        if trace:
                            trace_list.append(trace) 
    except Exception as e:
        #print('Connect to %s eye %s error: %s' %(url, eye, e))
        pass
    return trace_list
    
def tr_alsysdata(dst_ip, req): #http://lg.alsysdata.net/
    url = 'http://lg.alsysdata.net/lg.cgi'
    data = {'program': '2', 'target': dst_ip}
    return method_tr_post_base(url, req, data)
    
def tr_as25577(dst_ip, req): #https://lg.as25577.net/
    url = 'https://lg.as25577.net/'
    eyes = GetEyes(url, req)

    url = 'https://lg.as25577.net/execute.php'
    data = {'routers': '', 'query': 'traceroute', 'dontlook': '', 'parameter': dst_ip}
    return GetTraceListFromEyesPost(eyes, url, req, data, 'routers')
    
def tr_hugeserver(dst_ip, req): #http://lg.hugeserver.com/
    url = 'http://lg.hugeserver.com/'
    eyes = GetEyes(url, req)

    url = 'https://lg.as25577.net/execute.php'
    data = {'routers': '', 'query': 'traceroute', 'dontlook': '', 'parameter': dst_ip}
    return GetTraceListFromEyesPost(eyes, url, req, data, 'routers')
    
def tr_as26320(dst_ip, req): #http://lg.as26320.net
    url = 'http://lg.as26320.net/cgi-bin/bgplg?cmd=traceroute&req=%s' %dst_ip
    return method_tr_get_base(url, req, None, 60)
    
def tr_clearfly(dst_ip, req): #https://lg.clearfly.net/cgi-bin/bgplg/
    url = 'https://lg.clearfly.net/cgi-bin/bgplg?cmd=traceroute&req=%s' %dst_ip
    return method_tr_get_base(url, req, None, 60)
    
def tr_towardex(dst_ip, req): #http://www.towardex.com/cgi-bin/lg.cgi
    url = 'http://www.towardex.com/cgi-bin/lg.cgi'
    eyes = GetEyes(url, req)

    url = 'http://www.towardex.com/cgi-bin/lg.cgi'
    data = {'query': 'trace', 'protocol': 'IPv4', 'addr': dst_ip, 'router': ''}
    return GetTraceListFromEyesPost(eyes, url, req, data, 'router')
    
def tr_certto(dst_ip, req): #http://lg.certto.com.br/lg
    url = 'http://lg.certto.com.br/lg/'
    eyes = GetEyes(url, req)

    url = 'http://lg.certto.com.br/lg/'
    data = {'query': 'trace', 'protocol': 'IPv4', 'addr': dst_ip, 'router': ''}
    return GetTraceListFromEyesPost(eyes, url, req, data, 'router')
    
def tr_ensite(dst_ip, req): #http://lg.ensite.com.br/
    url = 'http://lg.ensite.com.br/'
    eyes = GetEyes(url, req)

    url = 'http://lg.ensite.com.br/lg/'
    data = {'query': 'trace', 'protocol': 'IPv4', 'addr': dst_ip, 'router': ''}
    return GetTraceListFromEyesPost(eyes, url, req, data, 'router')
    
def tr_g8(dst_ip, req): #https://lg.g8.net.br/
    url = 'https://lg.sp4.spo.g8.net.br/'
    eyes = GetEyes(url, req)

    url = 'https://lg.sp4.spo.g8.net.br/ajax.php?cmd=traceroute&host=' + dst_ip
    return GetTraceListFromEyesGet(eyes, url, req)
    
def tr_netbotanic(dst_ip, req): #http://lg.netbotanic.com.br/lg.php
    url = 'http://lg.netbotanic.com.br/tools3.php'
    data = {'host': dst_ip, 'trace': 'trace', 'abacur': 'Ferramentas', 'submit': 'Enviar'}
    return method_tr_post_base(url, req, data)
    
def tr_contato(dst_ip, req): #http://lg.contato.net/cgi-bin/bgplg
    url = 'http://lg.contato.net/cgi-bin/bgplg?cmd=traceroute&req=' + dst_ip
    return method_tr_get_base(url, req)
    
def tr_gtu(dst_ip, req): #http://lg.gtu.net.ua/
    url = 'http://lg.gtu.net.ua/ajax.php?cmd=traceroute&host=' + dst_ip
    return method_tr_get_base(url, req)
    
def tr_netbotanic(dst_ip, req): #http://looking-glass.galacsys.net/
    url = 'http://looking-glass.galacsys.net/'
    data = {'cmd': 'traceroute', 'arg': dst_ip, '.submit': '&#25552;&#20132;'}
    return method_tr_post_base(url, req, data)
    
def tr_as29140(dst_ip, req): #http://www.as29140.net/cgi-bin/bgplg
    url = 'http://www.as29140.net/cgi-bin/bgplg?cmd=traceroute&req=' + dst_ip
    return method_tr_get_base(url, req)
    
def tr_mastertel(dst_ip, req): #http://lg.mastertel.ru/
    eyes = ['mmts-9', 'mmts-10']
    url = 'http://lg.mastertel.ru/tracert.php'
    data = {'pop': '', 'query': 'tracerout', 'host_ip': dst_ip}
    return GetTraceListFromEyesPost(eyes, url, req, data, 'pop')
    
def tr_predkosci(dst_ip, req): #http://www.predkosci.pl/lg
    trace_list = []
    url = 'https://www.predkosci.pl/traceroute'
    data = {'AKCJA': 'TRACE', 'IP': dst_ip}
    try:
        resource = req.post(url, data=data) 
        if resource:                
            if resource.status_code == 200:
                trace = ''
                find_res = re.findall('<pre>(.+?)</pre>', resource.text, re.DOTALL)
                for elem in find_res:
                    if elem.__contains__('Loss') and elem.__contains__('Last') and \
                        elem.__contains__('Avg') and elem.__contains__('Best') and \
                        elem.__contains__('Wrst') and elem.__contains__('StDev'):
                        sub_find_res = re.findall('<a href=(.+?)</a>', elem, re.DOTALL)
                        for sub_elem in sub_find_res:
                            ip = FindIpInData(sub_elem)
                            if ip:
                                trace += ' ' + ip
                if trace:
                    trace_list.append(trace)
            else:
                print(url + ': ' + resource.status_code)
    except Exception as e:
        #print('Connect to %s eye %s error: %s' %(url, eye, e))
        pass
    return trace_list

    
def tr_bandit(dst_ip, req): #http://bandit.probe-networks.de/cgi-bin/trace
    trace_list = []
    url = 'http://bandit.probe-networks.de/traceit.php?host=' + dst_ip    
    try:
        resource = req.get(url) 
        if resource:                
            if resource.status_code == 200:
                trace = ''
                find_res = re.findall('<p>(.+?)</p>', resource.text, re.DOTALL)
                for elem in find_res:
                    if elem.strip(' ').split(' ')[0].isdigit():
                        ip = FindIpInData(elem)
                        if ip:
                            trace += ' ' + ip
                if trace:
                    trace_list.append(trace)
            else:
                print(url + ': ' + resource.status_code)
    except Exception as e:
        #print('Connect to %s eye %s error: %s' %(url, eye, e))
        pass
    return trace_list

def tr_probe_networks_de(dst_ip, req): #http://probe-networks.de/lg/
    url = 'https://probe-networks.de/lg/'
    eyes = GetEyes(url, req)

    data = {'router': '', 'query': 'trace', 'protocol': 'IPv6', 'addr': dst_ip}
    return GetTraceListFromEyesPost(eyes, url, req, data, 'router')

def tr_opticfusion(dst_ip, req): #http://lg.opticfusion.net
    url = 'http://lg.opticfusion.net/'
    eyes = GetEyes(url, req)

    url = 'http://lg.opticfusion.net/execute.php'
    data = {'routers': '', 'query': 'traceroute', 'dontlook': '', 'parameter': dst_ip}
    return GetTraceListFromEyesPost(eyes, url, req, data, 'routers')
    
def tr_phoenix(dst_ip, req): #http://lg-phoenix.serverhub.com/
    url = 'http://lg-phoenix.serverhub.com/ajax.php?cmd=traceroute&host=' + dst_ip
    return method_tr_get_base(url, req)

def tr_gtc_su(dst_ip, req): #http://lg.gtc.su/
    url = 'https://lg.gtc.su/'
    eyes = GetEyes(url, req)

    url = 'https://lg.gtc.su/'
    data = {'router': '', 'query': 'trace', 'protocol': 'IPv4', 'addr': dst_ip}
    return GetTraceListFromEyesPost(eyes, url, req, data, 'router', None, 60)

def tr_sileman(dst_ip, req): #http://noc.sileman.pl/
    url = 'https://noc.sileman.pl/'
    eyes = GetEyes(url, req)

    url = 'https://noc.sileman.pl/lg.pl?myselect=eye&myradio=3&mymtr=2&mycheck=yes&myip=4&myfield=%s&' %dst_ip
    return GetTraceListFromEyesGet(eyes, url, req)
    
def tr_marwan(dst_ip, req): #http://lg.marwan.ma/
    url = 'http://lg.marwan.ma/'
    eyes = GetEyes(url, req)

    url = 'http://lg.marwan.ma/execute.php'
    data = {'routers': '', 'query': 'traceroute', 'dontlook': '', 'parameter': dst_ip}
    return GetTraceListFromEyesPost(eyes, url, req, data, 'routers')

def tr_sbb(dst_ip, req): #http://lg.sbb.rs
    url = 'http://lg.sbb.rs/'
    data = {'router': 'Beograd', 'query': 'trace', 'protocol': 'IPv4', 'addr': dst_ip}
    return method_tr_post_base(url, req, data)

def tr_starnet_md(dst_ip, req): #https://lg.starnet.md
    url = 'https://lg.starnet.md/'
    eyes = []
    try:
        resource = req.get(url) 
        if resource:                
            if resource.status_code == 200:
                find_res = re.findall('\"_id\":\"(.+?)\"', resource.text, re.DOTALL)
                for elem in find_res:
                    if elem != 'global_routing_table':
                        eyes.append(elem)   
                trace_list = []
                url = 'https://lg.starnet.md/api/query/'
                for eye in eyes:
                    data = {'query_location': eye, 'query_target': dst_ip, 'query_type': "traceroute", 'query_vrf': "global_routing_table"}
                    resource = req.post(url, json=data, timeout=30)
                    if resource:                
                        if resource.status_code == 200:
                            trace = GetTrFromData(resource.text)
                            if trace:
                                trace_list.append(trace) 
    except Exception as e:
        #print('Connect to %s eye %s error: %s' %(url, eye, e))
        pass    
    return trace_list
    
def tr_nemox(dst_ip, req): #http://nemox.net/traceroute/
    url = 'http://nemox.net/traceroute/index.pl?t=' + dst_ip
    return method_tr_get_base(url, req)
    
def tr_transtelco(dst_ip, req): #https://lg.transtelco.net/
    trace_list = []
    try:
        url = 'https://lg.transtelco.net/'
        resource = req.get(url) 
        if resource:                
            if resource.status_code == 200:
                eyes = []
                soup = BeautifulSoup(resource.content.decode('utf-8'), 'html.parser')
                find_res = soup.find_all('option')
                for link in find_res:
                    if link['value'].isdigit():
                        eyes.append(link['value'])
                csrf_token = re.findall('meta name=\"csrf-token\" content=\"(.+?)\"', resource.text)
                headers = {'X-CSRF-Token': csrf_token[0]}
                url = 'https://lg.transtelco.net/raspberry/traceroute?query=%s&from_hosts%%5B%%5D=eye&to_host=&ipv=ipv4' %dst_ip
                trace_list = GetTraceListFromEyesGet(eyes, url, req, headers)
    except Exception as e:
        #print('Connect to %s eye %s error: %s' %(url, eye, e))
        pass
    return trace_list
    
def tr_liquidweb(dst_ip, req): #https://lg.liquidweb.com
    trace_list = []
    try:
        url = 'https://lg.liquidweb.com/'
        resource = req.get(url) 
        if resource:                
            if resource.status_code == 200:
                eyes = []
                soup = BeautifulSoup(resource.content.decode('utf-8'), 'html.parser')
                find_res = soup.find_all('option')
                for link in find_res:
                    eye = link['value']
                    if eye.isdigit() and eye not in eyes:
                        eyes.append(eye)
                csrf_token = re.findall('name=\"csrfmiddlewaretoken\" value=\"(.+?)\"', resource.text)
                headers = {'Referer': 'https://lg.liquidweb.com/'}
                for eye in eyes:
                    data = {'csrfmiddlewaretoken': csrf_token[0], 'node': eye, 'protocol': 'ipv4', 'commands': '2', 'params': dst_ip}
                    resource = req.post(url, data=data, headers=headers)
                    if resource:                
                        if resource.status_code == 200:
                            find_res = re.findall('<p>(.+?)</p>', resource.text, re.DOTALL)
                            for elem in find_res:
                                if elem.strip(' ').split(' ')[0] == '1':
                                    i = 0
                                    trace = ''
                                    elem = elem.replace('<br>', '\n')
                                    for cur_line in elem.strip('\n').split('\n'):
                                        elems = cur_line.strip('\t').strip(' ').split(' ')
                                        concat_ch = ''
                                        if not elems[0].strip('.').isdigit():
                                            if i <= 0: #还没开始
                                                continue
                                            else: #{}
                                                concat_ch = ','
                                        elif int(elems[0].strip('.')) != i + 1:
                                            continue
                                        else:
                                            i = int(elems[0].strip('.'))
                                            concat_ch = ' '
                                        ip = FindIpInData(cur_line)
                                        if not ip:     
                                            pass
                                        else:
                                            trace += concat_ch + ip
                                    trace_list.append(trace.strip(' '))
    except Exception as e:
        #print('Connect to %s eye %s error: %s' %(url, eye, e))
        pass
    return trace_list

def tr_steadfast(dst_ip, req): #https://www.steadfast.net/support/networktest.php
    trace_list = []
    try:
        for eye in ['chicago', 'edison']:
            url = 'https://www.steadfast.net/steadfast-widgets/ajax/swnwt?host=%s&city=%s&network=ipv4&cmd=traceroute' %(dst_ip, eye)
            resource = req.get(url) 
            if resource:                
                if resource.status_code == 200:
                    info = json.loads(resource.text)
                    id = info['id']
                    url = 'https://www.steadfast.net/steadfast-widgets/ajax/swnwt?id=' + id
                    resource = req.get(url)
                    if resource:                
                        if resource.status_code == 200:
                            trace = ''
                            info = json.loads(resource.text)
                            for elem in info['lines']:
                                if elem.strip(' ').split(' ')[0].isdigit():
                                    ip = FindIpInData(elem)
                                    if not ip:     
                                        pass
                                    else:
                                        trace += ' ' + ip
                            trace_list.append(trace.strip(' '))
    except Exception as e:
        #print('Connect to %s eye %s error: %s' %(url, eye, e))
        pass
    return trace_list
  
def tr_core_backbone(dst_ip, req): #http://lg.core-backbone.com/
    trace_list = []
    urls = ['http://lg.core-backbone.com/', 'http://fra.lg.core-backbone.com/', 'http://sin.lg.core-backbone.com/', 'http://ams.lg.core-backbone.com/', 'http://nyk.lg.core-backbone.com/']
    for url in urls:
        url_ = url + 'ajax.php?cmd=traceroute&host=' + dst_ip
        trace = GetTrFromUrlGet(url_, req)
        if trace:
            trace_list.append(trace)
    return trace_list
    
def tr_ntt_lt(dst_ip, req): #https://lg.ntt.lt/
    url = 'https://lg.ntt.lt/ajax.php?cmd=traceroute&host=' + dst_ip
    return method_tr_get_base(url, req)

def tr_cosmonova(dst_ip, req): #http://lg.cosmonova.net.ua/
    trace_list = []
    url = 'http://lg.cosmonova.net.ua/'
    eyes = GetEyes(url, req)
    try:
        for eye in eyes:
            print(eye)
            url = 'http://lg.cosmonova.net.ua/?command=trace&protocol=ipv4&query=%s&router=%s' %(dst_ip, eye)
            resource = req.get(url) 
            if resource:                
                if resource.status_code == 200:
                    trace = ''
                    find_res = re.findall('<code>(.+?)</code>', resource.text, re.DOTALL)
                    if find_res:
                        for elem in find_res[0].split('\n'):
                            if elem.strip(' ').split(' ')[0].isdigit():
                                ip = FindIpInData(elem)
                                if ip:
                                    trace += ' ' + ip
                    if trace:
                        trace_list.append(trace.strip(' '))
    except Exception as e:
        #print('Connect to %s eye %s error: %s' %(url, eye, e))
        pass
    return trace_list
    
def tr_as35266(dst_ip, req): #http://lg.as35266.net/
    url = 'http://lg.as35266.net/traceroute/collector/ipv4?q=' + dst_ip
    return method_tr_get_base(url, req)
    
def tr_coolhousing(dst_ip, req): #http://lg.coolhousing.net/
    url = 'https://lg.coolhousing.net/ajax.php?cmd=traceroute&host=' + dst_ip
    return method_tr_get_base(url, req)
    
def tr_limeline(dst_ip, req): #http://lg.limeline.sl/
    url = 'http://lg.limeline.sl/index.php?page=traceroute&do=traceroute'
    data = {'ip': dst_ip, 'tool': 'traceroute', 'Result': 'TRACEROUTE'}
    return method_tr_post_base(url, req, data)
    
def tr_speedtest(dst_ip, req): #http://www.speedtest.com.sg/
    url = 'http://www.speedtest.com.sg/tr.php'
    data = {'host': dst_ip, 'submit': '', 'submit1': 'Submit'}
    return method_tr_post_base(url, req, data)
    
def tr_grahamedia(dst_ip, req): #http://lg.grahamedia.net.id/
    url = 'https://lg.grahamedia.net.id/ajax.php?cmd=traceroute&host=' + dst_ip
    return method_tr_get_base(url, req)
    
def tr_sbp(dst_ip, req): #http://lg.sbp.net.id
    url = 'http://lg.sbp.net.id/ajax.php?cmd=traceroute&host=' + dst_ip
    return method_tr_get_base(url, req)
    
def tr_firenet(dst_ip, req): #http://lg.firenet.com.au/lg
    url = 'https://www.firenet.com.au/lg/index.php'
    eyes = GetEyes(url, req)

    url = 'https://www.firenet.com.au/lg/execute.php'
    data = {'routers': '', 'query': 'traceroute', 'dontlook': '', 'parameter': dst_ip}
    return GetTraceListFromEyesPost(eyes, url, req, data, 'routers')
    
def tr_i4networks(dst_ip, req): #http://lg.i4networks.nl/lg.cgi
    url = 'https://lg.i4networks.nl/lg.cgi'
    eyes = GetEyes(url, req)

    url = 'https://lg.i4networks.nl/execute.php'
    data = {'routers': '', 'query': 'traceroute', 'dontlook': '', 'parameter': dst_ip}
    return GetTraceListFromEyesPost(eyes, url, req, data, 'routers')
    
def tr_sitel(dst_ip, req): #http://lg.sitel.net.pl/
    url = 'http://lg.sitel.net.pl/ajax.php?cmd=traceroute&host=' + dst_ip
    return method_tr_get_base(url, req)
    
def tr_unix_solutions(dst_ip, req): #http://lg.unix-solutions.be/
    url = 'https://lg.unix-solutions.be/ajax.php?cmd=traceroute&host=' + dst_ip
    return method_tr_get_base(url, req)
    
def tr_lax_psychz(dst_ip, req): #http://lg.lax.psychz.net/
    url = 'http://lg.lax.psychz.net/ajax.php?cmd=traceroute&host=' + dst_ip
    return method_tr_get_base(url, req)
    
def tr_omnilance(dst_ip, req): #http://lg.omnilance.com
    url = 'http://lg.omnilance.com/ajax.php?cmd=traceroute&host=' + dst_ip
    return method_tr_get_base(url, req)
    
def tr_as41103(dst_ip, req): #https://as41103.net/cgi-bin/bgplg
    url = 'https://as41103.net/cgi-bin/bgplg?cmd=traceroute&req=' + dst_ip
    return method_tr_get_base(url, req)
    
def tr_as42093(dst_ip, req): #https://www.as42093.net/lg.php
    url = 'https://www.as42093.net/lg.php'
    data = {'portNum': '80', 'queryType': 'tr', 'target': dst_ip, 'Submit': 'Do It'}
    return method_tr_post_base(url, req, data)
    
# def tr_tbros(dst_ip, req): #http://lg.tbros.net/
#     url = 'https://lg.tbros.net/execute.php'
#     data = {'routers': 'router1', 'query': 'traceroute', 'dontlook': '', 'parameter': dst_ip}
#     return method_tr_post_base(url, req, data)
    
def tr_as42695(dst_ip, req): #http://lg.as42695.net/
    trace_list = []
    url = 'http://lg.as42695.net/'
    eyes = []
    data = JustGetRespFromUrl(url, req)
    if data:
        soup = BeautifulSoup(data.decode('utf-8'), 'html.parser')
        find_res = soup.find_all('option')
        if find_res:
            for link in find_res:
                eyes.append(link['name'])
    for eye in eyes:
        data = {'lg_lookup': dst_ip, 'lg_router': eye, 'lg_lookuptype': 'lg_type_traceroute', 'async': 'true'}
        try:
            resource = req.post(url, data=data) 
            if resource:                
                if resource.status_code == 200:
                    url_1 = 'http://lg.as42695.net/async.php'
                    data = {'async_id': resource.text.strip('\n'), 'nextchunk': '1'}
                    trace = GetTrFromUrlPost(url_1, req, data)
                    if trace:
                        trace_list.append(trace)
        except Exception as e:
            pass
    return trace_list
    
def tr_fotontel(dst_ip, req): #http://lg.fotontel.ru
    url = 'http://lg.fotontel.ru/lg/'
    eyes = GetEyes(url, req)
    data = {'router': '', 'query': 'trace', 'protocol': 'IPv4', 'addr': dst_ip}
    return GetTraceListFromEyesPost(eyes, url, req, data, 'router')
    
def tr_rezopole(dst_ip, req): #https://lg.rezopole.net/summary/vm-birdlg/ipv4
    url = 'https://lg.rezopole.net/traceroute/vm-birdlg/ipv4?q=' + dst_ip
    return method_tr_get_base(url, req)

def tr_as43289(dst_ip, req): #https://lg.as43289.net/
    url = 'https://lg.as43289.net/'
    eyes = GetEyes(url, req)
    url = 'https://lg.as43289.net/execute.php'
    data = {'routers': '', 'query': 'traceroute', 'dontlook': '', 'parameter': dst_ip}
    return GetTraceListFromEyesPost(eyes, url, req, data, 'routers')
    
def tr_netdirekt(dst_ip, req): #http://lg.netdirekt.com.tr/
    url = 'http://lg.netdirekt.com.tr/ajax.php?cmd=traceroute&host=' + dst_ip
    return method_tr_get_base(url, req)
    
def tr_tre_se(dst_ip, req): #http://lg.tre.se/
    url = 'http://lg.tre.se/ajax.php?cmd=traceroute&host=' + dst_ip
    return method_tr_get_base(url, req)
    
def tr_first_colo(dst_ip, req): #http://lg.first-colo.net/
    url = 'https://lg.first-colo.net/ajax.php?cmd=traceroute&host=' + dst_ip
    return method_tr_get_base(url, req)
    
def tr_bulgartel(dst_ip, req): #http://lg.bulgartel.bg/
    url = 'http://lg.bulgartel.bg/?query=trace&protocol=IPv4&addr=%s&router=br1.int.bulgartel.bg' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_23media(dst_ip, req): #https://lg.23media.com
    url = 'https://lg.23m.com/ajax.php?cmd=traceroute&host=%s' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_nessus(dst_ip, req): #http://lg.nessus.at/lg
    url = 'http://lg.as47692.net/tools/ajax.php?cmd=traceroute&host=%s' %dst_ip
    return method_tr_get_base(url, req)

def tr_ciklet(dst_ip, req): #http://lg.ciklet.net.tr
    try:
        url = 'https://lg.intibu.com/'
        resource = req.get(url) 
        if resource:                
            if resource.status_code == 200:
                csrf_token = re.findall('name=\"csrf\" value=\"(.+?)\"', resource.text)
                url = 'https://lg.intibu.com/ajax.php?cmd=traceroute&host=%s&csrf=%s' %(dst_ip, csrf_token[0])
                return method_tr_get_base(url, req)
    except Exception as e:
        #print('Connect to %s eye %s error: %s' %(url, eye, e))
        pass
    return []
    
def tr_mtw_ru(dst_ip, req): #http://lg.mtw.ru/
    url = 'https://lg.mtw.ru/api'
    data = {'request': 'trace', 'args': dst_ip}
    return method_tr_post_base(url, req, data, None, 240)
    
def tr_cable_st(dst_ip, req): #http://lg.cable.st/
    url = 'http://lg.cable.st/ajax.php?cmd=traceroute&host=%s' %dst_ip
    return method_tr_get_base(url, req)

def tr_milecom(dst_ip, req): #https://lg.milecom.ru/
    url = 'https://lg.milecom.ru/'
    eyes = GetEyes(url, req)
    data = {'router': '', 'query': 'trace', 'protocol': 'IPv4', 'addr': dst_ip}
    return GetTraceListFromEyesPost(eyes, url, req, data, 'router')
    
def tr_neocarrier(dst_ip, req): #http://lg.neocarrier.com/
    url = 'https://lg.neocarrier.com/ajax.php?cmd=traceroute&host=%s' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_kapper(dst_ip, req): #https://lg.kapper.net/
    url = 'https://lg.kapper.net/ajax.php?cmd=traceroute&host=%s' %dst_ip
    return method_tr_get_base(url, req)

def tr_en_dtln(dst_ip, req): #http://lg-en.dtln.ru/
    url = 'http://lg-en.dtln.ru/'
    eyes = GetEyes(url, req)
    data = {'router': '', 'query': 'trace', 'protocol': 'IPv4', 'addr': dst_ip}
    return GetTraceListFromEyesPost(eyes, url, req, data, 'router')

def tr_frankfurt_serverhub(dst_ip, req): #http://lg-frankfurt.serverhub.com/
    url = 'http://lg-frankfurt.serverhub.com/ajax.php?cmd=traceroute&host=%s' %dst_ip
    return method_tr_get_base(url, req)

def tr_aplitt(dst_ip, req): #http://lg.aplitt.net/
    url = 'http://lg.aplitt.net/'
    eyes = GetEyes(url, req)
    data = {'router': '', 'query': 'trace', 'protocol': 'IPv4', 'addr': dst_ip}
    return GetTraceListFromEyesPost(eyes, url, req, data, 'router')

def tr_eleusi(dst_ip, req): #http://lg.eleusi.com
    url = 'http://lg.eleusi.com/LookingGlass.pl'
    data = {'arg': dst_ip, 'qt':'trace'}
    return method_tr_post_base(url, req, data)

def tr_truenetwork(dst_ip, req): #http://lg.truenetwork.ru/
    url = 'http://lg.truenetwork.ru/ajax.php?cmd=traceroute&host=%s' %dst_ip
    return method_tr_get_base(url, req)

def tr_plutex(dst_ip, req): #https://lg.plutex.de/
    try:
        url = 'https://lg.plutex.de/'
        resource = req.get(url) 
        if resource:                
            if resource.status_code == 200:
                csrf_token = re.findall('name=\"csrf\" value=\"(.+?)\"', resource.text)
                url = 'https://lg.plutex.de/ajax.php?cmd=traceroute&host=%s&csrf=%s' %(dst_ip, csrf_token[0])
                return method_tr_get_base(url, req)
    except Exception as e:
        #print('Connect to %s eye %s error: %s' %(url, eye, e))
        pass
    return []

def tr_custdc(dst_ip, req): #https://lg.custdc.net
    url = 'https://lg.custdc.net/LookingGlass.pl'
    data = {'arg': dst_ip, 'qt':'trace'}
    return method_tr_post_base(url, req, data)

def tr_blix(dst_ip, req): #https://lg.blix.com/summary/lg/ipv4
    url = 'https://lg.blix.com/traceroute/lg/ipv4?q=%s' %dst_ip
    return method_tr_get_base(url, req)

def tr_campus_rv(dst_ip, req): #http://lg.campus-rv.net/
    url = 'http://lg.campus-rv.net/?command=trace&protocol=ipv4&query=%s&router=border' %dst_ip
    return method_tr_get_base(url, req)

#返回率不高
def tr_serverius(dst_ip, req): #https://lg.serverius.net/
    url = 'https://lg.serverius.net/'
    eyes = GetEyes(url, req)
    
    trace_list = []
    try:
        for eye in eyes:
            print(eye)
            time.sleep(1) 
            url = 'https://lg.serverius.net/ulg.py?action=runcommand'
            data = {'routerid': eye, 'commandid': '6', 'param0': dst_ip}            
            resource = req.post(url, data=data)
            if resource:                
                if resource.status_code == 200:
                    time.sleep(2) #不加延迟不行
                    url = 'https://lg.serverius.net/' + re.findall('url=(.+?)\"', resource.text)[0]
                    trace = GetTrFromUrlGet(url, req)
                    if trace:
                        trace_list.append(trace)
    except Exception as e:
        #print('Connect to %s eye %s error: %s' %(url, eye, e))
        pass
    return trace_list

def tr_grenode(dst_ip, req): #https://lg.grenode.net/summary/safran+batture/ipv4
    eyes = ['safran', 'batture']
    url = 'https://lg.grenode.net/traceroute/eye/ipv4?q=%s' %dst_ip
    return GetTraceListFromEyesGet(eyes, url, req)

def tr_mediainvent(dst_ip, req): #http://lg.mediainvent.net
    url = 'https://lg.mediainvent.net/ajax.php?cmd=traceroute&host=%s' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_arpnet(dst_ip, req): #http://lg.arpnet.pl/
    url = 'http://lg.arpnet.pl/ajax.php?cmd=traceroute&host=%s' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_ek_media_nl(dst_ip, req): #http://lg.ek-media.nl/summary/dlt.core/ipv4
    url = 'http://lg.ek-media.nl/traceroute/NOC/ipv4?q=%s' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_iveloz(dst_ip, req): #http://lg.iveloz.net.br
    url = 'http://lg.iveloz.net.br/execute.php'
    data = {'routers': 'router1', 'query': 'traceroute', 'dontlook': '', 'parameter': dst_ip}
    return method_tr_post_base(url, req, data, None, 120)
    
def tr_uepg(dst_ip, req): #http://lg.uepg.br/
    url = 'http://lg.uepg.br/ajax.php?cmd=traceroute&host=%s' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_k2telecom(dst_ip, req): #http://lg.k2telecom.net.br/
    url = 'https://lg.k2telecom.net.br/execute.php'
    data = {'routers': 'router4', 'query': 'traceroute', 'dontlook': '', 'parameter': dst_ip}
    return method_tr_post_base(url, req, data, None, 120)
    
def tr_comfortel(dst_ip, req): #http://lg.comfortel.pro/
    url = 'http://lg.comfortel.pro/'
    eyes = GetEyes(url, req)

    trace_list = []
    for eye in eyes:
        url = 'http://lg.comfortel.pro/?command=trace&protocol=ipv4&query=%s&router=%s' %(dst_ip, eye)
        resource = req.get(url) 
        if resource:                
            if resource.status_code == 200:
                trace = ''
                find_res = re.findall('<code>(.+?)</code>', resource.text, re.DOTALL)
                if find_res:
                    for elem in find_res[0].split('\n'):
                        if elem.strip(' ').split(' ')[0].isdigit():
                            ip = FindIpInData(elem)
                            if ip:
                                trace += ' ' + ip
                if trace:
                    trace_list.append(trace.strip(' '))
    return trace_list
    
def tr_wirehive(dst_ip, req): #https://lg.wirehive.net/summary/lg/ipv4
    url = 'https://lg.wirehive.net/traceroute/lg/ipv4?q=%s' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_datahata(dst_ip, req): #https://lg.datahata.by/cgi-bin/bgplg
    url = 'https://lg.datahata.by/cgi-bin/bgplg?cmd=traceroute&req=%s' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_sdi(dst_ip, req): #http://lg.sdi.net.id/
    url = 'http://lg.sdi.net.id/ajax.php?cmd=traceroute&host=%s' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_nitex(dst_ip, req): #http://lg.nitex.cz/
    url = 'http://lg.nitex.cz/index.php?host=%s&submit=Traceroute%%21' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_at_edis_at(dst_ip, req): #https://at.edis.at/ajax.php?cmd=traceroute&host=
    url = 'https://at.edis.at/ajax.php?cmd=traceroute&host=%s' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_medkirov_at(dst_ip, req): #https://lg.medkirov.ru/
    trace_list = []
    url = 'https://lg.medkirov.ru/ajax.php?cmd=traceroute&host=%s' %dst_ip
    try:
        resource = req.get(url, timeout=120) 
        if resource:                
            if resource.status_code == 200:
                trace = ''
                data = resource.text
                data = data.replace('<br />', '\n') #padding
                data = data.replace('&nbsp;', ' ') #padding
                i = -1
                for cur_line in data.strip('\n').split('\n'):
                    elems = cur_line.strip('\t').strip(' ').split(' ')
                    concat_ch = ''
                    if not elems[0].strip('.').isdigit():
                        if i <= 0: #还没开始
                            continue
                        else: #{}
                            concat_ch = ','
                    else:
                        i = int(elems[0].strip('.'))
                        concat_ch = ' '
                    ip = FindIpInData(cur_line)
                    if not ip:
                        #print('Error type: %s' %cur_line)        
                        pass
                    else:
                        trace += concat_ch + ip
                if trace:
                    trace_list.append(trace)
            else:
                print(url + ': ' + resource.status_code)
    except Exception as e:
        #print('Connect to %s eye %s error: %s' %(url, eye, e))
        pass
    return trace_list
    
def tr_secureax(dst_ip, req): #http://lg.secureax.com/
    url = 'http://lg.secureax.com/ajax.php?cmd=traceroute&host=%s' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_ginernet(dst_ip, req): #http://lg.ginernet.com/
    url = 'http://lg.ginernet.com/ajax.php?cmd=traceroute&host=%s' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_jettel(dst_ip, req): #http://lg.jettel.pl/
    url = 'http://lg.jettel.pl/ajax.php?cmd=traceroute&host=%s' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_zug_sinavps(dst_ip, req): #http://lg.zug.sinavps.ch/
    url = 'http://lg.zug.sinavps.ch/ajax.php?cmd=traceroute&host=%s' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_ist_citynethost(dst_ip, req): #http://lg.ist.citynethost.com/
    url = 'http://lg.ist.citynethost.com/ajax.php?cmd=traceroute&host=%s' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_amigonet(dst_ip, req): #https://lg.amigonet.cz/
    try:
        url = 'https://lg.amigonet.cz/'
        resource = req.get(url) 
        if resource:                
            if resource.status_code == 200:
                eyes = []
                soup = BeautifulSoup(resource.content.decode('utf-8'), 'html.parser')
                find_res = soup.find_all('option')
                for link in find_res:
                    eye = link['value']
                    if eye.__contains__('ping') or eye.__contains__('trace') or \
                        eye.__contains__('route') or eye.__contains__('peers'):
                        continue
                    eyes.append(eye)
                token = re.findall('name="_token_" value=\"(.+?)\"', resource.text)
                data = {'ip': dst_ip, 'router': '', 'akce': 'trace4', '_token_': token[0], '_do': 'glass-form-submit', 'send': 'Run'}
                return GetTraceListFromEyesPost(eyes, url, req, data, 'router')
    except Exception as e:
        #print('Connect to %s eye %s error: %s' %(url, eye, e))
        pass   
    return []
    
def tr_csti_ch(dst_ip, req): #https://tools.csti.ch/
    url = 'https://tools.csti.ch/ajax.php?cmd=traceroute&host=%s' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_ldn_fai(dst_ip, req): #https://lg.ldn-fai.net/summary/cannibalon+eternium/ipv6
    eyes = ['cannibalon', 'eternium']
    url = 'https://lg.ldn-fai.net/traceroute/eye/ipv4?q=%s' %dst_ip
    return GetTraceListFromEyesGet(eyes, url, req)
    
def tr_as60362(dst_ip, req): #http://lg.as60362.net/summary/router1.paris1/ipv4
    url = 'http://lg.as60362.net/traceroute/router1.paris1/ipv4?q=%s' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_nz_zappiehost(dst_ip, req): #http://lg-nz.zappiehost.com/
    url = 'https://lg-nz.zappiehost.com/action.php?mode=looking_glass&action=traceroute'
    data = {'id': '491', 'domain': dst_ip}
    return method_tr_post_base(url, req, data)
    
def tr_za_zappiehost(dst_ip, req): #http://lg-za.zappiehost.com/
    url = 'https://lg-nz.zappiehost.com/action.php?mode=looking_glass&action=traceroute'
    data = {'id': '492', 'domain': dst_ip}
    return method_tr_post_base(url, req, data)
    
def tr_gthost(dst_ip, req): #https://gthost.com/looking-glass/    
    trace_list = []
    try:
        url = 'https://gthost.com/looking-glass/'
        eye_urls = []
        resource = req.get(url) 
        if resource:                
            if resource.status_code == 200:# or resource.status_code == 304:
                eye_urls = re.findall('<option data-target=\"(.+?)\"', resource.text)                
        for eye_url in eye_urls:
            print(eye_url)
            url = eye_url + '?cmd=traceroute&host=%s' %dst_ip
            trace = GetTrFromUrlGet(url, req)
            if trace:
                trace_list.append(trace)
                #print(trace)
    except Exception as e:
            pass
    return trace_list   
    
def tr_baehost(dst_ip, req): #https://baehost.com/es-int/looking-glass/  
    try:
        url = 'https://baehost.com/es-int/looking-glass/'
        resource = req.get(url) 
        eyes = []
        if resource:                
            if resource.status_code == 200:
                token = re.findall('csrfToken = \'(.+?)\'', resource.text)[0]
                soup = BeautifulSoup(resource.content.decode('utf-8'), 'html.parser')
                find_res = soup.find_all('option')
                for link in find_res:
                    eyes.append(link['value'])                
                url = 'https://baehost.com/es-int/looking-glass/execute.php'
                data = {'token': token, 'routers': '', 'query': 'traceroute', 'dontlook': '', 'parameter': dst_ip}
                return GetTraceListFromEyesPost(eyes, url, req, data, 'routers', None, 120)
    except Exception as e:
        #print('Connect to %s eye %s error: %s' %(url, eye, e))
        pass
    return []
    
def tr_dallas_serverhub(dst_ip, req): #http://lg-dallas.serverhub.com/
    url = 'http://lg-dallas.serverhub.com/ajax.php?cmd=traceroute&host=%s' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_ipms_chinatelecomglobal(dst_ip, req): #http://ipms.chinatelecomglobal.com/public/lookglass/lookglassDisclaimer.html   
    trace_list = []
    try:
        url = 'https://ipms.chinatelecomglobal.com/public/lookglass/setSourceSelect.html?localLang=en&localCountry=US&ipType=ipv4&&lang=en_US'
        data = {'localLang': 'en', 'localCountry': 'US', 'ipType': 'ipv4', 'lang': 'en_US'}
        resource = req.post(url, data)
        if resource:                
            if resource.status_code == 200:
                eyes = re.findall('\"id\":\"(.+?)\"', resource.text)
                for eye in eyes:
                    print(eye)
                    url = 'https://ipms.chinatelecomglobal.com/public/lookglass/start.html?lang=en_US'
                    data = {'SOURCE': eye, 'DESTINATION': dst_ip, 'SERVICE': 'traceroute', 'NETWORK': 'off_net', 'IPTYPE': 'ipv4'}
                    resource = req.post(url, data) 
                    if resource:                
                        if resource.status_code == 200:
                            uuid = re.findall('uuid=\"(.+?)\"', resource.text)[0]
                            url = 'https://ipms.chinatelecomglobal.com/public/lookglass/query.html?lang=en_US'
                            data = {'uuid': uuid}
                            time.sleep(1)
                            trace = GetTrFromUrlPost(url, req, data)
                            if trace:
                                trace_list.append(trace)
    except Exception as e:
        #print('Connect to %s eye %s error: %s' %(url, eye, e))
        pass
    return trace_list
    
def tr_coloau(dst_ip, req): #https://lg.coloau.com.au/
    url = 'https://lg.coloau.com.au/'
    eyes = GetEyes(url, req)

    url = 'https://lg.coloau.com.au/execute.php'
    data = {'vrf': 'routing-instance international', 'routers': '', 'query': 'traceroute', 'dontlook': '', 'parameter': dst_ip}
    return GetTraceListFromEyesPost(eyes, url, req, data, 'routers')
    
def tr_as64476(dst_ip, req): #http://lg.as64476.net/
    url = 'https://lg.shadow.tech/'
    eyes = GetEyes(url, req)

    url = 'https://lg.shadow.tech/execute.php'
    data = {'routers': '', 'query': 'traceroute', 'dontlook': '', 'parameter': dst_ip}
    return GetTraceListFromEyesPost(eyes, url, req, data, 'routers')
    
def tr_jetspotspeed(dst_ip, req): #http://www.jetspotspeed.com/
    url = 'http://www.jetspotspeed.com/ajax.php?cmd=traceroute&host=%s' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_imcloud(dst_ip, req): #http://lg.imcloud.tw/
    url = 'http://lg.imcloud.tw/ajax.php?cmd=traceroute&host=%s' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_tetaneutral(dst_ip, req): #https://lg.tetaneutral.net/summary/h7/ipv4
    url = 'https://lg.tetaneutral.net/traceroute/h7/ipv4?q=%s' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_bgp4_pl(dst_ip, req): #http://lg.bgp4.pl/
    url = 'http://lg.bgp4.pl/ajax.php?cmd=traceroute&host=%s' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_qonnected(dst_ip, req): #https://lg.qonnected.net/
    url = 'https://lg.qonnected.net/'
    eyes = GetEyes(url, req)   
    trace_list = []
    try:
        for eye in eyes:
            print(eye)
            url = 'https://lg.qonnected.net/?command=trace&protocol=ipv4&query=%s&router=%s' %(dst_ip, eye)
            resource = req.get(url) 
            if resource:                
                if resource.status_code == 200:
                    trace = ''
                    find_res = re.findall('<code>(.+?)</code>', resource.text, re.DOTALL)
                    if find_res:
                        for elem in find_res[0].split('\n'):
                            if elem.strip(' ').split(' ')[0].isdigit():
                                ip = FindIpInData(elem)
                                if ip:
                                    trace += ' ' + ip
                    if trace:
                        trace_list.append(trace.strip(' '))
    except Exception as e:
        #print('Connect to %s eye %s error: %s' %(url, eye, e))
        pass
    return trace_list
    
def tr_as201206(dst_ip, req): #http://as201206.net/
    url = 'http://as201206.net/ajax.php?cmd=traceroute&host=%s' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_edsi_tech(dst_ip, req): #https://lg.edsi-tech.com/
    url = 'https://lg.edsi-tech.com/ajax.php?cmd=traceroute&host=%s' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_it_communicationsltd(dst_ip, req): #http://lg2.it-communicationsltd.co.uk/
    url = 'https://lg2.it-communicationsltd.co.uk/ajax.php?cmd=traceroute&host=%s' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_alt_tf(dst_ip, req): #https://lg.alt.tf/
    url = 'https://lg.alt.tf/execute.php'
    data = {'routers': 'mpl-router', 'query': 'traceroute', 'dontlook': '', 'parameter': dst_ip}
    return method_tr_post_base(url, req, data)
    
def tr_as204003(dst_ip, req): #https://lg.as204003.net/
    url = 'https://lg.as204003.net/ajax.php?cmd=traceroute&host=%s' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_hostoweb(dst_ip, req): #http://lg.hostoweb.com/
    url = 'http://lg.hostoweb.com/ajax.php?cmd=traceroute&host=%s' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_terrahost(dst_ip, req): #http://lg.terrahost.no/
    url = 'https://lg.terrahost.no/ajax.php?cmd=traceroute&host=%s' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_zcenter(dst_ip, req): #http://noc.zcenter.pl
    url = 'http://noc.zcenter.pl/ajax.php?cmd=traceroute&host=%s' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_abica(dst_ip, req): #http://lg.abica.co.uk/
    url = 'http://lg.abica.co.uk/ajax.php?cmd=traceroute&host=%s' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_as206479(dst_ip, req): #https://lg.as206479.net/
    trace_list = []
    url = 'https://lg.as206479.net/api/query/'
    data = {'query_location': "core1_fra1_de_as206479_net",  'query_target': dst_ip,  'query_type': "traceroute",  'query_vrf': "global"}
    try:
        resource = req.post(url, json=data, timeout=30)
        if resource:                
            if resource.status_code == 200:
                trace = GetTrFromData(resource.text)
                if trace:
                    trace_list.append(trace) 
    except Exception as e:
        #print('Connect to %s eye %s error: %s' %(url, eye, e))
        pass    
    return trace_list
    
def tr_fratec(dst_ip, req): #http://lg.fratec.net/
    url = 'http://lg.fratec.net/'
    eyes = GetEyes(url, req)

    url = 'http://lg.fratec.net/execute.php'
    data = {'routers': '', 'query': 'traceroute', 'dontlook': '', 'parameter': dst_ip}
    return GetTraceListFromEyesPost(eyes, url, req, data, 'routers')
    
def tr_dns1_byteweb(dst_ip, req): #http://dns1.byteweb.com.br/lg/
    url = 'http://dns1.byteweb.com.br/lg/ajax.php?cmd=traceroute&host=%s' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_web4africa(dst_ip, req): #http://lg.web4africa.net/
    url = 'http://lg.web4africa.net/ajax.php?cmd=traceroute&host=%s' %dst_ip
    return method_tr_get_base(url, req)
    
def tr_brascom(dst_ip, req): #http://lg.brascom.net.br/
    url = 'http://lg.brascom.net.br/'
    eyes = GetEyes(url, req)

    url = 'http://lg.brascom.net.br/?command=trace&protocol=ipv4&query=%s&router=eye' %dst_ip
    return GetTraceListFromEyesGet(eyes, url, req)
    
def tr_as328112(dst_ip, req): #https://lg.as328112.net/
    url = 'https://lg.as328112.net/'
    eyes = GetEyes(url, req)

    url = 'https://lg.as328112.net/?command=trace&protocol=ipv4&query=%s&router=eye' %dst_ip
    return GetTraceListFromEyesGet(eyes, url, req)
    
def tr_allpointsbroadband(dst_ip, req): #http://lg.allpointsbroadband.com/
    url = 'http://lg.allpointsbroadband.com/'
    eyes = GetEyes(url, req)

    data = {'query': 'trace', 'protocol': 'IPv4', 'addr': dst_ip, 'router': ''}
    return GetTraceListFromEyesPost(eyes, url, req, data, 'router')
    
def tr_la1_hostbrew(dst_ip, req): #http://lg.la1.hostbrew.com/
    url = 'http://lg.la1.hostbrew.com/ajax.php?cmd=traceroute&host=%s' %dst_ip
    return method_tr_get_base(url, req)

def tr_cloudsingularity(dst_ip, req): #http://lg.cloudsingularity.net
    url = 'https://lg.cloudsingularity.net/'
    eyes = []
    try:
        resource = req.get(url) 
        if resource:                
            if resource.status_code == 200:
                find_res = re.findall('\"_id\":\"(.+?)\"', resource.text, re.DOTALL)
                for elem in find_res:
                    if elem != 'default':
                        eyes.append(elem)   
                trace_list = []
                url = 'https://lg.cloudsingularity.net/api/query/'
                for eye in eyes:
                    data = {'query_location': eye, 'query_target': dst_ip, 'query_type': "traceroute", 'query_vrf': "default"}
                    resource = req.post(url, json=data, timeout=30)
                    if resource:                
                        if resource.status_code == 200:
                            trace = GetTrFromData(resource.text)
                            if trace:
                                trace_list.append(trace) 
    except Exception as e:
        #print('Connect to %s eye %s error: %s' %(url, eye, e))
        pass    
    return trace_list
    
def tr_prolixium(dst_ip, req): #hhttps://www.prolixium.com/lg
    url = 'https://www.prolixium.com/lg'
    data = {'host': dst_ip, 'submit': 'Execute Operation', 'protocol': 'IPv4', 'type': 'traceroute'}
    return method_tr_post_base(url, req, data)

def FindPeersOfLGs():
    PreGetSrcFilesInDirs()
    GetAsRankDict(2021, 4)
    GetAsNeighs(2021, 4)
    url_as_dict = dict()
    os.chdir('/home/slt/code/ana_c_d_incongruity/')
    with open('lg.dat', 'r') as rf:
        for curline in rf.read().split('\n'):
            elems = curline.strip(' ').strip('\t').split('\t')
            if len(elems) == 2:
                (asn, url) = elems
                url_as_dict[url] = asn
            else:
                print('Error1: %s' %curline)
    asn_set = set()
    with open('lg_conn_res_succeed_with_traceroute', 'r') as rf:
        for curline in rf.read().split('\n'):
            if not curline.__contains__('#'): # valid lg
                url = curline.strip(' ').strip('\t')
                if url not in url_as_dict.keys(): 
                    print('Error2: %s' %url)
                    continue
                asn_set.add(url_as_dict[url])
    tier_1_as_num = 0
    as_neighs = set()
    for asn in asn_set:
        as_rank = GetAsRankFromDict(asn)
        if as_rank == None:
            print('Error3 asn: %s' %asn)
        else:
            if as_rank < 20: #tier-1 as
                tier_1_as_num += 1
            as_neighs |= GetAsNeighsFromDict(asn)
    print(len(asn_set))
    print(tier_1_as_num)
    print(len(as_neighs))
    


if __name__ == '__main__':
    if False:
        FindPeersOfLGs()
    else:
        #CheckTrMethod('lg_remain')
        req = requests.session()
        sum_num = 0
        
        trace_list = tr_as206479('8.8.8.8', req)
        print('tr_as206479')
        sum_num += len(trace_list)    
        trace_list = tr_fratec('8.8.8.8', req)
        print('tr_fratec')
        sum_num += len(trace_list)
        trace_list = tr_dns1_byteweb('8.8.8.8', req)
        print('tr_dns1_byteweb')
        sum_num += len(trace_list)
        trace_list = tr_web4africa('8.8.8.8', req)
        print('tr_web4africa')
        sum_num += len(trace_list)
        trace_list = tr_brascom('8.8.8.8', req)
        print('tr_brascom')
        sum_num += len(trace_list)
        trace_list = tr_as328112('8.8.8.8', req)
        print('tr_as328112')
        sum_num += len(trace_list)
        trace_list = tr_allpointsbroadband('8.8.8.8', req)
        print('tr_allpointsbroadband')
        sum_num += len(trace_list)
        trace_list = tr_la1_hostbrew('8.8.8.8', req)
        print('tr_la1_hostbrew')
        sum_num += len(trace_list)
        trace_list = tr_cloudsingularity('8.8.8.8', req)
        print('tr_cloudsingularity')
        sum_num += len(trace_list)
        trace_list = tr_prolixium('8.8.8.8', req)
        print('tr_prolixium')
        sum_num += len(trace_list)

        trace_list = tr_nz_zappiehost('8.8.8.8', req)
        print('tr_nz_zappiehost')
        sum_num += len(trace_list)
        trace_list = tr_za_zappiehost('8.8.8.8', req)
        print('tr_za_zappiehost')
        sum_num += len(trace_list)
        trace_list = tr_gthost('8.8.8.8', req)
        print('tr_gthost')
        sum_num += len(trace_list)
        trace_list = tr_baehost('8.8.8.8', req)
        print('tr_baehost')
        sum_num += len(trace_list)
        trace_list = tr_dallas_serverhub('8.8.8.8', req)
        print('tr_dallas_serverhub')
        sum_num += len(trace_list)
        trace_list = tr_ipms_chinatelecomglobal('8.8.8.8', req)
        print('tr_ipms_chinatelecomglobal')
        sum_num += len(trace_list)
        trace_list = tr_coloau('8.8.8.8', req)
        print('tr_coloau')
        sum_num += len(trace_list)
        trace_list = tr_as64476('8.8.8.8', req)
        print('tr_as64476')
        sum_num += len(trace_list)
        trace_list = tr_jetspotspeed('8.8.8.8', req)
        print('tr_jetspotspeed')
        sum_num += len(trace_list)
        trace_list = tr_imcloud('8.8.8.8', req)
        print('tr_imcloud')
        sum_num += len(trace_list)
        trace_list = tr_tetaneutral('8.8.8.8', req)
        print('tr_tetaneutral')
        sum_num += len(trace_list)
        trace_list = tr_bgp4_pl('8.8.8.8', req)
        print('tr_bgp4_pl')
        sum_num += len(trace_list)
        trace_list = tr_qonnected('8.8.8.8', req)
        print('tr_qonnected')
        sum_num += len(trace_list)
        trace_list = tr_as201206('8.8.8.8', req)
        print('tr_as201206')
        sum_num += len(trace_list)
        trace_list = tr_edsi_tech('8.8.8.8', req)
        print('tr_edsi_tech')
        sum_num += len(trace_list)
        trace_list = tr_it_communicationsltd('8.8.8.8', req)
        print('tr_it_communicationsltd')
        sum_num += len(trace_list)
        trace_list = tr_alt_tf('8.8.8.8', req)
        print('tr_alt_tf')
        sum_num += len(trace_list)
        trace_list = tr_as204003('8.8.8.8', req)
        print('tr_as204003')
        sum_num += len(trace_list)
        trace_list = tr_hostoweb('8.8.8.8', req)
        print('tr_hostoweb')
        sum_num += len(trace_list)
        trace_list = tr_terrahost('8.8.8.8', req)
        print('tr_terrahost')
        sum_num += len(trace_list)
        trace_list = tr_zcenter('8.8.8.8', req)
        print('tr_zcenter')
        sum_num += len(trace_list)
        trace_list = tr_abica('8.8.8.8', req)
        print('tr_abica')
        sum_num += len(trace_list)

        trace_list = tr_medkirov_at('8.8.8.8', req)
        print('tr_medkirov_at')
        sum_num += len(trace_list)
        trace_list = tr_secureax('8.8.8.8', req)
        print('tr_secureax')
        sum_num += len(trace_list)
        trace_list = tr_ginernet('8.8.8.8', req)
        print('tr_ginernet')
        sum_num += len(trace_list)
        trace_list = tr_jettel('8.8.8.8', req)
        print('tr_jettel')
        sum_num += len(trace_list)
        trace_list = tr_zug_sinavps('8.8.8.8', req)
        print('tr_zug_sinavps')
        sum_num += len(trace_list)
        trace_list = tr_ist_citynethost('8.8.8.8', req)
        print('tr_ist_citynethost')
        sum_num += len(trace_list)
        trace_list = tr_amigonet('8.8.8.8', req)
        print('tr_amigonet')
        sum_num += len(trace_list)
        trace_list = tr_csti_ch('8.8.8.8', req)
        print('tr_csti_ch')
        sum_num += len(trace_list)
        trace_list = tr_ldn_fai('8.8.8.8', req)
        print('tr_ldn_fai')
        sum_num += len(trace_list)
        trace_list = tr_as60362('8.8.8.8', req)
        print('tr_as60362')
        sum_num += len(trace_list)

        trace_list = tr_aplitt('8.8.8.8', req)
        print('tr_aplitt')
        sum_num += len(trace_list)
        trace_list = tr_eleusi('8.8.8.8', req)
        print('tr_eleusi')
        sum_num += len(trace_list)
        trace_list = tr_truenetwork('8.8.8.8', req)
        print('tr_truenetwork')
        sum_num += len(trace_list)
        trace_list = tr_plutex('8.8.8.8', req)
        print('tr_plutex')
        sum_num += len(trace_list)
        trace_list = tr_custdc('8.8.8.8', req)
        print('tr_custdc')
        sum_num += len(trace_list)
        trace_list = tr_blix('8.8.8.8', req)
        print('tr_blix')
        sum_num += len(trace_list)
        trace_list = tr_campus_rv('8.8.8.8', req)
        print('tr_campus_rv')
        sum_num += len(trace_list)
        trace_list = tr_serverius('8.8.8.8', req)
        print('tr_serverius')
        sum_num += len(trace_list)
        trace_list = tr_grenode('8.8.8.8', req)
        print('tr_grenode')
        sum_num += len(trace_list)
        trace_list = tr_mediainvent('8.8.8.8', req)
        print('tr_mediainvent')
        sum_num += len(trace_list)
        trace_list = tr_arpnet('8.8.8.8', req)
        print('tr_arpnet')
        sum_num += len(trace_list)
        trace_list = tr_ek_media_nl('8.8.8.8', req)
        print('tr_ek_media_nl')
        sum_num += len(trace_list)
        trace_list = tr_iveloz('8.8.8.8', req)
        print('tr_iveloz')
        sum_num += len(trace_list)
        trace_list = tr_uepg('8.8.8.8', req)
        print('tr_uepg')
        sum_num += len(trace_list)
        trace_list = tr_k2telecom('8.8.8.8', req)
        print('tr_k2telecom')
        sum_num += len(trace_list)
        trace_list = tr_comfortel('8.8.8.8', req)
        print('tr_comfortel')
        sum_num += len(trace_list)
        trace_list = tr_wirehive('8.8.8.8', req)
        print('tr_wirehive')
        sum_num += len(trace_list)
        trace_list = tr_datahata('8.8.8.8', req)
        print('tr_datahata')
        sum_num += len(trace_list)
        trace_list = tr_sdi('8.8.8.8', req)
        print('tr_sdi')
        sum_num += len(trace_list)
        trace_list = tr_nitex('8.8.8.8', req)
        print('tr_nitex')
        sum_num += len(trace_list)
        trace_list = tr_at_edis_at('8.8.8.8', req)
        print('tr_at_edis_at')
        sum_num += len(trace_list)

        trace_list = tr_rezopole('8.8.8.8', req)
        print('tr_rezopole')
        sum_num += len(trace_list)
        trace_list = tr_as43289('8.8.8.8', req)
        print('tr_as43289')
        sum_num += len(trace_list)
        trace_list = tr_netdirekt('8.8.8.8', req)
        print('tr_netdirekt')
        sum_num += len(trace_list)
        trace_list = tr_tre_se('8.8.8.8', req)
        print('tr_tre_se')
        sum_num += len(trace_list)
        trace_list = tr_first_colo('8.8.8.8', req)
        print('tr_first_colo')
        sum_num += len(trace_list)
        trace_list = tr_bulgartel('8.8.8.8', req)
        print('tr_bulgartel')
        sum_num += len(trace_list)
        trace_list = tr_23media('8.8.8.8', req)
        print('tr_23media')
        sum_num += len(trace_list)
        trace_list = tr_nessus('8.8.8.8', req)
        print('tr_nessus')
        sum_num += len(trace_list)
        trace_list = tr_ciklet('8.8.8.8', req)
        print('tr_ciklet')
        sum_num += len(trace_list)
        trace_list = tr_mtw_ru('8.8.8.8', req)
        print('tr_mtw_ru')
        sum_num += len(trace_list)
        trace_list = tr_cable_st('8.8.8.8', req)
        print('tr_cable_st')
        sum_num += len(trace_list)
        trace_list = tr_neocarrier('8.8.8.8', req)
        print('tr_neocarrier')
        sum_num += len(trace_list)
        trace_list = tr_kapper('8.8.8.8', req)
        print('tr_kapper')
        sum_num += len(trace_list)
        trace_list = tr_frankfurt_serverhub('8.8.8.8', req)
        print('tr_frankfurt_serverhub')
        sum_num += len(trace_list)

        trace_list = tr_coolhousing('8.8.8.8', req)
        print('tr_coolhousing')
        sum_num += len(trace_list)
        trace_list = tr_limeline('8.8.8.8', req)
        print('tr_limeline')
        sum_num += len(trace_list)
        trace_list = tr_speedtest('8.8.8.8', req)
        print('tr_speedtest')
        sum_num += len(trace_list)
        trace_list = tr_grahamedia('8.8.8.8', req)
        print('tr_grahamedia')
        sum_num += len(trace_list)
        trace_list = tr_sbp('8.8.8.8', req)
        print('tr_sbp')
        sum_num += len(trace_list)
        trace_list = tr_firenet('8.8.8.8', req)
        print('tr_firenet')
        sum_num += len(trace_list)
        trace_list = tr_i4networks('8.8.8.8', req)
        print('tr_i4networks')
        sum_num += len(trace_list)
        trace_list = tr_sitel('8.8.8.8', req)
        print('tr_sitel')
        sum_num += len(trace_list)
        trace_list = tr_unix_solutions('8.8.8.8', req)
        print('tr_unix_solutions')
        sum_num += len(trace_list)
        trace_list = tr_lax_psychz('8.8.8.8', req)
        print('tr_lax_psychz')
        sum_num += len(trace_list)
        trace_list = tr_omnilance('8.8.8.8', req)
        print('tr_omnilance')
        sum_num += len(trace_list)
        trace_list = tr_as41103('8.8.8.8', req)
        print('tr_as41103')
        sum_num += len(trace_list)
        trace_list = tr_as42093('8.8.8.8', req)
        print('tr_as42093')
        sum_num += len(trace_list)
        trace_list = tr_as42695('8.8.8.8', req)
        print('tr_as42695')
        sum_num += len(trace_list)
        trace_list = tr_fotontel('8.8.8.8', req)
        print('tr_fotontel')
        sum_num += len(trace_list)

        trace_list = tr_predkosci('8.8.8.8', req)
        print('tr_predkosci')
        sum_num += len(trace_list)
        trace_list = tr_bandit('8.8.8.8', req)
        print('tr_bandit')
        sum_num += len(trace_list)
        trace_list = tr_probe_networks_de('8.8.8.8', req)
        print('tr_probe_networks_de')
        sum_num += len(trace_list)
        trace_list = tr_opticfusion('8.8.8.8', req)
        print('tr_opticfusion')
        sum_num += len(trace_list)
        trace_list = tr_phoenix('8.8.8.8', req)
        print('tr_phoenix')
        sum_num += len(trace_list)
        trace_list = tr_gtc_su('8.8.8.8', req)
        print('tr_gtc_su')
        sum_num += len(trace_list)
        trace_list = tr_g8('8.8.8.8', req)
        print('tr_g8')
        sum_num += len(trace_list)
        trace_list = tr_sileman('8.8.8.8', req)
        print('tr_sileman')
        sum_num += len(trace_list)
        trace_list = tr_marwan('8.8.8.8', req)
        print('tr_marwan')
        sum_num += len(trace_list)
        trace_list = tr_sbb('8.8.8.8', req)
        print('tr_sbb')
        sum_num += len(trace_list)
        trace_list = tr_starnet_md('8.8.8.8', req)
        print('tr_starnet_md')
        sum_num += len(trace_list)
        trace_list = tr_nemox('8.8.8.8', req)
        print('tr_nemox')
        sum_num += len(trace_list)
        trace_list = tr_transtelco('8.8.8.8', req)
        print('tr_transtelco')
        sum_num += len(trace_list)
        trace_list = tr_liquidweb('8.8.8.8', req)
        print('tr_liquidweb')
        sum_num += len(trace_list)
        trace_list = tr_steadfast('8.8.8.8', req)
        print('tr_steadfast')
        sum_num += len(trace_list)
        trace_list = tr_core_backbone('8.8.8.8', req)
        print('tr_core_backbone')
        sum_num += len(trace_list)
        trace_list = tr_ntt_lt('8.8.8.8', req)
        print('tr_ntt_lt')
        sum_num += len(trace_list)
        trace_list = tr_cosmonova('8.8.8.8', req)
        print('tr_cosmonova')
        sum_num += len(trace_list)
        trace_list = tr_as35266('8.8.8.8', req)
        print('tr_as35266')
        sum_num += len(trace_list)

        trace_list = tr_as25369('8.8.8.8', req)
        print('tr_as25369')
        sum_num += len(trace_list)
        trace_list = tr_arpnetworks('8.8.8.8', req)
        print('tr_arpnetworks')
        sum_num += len(trace_list)
        trace_list = tr_active24('8.8.8.8', req)
        print('tr_active24')
        sum_num += len(trace_list)
        trace_list = tr_cyfra('8.8.8.8', req)
        print('tr_cyfra')
        sum_num += len(trace_list)
        trace_list = tr_alsysdata('8.8.8.8', req)
        print('tr_alsysdata')
        sum_num += len(trace_list)
        trace_list = tr_as25577('8.8.8.8', req)
        print('tr_as25577')
        sum_num += len(trace_list)
        trace_list = tr_as26320('8.8.8.8', req)
        print('tr_as26320')
        sum_num += len(trace_list)
        trace_list = tr_clearfly('8.8.8.8', req)
        print('tr_clearfly')
        sum_num += len(trace_list)
        trace_list = tr_towardex('8.8.8.8', req)
        print('tr_towardex')
        sum_num += len(trace_list)
        trace_list = tr_certto('8.8.8.8', req)
        print('tr_certto')
        sum_num += len(trace_list)
        trace_list = tr_ensite('8.8.8.8', req)
        print('tr_ensite')
        sum_num += len(trace_list)
        trace_list = tr_netbotanic('8.8.8.8', req)
        print('tr_netbotanic')
        sum_num += len(trace_list)
        trace_list = tr_contato('8.8.8.8', req)
        print('tr_contato')
        sum_num += len(trace_list)
        trace_list = tr_gtu('8.8.8.8', req)
        print('tr_gtu')
        sum_num += len(trace_list)
        trace_list = tr_netbotanic('8.8.8.8', req)
        print('tr_netbotanic')
        sum_num += len(trace_list)
        trace_list = tr_as29140('8.8.8.8', req)
        print('tr_as29140')
        sum_num += len(trace_list)
        trace_list = tr_mastertel('8.8.8.8', req)
        print('tr_mastertel')
        sum_num += len(trace_list)

        trace_list = tr_les('8.8.8.8', req)
        print('tr_les')
        sum_num += len(trace_list)
        trace_list = tr_isomedia('8.8.8.8', req)
        print('tr_isomedia')
        sum_num += len(trace_list)
        trace_list = tr_egihosting('8.8.8.8', req)
        print('tr_egihosting')
        sum_num += len(trace_list)
        trace_list = tr_as19531('8.8.8.8', req)
        print('tr_as19531')
        sum_num += len(trace_list)
        trace_list = tr_joesdatacenter('8.8.8.8', req)
        print('tr_joesdatacenter')
        sum_num += len(trace_list)
        trace_list = tr_buf1_as20278('8.8.8.8', req)
        print('tr_buf1_as20278')
        sum_num += len(trace_list)
        trace_list = tr_chi1_as20278('8.8.8.8', req)
        print('tr_chi1_as20278')
        sum_num += len(trace_list)
        trace_list = tr_speedtest('8.8.8.8', req)
        print('tr_speedtest')
        sum_num += len(trace_list)
        trace_list = tr_gitoyen('8.8.8.8', req)
        print('tr_gitoyen')
        sum_num += len(trace_list)
        trace_list = tr_core_heg('8.8.8.8', req)
        print('tr_core_heg')
        sum_num += len(trace_list)
        trace_list = tr_topnet('8.8.8.8', req)
        print('tr_topnet')
        sum_num += len(trace_list)
        trace_list = tr_itandtel('8.8.8.8', req)
        print('tr_itandtel')
        sum_num += len(trace_list)
        trace_list = tr_ggamaur('8.8.8.8', req)
        print('tr_ggamaur')
        sum_num += len(trace_list)
        trace_list = tr_tnib_de('8.8.8.8', req)
        print('tr_tnib_de')
        sum_num += len(trace_list)
        trace_list = tr_flex('8.8.8.8', req)
        print('tr_flex')
        sum_num += len(trace_list)
        trace_list = tr_channel_11('8.8.8.8', req)
        print('tr_channel_11')
        sum_num += len(trace_list)
        trace_list = tr_sg_gs('8.8.8.8', req)
        print('tr_sg_gs')
        sum_num += len(trace_list)
        trace_list = tr_virtutel('8.8.8.8', req)
        print('tr_virtutel')
        sum_num += len(trace_list)
        trace_list = tr_atman('8.8.8.8', req)
        print('tr_atman')
        sum_num += len(trace_list)
        trace_list = tr_kloth('8.8.8.8', req)
        print('tr_kloth')
        sum_num += len(trace_list)
        trace_list = tr_gaertner('8.8.8.8', req)
        print('tr_gaertner')
        sum_num += len(trace_list)
        trace_list = tr_as24961('8.8.8.8', req)
        print('tr_as24961')
        sum_num += len(trace_list)
        trace_list = tr_masterinter('8.8.8.8', req)
        print('tr_masterinter')
        sum_num += len(trace_list)

        trace_list = tr_internet2('2001:12d8::1', req)
        print('tr_internet2')
        sum_num += len(trace_list)
        trace_list = tr_globedom('8.8.8.8', req)
        print('tr_globedom')
        sum_num += len(trace_list)
        trace_list = tr_seeweb('8.8.8.8', req)
        print('tr_seeweb')
        sum_num += len(trace_list)
        trace_list = tr_as13030('8.8.8.8', req)
        print('tr_as13030')
        sum_num += len(trace_list)
        trace_list = tr_thunderworx('8.8.8.8', req)
        print('tr_thunderworx')
        sum_num += len(trace_list)
        trace_list = tr_fmc('8.8.8.8', req)
        print('tr_fmc')
        sum_num += len(trace_list)
        trace_list = tr_bite('8.8.8.8', req)
        print('tr_bite')
        sum_num += len(trace_list)
        trace_list = tr_as14442('8.8.8.8', req)
        print('tr_as14442')
        sum_num += len(trace_list)
        trace_list = tr_customers_datapipe('8.8.8.8', req)
        print('tr_customers_datapipe')
        sum_num += len(trace_list)
        trace_list = tr_rhnet('8.8.8.8', req)
        print('tr_rhnet')
        sum_num += len(trace_list)
        trace_list = tr_rhnet_2('8.8.8.8', req)
        print('tr_rhnet_2')
        sum_num += len(trace_list)
        trace_list = tr_ix('8.8.8.8', req)
        print('tr_ix')
        sum_num += len(trace_list)
        trace_list = tr_bix('8.8.8.8', req)
        print('tr_bix')
        sum_num += len(trace_list)
        trace_list = tr_telecoms_bg('8.8.8.8', req)
        print('tr_telecoms_bg')
        sum_num += len(trace_list)
        trace_list = tr_ovh('8.8.8.8', req)
        print('tr_ovh')
        sum_num += len(trace_list)
        trace_list = tr_mbix('8.8.8.8', req)
        print('tr_mbix')
        sum_num += len(trace_list)
        trace_list = tr_alog('166.211.2.1', req)
        print('tr_alog')
        sum_num += len(trace_list)
        trace_list = tr_opt_nc('8.8.8.8', req)
        print('tr_opt_nc')
        sum_num += len(trace_list)

        trace_list = tr_zettagrid('8.8.8.8', req)
        print('tr_zettagrid')
        sum_num += len(trace_list)
        trace_list = tr_as8218('8.8.8.8', req)
        print('tr_as8218')
        sum_num += len(trace_list)
        trace_list = tr_atom86('8.8.8.8', req) 
        print('tr_atom86')
        sum_num += len(trace_list)#readtimeout，但是网页连接可以*****************
        trace_list = tr_evolink('8.8.8.8', req)
        print('tr_evolink')
        sum_num += len(trace_list)
        trace_list = tr_macomnet('8.8.8.8', req)
        print('tr_macomnet')
        sum_num += len(trace_list)
        trace_list = tr_kamp('8.8.8.8', req)
        print('tr_kamp')
        sum_num += len(trace_list)
        trace_list = tr_cprm('8.8.8.8', req)
        print('tr_cprm')
        sum_num += len(trace_list)
        trace_list = tr_zyx('8.8.8.8', req)
        print('tr_zyx')
        sum_num += len(trace_list)
        trace_list = tr_sdv('8.8.8.8', req)
        print('tr_sdv')
        sum_num += len(trace_list)
        trace_list = tr_ucom('8.8.8.8', req)
        print('tr_ucom')
        sum_num += len(trace_list)
        trace_list = tr_webpartner('8.8.8.8', req)
        print('tr_webpartner')
        sum_num += len(trace_list)
        trace_list = tr_as9371('8.8.8.8', req)
        print('tr_as9371')
        sum_num += len(trace_list)
        trace_list = tr_bbtower('8.8.8.8', req)
        print('tr_bbtower')
        sum_num += len(trace_list)
        trace_list = tr_hafey('8.8.8.8', req)
        print('tr_hafey')
        sum_num += len(trace_list)
        trace_list = tr_uecomm('8.8.8.8', req)
        print('tr_uecomm')
        sum_num += len(trace_list)
        trace_list = tr_bluemoon('8.8.8.8', req)
        print('tr_bluemoon')
        sum_num += len(trace_list)
        trace_list = tr_as10929('8.8.8.8', req)
        print('tr_as10929')
        sum_num += len(trace_list)
        trace_list = tr_davespeed('8.8.8.8', req)
        print('tr_davespeed')
        sum_num += len(trace_list)
        trace_list = tr_unlimitednet('8.8.8.8', req)
        print('tr_unlimitednet')
        sum_num += len(trace_list)

        trace_list = tr_as59('8.8.8.8', req)
        print('tr_as59')
        sum_num += len(trace_list)
        trace_list = tr_priceton('8.8.8.8', req)
        print('tr_priceton')
        sum_num += len(trace_list)
        trace_list = tr_garr('8.8.8.8', req)
        print('tr_garr')
        sum_num += len(trace_list)
        trace_list = tr_belwue('8.8.8.8', req)
        print('tr_belwue')
        sum_num += len(trace_list)
        trace_list = tr_switch_ch('8.8.8.8', req)
        print('tr_switch_ch')
        sum_num += len(trace_list)
        trace_list = tr_dfn('8.8.8.8', req)
        print('tr_dfn')
        sum_num += len(trace_list)
        trace_list = tr_rediris('8.8.8.8', req)
        print('tr_rediris')
        sum_num += len(trace_list)
        trace_list = tr_telus('8.8.8.8', req)
        print('tr_telus')
        sum_num += len(trace_list)
        trace_list = tr_telstra('8.8.8.8', req)
        print('tr_telstra')
        sum_num += len(trace_list)
        trace_list = tr_han_de('8.8.8.8', req)
        print('tr_han_de')
        sum_num += len(trace_list)
        trace_list = tr_as1403('8.8.8.8', req)
        print('tr_as1403')
        sum_num += len(trace_list)
        trace_list = tr_sunset_se('8.8.8.8', req)
        print('tr_sunset_se')
        sum_num += len(trace_list)
        trace_list = tr_rnp_br('8.8.8.8', req)
        print('tr_rnp_br')
        sum_num += len(trace_list)
        trace_list = tr_telenor_se('8.8.8.8', req)
        print('tr_telenor_se')
        sum_num += len(trace_list)
        trace_list = tr_wiscnet('8.8.8.8', req)
        print('tr_wiscnet')
        sum_num += len(trace_list)
        trace_list = tr_nordu('8.8.8.8', req)
        print('tr_nordu')
        sum_num += len(trace_list)
        trace_list = tr_tpg_appt('8.8.8.8', req)
        print('tr_tpg_appt')
        sum_num += len(trace_list)
        trace_list = tr_cesnet('8.8.8.8', req)
        print('tr_cesnet')
        sum_num += len(trace_list)
        trace_list = tr_beeline('8.8.8.8', req)
        print('tr_beeline')
        sum_num += len(trace_list)
        trace_list = tr_uar('8.8.8.8', req)
        print('tr_uar')
        sum_num += len(trace_list)
        trace_list = tr_as3326('8.8.8.8', req)
        print('tr_as3326')
        sum_num += len(trace_list)
        trace_list = tr_centurylink('8.8.8.8', req)
        print('tr_centurylink')
        sum_num += len(trace_list)
        trace_list = tr_globalcrossing('8.8.8.8', req) #******************
        print('tr_globalcrossing')
        sum_num += len(trace_list)
        trace_list = tr_slac_stanford('8.8.8.8', req)
        print('tr_slac_stanford')
        sum_num += len(trace_list)
        trace_list = tr_iinet('8.8.8.8', req)
        print('tr_iinet')
        sum_num += len(trace_list)
        trace_list = tr_idola('8.8.8.8', req)
        print('tr_idola')
        sum_num += len(trace_list)
        trace_list = tr_vocus('8.8.8.8', req)
        print('tr_vocus')
        sum_num += len(trace_list)
        trace_list = tr_rr('8.8.8.8', req)
        print('tr_rr')
        sum_num += len(trace_list)
        trace_list = tr_grnet('8.8.8.8', req)
        print('tr_grnet')
        sum_num += len(trace_list)
        trace_list = tr_olsson('8.8.8.8', req)
        print('tr_olsson')
        sum_num += len(trace_list)
        trace_list = tr_rcn('8.8.8.8', req)
        print('tr_rcn')
        sum_num += len(trace_list)
        trace_list = tr_fifi('8.8.8.8', req)
        print('tr_fifi')
        sum_num += len(trace_list)
        trace_list = tr_he('8.8.8.8', req)
        print('tr_he')
        sum_num += len(trace_list)
        trace_list = tr_aarnet('8.8.8.8', req)
        print('tr_aarnet')
        sum_num += len(trace_list)
        # for trace in trace_list:
        #     print(trace)
        print(sum_num)
