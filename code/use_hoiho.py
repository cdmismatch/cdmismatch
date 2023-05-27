
import re
import pandas as pd
import json
import os

def annotate_asn(node_filename, regex_filename, out_filename):
    regex_info = {}
    with open(regex_filename, 'r') as rf:
        for line in rf:
            data = line.split(',')[0]
            (suffix, regex) = data.split(': ')
            regex_info[suffix] = regex
    node_info = {}
    total = 0
    mismatch = 0
    addrs = []
    tasns = []
    with open(node_filename, 'r') as rf:
        cur_asn = None
        for line in rf:
            if line.__contains__('node2as'):
                cur_asn = line.strip('\n').split(': ')[1]
            elif line.__contains__('.'):
                elems = line.strip('\n').strip(' ').split(' ')
                if len(elems) == 2:
                    (addr, host_name) = elems
                    match_suffix = {x for x in regex_info.keys() if host_name.endswith('.' + x)}
                    if match_suffix:
                        if len(match_suffix) > 1:
                            print('note: addr:{}, match_suffix:{}'.format(addr, match_suffix))
                        else:
                            find_res = re.findall(regex_info[list(match_suffix)[0]], host_name)
                            if find_res:
                                node_info[addr] = [cur_asn, find_res[0]]
                                addrs.append(addr)
                                tasns.append(find_res[0])
                                if cur_asn != find_res[0]:
                                    #print('addr:{}, ori_asn:{}, extracted_asn:{}'.format(addr, cur_asn, find_res[0]))
                                    mismatch += 1
                                total += 1
    dataframe = pd.DataFrame({'addr':addrs,'tasn':tasns})
    dataframe.to_csv(out_filename,index=False,sep=',')
    print(mismatch)
    print(total)

def get_netnames_asns(filename, out_filename):
    data = {}
    addrs = []
    tasns = []
    total = 0
    mismatch = 0
    with open(filename, 'r') as rf:
        for line in rf:
            info = json.loads(line.strip('\n'))
            if 'routers' in info.keys():
                for router in info['routers']:
                    if 'asn' in router.keys():
                        asn = router['asn']
                    if 'ifaces' in router.keys():
                        for inface in router['ifaces']:
                            if 'addr' in inface.keys():
                                addr = inface['addr']
                                
                                # iasn = inface['name2asn'] if 'name2asn' in inface.keys() else asn
                                # if iasn != asn:
                                #     mismatch += 1
                                # data[addr] = iasn
                                # addrs.append(addr)
                                # tasns.append(iasn)
                                # total += 1
                                
                                if 'name2asn' in inface.keys():
                                    iasn = inface['name2asn']
                                    data[addr] = iasn
                                    addrs.append(addr)
                                    tasns.append(iasn)
                                    total += 1
    dataframe = pd.DataFrame({'addr':addrs,'tasn':tasns})
    dataframe.to_csv(out_filename,index=False,sep=',')
    print(mismatch)
    print(total)

def search_domain_name():
    hoiho_domains = {}
    with open('/mountdisk2/collector_vps/202008-midar-iff.geo-re.json', 'r') as rf:
        for line in rf:
            elem = json.loads(line)
            hoiho_domains[elem['domain']] = elem['re']
    bgp_info = {}
    with open('/mountdisk2/collector_vps/bgp_asn_ips_nds.json') as rf:
        bgp_info = json.load(rf)
    res = {}
    cnt = 0
    for asn, val in bgp_info.items():
        for ip, hostname in val.items():
            for temp in hoiho_domains.keys():
                if temp in hostname:
                    res[asn] = [temp, hoiho_domains[temp]]
                    cnt += len(val)
            break
    with open('/mountdisk2/collector_vps/bgp_asn_hoiho_info.json', 'w') as wf:
        json.dump(res, wf, indent=1)
    print(len(res))
    print(cnt)

def find_common_vps_test1():
    atlas_asn_info = {}
    with open('/mountdisk2/atlas/builtin_asn.json', 'r') as rf:
        atlas_asn_info = json.load(rf)
    bgp_asn_info = {}
    with open('/mountdisk2/collector_vps/bgp_asn_hoiho_info.json', 'r') as rf:
        bgp_asn_info = json.load(rf)
    print(len(set(atlas_asn_info.keys()) & set(bgp_asn_info.keys())))
    
if __name__ == '__main__':
    if True:
        find_common_vps_test1()
        #search_domain_name()
    else:
        os.chdir('/mountdisk1/ana_c_d_incongruity/hoiho/')
        # routers_filenames = ['201803-midar-iff.routers', '201901-midar-iff.routers', '201904-midar-iff.routers', '202001-midar-iff.routers', '20200215-peeringdb.routers']
        # re_filenames = ['201803-node2as.re', '201901-node2as.re', '201904-node2as.re', '202001-node2as.re', '20200215-peeringdb.re']
        # for (routers_filename, re_filename) in zip(routers_filenames, re_filenames):
        #     date = routers_filename.split('-')[0]
        #     annotate_asn(routers_filename, re_filename, 'out/' + date + '_asn.csv')
        
        #json_filenames = ['201803-midar-iff-asnames.json', '201901-midar-iff-asnames.json', '201904-midar-iff-asnames.json', '202001-midar-iff-asnames.json', '202008-midar-iff-asnames.json']
        json_filenames = ['202103-midar-iff-asnames.json']
        for json_filename in json_filenames:
            get_netnames_asns(json_filename, 'out/' + json_filename.split('-')[0] + '.netnames_small.csv')
