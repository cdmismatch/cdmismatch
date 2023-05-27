
import json
from compare_cd import InitPref2ASInfo, GetBGPPath_Or_OriASN
from traceutils.bgp.bgp import BGP
from traceutils.ixps import PeeringDB
from rect_bdrmapit import CheckSiblings
from get_ip2as_from_bdrmapit import ConnectToBdrMapItDb, GetIp2ASFromBdrMapItDb, CloseBdrMapItDb, InitBdrCache, \
                                    ConstrBdrCache, GetBdrCache
from get_mapping_status import CompressTrace, GetIPAttr
from collections import Counter, defaultdict
from dataclasses import dataclass
from utils_v2 import GetIxpAsSet, IsIxpAs, GetIxpPfxDict_2, IsIxpIp
import os
import sys
from multiprocessing import Process, Pool
sys.path.append('/home/slt/code/ML_Bdrmaplt/')
from xgboost_pred import get_pred_result_per_attr_v3, get_pred_result_per_attr_v3_old, specificity_score
import glob
import xgboost as xgb
import numpy

g_parell_num = os.cpu_count()

considerscores = False

class CollectCandidates():
    PEER_BOTTOM_PERCNT = 0.8
    WITH_APPEARANCE_WEIGHT = True
    #STRICT_VALLEY_FREE = True
    TIEBREAK_CC_SIZE = 1 #pick the biggest first
    # IsValleyFree_Loose = {'11': True, '12': False, '13': True, '14': True, '15': True, '21': True, '22': True, '23': True, '24': True, '25': True, '31': True, '32': True, '33': True, '34': True, '35': True, '41': True, '42': True, '43': True, '44': False, '45': True, '51': True, '52': True, '53': True, '54': True, '55': True, '1': True, '2': True, '3': True, '4': True, '5': True, '': True}
    # IsValleyFree_Strict = {'11': True, '12': False, '13': False, '14': False, '15': True, '21': True, '22': True, '23': True, '24': False, '25': True, '31': True, '32': False, '33': False, '34': False, '35': True, '41': False, '42': False, '43': False, '44': False, '45': False, '51': True, '52': True, '53': True, '54': True, '55': True, '1': True, '2': True, '3': True, '4': False, '5': True, '': True}
    score_threshold = 0.3
        
    def __init__(self, date, vp, rib_mappings=None, checksiblings=None, peeringdb=None):
        if not rib_mappings:
            rib_mappings = {}
            InitPref2ASInfo('/mountdisk1/ana_c_d_incongruity/out_ip2as_data/%s.ip2as.prefixes' %date, rib_mappings)
        self.rib_mappings = rib_mappings
        self.ips_trip_AS = {} 
        if vp:   
            with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/ipattr_trip_AS_%s_%s.json' %(vp, vp, date), 'r') as rf:
                self.ips_trip_AS = json.load(rf)
        else:
            with open('/mountdisk2/common_vps/%s/atlas/ipattr_trip_AS_%s.json' %(date, date), 'r') as rf:
                self.ips_trip_AS = json.load(rf)
        #bgp_db = BGP('/mountdisk1/ana_c_d_incongruity/as_rel_cc_data/' + date[:6] + '01.as-rel3.txt', asrel_dir + use_date[:6] + '01.ppdc-ases.txt')
        if not checksiblings:
            checksiblings = CheckSiblings(date)
        self.checksiblings = checksiblings
        if not peeringdb:
            peeringdb = PeeringDB('/mountdisk1/ana_c_d_incongruity/peeringdb_data/peeringdb_%s_%s_%s.json' %(date[:4], date[4:6], date[6:8]))
        self.peeringdb = peeringdb
        self.ixp_ip_asn = defaultdict(set)
        for addr, asn in self.peeringdb.addrs.items():
            self.ixp_ip_asn[addr] = asn
        #self.valleystate = {'11':'normal','12':'abnormal','13':'semi','14':'semi','15':'normal','21':'normal','22':'normal','23':'normal','24':'semi','25':'normal','31':'normal','32':'semi','33':'semi','34':'semi','35':'normal','41':'semi','42':'semi','43':'semi','44':'abnormal','45':'semi','51':'normal','52':'normal','53':'normal','54':'semi','55':'normal','1':'normal','2':'normal','3':'normal','4':'semi','5':'normal','':'normal'} #这里改成三态的，配合模型参数
        self.valleystate = {'11':'normal','12':'normal','13':'normal','14':'semi','15':'normal','21':'abnormal','22':'normal','23':'abnormal','24':'semi','25':'normal','31':'abnormal','32':'normal','33':'semi','34':'semi','35':'normal','41':'semi','42':'semi','43':'semi','44':'abnormal','45':'semi','51':'normal','52':'normal','53':'normal','54':'semi','55':'normal','1':'normal','2':'normal','3':'normal','4':'semi','5':'normal','':'normal'} #这里改成三态的，配合模型参数

    def IsLegalASN(self, asn):
        return asn.isdigit() and int(asn) < 0xFFFFFF and int(asn) > 0

    def GetRel(self, asn1, asn2, asn1_rel_pointer=None, fst_asn=True):
        #if self.checksiblings.check_sibling(asn1, asn2):
        if asn1 == asn2:
            return 5
        else:
            # a = self.checksiblings.bgp_v2.get(asn1)
            # if a:
            #     b = a.get(asn2)
            #     if b:
            #         return b
            # return 4
            if not asn1_rel_pointer:
                tmp_type = self.checksiblings.bgp.reltype(asn1, asn2)
                if tmp_type == 1: return 2
                if tmp_type == 2: return 1
                return tmp_type
            # providers, customers, peers, _ = asn1_rel_pointer
            # if asn2 in providers:
            #     if fst_asn: return 1
            #     else: return 2
            # if asn2 in customers:
            #     if fst_asn: return 2
            #     else: return 1
            # if asn2 in peers:
            #     return 3
            # return 4
    
    def GetRemoteRel(self, asn1, asn2, asn1_rel_pointer=None, fst_asn=True):
        #if self.checksiblings.check_sibling(asn1, asn2):
        if asn1 == asn2:
            return 5
        else:
            direct_rel = None
            if not asn1_rel_pointer:
                direct_rel = self.GetRel(asn1, asn2)
                if direct_rel != 4:
                    return direct_rel
                if asn2 in self.checksiblings.bgp.cone[asn1]:
                    if fst_asn: return 2
                    else: return 1
                if asn1 in self.checksiblings.bgp.cone[asn2]:
                    if fst_asn: return 1
                    else: return 2
                return 4
            # else:
            #     direct_rel = self.GetRel(asn1, asn2, asn1_rel_pointer, fst_asn)
            #     if direct_rel != 4:
            #         return direct_rel
            #     _, __, ___, cones = asn1_rel_pointer
            #     if asn2 in cones:
            #         if fst_asn: return 2
            #         else: return 1
            #     if asn1 in self.checksiblings.bgp.cone[asn2]:
            #         if fst_asn: return 1
            #         else: return 2
            #     return 4
        
    def GetRel_v2(self, rel_pointer, asn1, asn2, reverse_flag):
        #if self.checksiblings.check_sibling(asn1, asn2):
        if asn1 == asn2:
            return 5
        elif rel_pointer:
            b = rel_pointer.get(asn2)
            if b:
                if reverse_flag:
                    if b == 1: b = 2
                    elif b == 2: b = 1
                return b
        return 4
    
    # def CheckPeerBothEnds(self, trip_AS, node):
    #     peer_stat = {}
    #     for trip, c in trip_AS.items():
    #         prev, succ = trip.strip('*').split(',')
    #         if self.IsLegalASN(prev):
    #             prev = int(prev)
    #             prev_rel = self.GetRel(prev, node)
    #             if prev_rel != 4:
    #                 if self.IsLegalASN(succ):
    #                     succ = int(succ)
    #                     succ_rel = self.GetRel(succ, node)
    #                     if succ_rel != 4:
    #                         peer_stat[trip + '|' + str(prev_rel) + ',' + str(succ_rel)] = c
    #                 else:
    #                     peer_stat[trip + '|' + str(prev_rel) + ',*'] = c
    #         else:
    #             if self.IsLegalASN(succ):
    #                 succ = int(succ)
    #                 succ_rel = self.GetRel(succ, node)
    #                 if succ_rel != 4:
    #                     peer_stat[trip + '|' + '*,' + str(succ_rel)] = c
    #             else:
    #                 peer_stat[trip + '|' + '*,*'] = c
    #     #s = sorted(trip_AS.items(), key=lambda x:x[1], reverse=True)
    #     if len(peer_stat) >= len(trip_AS) * self.PEER_BOTTOM_PERCNT:
    #         if not self.WITH_APPEARANCE_WEIGHT or sum(peer_stat.values()) >= sum(trip_AS.values()) * self.PEER_BOTTOM_PERCNT:
    #             return peer_stat
    #     return {}

    # def CheckValleyFree(self, asn1, asn2, asn3):
    #     if not self.IsLegalASN(asn2):
    #         return False
    #     prev_rel = self.GetRel(int(asn1), int(asn2)) if self.IsLegalASN(asn1) else ''
    #     succ_rel = self.GetRel(int(asn2), int(asn3)) if self.IsLegalASN(asn3) else ''
    #     IsValleyFree = self.IsValleyFree_Strict if self.STRICT_VALLEY_FREE else self.IsValleyFree_Loose
    #     return IsValleyFree[prev_rel+succ_rel] 

    # def CheckValleyFreeNum(self, peer_stat):
    #     valleyfree_c = {}
    #     IsValleyFree = self.IsValleyFree_Strict if self.STRICT_VALLEY_FREE else self.IsValleyFree_Loose
    #     for key, c in peer_stat.items():
    #         prev_rel, succ_rel = key.split('|')[-1].strip('*').split(',')
    #         if IsValleyFree[prev_rel+succ_rel]:
    #             valleyfree_c[key] = c
    #     #return (len(valleyfree_c), sum(valleyfree_c.values()))
    #     return len(valleyfree_c) #暂时不考虑triple所在的trace数量

    # def FilterAndSortCands(self, ip, nodes):
    #     trip_AS = self.ips_trip_AS[ip]
    #     filtered = {}
    #     for node in nodes:
    #         peer_stat = self.CheckPeerBothEnds(trip_AS, node)
    #         if peer_stat:
    #             filtered[node] = self.CheckValleyFreeNum(peer_stat)
    #     s = sorted(filtered.keys(), key=lambda x:(filtered[x], self.TIEBREAK_CC_SIZE * len(self.checksiblings.bgp.cone[x]), -1 * x), reverse=True)
    #     return s
    
    def GetRibCands(self, ip, bdr_res, prev_cands):
        set_prev_cands = set(prev_cands)
        rib_res = GetBGPPath_Or_OriASN(self.rib_mappings, ip, 'get_all_2')
        #diff_res = {int(elem) for elem in rib_res[1].split('_') if self.IsLegalASN(elem) and int(elem) != int(bdr_res)}
        diff_res = {int(elem) for elem in rib_res[1].split('_') if self.IsLegalASN(elem)}
        if diff_res:
            diff_res = diff_res.difference(set_prev_cands)
            return sorted(diff_res, key=lambda x:(len(self.checksiblings.bgp.cone[x]), x))
            #return list(diff_res)
        else:
            return []
    
    def GetSiblingCands(self, ip, bdr_res):
        siblings = self.checksiblings.get_all_siblings(bdr_res)
        if siblings:
            return {str(elem) for elem in siblings}
            #return self.FilterAndSortCands(ip, siblings)
        else:
            return set()
        
    def GetNeighborMidCands(self, checked_cands, s_tmp_trip_asn_counter):
        tmp_cands = Counter()
        for elem in s_tmp_trip_asn_counter:
            trip, c = elem
            prev_asn, succ_asn = trip
            prev_asn = prev_asn.strip('*')
            succ_asn = succ_asn.strip('$').strip('*')
            if not self.IsLegalASN(prev_asn) or not self.IsLegalASN(succ_asn):
                continue
            prev_asn = int(prev_asn)
            succ_asn = int(succ_asn)
            if self.GetRel(prev_asn, succ_asn) != 4:
                return set()
            prev_providers = self.checksiblings.bgp.providers[prev_asn]
            prev_customers = self.checksiblings.bgp.customers[prev_asn]
            prev_peers = self.checksiblings.bgp.peers[prev_asn]
            succ_providers = self.checksiblings.bgp.providers[succ_asn]
            succ_customers = self.checksiblings.bgp.customers[succ_asn]
            succ_peers = self.checksiblings.bgp.peers[succ_asn]
            t1 = prev_providers & succ_providers
            t2 = prev_providers & succ_customers
            t3 = prev_providers & succ_peers
            t4 = prev_peers & succ_peers
            t5 = prev_peers & succ_providers
            t6 = prev_customers & succ_providers
            t7 = t1 | t2
            t8 = t3 | t4
            t9 = t5 | t6
            t10 = t7 | t8
            t11 = {str(elem) for elem in t10 | t9}
            for c in t11.difference(checked_cands):
                tmp_cands[c] += 1
        cands = sorted(tmp_cands.keys(), key=lambda x:(tmp_cands[x], -1*int(x)), reverse=True) if tmp_cands else []
        final_cands = []
        total_w = sum([elem[1] for elem in s_tmp_trip_asn_counter])
        for cand in cands:
            if not cand or not self.IsLegalASN(cand):
                continue
            valley = Counter()
            to_filter = False
            for trip, w in s_tmp_trip_asn_counter:
                prev, succ = trip
                prev_rel = ''
                if self.IsLegalASN(prev.strip('*')):
                    if prev[-1] != '*': prev_rel = str(self.GetRel(int(prev), int(cand)))
                    else: prev_rel = str(self.GetRemoteRel(int(prev[:-1]), int(cand)))
                succ_rel = ''
                if self.IsLegalASN(succ.strip('*')):
                    if succ[-1] != '*': succ_rel = str(self.GetRel(int(cand), int(succ)))
                    else: succ_rel = str(self.GetRemoteRel(int(cand), int(succ[:-1])))
                valley[self.valleystate[prev_rel+succ_rel]] += w#.add(trip) #IP级别
                if valley.get('abnormal', 0) / total_w > 0.5:
                    to_filter = True
                    break
            if not to_filter:
                final_cands.append(cand)
        #s = sorted(final_cands.keys(), key=lambda x:final_cands[x], reverse=True)
        return final_cands
        
            
    def GetNeighborCands(self, ip):
        cur_cands = set()
        trips = set(self.ips_trip_AS[ip].values())
        for trip in trips: #trip_AS[ips[j]][trace_idx] = ips[j-1] + ',' + succ_ip + '|' + prev_cpr_hop + ',' + succ_cpr_hop
            _, asn_val = trip.split('|')
            prev, succ = asn_val.strip('$').strip('*').split(',')
            if self.IsLegalASN(prev):
                # prev = int(prev)
                # if self.IsLegalASN(succ):
                #     succ = int(succ)
                #     rel = self.GetRel(prev, succ)
                #     if rel != 4:
                #         if prev not in set_prev_cands: neighs[prev] += c
                #         if succ not in set_prev_cands: neighs[succ] += c
                # else:
                #     if prev not in set_prev_cands: neighs[prev] += c
                #cands[prev] += 1
                cur_cands.add(prev)
            #elif self.IsLegalASN(succ):
                # succ = int(succ)
                # if succ not in set_prev_cands: neighs[succ] += c
            if self.IsLegalASN(succ):
                #cands[succ] += 1
                cur_cands.add(succ)
        return cur_cands
                    
    def GetPeerCands(self, ip, bdr_res):
        customers = self.checksiblings.bgp.customers[bdr_res]
        providers = self.checksiblings.bgp.providers[bdr_res]
        peers = self.checksiblings.bgp.peers[bdr_res]
        all_peers = customers | providers
        #all_peers = all_peers | peers
        if all_peers:
            return {str(elem) for elem in all_peers}
            #return self.FilterAndSortCands(ip, all_peers)
        else:
            return set()
        
    def GetCandsForIP(self, ip, cur_map, rib_cands, get_res_attrs, s_tmp_trip_asn_counter):
        # cands = self.GetRibCands(ip, cur_map, prev_cands)
        # rib_res = [elem for elem in cands]
        # prev_cands = prev_cands + cands
        cands_c = Counter()
        for c in rib_cands:
            cands_c[c] = 1
        neigh_cands = self.GetNeighborCands(ip)
        for c in neigh_cands:
            cands_c[c] += 1
        other_cands = set()
        bdrmap = get_res_attrs.cache.get(ip)
        if bdrmap and self.IsLegalASN(bdrmap):
            other_cands = self.GetPeerCands(ip, int(bdrmap)) | self.GetSiblingCands(ip, int(bdrmap))
            for c in other_cands:
                cands_c[c] += 1
        rib_cands = [elem for elem in rib_cands if elem and self.IsLegalASN(elem)]
        s1 = sorted(rib_cands, key=lambda x:(cands_c[x], -1*int(x)), reverse=True) if rib_cands else []
        s2 = sorted(neigh_cands.difference(rib_cands), key=lambda x:(cands_c[x], -1*int(x)), reverse=True) if neigh_cands else []
        s3 = sorted(other_cands.difference(neigh_cands | set(rib_cands)), key=lambda x:(cands_c[x], -1*int(x)), reverse=True) if other_cands else []
        cands = s1 + s2
        cands = cands + s3
        final_cands = []
        total_w = sum([elem[1] for elem in s_tmp_trip_asn_counter])
        for cand in cands:
            # if cand in prev_cands:
            #     continue
            if not cand or not self.IsLegalASN(cand):
                continue
            valley = Counter()
            to_filter = False
            for trip, w in s_tmp_trip_asn_counter:
                prev, succ = trip
                prev_rel = ''
                if self.IsLegalASN(prev.strip('*')):
                    if prev[-1] != '*': prev_rel = str(self.GetRel(int(prev), int(cand)))
                    else: prev_rel = str(self.GetRemoteRel(int(prev[:-1]), int(cand)))
                succ_rel = ''
                if self.IsLegalASN(succ.strip('*')):
                    if succ[-1] != '*': succ_rel = str(self.GetRel(int(cand), int(succ)))
                    else: succ_rel = str(self.GetRemoteRel(int(cand), int(succ[:-1])))
                valley[self.valleystate[prev_rel+succ_rel]] += w#.add(trip) #IP级别
                if valley.get('abnormal', 0) / total_w > 0.5:
                    to_filter = True
                    break
            if not to_filter:
                final_cands.append(cand)
        #s = sorted(final_cands.keys(), key=lambda x:final_cands[x], reverse=True)
        return (cands, final_cands)
    
    def CheckCands(self, get_res_attrs, ip, cands, rib_res, trip_asn_counter, prev_asn_stat, succ_counter, ipattr_names, last_step=False, debug=False):
        prbs = {}
        attrs = {}
        # bdrmap = get_res_attrs.cache.get(ip, '')
        # cur_cands = [bdrmap] + cands if bdrmap else cands
        # if last_step:
        #     if bdrmap:
        #         cur_cands = [bdrmap] + [str(elem) for elem in rib_res]
        #         cur_cands = cur_cands + cands
        #     else:
        #         cur_cands = [str(elem) for elem in rib_res] + cands
        cur_cands = cands
        tmp_attrs = []
        tmp_cands_0 = []
        for cand in cur_cands:
            attr = None
            if considerscores:
                attr = get_res_attrs.GetIPAttr_ForACand_ConsiderScores(ip, cand, rib_res, trip_asn_counter, prev_asn_stat, succ_counter)
            else:
                attr = get_res_attrs.GetIPAttr_ForACand(ip, cand, rib_res, trip_asn_counter, prev_asn_stat, succ_counter)
            if debug:
                print(cand)
                print(attr)
            if attr:
                tmp_attrs.append(attr)
                tmp_cands_0.append(cand)
                attrs[cand] = attr
        if tmp_attrs:
            tmp_prbs = get_pred_result_per_attr_v3(tmp_attrs, get_res_attrs.model, ipattr_names)
            for i in range(len(tmp_prbs)):
                prbs[tmp_cands_0[i]] = tmp_prbs[i]
        if prbs:
            #s = max(prbs.keys(), key=lambda x:prbs[x])
            max_prb = max(prbs.values())
            #if max_prb > 0.5:
            if True:
                # max_cands = [cand for cand in prbs if prbs[cand] == max_prb]
                # fst_idx = min([cands.index(cand) for cand in max_cands])
                # return [cands[fst_idx], max_prb]
                tmp_cands = {cand for cand in prbs if max_prb - prbs[cand] < g_max_thresh}
                tmp_cands = tmp_cands | {cand for cand in prbs if prbs[cand] > 0.5}
                max_cands = set()
                for cand in tmp_cands:
                    if all(attrs[cand].prev_asnrel_unknown_rate <= attrs[other].prev_asnrel_unknown_rate and \
                    attrs[cand].succ_asnrel_unknown_rate <= attrs[other].succ_asnrel_unknown_rate and \
                    (attrs[cand].prev_asnrel_unknown_rate + attrs[cand].succ_asnrel_unknown_rate) <= (attrs[other].prev_asnrel_unknown_rate + attrs[other].succ_asnrel_unknown_rate) and \
                    attrs[cand].valley_abnormal_rate <= attrs[other].valley_abnormal_rate for other in tmp_cands if other != cand):
                        max_cands.add(cand)
                if max_cands:
                    s = sorted(max_cands, key=lambda x:(prbs[x], cur_cands.index(x) * -1), reverse=True)
                    return [s[0], prbs[s[0]]]
                else:
                    max_cands = [cand for cand in prbs if prbs[cand] == max_prb]
                    fst_idx = min([cur_cands.index(cand) for cand in max_cands])
                    return [cur_cands[fst_idx], max_prb]
        return [None, None]

@dataclass
class ipattr():
    # is_moas = False
    # prev_ixp_rate = 0.0
    # succ_ixp_rate = 0.0
    # prev_ixp_rate_trace_weight = 0.0
    # succ_ixp_rate_trace_weight = 0.0
    # prev_ip_num = 0
    # succ_ip_num = 0
    # prev_asn_num = 0
    # succ_asn_num = 0
    # prev_asnrel_unknown_rate_trace_weight = 0.0
    # succ_asnrel_unknown_rate_trace_weight = 0.0
    # # prev_sameAS_rate_trace_weight = 0.0
    # # succ_sameAS_rate_trace_weight = 0.0
    # valley_normal_rate_trace_weight = 0.0
    # valley_abnormal_rate_trace_weight = 0.0
    # valley_seminormal_rate_trace_weight = 0.0
    
    is_ixp = False
    
    bdr_rib_rel = 0
    valley_normal_rate = 0.0
    valley_abnormal_rate = 0.0
    valley_seminormal_rate = 0.0
    prev_asnrel_unknown_rate = 0.0
    succ_asnrel_unknown_rate = 0.0
    prev_asn_uncertain_rate = 0.0
    succ_asn_uncertain_rate = 0.0
    prev_sameAS_rate = 0.0
    succ_sameAS_rate = 0.0
    prev_ip_uncertain_rate = 0.0
    succ_ip_uncertain_rate = 0.0
    #prev_succ_norel_rate = 0.0
    def __init__(self):
        pass
    def __str__(self) -> str:
        a = ''
        for elem in dir(ipattr):
            if not elem.startswith('_'):
                a = a + '%s:%s,' %(elem, getattr(self, elem))
        return a

def GetAttrScores(vp, date):
    map_score = {}
    if not os.path.exists('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/ipattr_score_%s_%s.json' %(vp, vp, date)):
        # ips = None
        # with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ipattr_trip_AS_%s_%s.json' %(vp, vp, date), 'r') as rf:
        #     data = json.load(rf) #只用了data.keys()
        #     ips = data.keys()
        # for ip in ips:
        #     map_score[ip] = 1
        # with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ipattr_score_%s_%s.json' %(vp, vp, date), 'w') as wf:
        #     json.dump(map_score, wf, indent=1)
        return map_score
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/ipattr_score_%s_%s.json' %(vp, vp, date), 'r') as rf:
        map_score = json.load(rf)
        return map_score
    
def ConstrAttrScores(date, vp, rib_mappings, checksiblings, peeringdb, model=None):
    print('begin construct attr scores in %s' %vp)    
    # 加载模型
    if model:
        model = xgb.XGBClassifier()
        model.load_model('/home/slt/code/ML_Bdrmaplt/xgboost_model_v0.model')
    ips = None
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/ipattr_trip_AS_%s_%s.json' %(vp, vp, date), 'r') as rf:
        data = json.load(rf)
        ips = data.keys()
    collect_cands = CollectCandidates(date, vp, rib_mappings, checksiblings, peeringdb)
    get_res_attrs = GetResAttr(date, vp, collect_cands)
    map_score = {}
    stat = Counter()
    print('begin')
    ips_not_in_bdr = set()
    for ip in ips:
        if ip not in get_res_attrs.cache.keys() or not collect_cands.IsLegalASN(get_res_attrs.cache[ip]):
            map_score[ip] = 0
            ips_not_in_bdr.add(ip)
            continue
        rib_res, trip_asn_counter, prev_asn_stat, succ_counter = get_res_attrs.GetIPAttr_Prepare_ForACand(ip)
        attr = get_res_attrs.GetIPAttr_ForACand(ip, get_res_attrs.cache.get(ip), rib_res, trip_asn_counter, prev_asn_stat, succ_counter)
        map_score[ip] = get_pred_result_per_attr_v3_old(attr, get_res_attrs.model).tolist()
        if len(map_score) % 10000 == 0:
            print(len(map_score))
        stat[map_score[ip] > 0.5] += 1 
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/ipattr_score_%s_%s.json' %(vp, vp, date), 'w') as wf:
        json.dump(map_score, wf, indent=1)
    print('ips_not_in_bdr: %d' %(len(ips_not_in_bdr)))
    print(stat)
    
class GetResAttr():
    valleystate = {'11':'normal','12':'normal','13':'normal','14':'semi','15':'normal','21':'abnormal','22':'normal','23':'abnormal','24':'semi','25':'normal','31':'abnormal','32':'normal','33':'semi','34':'semi','35':'normal','41':'semi','42':'semi','43':'semi','44':'abnormal','45':'semi','51':'normal','52':'normal','53':'normal','54':'semi','55':'normal','1':'normal','2':'normal','3':'normal','4':'semi','5':'normal','':'normal'} #这里改成三态的，配合模型参数
    #valleystate = {'11':'normal','12':'normal','13':'normal','14':'abnormal','15':'normal','21':'abnormal','22':'normal','23':'abnormal','24':'abnormal','25':'normal','31':'abnormal','32':'normal','33':'abnormal','34':'abnormal','35':'normal','41':'abnormal','42':'abnormal','43':'abnormal','44':'abnormal','45':'abnormal','51':'normal','52':'normal','53':'normal','54':'abnormal','55':'normal','1':'normal','2':'normal','3':'normal','4':'abnormal','5':'normal','':'normal'} #这里改成三态的，配合模型参数
    def __init__(self, date, vp, collect_cands):
        self.collect_cands = collect_cands
        if vp:
            with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/ipattr_is_ixp_%s_%s.json' %(vp, vp, date), 'r') as rf:
                self.is_ixp = json.load(rf)
            with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/ipattr_prev_ips_%s_%s.json' %(vp, vp, date), 'r') as rf:
                self.prev_ips = json.load(rf)
            with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/ipattr_succ_ips_%s_%s.json' %(vp, vp, date), 'r') as rf:
                self.succ_ips = json.load(rf)
            # with open('ipattr_trip_%s.json' %vp, 'r') as rf:
            #     self.trip = json.load(rf)
            with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/ipattr_trip_AS_%s_%s.json' %(vp, vp, date), 'r') as rf:
                self.trip_AS = json.load(rf)
            self.map_score = GetAttrScores(vp, date)
            with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/ipattr_traces_%s_%s' %(vp, vp, date), 'r') as rf:
                self.ip_traces = rf.readlines()
        else:
            with open('/mountdisk2/common_vps/%s/atlas/ipattr_is_ixp_%s.json' %(date, date), 'r') as rf:
                self.is_ixp = json.load(rf)
            with open('/mountdisk2/common_vps/%s/atlas/ipattr_prev_ips_%s.json' %(date, date), 'r') as rf:
                self.prev_ips = json.load(rf)
            with open('/mountdisk2/common_vps/%s/atlas/ipattr_succ_ips_%s.json' %(date, date), 'r') as rf:
                self.succ_ips = json.load(rf)
            # with open('ipattr_trip_%s.json' %vp, 'r') as rf:
            #     self.trip = json.load(rf)
            with open('/mountdisk2/common_vps/%s/atlas/ipattr_trip_AS_%s.json' %(date, date), 'r') as rf:
                self.trip_AS = json.load(rf)
            self.map_score = {}
            if os.path.exists('/mountdisk2/common_vps/%s/atlas/ipattr_score_%s.json' %(date, date)):
                with open('/mountdisk2/common_vps/%s/atlas/ipattr_score_%s.json' %(date, date), 'r') as rf:
                    self.map_score = json.load(rf)
            with open('/mountdisk2/common_vps/%s/atlas/ipattr_traces_%s' %(date, date), 'r') as rf:
                self.ip_traces = rf.readlines()
        self.model = None
        if os.path.exists('/home/slt/code/ML_Bdrmaplt/xgboost_model_v0.model'):
            self.model = xgb.XGBClassifier()
            self.model.load_model('/home/slt/code/ML_Bdrmaplt/xgboost_model_v0.model')
        if vp:
            ConnectToBdrMapItDb('/mountdisk1/ana_c_d_incongruity/out_bdrmapit/ori_bdr/bdrmapit_%s_%s.db' %(vp, date))
            #ConnectToBdrMapItDb('/mountdisk1/ana_c_d_incongruity/out_bdrmapit/sxt_bdr/bdrmapit_%s_%s.db' %(vp, date))
        else:
            #ConnectToBdrMapItDb('/mountdisk2/common_vps/%s/atlas/bdrmapit/ori_bdr.db' %date)
            ConnectToBdrMapItDb('/mountdisk2/common_vps/%s/atlas/bdrmapit/sxt_bdr.db' %date)
        ConstrBdrCache()
        self.cache = GetBdrCache()
        self.version = 'new'     
    
    # def GetIPAttr(self, ip, new_res):
    #     if not new_res or not self.collect_cands.IsLegalASN(new_res):
    #         return None
    #     tmp = {}
    #     rib_res = GetBGPPath_Or_OriASN(self.collect_cands.rib_mappings, ip, 'get_all_2')
    #     rib_res = [int(elem) for elem in rib_res[1].split('_') if self.collect_cands.IsLegalASN(elem)]
    #     tmp['is_moas'] = (len(rib_res) > 1)
    #     new_res = int(new_res)
    #     if not rib_res:
    #         tmp['bdr_rib_rel'] = 0
    #     elif new_res in rib_res:
    #         tmp['bdr_rib_rel'] = 1
    #     elif any(self.collect_cands.checksiblings.check_sibling(elem, new_res) for elem in rib_res):
    #         tmp['bdr_rib_rel'] = 2
    #     elif any(self.collect_cands.GetRel(elem, new_res) != 4 for elem in rib_res):
    #         tmp['bdr_rib_rel'] = 3
    #     else:
    #         tmp['bdr_rib_rel'] = 4
    #     tmp['is_ixp'] = (IsIxpIp(ip) or IsIxpAs(new_res))
    #     prev_rels = Counter()
    #     prev_rels_trace_weight = Counter()
    #     succ_rels = Counter()
    #     succ_rels_trace_weight = Counter()
    #     prev_or_succ_rels = Counter()
    #     valley = Counter()
    #     valley_trace_weight = Counter()
    #     # succ_ips = defaultdict(set)
    #     # succ_asns = set()
    #     #new_res = str(new_res)
    #     trip_asn_counter = {}
    #     trip_counter = Counter(self.trip_AS[ip].values()) #trip_AS[ips[j]][trace_idx] = ips[j-1] + ',' + succ_ip + '|' + prev_cpr_hop + ',' + succ_cpr_hop
    #     for trip, trace_c in trip_counter.items():
    #         _, asn_val = trip.split('|')
    #         #prev_asn, ori_succ_asn = asn_val.split(',')
    #         if asn_val not in trip_asn_counter.keys():
    #             trip_asn_counter[asn_val] = [0, 0]
    #             # trip_asn_counter[trip] = [0, 0, False, '', False, '']
    #             # if self.collect_cands.IsLegalASN(prev_asn):
    #             #     trip_asn_counter[trip][2] = True
    #             #     if int(prev_asn) in self.collect_cands.checksiblings.bgp_v2.keys():
    #             #         trip_asn_counter[trip][3] = self.collect_cands.checksiblings.bgp_v2[int(prev_asn)]
    #             # succ_asn = ori_succ_asn.strip('$')
    #             # if self.collect_cands.IsLegalASN(succ_asn):
    #             #     trip_asn_counter[trip][4] = True
    #             #     if int(succ_asn) in self.collect_cands.checksiblings.bgp_v2.keys():
    #             #         trip_asn_counter[trip][5] = self.collect_cands.checksiblings.bgp_v2[int(succ_asn)]
    #         trip_asn_counter[asn_val][0] += 1
    #         trip_asn_counter[asn_val][1] += trace_c
    #     for trip_asn, c in trip_asn_counter.items():
    #         prev_asn, ori_succ_asn = trip_asn.split(',')
    #         #ip_c, trace_c, prev_legal, prev_asn_rel_pointer, succ_legal, succ_asn_rel_pointer = c
    #         ip_c, trace_c = c
    #         #prev_rel = str(self.collect_cands.GetRel_v2(prev_asn_rel_pointer, int(prev_asn), int(new_res), False)) if prev_legal else ''
    #         prev_rel = str(self.collect_cands.GetRel(int(prev_asn), int(new_res))) if self.collect_cands.IsLegalASN(prev_asn) else ''
    #         prev_rels[prev_rel] += ip_c#.add(trip) #IP级别
    #         #prev_rels_trace_weight[prev_rel] += trace_c
    #         succ_asn = ori_succ_asn.strip('$')
    #         #succ_rel = str(self.collect_cands.GetRel_v2(succ_asn_rel_pointer, int(succ_asn), int(new_res), True)) if succ_legal else ''
    #         succ_rel = str(self.collect_cands.GetRel(int(new_res), int(succ_asn))) if self.collect_cands.IsLegalASN(succ_asn) else ''
    #         if ori_succ_asn[-1] != '$' or succ_rel != '4':
    #             succ_rels[succ_rel] += ip_c#.add(trip) #IP级别
    #             valley[self.valleystate[prev_rel+succ_rel]] += ip_c#.add(trip) #IP级别
    #             prev_or_succ_rels[prev_rel == '4' or succ_rel == '4'] += 1
    #             # succ_rels_trace_weight[succ_rel] += trace_c
    #             # valley_trace_weight[self.valleystate[prev_rel+succ_rel]] += trace_c
    #         else:
    #             succ_rels[''] += ip_c#.add(trip) #IP级别
    #             valley[self.valleystate[prev_rel]] += ip_c#.add(trip) #IP级别
    #             prev_or_succ_rels[prev_rel == '4'] += 1
    #             # succ_rels_trace_weight[''] += trace_c
    #             # valley_trace_weight[self.valleystate[prev_rel]] += trace_c
    #     sum_valley_value = sum(valley.values())
    #     tmp['valley_normal_rate'] = valley['normal'] / sum_valley_value if 'normal' in valley.keys() else 0
    #     tmp['valley_abnormal_rate'] = valley['abnormal'] / sum_valley_value if 'abnormal' in valley.keys() else 0
    #     tmp['valley_seminormal_rate'] = valley['semi'] / sum_valley_value if 'semi' in valley.keys() else 0
    #     # if pre_filter and tmp['valley_abnormal_rate'] > 0.5:
    #     #     return None
    #     # sum_valley_weight_value = sum(valley_trace_weight.values())
    #     # tmp['valley_normal_rate_trace_weight'] = valley_trace_weight['normal'] / sum_valley_weight_value if 'normal' in valley_trace_weight.keys() else 0
    #     # tmp['valley_abnormal_rate_trace_weight'] = valley_trace_weight['abnormal'] / sum_valley_weight_value if 'abnormal' in valley_trace_weight.keys() else 0
    #     # tmp['valley_seminormal_rate_trace_weight'] = valley_trace_weight['semi'] / sum_valley_weight_value if 'semi' in valley_trace_weight.keys() else 0
    #     new_res = str(new_res)
    #     prev_set = set(self.prev_ips[ip].values()) #prev_ips[ips[j]][trace_idx] = ips[j-1] + '|' + hops[j-1]
    #     tmp_stat = [elem.split('|')[1] for elem in prev_set] #rate是指ip的比重
    #     tmp['prev_sameAS_rate'] = tmp_stat.count(new_res) / len(tmp_stat)
    #     succ_counter = Counter(self.succ_ips[ip].values()) #succ_ips[ips[j]][trace_idx] = succ_ip + '|' + succ_hop
    #     tmp_stat = [elem for elem in succ_counter.keys() if elem.split('|')[1].strip('$') == new_res] #rate是指ip的比重
    #     tmp['succ_sameAS_rate'] = len(tmp_stat) / len(succ_counter.keys())
    #     tmp['prev_asnrel_unknown_rate'] = prev_rels['4'] / sum(prev_rels.values()) if '4' in prev_rels.keys() else 0
    #     tmp['succ_asnrel_unknown_rate'] = succ_rels['4'] / sum(succ_rels.values()) if '4' in succ_rels.keys() else 0
    #     tmp['prev_or_succ_asnrel_unknown_rate'] = prev_or_succ_rels['4'] / sum(prev_or_succ_rels.values())
    #     # tmp['succ_sameAS_rate_trace_weight'] = sum([succ_counter[elem] for elem in tmp_stat]) / sum(succ_counter.values())
    #     # tmp['prev_asnrel_unknown_rate_trace_weight'] = prev_rels_trace_weight['4'] / sum(prev_rels_trace_weight.values()) if '4' in prev_rels_trace_weight.keys() else 0
    #     # tmp['succ_asnrel_unknown_rate_trace_weight'] = succ_rels_trace_weight['4'] / sum(succ_rels_trace_weight.values()) if '4' in succ_rels_trace_weight.keys() else 0
    #     attr = ipattr()
    #     for attr_name in [elem for elem in dir(ipattr) if not elem.startswith('_')]:
    #         setattr(attr, attr_name, tmp[attr_name])
    #     return attr

    def GetIPAttr_Prepare_ForACand(self, ip):
        if self.version == 'old':
            rib_res = GetBGPPath_Or_OriASN(self.collect_cands.rib_mappings, ip, 'get_all_2')
            rib_res = [int(elem) for elem in rib_res[1].split('_') if self.collect_cands.IsLegalASN(elem)]
            trip_asn_counter = {}
            trip_counter = Counter(self.trip_AS[ip].values()) #trip_AS[ips[j]][trace_idx] = ips[j-1] + ',' + succ_ip + '|' + prev_cpr_hop + ',' + succ_cpr_hop
            for trip, trace_c in trip_counter.items():
                _, asn_val = trip.split('|')
                if asn_val not in trip_asn_counter.keys():
                    trip_asn_counter[asn_val] = [0, 0]
                trip_asn_counter[asn_val][0] += 1
                trip_asn_counter[asn_val][1] += trace_c
            prev_set = set(self.prev_ips[ip].values()) #prev_ips[ips[j]][trace_idx] = ips[j-1] + '|' + hops[j-1]
            prev_asn_stat = [elem.split('|')[1] for elem in prev_set] #rate是指ip的比重
            succ_counter = Counter(self.succ_ips[ip].values()) #succ_ips[ips[j]][trace_idx] = succ_ip + '|' + succ_hop
            return rib_res, trip_asn_counter, prev_asn_stat, succ_counter
        else:
            rib_res = GetBGPPath_Or_OriASN(self.collect_cands.rib_mappings, ip, 'get_all_2')
            rib_res = [int(elem) for elem in rib_res[1].split('_') if self.collect_cands.IsLegalASN(elem)]
            trip_counter = Counter(self.trip_AS[ip].values()) #trip_AS[ips[j]][trace_idx] = ips[j-1] + ',' + succ_ip + '|' + prev_cpr_hop + ',' + succ_cpr_hop
            prev_counter = Counter(self.prev_ips[ip].values()) #prev_ips[ips[j]][trace_idx] = ips[j-1] + '|' + hops[j-1]
            succ_counter = Counter(self.succ_ips[ip].values()) #succ_ips[ips[j]][trace_idx] = succ_ip + '|' + succ_hop
            return rib_res, trip_counter, prev_counter, succ_counter        
                
    def GetIPAttr_ForACand(self, ip, new_res, rib_res, trip_asn_counter, prev_asn_stat, succ_counter):
        if self.version == 'old':
            tmp = {}
            new_res = int(new_res)
            if not rib_res:
                tmp['bdr_rib_rel'] = 0
            elif new_res in rib_res:
                tmp['bdr_rib_rel'] = 1
            elif any(self.collect_cands.checksiblings.check_sibling(elem, new_res) for elem in rib_res):
                tmp['bdr_rib_rel'] = 2
            elif any(self.collect_cands.GetRel(elem, new_res) != 4 for elem in rib_res):
                tmp['bdr_rib_rel'] = 3
            else:
                tmp['bdr_rib_rel'] = 4
            tmp['is_ixp'] = (ip in self.collect_cands.ixp_ip_asn.keys() or IsIxpAs(new_res))
            prev_rels = Counter()
            prev_rels_trace_weight = Counter()
            succ_rels = Counter()
            succ_rels_trace_weight = Counter()
            prev_or_succ_rels = Counter()
            valley = Counter()
            valley_trace_weight = Counter()
            prev_succ_rels = Counter()
            for trip_asn, c in trip_asn_counter.items():
                prev_asn, ori_succ_asn = trip_asn.split(',')
                ip_c, trace_c = c
                prev_rel = str(self.collect_cands.GetRel(int(prev_asn), int(new_res))) if self.collect_cands.IsLegalASN(prev_asn) else ''
                prev_rels[prev_rel] += ip_c#.add(trip) #IP级别
                #prev_rels_trace_weight[prev_rel] += trace_c
                succ_asn = ori_succ_asn.strip('$')
                succ_rel = str(self.collect_cands.GetRel(int(new_res), int(succ_asn))) if self.collect_cands.IsLegalASN(succ_asn) else ''
                if ori_succ_asn[-1] != '$' or succ_rel != '4':
                    succ_rels[succ_rel] += ip_c#.add(trip) #IP级别
                    valley[self.valleystate[prev_rel+succ_rel]] += ip_c#.add(trip) #IP级别
                    prev_or_succ_rels[prev_rel == '4' or succ_rel == '4'] += 1
                    # succ_rels_trace_weight[succ_rel] += trace_c
                    # valley_trace_weight[self.valleystate[prev_rel+succ_rel]] += trace_c
                else:
                    succ_rels[''] += ip_c#.add(trip) #IP级别
                    valley[self.valleystate[prev_rel]] += ip_c#.add(trip) #IP级别
                    prev_or_succ_rels[prev_rel == '4'] += 1
                    # succ_rels_trace_weight[''] += trace_c
                    # valley_trace_weight[self.valleystate[prev_rel]] += trace_c
                prev_succ_rel = str(self.collect_cands.GetRel(int(prev_asn), int(succ_asn))) if self.collect_cands.IsLegalASN(prev_asn) and self.collect_cands.IsLegalASN(succ_asn) else '4'
                prev_succ_rels[prev_succ_rel] += 1
            sum_valley_value = sum(valley.values())
            tmp['valley_normal_rate'] = valley['normal'] / sum_valley_value if 'normal' in valley.keys() else 0
            tmp['valley_abnormal_rate'] = valley['abnormal'] / sum_valley_value if 'abnormal' in valley.keys() else 0
            tmp['valley_seminormal_rate'] = valley['semi'] / sum_valley_value if 'semi' in valley.keys() else 0
            tmp['prev_succ_norel_rate'] = prev_succ_rels['4'] / sum(prev_succ_rels.values()) if sum(prev_succ_rels.values()) > 0 else 0
            # if pre_filter and tmp['valley_abnormal_rate'] > 0.5:
            #     return None
            # sum_valley_weight_value = sum(valley_trace_weight.values())
            # tmp['valley_normal_rate_trace_weight'] = valley_trace_weight['normal'] / sum_valley_weight_value if 'normal' in valley_trace_weight.keys() else 0
            # tmp['valley_abnormal_rate_trace_weight'] = valley_trace_weight['abnormal'] / sum_valley_weight_value if 'abnormal' in valley_trace_weight.keys() else 0
            # tmp['valley_seminormal_rate_trace_weight'] = valley_trace_weight['semi'] / sum_valley_weight_value if 'semi' in valley_trace_weight.keys() else 0
            new_res = str(new_res)
            tmp['prev_sameAS_rate'] = prev_asn_stat.count(new_res) / len(prev_asn_stat)
            tmp_stat = [elem for elem in succ_counter.keys() if elem.split('|')[1].strip('$') == new_res] #rate是指ip的比重
            tmp['succ_sameAS_rate'] = len(tmp_stat) / len(succ_counter.keys())
            tmp['prev_asnrel_unknown_rate'] = prev_rels['4'] / sum(prev_rels.values()) if '4' in prev_rels.keys() else 0
            tmp['succ_asnrel_unknown_rate'] = succ_rels['4'] / sum(succ_rels.values()) if '4' in succ_rels.keys() else 0
            tmp['prev_or_succ_asnrel_unknown_rate'] = prev_or_succ_rels['4'] / sum(prev_or_succ_rels.values())
            # tmp['succ_sameAS_rate_trace_weight'] = sum([succ_counter[elem] for elem in tmp_stat]) / sum(succ_counter.values())
            # tmp['prev_asnrel_unknown_rate_trace_weight'] = prev_rels_trace_weight['4'] / sum(prev_rels_trace_weight.values()) if '4' in prev_rels_trace_weight.keys() else 0
            # tmp['succ_asnrel_unknown_rate_trace_weight'] = succ_rels_trace_weight['4'] / sum(succ_rels_trace_weight.values()) if '4' in succ_rels_trace_weight.keys() else 0
            attr = ipattr()
            for attr_name in [elem for elem in dir(ipattr) if not elem.startswith('_')]:
                setattr(attr, attr_name, tmp[attr_name])
            return attr
        else:
            trip_counter = trip_asn_counter
            prev_counter = prev_asn_stat
            tmp = {}
            if not new_res or not self.collect_cands.IsLegalASN(new_res):
                return None
            new_res = int(new_res)
            tmp['is_moas'] = (len(rib_res) > 1)
            if not rib_res:
                #tmp['bdr_rib_rel'] = 0
                tmp['bdr_rib_rel'] = 0
            elif new_res in rib_res:
                tmp['bdr_rib_rel'] = 1
            elif any(self.collect_cands.GetRel(elem, new_res) != 4 for elem in rib_res):
                tmp['bdr_rib_rel'] = 2
            elif any(self.collect_cands.checksiblings.check_sibling(elem, new_res) for elem in rib_res):
                #tmp['bdr_rib_rel'] = 2
                tmp['bdr_rib_rel'] = 0
            else:
                tmp['bdr_rib_rel'] = 0
            tmp['is_ixp'] = (ip in self.collect_cands.ixp_ip_asn.keys() or IsIxpAs(new_res))
            prev_rels = Counter()
            prev_rels_trace_weight = Counter()
            succ_rels = Counter()
            succ_rels_trace_weight = Counter()
            prev_or_succ_rels = Counter()
            valley = Counter()
            valley_trace_weight = Counter()
            prev_asn_uncertain = Counter()
            succ_asn_uncertain = Counter()
            prev_asns = set()
            succ_asns = set()
            prev_succ_rels = Counter()
            for trip, trace_c in trip_counter.items():
                trip_ip, trip_asn = trip.split('|')
                prev_ip, succ_ip = trip_ip.split(',')
                prev_asn, ori_succ_asn = trip_asn.split(',')
                prev_asns.add(prev_asn)
                succ_asns.add(ori_succ_asn.strip('$'))
                prev_rel = ''
                # prev_ixp = (IsIxpIp(prev_ip.strip('*')) or IsIxpAs(prev_asn))
                # succ_ixp = (IsIxpIp(succ_ip.strip('*')) or IsIxpAs(ori_succ_asn.strip('$')))
                if self.collect_cands.IsLegalASN(prev_asn):
                    if prev_ip[-1] != '*':
                        prev_rel = str(self.collect_cands.GetRel(int(prev_asn), int(new_res)))
                    else:
                        prev_rel = str(self.collect_cands.GetRemoteRel(int(prev_asn), int(new_res)))
                # if prev_rel == '4':
                #     if tmp['is_ixp'] and prev_ixp:
                #         prev_rel = '3'
                #prev_asn_uncertain[str(prev_asn) != str(new_res) and prev_ip[-1] == '*'] += 1
                prev_asn_uncertain[prev_rel == '' or (prev_rel == '4' and prev_ip[-1] == '*')] += 1
                prev_rels[prev_rel] += 1#.add(trip) #IP级别
                prev_rels_trace_weight[prev_rel] += trace_c
                succ_asn = ori_succ_asn.strip('$')
                succ_rel = ''
                if self.collect_cands.IsLegalASN(succ_asn):
                    if succ_ip[-1] != '*':
                        succ_rel = str(self.collect_cands.GetRel(int(new_res), int(succ_asn)))
                    else:
                        succ_rel = str(self.collect_cands.GetRemoteRel(int(new_res), int(succ_asn)))
                # if succ_rel == '4':
                #     if tmp['is_ixp'] and succ_ixp:
                #         succ_rel = '3'
                #succ_asn_uncertain[str(succ_asn) != str(new_res) and succ_ip[-1] == '*'] += 1
                succ_asn_uncertain[succ_rel == '' or (succ_rel == '4' and succ_ip[-1] == '*')] += 1
                #if ori_succ_asn[-1] != '$' or succ_rel != '4':
                if True:
                    succ_rels[succ_rel] += 1#.add(trip) #IP级别
                    succ_rels_trace_weight[succ_rel] += trace_c
                    valley[self.valleystate[prev_rel+succ_rel]] += 1#.add(trip) #IP级别
                    valley_trace_weight[self.valleystate[prev_rel+succ_rel]] += trace_c
                    #prev_or_succ_rels[prev_rel == '4' or succ_rel == '4'] += 1
                    prev_succ_rel = str(self.collect_cands.GetRel(int(prev_asn), int(succ_asn))) if self.collect_cands.IsLegalASN(prev_asn) and self.collect_cands.IsLegalASN(succ_asn) else '4'
                    prev_succ_rels[prev_succ_rel] += 1
                else:
                    succ_rels[''] += 1#.add(trip) #IP级别
                    succ_rels_trace_weight[''] += trace_c
                    valley[self.valleystate[prev_rel]] += 1#.add(trip) #IP级别
                    valley_trace_weight[self.valleystate[prev_rel]] += trace_c
                    #prev_or_succ_rels[prev_rel == '4'] += 1
            tmp['prev_succ_norel_rate'] = prev_succ_rels['4'] / sum(prev_succ_rels.values()) if sum(prev_succ_rels.values()) > 0 else 0
            tmp['prev_asn_num'] = len(prev_asns)
            tmp['succ_asn_num'] = len(succ_asns)
            sum_valley_value = sum(valley.values())
            sum_valley_value_trace_weight = sum(valley_trace_weight.values())
            tmp['valley_normal_rate'] = valley.get('normal', 0) / sum_valley_value
            tmp['valley_normal_rate_trace_weight'] = valley_trace_weight.get('normal', 0) / sum_valley_value_trace_weight
            tmp['valley_abnormal_rate'] = valley.get('abnormal', 0) / sum_valley_value
            tmp['valley_abnormal_rate_trace_weight'] = valley_trace_weight.get('abnormal', 0) / sum_valley_value_trace_weight
            tmp['valley_seminormal_rate'] = valley.get('semi', 0) / sum_valley_value
            tmp['valley_seminormal_rate_trace_weight'] = valley_trace_weight.get('seminormal', 0) / sum_valley_value_trace_weight
            tmp['prev_asnrel_unknown_rate'] = prev_rels.get('4', 0) / sum(prev_rels.values())
            tmp['prev_asnrel_unknown_rate_trace_weight'] = prev_rels_trace_weight.get('4', 0) / sum(prev_rels_trace_weight.values())
            tmp['succ_asnrel_unknown_rate'] = succ_rels.get('4', 0) / sum(succ_rels.values())
            tmp['succ_asnrel_unknown_rate_trace_weight'] = succ_rels_trace_weight.get('4', 0) / sum(succ_rels_trace_weight.values())
            tmp['prev_asn_uncertain_rate'] = prev_asn_uncertain.get(True, 0) / sum(prev_asn_uncertain.values())
            tmp['succ_asn_uncertain_rate'] = succ_asn_uncertain.get(True, 0) / sum(succ_asn_uncertain.values())
            new_res = str(new_res)
            prev_sameAS = Counter()
            prev_sameAS_trace_weight = Counter()
            prev_ips = set()
            tmp['prev_ixp_rate'] = 0.0
            tmp['succ_ixp_rate'] = 0.0
            tmp['prev_ixp_rate_trace_weight'] = 0.0
            tmp['succ_ixp_rate_trace_weight'] = 0.0
            prev_ixp_rate = Counter()
            prev_ixp_rate_trace_weight = Counter()
            for prev, trace_c in prev_counter.items():
                prev_ip, prev_asn = prev.split('|')
                # prev_ixp = (IsIxpIp(prev_ip.strip('*')) or IsIxpAs(prev_asn))
                # prev_ixp_rate[prev_ixp] += 1
                # prev_ixp_rate_trace_weight[prev_ixp] += trace_c
                prev_ips.add(prev_ip)
                if prev_asn == new_res:
                    prev_sameAS[1] += 1
                    prev_sameAS_trace_weight[1] += trace_c
                else:
                    if prev_ip[-1] == '*':
                        prev_sameAS[0.5] += 1
                        prev_sameAS_trace_weight[0.5] += trace_c
                    else:
                        prev_sameAS[0] += 1
                        prev_sameAS_trace_weight[0] += trace_c
            # tmp['prev_ixp_rate'] = prev_ixp_rate.get(True, 0) / sum(prev_ixp_rate.values())
            # tmp['prev_ixp_rate_trace_weight'] = prev_ixp_rate_trace_weight.get(True, 0) / sum(prev_ixp_rate_trace_weight.values())
            tmp['prev_ip_num'] = len(prev_ips)
            tmp['prev_sameAS_rate'] = prev_sameAS.get(1, 0) / sum(prev_sameAS.values())
            tmp['prev_ip_uncertain_rate'] = prev_sameAS.get(0.5, 0) / sum(prev_sameAS.values())
            tmp['prev_sameAS_rate_trace_weight'] = prev_sameAS_trace_weight.get(1, 0) / sum(prev_sameAS_trace_weight.values())
            succ_sameAS = Counter()
            succ_sameAS_trace_weight = Counter()
            succ_ips = set()
            succ_ixp_rate = Counter()
            succ_ixp_rate_trace_weight = Counter()
            for succ, trace_c in succ_counter.items():
                succ_ip, succ_asn = succ.split('|')
                # succ_ixp = (IsIxpIp(succ_ip.strip('*')) or IsIxpAs(succ_asn.strip('$')))
                # succ_ixp_rate[succ_ixp] += 1
                # succ_ixp_rate_trace_weight[succ_ixp] += trace_c
                succ_ips.add(succ_ip)
                if succ_asn == new_res:
                    succ_sameAS[1] += 1
                    succ_sameAS_trace_weight[1] += trace_c
                else:
                    if succ_ip[-1] == '*':
                        succ_sameAS[0.5] += 1
                        succ_sameAS_trace_weight[0.5] += trace_c
                    else:
                        succ_sameAS[0] += 1
                        succ_sameAS_trace_weight[0] += trace_c
            # tmp['succ_ixp_rate'] = succ_ixp_rate.get(True, 0) / sum(succ_ixp_rate.values())
            # tmp['succ_ixp_rate_trace_weight'] = succ_ixp_rate_trace_weight.get(True, 0) / sum(succ_ixp_rate_trace_weight.values())
            tmp['succ_ip_num'] = len(succ_ips)
            tmp['succ_sameAS_rate'] = succ_sameAS.get(1, 0) / sum(succ_sameAS.values())
            tmp['succ_ip_uncertain_rate'] = succ_sameAS.get(0.5, 0) / sum(succ_sameAS.values())
            tmp['succ_sameAS_rate_trace_weight'] = succ_sameAS_trace_weight.get(1, 0) / sum(succ_sameAS_trace_weight.values())
            attr = ipattr()
            for attr_name in [elem for elem in dir(ipattr) if not elem.startswith('_')]:
                setattr(attr, attr_name, tmp[attr_name])
            return attr
    
    def GetIPAttr_Prepare_ForACand_ConsiderScores(self, ip):
        if self.version == 'old':
            ori_rib_res = GetBGPPath_Or_OriASN(self.collect_cands.rib_mappings, ip, 'get_all_2')
            pref, ori_asns = ori_rib_res
            rib_res = [int(elem) for elem in ori_asns.split('_') if self.collect_cands.IsLegalASN(elem)]
            trip_asn_counter = {}
            trip_counter = Counter(self.trip_AS[ip].values()) #trip_AS[ips[j]][trace_idx] = ips[j-1] + ',' + succ_ip + '|' + prev_cpr_hop + ',' + succ_cpr_hop
            for trip, trace_c in trip_counter.items():
                _, asn_val = trip.split('|')
                if asn_val not in trip_asn_counter.keys():
                    trip_asn_counter[asn_val] = [0, 0]
                trip_asn_counter[asn_val][0] += 1
                trip_asn_counter[asn_val][1] += trace_c
            prev_set = set(self.prev_ips[ip].values()) #prev_ips[ips[j]][trace_idx] = ips[j-1] + '|' + hops[j-1]
            prev_asn_stat = Counter()
            for prev in prev_set:
                prev_ip, prev_asn = prev.split('|')
                modify = False
                if prev_ip in self.map_score.keys() and self.map_score[prev_ip] < self.collect_cands.score_threshold:# and abs(self.map_score[prev_ip] - self.map_score[ip]) < 0.1:
                    ori_prev_rib_res = GetBGPPath_Or_OriASN(self.collect_cands.rib_mappings, prev_ip, 'get_all_2')
                    prev_pref, ori_pref_asns = ori_prev_rib_res
                    #if ori_prev_rib_res[0] == pref:
                    if set(ori_pref_asns.split('_')) & set(ori_asns.split('_')):
                        prev_asn_stat['?'] += 1
                        modify = True
                if not modify:
                    prev_asn_stat[prev_asn] += 1
            succ_set = set(self.succ_ips[ip].values()) 
            succ_asn_stat = Counter()
            for succ in succ_set:
                succ_ip, succ_asn = succ.split('|')
                modify = False
                if succ_ip in self.map_score.keys() and self.map_score[succ_ip] < self.collect_cands.score_threshold:# and abs(self.map_score[succ_ip] - self.map_score[ip]) < 0.1:
                    ori_succ_rib_res = GetBGPPath_Or_OriASN(self.collect_cands.rib_mappings, succ_ip, 'get_all_2')
                    succ_pref, ori_succ_asns = ori_succ_rib_res
                    #if ori_succ_rib_res[0] == pref:
                    if set(ori_succ_asns.split('_')) & set(ori_asns.split('_')):
                        succ_asn_stat['?'] += 1
                        modify = True
                if not modify:
                    succ_asn_stat[succ_asn] += 1
            return rib_res, trip_asn_counter, prev_asn_stat, succ_asn_stat
        else:
            ori_rib_res = GetBGPPath_Or_OriASN(self.collect_cands.rib_mappings, ip, 'get_all_2')
            pref, ori_asns = ori_rib_res
            rib_res = [int(elem) for elem in ori_asns.split('_') if self.collect_cands.IsLegalASN(elem)]
            trip_counter = Counter(self.trip_AS[ip].values()) #trip_AS[ips[j]][trace_idx] = ips[j-1] + ',' + succ_ip + '|' + prev_cpr_hop + ',' + succ_cpr_hop
            prev_counter = Counter(self.prev_ips[ip].values()) #prev_ips[ips[j]][trace_idx] = ips[j-1] + '|' + hops[j-1]
            succ_counter = Counter(self.succ_ips[ip].values()) #succ_ips[ips[j]][trace_idx] = succ_ip + '|' + succ_hop
            for prev, trace_c in prev_counter.items():
                prev_ip, prev_asn = prev.split('|')
                tmp_prev_ip = prev_ip.strip('*')
                modify = False
                if tmp_prev_ip in self.map_score.keys() and self.map_score[tmp_prev_ip] < self.collect_cands.score_threshold:
                    ori_prev_rib_res = GetBGPPath_Or_OriASN(self.collect_cands.rib_mappings, tmp_prev_ip, 'get_all_2')
                    prev_pref, ori_prev_asns = ori_prev_rib_res
                    #if ori_prev_rib_res[0] == pref:
                    if set(ori_prev_asns.split('_')) & set(ori_asns.split('_')):
                        prev_counter[prev] = -1 * trace_c #不好改keys，只能通过改values标识
            for succ, trace_c in succ_counter.items():
                succ_ip, succ_asn = succ.split('|')
                tmp_succ_ip = succ_ip.strip('*')
                modify = False
                # if tmp_succ_ip in self.map_score.keys():
                #     if not isinstance(self.map_score[tmp_succ_ip], float):
                #         print(tmp_succ_ip)
                #         print(self.map_score[tmp_succ_ip])
                if tmp_succ_ip in self.map_score.keys() and self.map_score[tmp_succ_ip] < self.collect_cands.score_threshold:
                    ori_succ_rib_res = GetBGPPath_Or_OriASN(self.collect_cands.rib_mappings, tmp_succ_ip, 'get_all_2')
                    succ_pref, ori_succ_asns = ori_succ_rib_res
                    #if ori_succ_rib_res[0] == pref:
                    if set(ori_succ_asns.split('_')) & set(ori_asns.split('_')):
                        succ_counter[succ] = -1 * trace_c #不好改keys，只能通过改values标识
            return rib_res, trip_counter, prev_counter, succ_counter      
    
    def GetIPAttr_ForACand_ConsiderScores(self, ip, new_res, rib_res, trip_asn_counter, prev_asn_stat, succ_counter):
        if self.version == 'old':
            tmp = {}
            if not new_res or not self.collect_cands.IsLegalASN(new_res):
                return None
            new_res = int(new_res)
            if not rib_res:
                tmp['bdr_rib_rel'] = 0
            elif new_res in rib_res:
                tmp['bdr_rib_rel'] = 1
            elif any(self.collect_cands.checksiblings.check_sibling(elem, new_res) for elem in rib_res):
                tmp['bdr_rib_rel'] = 2
            elif any(self.collect_cands.GetRel(elem, new_res) != 4 for elem in rib_res):
                tmp['bdr_rib_rel'] = 3
            else:
                tmp['bdr_rib_rel'] = 4
            tmp['is_ixp'] = (ip in self.collect_cands.ixp_ip_asn.keys() or IsIxpAs(new_res))
            prev_rels = Counter()
            prev_rels_trace_weight = Counter()
            succ_rels = Counter()
            succ_rels_trace_weight = Counter()
            prev_or_succ_rels = Counter()
            valley = Counter()
            valley_trace_weight = Counter()
            for trip_asn, c in trip_asn_counter.items():
                prev_asn, ori_succ_asn = trip_asn.split(',')
                ip_c, trace_c = c
                prev_rel = str(self.collect_cands.GetRel(int(prev_asn), int(new_res))) if self.collect_cands.IsLegalASN(prev_asn) else ''
                prev_rels[prev_rel] += ip_c#.add(trip) #IP级别
                #prev_rels_trace_weight[prev_rel] += trace_c
                succ_asn = ori_succ_asn.strip('$')
                succ_rel = str(self.collect_cands.GetRel(int(new_res), int(succ_asn))) if self.collect_cands.IsLegalASN(succ_asn) else ''
                if ori_succ_asn[-1] != '$' or succ_rel != '4':
                    succ_rels[succ_rel] += ip_c#.add(trip) #IP级别
                    valley[self.valleystate[prev_rel+succ_rel]] += ip_c#.add(trip) #IP级别
                    prev_or_succ_rels[prev_rel == '4' or succ_rel == '4'] += 1
                    # succ_rels_trace_weight[succ_rel] += trace_c
                    # valley_trace_weight[self.valleystate[prev_rel+succ_rel]] += trace_c
                else:
                    succ_rels[''] += ip_c#.add(trip) #IP级别
                    valley[self.valleystate[prev_rel]] += ip_c#.add(trip) #IP级别
                    prev_or_succ_rels[prev_rel == '4'] += 1
                    # succ_rels_trace_weight[''] += trace_c
                    # valley_trace_weight[self.valleystate[prev_rel]] += trace_c
            sum_valley_value = sum(valley.values())
            tmp['valley_normal_rate'] = valley['normal'] / sum_valley_value if 'normal' in valley.keys() else 0
            tmp['valley_abnormal_rate'] = valley['abnormal'] / sum_valley_value if 'abnormal' in valley.keys() else 0
            tmp['valley_seminormal_rate'] = valley['semi'] / sum_valley_value if 'semi' in valley.keys() else 0
            # if pre_filter and tmp['valley_abnormal_rate'] > 0.5:
            #     return None
            # sum_valley_weight_value = sum(valley_trace_weight.values())
            # tmp['valley_normal_rate_trace_weight'] = valley_trace_weight['normal'] / sum_valley_weight_value if 'normal' in valley_trace_weight.keys() else 0
            # tmp['valley_abnormal_rate_trace_weight'] = valley_trace_weight['abnormal'] / sum_valley_weight_value if 'abnormal' in valley_trace_weight.keys() else 0
            # tmp['valley_seminormal_rate_trace_weight'] = valley_trace_weight['semi'] / sum_valley_weight_value if 'semi' in valley_trace_weight.keys() else 0
            new_res = str(new_res)
            tmp['prev_sameAS_rate'] = (prev_asn_stat.get(new_res, 0) + prev_asn_stat.get('?', 0)) / sum(prev_asn_stat.values())
            tmp['succ_sameAS_rate'] = (succ_counter.get(new_res, 0) + succ_counter.get('?', 0)) / sum(succ_counter.values())
            tmp['prev_asnrel_unknown_rate'] = prev_rels['4'] / sum(prev_rels.values()) if '4' in prev_rels.keys() else 0
            tmp['succ_asnrel_unknown_rate'] = succ_rels['4'] / sum(succ_rels.values()) if '4' in succ_rels.keys() else 0
            tmp['prev_or_succ_asnrel_unknown_rate'] = prev_or_succ_rels['4'] / sum(prev_or_succ_rels.values())
            # tmp['succ_sameAS_rate_trace_weight'] = sum([succ_counter[elem] for elem in tmp_stat]) / sum(succ_counter.values())
            # tmp['prev_asnrel_unknown_rate_trace_weight'] = prev_rels_trace_weight['4'] / sum(prev_rels_trace_weight.values()) if '4' in prev_rels_trace_weight.keys() else 0
            # tmp['succ_asnrel_unknown_rate_trace_weight'] = succ_rels_trace_weight['4'] / sum(succ_rels_trace_weight.values()) if '4' in succ_rels_trace_weight.keys() else 0
            attr = ipattr()
            for attr_name in [elem for elem in dir(ipattr) if not elem.startswith('_')]:
                setattr(attr, attr_name, tmp[attr_name])
            return attr
        else:
            trip_counter = trip_asn_counter
            prev_counter = prev_asn_stat
            tmp = {}
            if not new_res or not self.collect_cands.IsLegalASN(new_res):
                return None
            new_res = int(new_res)
            if not rib_res:
                #tmp['bdr_rib_rel'] = 0
                tmp['bdr_rib_rel'] = 0
            elif new_res in rib_res:
                tmp['bdr_rib_rel'] = 1
            elif any(self.collect_cands.checksiblings.check_sibling(elem, new_res) for elem in rib_res):
                tmp['bdr_rib_rel'] = 2
            elif any(self.collect_cands.GetRel(elem, new_res) != 4 for elem in rib_res):
                #tmp['bdr_rib_rel'] = 3
                tmp['bdr_rib_rel'] = 2
            else:
                tmp['bdr_rib_rel'] = 0
            tmp['is_ixp'] = (ip in self.collect_cands.ixp_ip_asn.keys() or IsIxpAs(new_res))
            prev_rels = Counter()
            prev_rels_trace_weight = Counter()
            succ_rels = Counter()
            succ_rels_trace_weight = Counter()
            prev_or_succ_rels = Counter()
            valley = Counter()
            valley_trace_weight = Counter()
            prev_asn_uncertain = Counter()
            succ_asn_uncertain = Counter()
            for trip, trace_c in trip_counter.items():
                trip_ip, trip_asn = trip.split('|')
                prev_ip, succ_ip = trip_ip.split(',')
                prev_asn, ori_succ_asn = trip_asn.split(',')
                prev_rel = ''
                # prev_ixp = (IsIxpIp(prev_ip.strip('*')) or IsIxpAs(prev_asn))
                # succ_ixp = (IsIxpIp(succ_ip.strip('*')) or IsIxpAs(ori_succ_asn.strip('$')))
                # if prev_ip.strip('*') in self.map_score.keys():
                #     if not isinstance(self.map_score[prev_ip.strip('*')], float):
                #         print(prev_ip)
                #         print(self.map_score[prev_ip.strip('*')])
                if (prev_ip.strip('*') not in self.map_score.keys()) or (self.map_score[prev_ip.strip('*')] >= self.collect_cands.score_threshold):
                    if self.collect_cands.IsLegalASN(prev_asn):
                        if prev_ip[-1] != '*':
                            prev_rel = str(self.collect_cands.GetRel(int(prev_asn), int(new_res)))
                        else:
                            prev_rel = str(self.collect_cands.GetRemoteRel(int(prev_asn), int(new_res)))
                # else:
                #     cur_ip_trace_idxs = set(self.trip_AS[ip].keys())
                #     prev_ip_trace_idx = set(self.succ_ips[prev_ip].strip('*').keys())
                #     succ_ip_trace_idx = set(self.prev_ips[succ_ip].strip('*').keys())
                #     tmp_idxs = cur_ip_trace_idxs & prev_ip_trace_idx
                #     tmp_idxs = tmp_idxs & succ_ip_trace_idx
                # if prev_rel == '4':
                #     if tmp['is_ixp'] and prev_ixp:
                #         prev_rel = '3'
                #prev_asn_uncertain[str(prev_asn) != str(new_res) and prev_ip[-1] == '*'] += 1
                prev_asn_uncertain[prev_rel == '' or (prev_rel == '4' and prev_ip[-1] == '*')] += 1
                prev_rels[prev_rel] += 1#.add(trip) #IP级别
                prev_rels_trace_weight[prev_rel] += trace_c
                succ_asn = ori_succ_asn.strip('$')
                succ_rel = ''
                if (succ_ip.strip('*') not in self.map_score.keys()) or self.map_score[succ_ip.strip('*')] >= self.collect_cands.score_threshold:
                    if self.collect_cands.IsLegalASN(succ_asn):
                        if succ_ip[-1] != '*':
                            succ_rel = str(self.collect_cands.GetRel(int(new_res), int(succ_asn)))
                        else:
                            succ_rel = str(self.collect_cands.GetRemoteRel(int(new_res), int(succ_asn)))
                # if succ_rel == '4':
                #     if tmp['is_ixp'] and succ_ixp:
                #         succ_rel = '3'
                #succ_asn_uncertain[str(succ_asn) != str(new_res) and succ_ip[-1] == '*'] += 1
                succ_asn_uncertain[succ_rel == '' or (succ_rel == '4' and succ_ip[-1] == '*')] += 1
                #if ori_succ_asn[-1] != '$' or succ_rel != '4':
                if True:
                    succ_rels[succ_rel] += 1#.add(trip) #IP级别
                    succ_rels_trace_weight[succ_rel] += trace_c
                    valley[self.valleystate[prev_rel+succ_rel]] += 1#.add(trip) #IP级别
                    valley_trace_weight[self.valleystate[prev_rel+succ_rel]] += trace_c
                    #prev_or_succ_rels[prev_rel == '4' or succ_rel == '4'] += 1
                else:
                    succ_rels[''] += 1#.add(trip) #IP级别
                    succ_rels_trace_weight[''] += trace_c
                    valley[self.valleystate[prev_rel]] += 1#.add(trip) #IP级别
                    valley_trace_weight[self.valleystate[prev_rel]] += trace_c
                    #prev_or_succ_rels[prev_rel == '4'] += 1
            sum_valley_value = sum(valley.values())
            sum_valley_value_trace_weight = sum(valley_trace_weight.values())
            tmp['valley_normal_rate'] = valley.get('normal', 0) / sum_valley_value
            tmp['valley_normal_rate_trace_weight'] = valley_trace_weight.get('normal', 0) / sum_valley_value_trace_weight
            tmp['valley_abnormal_rate'] = valley.get('abnormal', 0) / sum_valley_value
            tmp['valley_abnormal_rate_trace_weight'] = valley_trace_weight.get('abnormal', 0) / sum_valley_value_trace_weight
            tmp['valley_seminormal_rate'] = valley.get('semi', 0) / sum_valley_value
            tmp['valley_seminormal_rate_trace_weight'] = valley_trace_weight.get('seminormal', 0) / sum_valley_value_trace_weight
            tmp['prev_asnrel_unknown_rate'] = prev_rels.get('4', 0) / sum(prev_rels.values())
            tmp['prev_asnrel_unknown_rate_trace_weight'] = prev_rels_trace_weight.get('4', 0) / sum(prev_rels_trace_weight.values())
            tmp['succ_asnrel_unknown_rate'] = succ_rels.get('4', 0) / sum(succ_rels.values())
            tmp['succ_asnrel_unknown_rate_trace_weight'] = succ_rels_trace_weight.get('4', 0) / sum(succ_rels_trace_weight.values())
            tmp['prev_asn_uncertain_rate'] = prev_asn_uncertain.get(True, 0) / sum(prev_asn_uncertain.values())
            tmp['succ_asn_uncertain_rate'] = succ_asn_uncertain.get(True, 0) / sum(succ_asn_uncertain.values())
            new_res = str(new_res)
            prev_sameAS = Counter()
            prev_sameAS_trace_weight = Counter()
            for prev, trace_c in prev_counter.items():
                prev_ip, prev_asn = prev.split('|')
                if prev_asn == new_res:
                    prev_sameAS[1] += 1
                    prev_sameAS_trace_weight[1] += abs(trace_c)
                else:
                    if trace_c < 0:
                        prev_sameAS[1] += 1
                        prev_sameAS_trace_weight[1] += abs(trace_c)
                    elif prev_ip[-1] == '*':
                        prev_sameAS[0.5] += 1
                        prev_sameAS_trace_weight[0.5] += abs(trace_c)
                    else:
                        prev_sameAS[0] += 1
                        prev_sameAS_trace_weight[0] += abs(trace_c)
            tmp['prev_sameAS_rate'] = prev_sameAS.get(1, 0) / sum(prev_sameAS.values())
            tmp['prev_ip_uncertain_rate'] = prev_sameAS.get(0.5, 0) / sum(prev_sameAS.values())
            tmp['prev_sameAS_rate_trace_weight'] = prev_sameAS_trace_weight.get(1, 0) / sum(prev_sameAS_trace_weight.values())
            succ_sameAS = Counter()
            succ_sameAS_trace_weight = Counter()
            for succ, trace_c in succ_counter.items():
                succ_ip, succ_asn = succ.split('|')
                if succ_asn == new_res:
                    succ_sameAS[1] += 1
                    succ_sameAS_trace_weight[1] += abs(trace_c)
                else:
                    if trace_c < 0:
                        succ_sameAS[1] += 1
                        succ_sameAS_trace_weight[1] += abs(trace_c)
                    elif succ_ip[-1] == '*':
                        succ_sameAS[0.5] += 1
                        succ_sameAS_trace_weight[0.5] += abs(trace_c)
                    else:
                        succ_sameAS[0] += 1
                        succ_sameAS_trace_weight[0] += abs(trace_c)
            tmp['succ_sameAS_rate'] = succ_sameAS.get(1, 0) / sum(succ_sameAS.values())
            tmp['succ_ip_uncertain_rate'] = succ_sameAS.get(0.5, 0) / sum(succ_sameAS.values())
            tmp['succ_sameAS_rate_trace_weight'] = succ_sameAS_trace_weight.get(1, 0) / sum(succ_sameAS_trace_weight.values())
            attr = ipattr()
            for attr_name in [elem for elem in dir(ipattr) if not elem.startswith('_')]:
                setattr(attr, attr_name, tmp[attr_name])
            return attr
    
    # def ModifyAttrData(self):
    #     modified_ips = set()
    #     for ip, val in self.trip_AS.items(): #get_res_attrs.trip_AS[ips[j]][trace_idx] = ips[j-1] + ',' + succ_ip + '|' + prev_cpr_hop + ',' + succ_cpr_hop
    #         cur_ip_score = self.map_score[ip]
    #         if cur_ip_score >= self.collect_cands.score_threshold:
    #             continue
    #         for trace_idx, subval in self.prev_ips[ip].items(): #prev_ips[ips[j]][trace_idx] = ips[j-1] + '|' + hops[j-1]
    #             prev_ip, prev_hop = subval.split('|')
    #             if prev_ip in self.map_score.keys() and self.map_score[prev_ip] < self.collect_cands.score_threshold and self.map_score[prev_ip] < cur_ip_score:
    #                 self.prev_ips[ip][trace_idx] = prev_ip + '|' + '?' #将其置空
    #                 modified_ips.add(ip)
    #             # elif prev_ip in self.map_score.keys() and self.map_score[prev_ip] > cur_ip_score and prev_hop == '?':
    #             #     prev_hop = cache.get(prev_ip)
    #             #     if not prev_hop: prev_hop = '?'
    #             #     self.prev_ips[ip][trace_idx] = prev_ip + '|' + prev_hop #置回来
    #         for trace_idx, subval in self.succ_ips[ip].items(): #prev_ips[ips[j]][trace_idx] = ips[j-1] + '|' + hops[j-1]
    #             succ_ip, succ_hop = subval.split('|')
    #             if succ_hop[-1] != '$' and succ_ip in self.map_score.keys() and self.map_score[succ_ip] < self.collect_cands.score_threshold and self.map_score[succ_ip] < cur_ip_score:
    #                 self.succ_ips[ip][trace_idx] = succ_ip + '|' + '?' #将其置空
    #                 modified_ips.add(ip)
    #             # elif succ_ip in self.map_score.keys() and self.map_score[succ_ip] > cur_ip_score and succ_hop == '?':
    #             #     succ_hop = cache.get(succ_ip)
    #             #     if not succ_hop: succ_hop = '?'
    #             #     self.succ_ips[ip][trace_idx] = succ_ip + '|' + succ_hop #置回来
    #         for trace_idx, subval in val.items():
    #             trip_ip, trip_asn = subval.split('|')
    #             prev_ip, succ_ip = trip_ip.split(',')
    #             prev_asn, succ_asn = trip_asn.split(',')
    #             if prev_ip in self.map_score.keys() and self.map_score[prev_ip] < self.collect_cands.score_threshold and self.map_score[prev_ip] < cur_ip_score:
    #                 prev_asn = '?'
    #             if succ_hop[-1] != '$' and succ_ip in self.map_score.keys() and self.map_score[succ_ip] < self.collect_cands.score_threshold and self.map_score[succ_ip] < cur_ip_score:
    #                 succ_asn = '?'
    #             if prev_asn == '?' or succ_asn == '?':
    #                 self.trip_AS[ip][trace_idx] = trip_ip + '|' + prev_asn + ',' + succ_asn #将其置空
    #                 modified_ips.add(ip)
    #     return modified_ips
    
    # def ReCalScoreBasedOnOld(self):
    #     modified_ips = self.ModifyAttrData()
    #     stat = Counter()
    #     for ip in modified_ips:
    #         attr = self.GetIPAttr(ip, self.cache.get(ip))
    #         if attr:
    #             self.map_score[ip] = get_pred_result_per_attr_v3(attr, self.model).tolist()
    #         else:
    #             self.map_score[ip] = 0
    #         stat[self.map_score[ip] > self.collect_cands.score_threshold] += 1
    #     print(stat)
    #     # print(self.map_score['80.93.127.183'])
    #     # print(self.map_score['80.93.125.246'])
    
    def CheckIfModifyNow(self, ip, cur_map):
        if (ip in self.collect_cands.ixp_ip_asn.keys() or IsIxpAs(cur_map)):
            return True
        cur_ip_score = self.map_score[ip]
        prev_ips = {elem.split('|')[0].strip('*') for elem in self.prev_ips[ip].values()} #self.prev_ips[ip][trace_idx] = prev_ip + '|' + '?' #将其置空
        succ_ips = {elem.split('|')[0].strip('*') for elem in self.succ_ips[ip].values()}
        for elem in self.trip_AS[ip].values(): #get_res_attrs.trip_AS[ips[j]][trace_idx] = last_prev_ip + ',' + fst_succ_ip + '|' + prev_cpr_hop + ',' + succ_cpr_hop
            prev_ip, succ_ip = elem.split('|')[0].split(',')
            prev_ips.add(prev_ip.strip('*'))
            succ_ips.add(succ_ip.strip('*'))
        neighs = prev_ips | succ_ips
        for neigh in neighs:
            if neigh in self.map_score.keys() and self.map_score[neigh] < self.collect_cands.score_threshold:
                if neigh in self.collect_cands.ixp_ip_asn.keys() or self.map_score[neigh] < cur_ip_score - 0.1:
                    return False
        for neigh in prev_ips:
            if neigh in self.map_score.keys() and abs(self.map_score[neigh] - cur_ip_score) < 0.1:
                return False
        return True
    
def GetIpCand(ip, collect_cands, get_res_attrs, cur_map, ori_scores, ipattr_names, debug=False):
    #step0, prepare
    rib_res, trip_counter, prev_counter, succ_counter = None, None, None, None
    if considerscores:
        rib_res, trip_counter, prev_counter, succ_counter = get_res_attrs.GetIPAttr_Prepare_ForACand_ConsiderScores(ip)
    else:
        rib_res, trip_counter, prev_counter, succ_counter = get_res_attrs.GetIPAttr_Prepare_ForACand(ip)
    #step1, check IXP
    if ip in collect_cands.ixp_ip_asn.keys():
        cand = str(collect_cands.ixp_ip_asn[ip])
        if cand:
            # attr = None
            # if considerscores:
            #     attr = get_res_attrs.GetIPAttr_ForACand_ConsiderScores(ip, cand, rib_res, trip_counter, prev_counter, succ_counter)
            # else:
            #     attr = get_res_attrs.GetIPAttr_ForACand(ip, cand, rib_res, trip_counter, prev_counter, succ_counter)
            # prb = get_pred_result_per_attr_v3(attr, get_res_attrs.model)
            # if debug:
            #     print('ixp')
            #     print(cand)
            #     print(attr)
            #     print(prb)
            return [cand, 1]
    #step2, check RIB
    cands = [cur_map] + [str(elem) for elem in rib_res if str(elem) != cur_map]
    cands = [elem for elem in cands if elem != '?' and elem != '-1']
    # cand, prb = collect_cands.CheckCands(get_res_attrs, ip, cands, rib_res, trip_counter, prev_counter, succ_counter, False, debug)
    # if cand:
    #     #if prb - get_res_attrs.map_score[ip] > 0.3:
    #     #if prb.tolist() - ori_scores[ip] > 0.3:
    #     if prb.tolist() - ori_scores[ip] > 0.2:
    #         return [cand, prb]
    # #step3, check others
    # prev_cands = [elem for elem in cands]
    tmp_trip_asn_counter = Counter()
    for trip in trip_counter.keys(): #ips[j-1] + ',' + succ_ip + '|' + prev_cpr_hop + ',' + succ_cpr_hop
        trip_ip, trip_asn = trip.split('|')
        prev_ip, succ_ip = trip_ip.split(',')
        prev_asn, succ_asn = trip_asn.split(',')
        if prev_ip[-1] == '*':
            prev_asn = prev_asn + '*'
        if succ_ip[-1] == '*':
            succ_asn = succ_asn.strip('$') + '*'
        tmp_trip_asn_counter[(prev_asn, succ_asn)] += 1
    # for key in tmp_trip_asn_counter.keys():
    #     print(key)
    s_tmp_trip_asn_counter = sorted(tmp_trip_asn_counter.items(), key=lambda x:x[1], reverse=True)
    checked_cands, cands = collect_cands.GetCandsForIP(ip, cur_map, cands, get_res_attrs, s_tmp_trip_asn_counter)
    #print('%s: %d' %(cur_map, len(cands)))
    if debug:
        print(cands)
    cand, prb = collect_cands.CheckCands(get_res_attrs, ip, cands, rib_res, trip_counter, prev_counter, succ_counter, ipattr_names, True, debug)
    if cand:
        return [cand, prb]
    
    #获取prev和succ的中间asn
    cands = collect_cands.GetNeighborMidCands(set(checked_cands), s_tmp_trip_asn_counter)
    cand, prb = collect_cands.CheckCands(get_res_attrs, ip, cands, rib_res, trip_counter, prev_counter, succ_counter, ipattr_names, True, debug)
    if cand:
        return [cand, prb]
    return [None, None]

# def ModifyRelatedIps_back(ip, get_res_attrs):
#     cur_ip_score = get_res_attrs.map_score[ip]
#     related_ips = set()
#     trace_idxs = set(get_res_attrs.prev_ips[ip].keys()) | set(get_res_attrs.succ_ips[ip].keys())
#     for trace_idx in trace_idxs:
#         int_trace_idx = ''
#         if trace_idx[-2:] == '.0':
#             int_trace_idx = int(trace_idx[:-2])
#         ip_trace, dst = get_res_attrs.ip_traces[int_trace_idx].split('|')
#         ips = ip_trace.split(' ')
#         related_ips = related_ips | set(ips)
#         dst_ip, dst_asn = dst.split(',')
#         hops = []
#         for tmp_ip in ips:
#             if tmp_ip in get_res_attrs.cache.keys():
#                 hops.append(get_res_attrs.cache[tmp_ip])
#             else:
#                 hops.append('?')
#         cpr_hops, _, loop = CompressTrace(hops, dst_asn)
#         if loop:
#             print(ori_ips)
#         for j in range(len(ips)):            
#             if j > 0:
#                 #if ips[j-1] in get_res_attrs.map_score.keys() and get_res_attrs.map_score[ips[j-1]] < 0.5 and get_res_attrs.map_score[ips[j-1]] < cur_ip_score:
#                 if False:
#                     get_res_attrs.prev_ips[ips[j]][trace_idx] = ips[j-1] + '|?'
#                 else:
#                     get_res_attrs.prev_ips[ips[j]][trace_idx] = ips[j-1] + '|' + hops[j-1]
#             if ips[j] != dst_ip:  #最后一跳是dst_ip的，丢掉不管
#                 succ_ip = ips[j+1] if j < len(ips) - 1 else dst_ip
#                 succ_hop = hops[j+1] if j < len(ips) - 1 else dst_asn+'$'
#                 #if succ_hop[-1] != '$' and succ_ip in get_res_attrs.map_score.keys() and get_res_attrs.map_score[succ_ip] < 0.5 and get_res_attrs.map_score[succ_ip] < cur_ip_score:
#                 if False:
#                     get_res_attrs.succ_ips[ips[j]][trace_idx] = succ_ip + '|?'
#                 else:
#                     get_res_attrs.succ_ips[ips[j]][trace_idx] = succ_ip + '|' + succ_hop
#                 if j > 0:
#                     tmp_idx = cpr_hops.index(hops[j])
#                     prev_cpr_hop = cpr_hops[tmp_idx-1] if tmp_idx > 0 else cpr_hops[0]
#                     succ_cpr_hop = cpr_hops[tmp_idx+1] if tmp_idx < len(cpr_hops) - 1 else dst_asn+'$'
#                     last_prev_ip = ips[0]
#                     if tmp_idx > 0:
#                         last_prev_idx = hops.index(prev_cpr_hop)
#                         while hops[last_prev_idx + 1] == prev_cpr_hop: last_prev_idx += 1
#                         last_prev_ip = ips[last_prev_idx]
#                     # if last_prev_ip in get_res_attrs.map_score.keys() and get_res_attrs.map_score[last_prev_ip] < 0.5 and get_res_attrs.map_score[last_prev_ip] < cur_ip_score:
#                     #     prev_cpr_hop = '?'
#                     fst_succ_ip = dst_ip
#                     if tmp_idx < len(cpr_hops) - 1:                              
#                         fst_succ_idx = hops.index(succ_cpr_hop)
#                         fst_succ_ip = ips[fst_succ_idx]
#                     # if succ_cpr_hop[-1] != '$' and fst_succ_ip in get_res_attrs.map_score.keys() and get_res_attrs.map_score[fst_succ_ip] < 0.5 and get_res_attrs.map_score[fst_succ_ip] < cur_ip_score:
#                     #     succ_cpr_hop = '?'
#                     get_res_attrs.trip_AS[ips[j]][trace_idx] = last_prev_ip + ',' + fst_succ_ip + '|' + prev_cpr_hop + ',' + succ_cpr_hop
#     return related_ips

def StripHops(ori_ips, ori_hops):
    ips = []
    hops = []
    for i in range(0, len(ori_hops)):
        if (i == 0 or ori_ips[i] != ori_ips[i-1]) and ori_ips[i] != '*':
            ips.append(ori_ips[i])
            hops.append(ori_hops[i])
    return ips, hops

#def ModifyRelatedIps(ip, get_res_attrs):
def ModifyRelatedIps(trace_idx, get_res_attrs, need_recal_scores):
    #related_ips = []
    # trace_idxs = set(get_res_attrs.prev_ips[ip].keys()) | set(get_res_attrs.succ_ips[ip].keys())
    # for trace_idx in trace_idxs:
    if True:
        int_trace_idx = ''
        if len(trace_idx) > 2 and trace_idx[-2:] == '.0':
            int_trace_idx = int(trace_idx[:-2])
        else:
            int_trace_idx = int(trace_idx)
        ip_trace, dst = get_res_attrs.ip_traces[int_trace_idx].split('|')
        ori_ips = ip_trace.split(' ')
        if "80.249.208.50" in ori_ips:
            a = 1
        dst_ip, dst_asn = dst.split(',')
        ori_hops = []
        for ip in ori_ips:
            if ip == '*': ori_hops.append('*')
            else: ori_hops.append(get_res_attrs.cache.get(ip, '?'))
        ips, hops = StripHops(ori_ips, ori_hops) #把*去掉
        #related_ips = [elem for elem in ips]
        if len(ips) != len(hops):
            print('ips and hops not consistent!')
        #cpr_hops, tmp_modify, loop = CompressTrace(hops, bgp_hops[-1]) #把重复的，？和-1都去掉
        # for j, val in tmp_modify.items():
        #     modify[ips[j]].add(val)
        for j in range(len(ips)):
            #ip_trace_idxs[ips[j]].add(trace_idx)
            cur_ip = ips[j]
            if cur_ip in need_recal_scores.keys():
                need_recal_scores[cur_ip] = 1
            cur_hop = hops[j]
            ori_ip_idx = ori_ips.index(cur_ip)
            #is_ixp[cur_ip] = (IsIxpIp(cur_ip) or IsIxpAs(cur_hop))                    
            if j > 0:
                prev_ip = ips[j-1]
                ori_prev_ip_idx = ori_ips.index(prev_ip)
                stars_flag = '*' if (ori_ip_idx - ori_prev_ip_idx > 1) else ''
                get_res_attrs.prev_ips[cur_ip][trace_idx] = prev_ip + stars_flag + '|' + hops[j-1]
            if cur_ip != dst_ip:  #最后一跳是dst_ip的，丢掉不管
                succ_ip = ips[j+1] if j < len(ips) - 1 else dst_ip
                stars_flag = '*'
                if j < len(ips) - 1:
                    ori_succ_ip_idx = ori_ips.index(ips[j + 1])
                    if ori_succ_ip_idx - ori_ip_idx == 1:
                        stars_flag = ''
                succ_hop = hops[j+1] if j < len(ips) - 1 else dst_asn+'$'
                get_res_attrs.succ_ips[cur_ip][trace_idx] = succ_ip + stars_flag + '|' + succ_hop
                if j > 0:
                    prev_k = j - 1
                    if not get_res_attrs.collect_cands.IsLegalASN(cur_hop):
                        while prev_k > 0 and not get_res_attrs.collect_cands.IsLegalASN(hops[prev_k]): prev_k = prev_k - 1
                    else:
                        while prev_k > 0 and (not get_res_attrs.collect_cands.IsLegalASN(hops[prev_k]) or hops[prev_k]==cur_hop): prev_k = prev_k - 1
                    prev_hop = hops[prev_k]
                    last_prev_ip = ips[prev_k]
                    ori_last_prev_ip_idx = ori_ips.index(last_prev_ip)
                    if any(not get_res_attrs.collect_cands.IsLegalASN(elem) for elem in ori_hops[ori_last_prev_ip_idx+1:ori_ip_idx]):
                        last_prev_ip = last_prev_ip + '*'
                    succ_k = j + 1
                    if not get_res_attrs.collect_cands.IsLegalASN(cur_hop):
                        while succ_k < len(hops) and not get_res_attrs.collect_cands.IsLegalASN(hops[succ_k]): succ_k = succ_k + 1
                    else:
                        while succ_k < len(hops) and (not get_res_attrs.collect_cands.IsLegalASN(hops[succ_k]) or hops[succ_k]==cur_hop): succ_k = succ_k + 1
                    succ_hop = hops[succ_k] if succ_k < len(hops) else dst_asn+'$'
                    fst_succ_ip = ips[succ_k] if succ_k < len(hops) else dst_ip+'*'
                    if succ_k < len(hops):
                        ori_fst_succ_ip_idx = ori_ips.index(fst_succ_ip.strip('*'))
                        if any(not get_res_attrs.collect_cands.IsLegalASN(elem) for elem in ori_hops[ori_ip_idx+1:ori_fst_succ_ip_idx]):
                                fst_succ_ip = fst_succ_ip + '*'
                    get_res_attrs.trip_AS[cur_ip][trace_idx] = last_prev_ip + ',' + fst_succ_ip + '|' + prev_hop + ',' + succ_hop
    #return related_ips

def DebugTest3():
    with open('/home/slt/code/ana_c_d_incongruity/ipattr_score_ams-nl.json', 'r') as rf:
        data = json.load(rf)
        stat = Counter()
        for score in data.values():
            stat[int(score * 10)] += 1
        s = sorted(stat.items(), key=lambda x:x[0])
        print(s)
        
def DebugTest5(recal=False):
    vp = 'ams-nl'
    scores = {}
    if not recal:
        with open('/home/slt/code/ana_c_d_incongruity/ipattr_score_%s.json' %vp, 'r') as rf:
            scores = json.load(rf)
    else:
        with open('/home/slt/code/ana_c_d_incongruity/ipattr_score_recal_%s.json' %vp, 'r') as rf:
            scores = json.load(rf)
    state_scores = defaultdict(list)
    with open('/home/slt/code/ana_c_d_incongruity/modify_ip_mapping_status_%s.json' %vp, 'r') as rf:
    #with open('/home/slt/code/ana_c_d_incongruity/modify4_ip_mapping_status.json', 'r') as rf:
        data = json.load(rf)
        for ip, val in data.items():
            state = val[0]
            #state = val[1]
            state_scores[state].append(scores.get(ip, -1))
    s_stat = {}
    for state, scores in state_scores.items():
        tmp = Counter()
        for score in scores:
            tmp[int(score * 10)] += 1
        s_stat[state] = sorted(tmp.items(), key=lambda x:x[0])
    for state, val in s_stat.items():
        print(state)
        print(val)

def DebugTest4():
    attr = ipattr()
    attr.valley_normal_rate = 1
    attr.valley_abnormal_rate = 0 
    attr.valley_seminormal_rate = 0 
    attr.prev_asnrel_unknown_rate = 0
    attr.succ_asnrel_unknown_rate =  0
    model = xgb.XGBClassifier()
    model.load_model('/home/slt/code/ML_Bdrmaplt/xgboost_model_v0.model')
    score = {}
    for attr.bdr_rib_rel in range(5):
        print(attr.bdr_rib_rel)
        for attr.succ_sameAS_rate in numpy.arange(0, 1.1, 0.1):
            for attr.prev_sameAS_rate in numpy.arange(0, 1.1, 0.1):
                tmp = get_pred_result_per_attr_v3_old(attr, model)        
                print(round(tmp, 2), end=' ')
            print('')
        print('')
    print(score)

def S_IsLegalASN(asn):
    return asn and asn.isdigit() and int(asn) < 0xFFFFFF and int(asn) > 0

def DebugCheckModifiedMappings():
    # ConnectToBdrMapItDb('/mountdisk1/ana_c_d_incongruity/out_bdrmapit/sxt_bdr/bdrmapit_ams-nl_20220215.db')
    # ConstrBdrCache()
    # cache = GetBdrCache()
    # print(len(cache))
    # return
    # data1 = {}
    # with open('/home/slt/code/ana_c_d_incongruity/ipattr_trip_AS_ams-nl.json', 'r') as rf:
    #     data1 = json.load(rf)
    #     print(len(data1))
    # return
    data1 = {}
    with open('/home/slt/code/ana_c_d_incongruity/modify_ip_mapping_status_ams-nl.json', 'r') as rf:
        data1 = json.load(rf)
    data2 = {}
    with open('/home/slt/code/ana_c_d_incongruity/model_modified_ip_mappings_ams-nl.json', 'r') as rf:
        data2 = json.load(rf)
    
    keyset1 = set(data1.keys())
    keyset2 = set(data2.keys())
    unique1 = keyset1.difference(keyset2)
    print('unique1: %d' %len(unique1))
    unique2 = keyset2.difference(keyset1)
    print('unique2: %d' %len(unique2))
    unknown_to_known = 0
    known_to_unknown = 0
    keep_unknown = 0
    succ_to_others = set()
    others_to_succ = 0
    same = 0
    for key in keyset1 & keyset2:
        state1, asn1 = data1[key]
        asn2 = data2[key]
        if asn1 != asn2:
            if not S_IsLegalASN(asn1):
                if S_IsLegalASN(asn2):
                    unknown_to_known += 1
                else:
                    keep_unknown += 1
            else:
                if not S_IsLegalASN(asn2):
                    known_to_unknown += 1
                else:
                    if state1 == 'succ':
                        succ_to_others.add(key)
                    else:
                        others_to_succ += 1
        else:
            same += 1
    print('unknown_to_known: %d' %unknown_to_known)
    print('known_to_unknown: %d' %known_to_unknown)
    print('keep_unknown: %d' %keep_unknown)
    print('succ_to_others: %d' %len(succ_to_others))
    print('others_to_succ: %d' %others_to_succ)
    print(succ_to_others)

def DebugCheckMM(vp):
    old_match_dsts = {}
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/match_%s.20220215' %(vp, vp), 'r') as rf:
        lines = [rf.readline() for _ in range(3)]
        while lines[0]:
            old_match_dsts[lines[0][1:].split(']')[0]] = lines
            lines = [rf.readline() for _ in range(3)]
    new_match_dsts = {}
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map/match_%s.20220215' %(vp, vp), 'r') as rf:
        lines = [rf.readline() for _ in range(3)]
        while lines[0]:
            new_match_dsts[lines[0][1:].split(']')[0]] = lines
            lines = [rf.readline() for _ in range(3)]
    # with open('debug_new_mm', 'w') as wf:
    #     with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/ams-nl/ml_map/mm_ams-nl.20220215', 'r') as rf:
    #         lines = [rf.readline() for _ in range(3)]
    #         while lines[0]:
    #             mm_dst = lines[0][1:].split(']')[0]
    #             if mm_dst in old_match_dsts:
    #                 wf.write(''.join(lines))
    #                 wf.write(''.join(old_match_dsts[mm_dst]))
    #                 wf.write('\n')
    #             lines = [rf.readline() for _ in range(3)]
    old_mm_dsts = {}
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/sxt_bdr/mm_%s.20220215' %(vp, vp), 'r') as rf:
        lines = [rf.readline() for _ in range(3)]
        while lines[0]:
            old_mm_dsts[lines[0][1:].split(']')[0]] = lines
            lines = [rf.readline() for _ in range(3)]
    new_mm_dsts = {}
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map/mm_%s.20220215' %(vp, vp), 'r') as rf:
        lines = [rf.readline() for _ in range(3)]
        while lines[0]:
            new_mm_dsts[lines[0][1:].split(']')[0]] = lines
            lines = [rf.readline() for _ in range(3)]
    print('common match: %d' %len(set(old_match_dsts.keys()) & set(new_match_dsts.keys())))
    print('common mm: %d' %len(set(old_mm_dsts.keys()) & set(new_mm_dsts.keys())))    
    print('solved mm: %d' %len(set(old_mm_dsts.keys()).difference(set(new_mm_dsts.keys()))))
    print('new mm: %d' %len(set(new_mm_dsts.keys()).difference(set(old_mm_dsts.keys()))))
    with open('common_mm', 'w') as wf:
        for key in set(old_mm_dsts.keys()) & set(new_mm_dsts.keys()):
            wf.write(''.join(new_mm_dsts[key]))
            wf.write(''.join(old_mm_dsts[key]))
            wf.write('\n')
    with open('new_mm', 'w') as wf:
        for key in set(old_match_dsts.keys()) & set(new_mm_dsts.keys()):
            wf.write(''.join(new_mm_dsts[key]))
            wf.write(''.join(old_match_dsts[key]))
            wf.write('\n')
    
# def DebugCheckCandScores():    
#     vp = 'ams-nl'
#     scores = {}
#     with open('/home/slt/code/ana_c_d_incongruity/ipattr_score_ams-nl.json', 'r') as rf:
#         scores = json.load(rf)            
#     mapping_res = defaultdict(defaultdict)
#     with open('/home/slt/code/ana_c_d_incongruity/modify_ip_mapping_status_%s.json' %vp, 'r') as rf:
#         data = json.load(rf)
#         for ip, val in data.items():
#             if ip in scores.keys() and scores[ip] < 0.5:
#                 state, asn = val
#                 mapping_res[state][ip] = [asn, scores[ip]]
#     date = '20220215'
#     collect_cands = CollectCandidates(date, vp)
#     get_res_attrs = GetResAttr(date, vp, collect_cands)
#     res = defaultdict(Counter)
#     rec = defaultdict(defaultdict)
#     i = 0
#     for state, val in mapping_res.items():
#         if state != 'succ':
#             continue
#         for ip, subval in val.items():
#             asn, score = subval
#             if score < 0.1:
#                 print(ip)
#                 cand, prb = GetIpCand(ip, collect_cands, get_res_attrs, asn)
#                 if not cand:
#                     res[state][-2] += 1
#                 elif cand == asn:
#                     res[state][-1] += 1
#                 else:
#                     if prb < score:
#                         print('error: {}: {}, {}'.format(ip, prb, score))
#                         continue
#                     res[state][int((prb-score)*100)] += 1
#                     rec[state][ip] = [asn, score, cand, prb]
#                     # if i > 2:
#                     #     break
#                     # i += 1
#     s_rec = sorted(rec['succ'].items(), key=lambda x:(x[1][3]-x[1][1]), reverse=True)
#     print(s_rec)
    
def ReConstrAttrScores_ConsiderScores(vp, date, collect_cands, get_res_attrs):
    print('begin reconstruct attr scores in %s' %vp)    
    # 加载模型
    model = xgb.XGBClassifier()
    model.load_model('/home/slt/code/ML_Bdrmaplt/xgboost_model_v0.model')
    ips = None
    # #先刷新get_res_attrs.map_score
    # if os.path.exists('/home/slt/code/ana_c_d_incongruity/ipattr_score_recal_%s.json' %vp):
    #     with open('/home/slt/code/ana_c_d_incongruity/ipattr_score_recal_%s.json' %vp, 'r') as rf:
    #         get_res_attrs.map_score = json.load(rf)
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ipattr_trip_AS_%s_%s.json' %(vp, vp, date), 'r') as rf:
        data = json.load(rf)
        ips = data.keys()
    stat = Counter()
    print('begin')
    ips_not_in_bdr = set()
    map_score = {}
    i = 0
    for ip in ['80.249.212.241']:#ips:
        if ip not in get_res_attrs.cache.keys() or not collect_cands.IsLegalASN(get_res_attrs.cache[ip]):
            map_score[ip] = 0
            ips_not_in_bdr.add(ip)
            continue
        attr = None
        if considerscores:
            rib_res, trip_asn_counter, prev_asn_stat, succ_counter = get_res_attrs.GetIPAttr_Prepare_ForACand_ConsiderScores(ip)
            attr = get_res_attrs.GetIPAttr_ForACand_ConsiderScores(ip, get_res_attrs.cache.get(ip), rib_res, trip_asn_counter, prev_asn_stat, succ_counter)
        else:
            rib_res, trip_asn_counter, prev_asn_stat, succ_counter = get_res_attrs.GetIPAttr_Prepare_ForACand(ip)
            attr = get_res_attrs.GetIPAttr_ForACand(ip, get_res_attrs.cache.get(ip), rib_res, trip_asn_counter, prev_asn_stat, succ_counter)
        map_score[ip] = get_pred_result_per_attr_v3_old(attr, get_res_attrs.model).tolist()
        if i % 10000 == 0: print(i)
        i += 1
        stat[map_score[ip] > collect_cands.score_threshold] += 1 
    get_res_attrs.map_score.update(map_score)
    print(stat)
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ipattr_score_recal_%s_%s.json' %(vp, vp, date), 'w') as wf:
        json.dump(get_res_attrs.map_score, wf, indent=1)

def DebugCmpScores():
    ori_score = {}
    with open('/home/slt/code/ana_c_d_incongruity/ipattr_score_ams-nl.json', 'r') as rf:
        ori_score = json.load(rf)
    cur_score = {}
    with open('/home/slt/code/ana_c_d_incongruity/ipattr_score_recal_ams-nl.json', 'r') as rf:
        cur_score = json.load(rf)
    cmp_data = defaultdict(lambda:defaultdict(list))
    stat = defaultdict(Counter)
    for ip, ori_v in ori_score.items():
        if (int(ori_v * 10) - 5) * (int(cur_score[ip] * 10) - 5) < 0:
            cmp_data[int(ori_v * 10)][int(cur_score[ip] * 10)].append(ip)
            stat[int(ori_v * 10)][int(cur_score[ip] * 10)] += 1
    # with open('cmp_scores.json', 'w') as wf:
    #     json.dump(cmp_data, wf, indent=1)
        # if int(ori_v * 10) < 2:
        #     stat[int(ori_v * 10)][int(cur_score[ip] * 10)] += 1
    print(stat)

def CheckFailedIP(vp, date):
    scores = {}
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ipattr_score_recal_%s_%s.json' %(vp, vp, date), 'r') as rf:
        scores = json.load(rf)
    not_in_scores = []
    low_scores = []
    others = []
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/ipclass_nodstip_%s.%s.json' %(vp, vp, date), 'r') as rf:
        data = json.load(rf)
        for ip, val in data['fail'].items():
            if val[0] == 1:
                if ip not in scores.keys():
                    not_in_scores.append(ip)
                elif scores[ip] < 0.5:
                    low_scores.append(ip)
                else:
                    others.append(ip)
    ip_case_indxs = defaultdict(set)
    cases = []
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/mm_%s.%s' %(vp, vp, date), 'r') as rf:
        lines = [rf.readline() for _ in range(3)]
        i = 0
        while lines[0]:
            mm_ips, pm_ips, _ = lines[2].split(']')
            for ip in mm_ips[1:].split(','):
                ip_case_indxs[ip].add(i)
            for ip in pm_ips[1:].split(','):
                ip_case_indxs[ip].add(i)
            cases.append(''.join(lines))
            i += 1
            lines = [rf.readline() for _ in range(3)]
    with open('low_occurence_low_score_trace', 'w') as wf:
        for ip in low_scores:
            if ip in ip_case_indxs.keys():
                wf.write(ip + '\n')
                for indx in ip_case_indxs[ip]:
                    wf.write(cases[indx])
    with open('low_occurence_not_in_score_trace', 'w') as wf:
        for ip in not_in_scores:
            if ip in ip_case_indxs.keys():
                wf.write(ip + '\n')
                for indx in ip_case_indxs[ip]:
                    wf.write(cases[indx])
    with open('low_occurence_other_trace', 'w') as wf:
        for ip in others:
            if ip in ip_case_indxs.keys():
                wf.write(ip + '\n')
                for indx in ip_case_indxs[ip]:
                    wf.write(cases[indx])
            
    #print(res)
    print(len(not_in_scores))
    print(len(low_scores))
    print(len(others))

def DebugCheck3257_6461():
    a = 0
    b = 0
    c = Counter()
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/ams-nl/ml_map/mm_ams-nl.20220215', 'r') as rf:
        lines = [rf.readline() for _ in range(3)]
        while lines[0]:
            trace_hops = lines[0].split(']')[-1].strip('\n').split(' ')
            bgp_hops = lines[1].strip('\t').strip('\n').split(' ')
            mm_ips, pm_ips, _ = lines[2].split(']')
            for ip in mm_ips[1:].split(','):
                c[ip] += 1
            for ip in pm_ips[1:].split(','):
                c[ip] += 1
            if '3257' in trace_hops and '6461' in bgp_hops:
                a += 1
            b += 1
            lines = [rf.readline() for _ in range(3)]    
    print(a)
    print(b)
    s = sorted(c.items(), key=lambda x:x[1], reverse=True)
    print(s[:100])
    print(len(s))

def DebugModify4():
    res = {}
    with open("modify4_ip_mapping_status.json", 'r') as rf:
        res = json.load(rf)
    ips = set(res.keys())
    ips2 = set()
    fns = ['/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/ams-nl/sxt_bdr/ana_compare_res/continuous_mm.ams-nl.20220215', '/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/ams-nl/sxt_bdr/ana_compare_res/truncate_mm.ams-nl.20220215']
    for fn in fns:
        with open(fn, 'r') as rf:
            lines = [rf.readline() for _ in range(3)]
            while lines[0]:
                ips2 = ips2 | set(lines[2].split(']')[-1].strip('\n').split(' '))
                lines = [rf.readline() for _ in range(3)]
    print(len(ips))
    print(len(ips & ips2))
    stat = Counter()
    for ip in ips & ips2:
        stat[res[ip][1]] += 1
    print(stat)
    
def get_mapping_status_v2(vp, date):
    print('{}{}'.format(date, vp))
    rm_dsts = set()
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ori_bdr_filtersndmm/real_mm_%s.%s' %(vp, vp, date), 'r') as rf:
        lines = [rf.readline() for _ in range(3)]
        while lines[0]:
            dst_ip, _ = lines[0].split(']')
            rm_dsts.add(dst_ip[1:])
            lines = [rf.readline() for _ in range(3)]
    trips = defaultdict(set)
    status = defaultdict(Counter)
    fns = ['/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ori_bdr_filtersndmm/match_%s.%s' %(vp, vp, date), \
            '/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ori_bdr_filtersndmm/mm_%s.%s' %(vp, vp, date)]
    for fn in fns:
        with open(fn, 'r') as rf:
            lines = [rf.readline() for _ in range(3)]
            while lines[0]:
                dst_ip, _ = lines[0].split(']')
                elems = lines[2].strip('\n').split(']')
                mm_ips, pm_ips, ips = '', '', ''
                if "58.138.85.222" in elems[-1]:
                    a = 1
                if len(elems) == 3:
                    if dst_ip[1:] in rm_dsts:
                        lines = [rf.readline() for _ in range(3)]
                        continue
                    mm_ips, pm_ips, ips = elems
                else:
                    ips = elems[0]
                ip_list = ips.split(' ')
                for i in range(1, len(ip_list)):
                    ip = ip_list[i]
                    triple_key = None
                    if i < len(ip_list) - 1:
                        triple_key = ip_list[i-1]+','+ip_list[i+1] 
                    elif ip != dst_ip:
                        triple_key = ip_list[i-1]+','+dst_ip
                    if triple_key:
                        trips[ip].add(triple_key)
                        if ip in mm_ips:
                            status[ip]['mm'] += 1
                        elif ip in pm_ips:
                            status[ip]['partial'] += 1
                        else:
                            status[ip]['match'] += 1
                lines = [rf.readline() for _ in range(3)]
    
    res = {}
    for ip, val in trips.items():
        if len(val) < 5:
            continue
        if status[ip]['match'] > 10 * status[ip]['mm']:
            res[ip] = 'succ'
        elif status[ip]['mm'] > 10 * status[ip]['match'] or status[ip]['partial'] > 10 * status[ip]['match']:
            res[ip] = 'fail'
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ori_bdr_filtersndmm/for_ml_ipclass_%s_%s' %(vp, vp, date), 'w') as wf:
        json.dump(res, wf, indent=1)
    print('{}{} end'.format(vp, date))

def get_mapping_status_v2_par():
    pool = Pool(processes=80)
    paras = []
    fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/*/ml_map_filtersndmm/ipattr_traces*')
    for fn in fns:
        elems = fn.split('_')
        vp = elems[-2]
        date = elems[-1].split('.')[0]
        paras.append((vp, date))
    pool.starmap(get_mapping_status_v2, paras)
    pool.close()
    pool.join()
    
def CheckModelScore(vp, date):
    tn, fn, tp, fp = 0, 0, 0, 0
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/ipattr_score_%s_%s.json' %(vp, vp, date), 'r') as rf:
        predicate = json.load(rf)
        with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ori_bdr_filtersndmm/for_ml_ipclass_%s_%s' %(vp, vp, date), 'r') as rf:
            truth = json.load(rf)
            for ip, val in truth.items():
                if ip not in predicate.keys():
                    continue
                if predicate[ip] >= 0.5:
                    if val == 'succ':
                        tp += 1
                    else:
                        fp += 1
                else:
                    if val == 'fail':
                        tn += 1
                    else:
                        fn += 1
    precision = tp / (tp + fp)
    recall = tp / (tp + fn)
    f1 = tp / (tp + (fp + fn) / 2)
    spc = tn / (tn + fp)
    return [precision, recall, f1, spc]
    # print(precision)
    # print(recall)
    # print(f1)
    # print(spc)

def CheckModelScore_Par():
    precisions = []
    recalls = []
    f1s = []
    spcs = []
    fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/*/ml_map_filtersndmm/ipattr_trip_AS_*.json')
    for fn in fns:
        elems = fn.split('_')
        vp = elems[-2]
        date = elems[-1].split('.')[0]
        [precision, recall, f1, spc] = CheckModelScore(vp, date)
        print('{}, {}, {}, {}'.format(precision, recall, f1, spc))
        precisions.append(precision)
        recalls.append(recall)
        f1s.append(f1)
        spcs.append(spc)
    # print(precisions)
    # print(recalls)
    # print(f1s)
    # print(spcs)
    
def ConstrTrainingData():
    states = defaultdict(set)
    for vp in ['ams-nl', 'nrt-jp', 'sao-br']:
        ConnectToBdrMapItDb('/mountdisk1/ana_c_d_incongruity/out_bdrmapit/ori_bdr/bdrmapit_%s_20220215.db' %vp)
        ConstrBdrCache()
        with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ori_bdr_filtersndmm/for_ml_ipclass_%s_20220215' %(vp, vp), 'r') as rf:
            data = json.load(rf)
            for ip, state in data.items():
                if ip == "38.99.196.142":
                    a = 1
                cur_map = GetIp2ASFromBdrMapItDb(ip)
                if str(cur_map) == "-1" or not cur_map:
                    continue
                states[ip].add((cur_map, state))
        CloseBdrMapItDb()
        InitBdrCache()
    rec = {ip:list(val)[0] for ip, val in states.items() if len(val) == 1}
    print(len(rec))
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/modify5_ip_mapping_status.json', 'w') as wf:
        json.dump(rec, wf, indent=1)
        
            
def ConstrTrainingData_perDate(date):
    states = defaultdict(set)
    fns = glob.glob('/mountdisk1/ana_c_d_incongruity/out_bdrmapit/ori_bdr/bdrmapit_*_%s*.db' %date[:6])
    for fn in fns:
        elems = fn.split('_')
        vp = elems[-2]
        if vp not in ['ams-nl', 'sao-br', 'nrt-jp', 'sjc2-us', 'syd-au']:
            continue
        cur_date = elems[-1].split('.')[0]    
        print(vp+cur_date)
        ConnectToBdrMapItDb(fn)
        ConstrBdrCache()
        with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ori_bdr_filtersndmm/for_ml_ipclass_%s_%s' %(vp, vp, cur_date), 'r') as rf:
            data = json.load(rf)
            for ip, state in data.items():
                if ip == "38.99.196.142":
                    a = 1
                cur_map = GetIp2ASFromBdrMapItDb(ip)
                if str(cur_map) == "-1" or not cur_map:
                    continue
                states[ip].add((cur_map, state))
        CloseBdrMapItDb()
        InitBdrCache()
        print(vp+cur_date+' end')
    rec = {ip:list(val)[0] for ip, val in states.items() if len(val) == 1}
    print(len(rec))
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/modify5_ip_mapping_status_%s.json' %date, 'w') as wf:
        json.dump(rec, wf, indent=1)

def ConstrTrainingData_par():
    paras = []
    fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/*/ml_map_filtersndmm/ipattr_traces*')
    date = {fn.split('_')[-1][:6] for fn in fns}
    paras = list(date)
    pool = Pool(processes=len(paras))
    pool.map(ConstrTrainingData_perDate, paras)
    pool.close()
    pool.join()

def GetIPAttr_par():
    paras = defaultdict(list)
    fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/*/ml_map_filtersndmm/ipattr_traces_*')
    for fn in fns:
        elems = fn.split('_')
        vp = elems[-2]
        date = elems[-1].split('.')[0]
        if not os.path.exists('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/ipattr_trip_AS_%s_%s.json' %(vp, vp, date)):
            paras[date[:6]].append((date, vp))
    for date, val in paras.items():
        pool = Pool(processes=len(val))
        pool.starmap(GetIPAttr, val)
        pool.close()
        pool.join()

def CalScores_ForValidate(vp, date):
    print('begin construct attr scores in %s%s' %(date, vp))
    ipattr_names = sorted([elem for elem in dir(ipattr) if not elem.startswith('_')])
    map_score = {}
    test_data1 = {}
    if not os.path.exists('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ori_bdr_filtersndmm/for_ml_ipclass_%s_%s' %(vp, vp, date)):
        print('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ori_bdr_filtersndmm/for_ml_ipclass_%s_%s not exists' %(vp, vp, date))
        return
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ori_bdr_filtersndmm/for_ml_ipclass_%s_%s' %(vp, vp, date), 'r') as rf:
        test_data1 = json.load(rf)
    test_data2 = {}
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/modify5_ip_mapping_status_%s.json' %date[:6], 'r') as rf:
        test_data2 = json.load(rf)
    tmp_attrs = []
    tmp_ips = []
    collect_cands = CollectCandidates(date, vp)
    get_res_attrs = GetResAttr(date, vp, collect_cands)
    for ip in set(test_data1.keys()) & set(test_data2.keys()):
        rib_res, trip_asn_counter, prev_asn_stat, succ_counter = get_res_attrs.GetIPAttr_Prepare_ForACand(ip)
        attr = get_res_attrs.GetIPAttr_ForACand(ip, test_data2[ip][0], rib_res, trip_asn_counter, prev_asn_stat, succ_counter)
        if not attr:
            print(ip)
            print(test_data1[ip])
            print(test_data2[ip])
        tmp_attrs.append(attr)
        tmp_ips.append(ip)
        # if len(tmp_attrs) % 10000 == 0:
        #     print(len(tmp_attrs))
    tmp_scores = get_pred_result_per_attr_v3(tmp_attrs, get_res_attrs.model, ipattr_names)
    for tmp_i in range(len(tmp_scores)):
        map_score[tmp_ips[tmp_i]] = tmp_scores[tmp_i].tolist()
    # with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/for_validate_ipattr_score_%s_%s.json' %(vp, vp, date), 'w') as wf:
    #     json.dump(map_score, wf, indent=1)
    succ_c = 0
    fail_c = 0
    tn, fn, tp, fp = 0, 0, 0, 0
    for ip, score in map_score.items():
        #print(test_data2[ip])
        if score < 0.5: #fail
            if test_data2[ip][1] == 'succ':
                succ_c += 1
                fn += 1
            else:
                fail_c += 1
                tn += 1
        else:
            if test_data2[ip][1] == 'succ':
                succ_c += 1
                tp += 1
            else:
                fail_c += 1
                fp += 1
    precision = tp / (tp + fp)
    recall = tp / (tp + fn)
    f1 = tp / (tp + (fp + fn) / 2)
    spc = tn / (tn + fp)
    print('{}, {}: {}, {}, {}, {}'.format(succ_c, fail_c, precision, recall, f1, spc))

def CalScores_ForValidate_par():    
    paras = defaultdict(list)
    fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/*/ml_map_filtersndmm/ipattr_traces_*')
    #done = {'201810', '201908', '202008', '201811', '201903', '202101', '202005', '201909', '202202', '201803', '201802', '201902', '201808', '202201', '202109', '202104', '202010', '201910', '202106', '202006', '201907', '201805', '202012', '201801', '201809', '201806', '202111', '202112', '201911', '201905', '201904', '201912', '202007', '202108', '202011', '201804', '202004', '202009', '201812', '201906', '202110', '202002'}
    #to_check = {'201901'}
    for fn in fns:
        elems = fn.split('_')
        vp = elems[-2]
        date = elems[-1].split('.')[0]
        #if date[:6] in done or date[:6] in to_check:
        if date[:4] != '2018':
            continue
        paras[date[:6]].append((vp, date))
    for date, val in paras.items():
        pool = Pool(processes=len(val))
        pool.starmap(CalScores_ForValidate, val)
        pool.close()
        pool.join()

def debug_write_f1():
    data = {}
    with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/model_validate.json', 'r') as rf:
        data = json.load(rf)
    s = sorted(data.items(), key=lambda x:x[0])
    res1 = {}
    res2 = []
    for elem in s:        
        a = [subelem[3] for subelem in elem[1]]
        res1[elem[0]] = sum(a) / len(a)
        res2 = res2 + a
    s = sorted(res1.items(), key=lambda x:x[0])
    print(s)
    #print(res2)

g_max_thresh = 0.2#0.3#0.1
g_correct_thresh = 0.5#0.4#0.3#0.2#
    
def main_func():
    global g_max_thresh
    global g_correct_thresh
    print(g_max_thresh)
    print(g_correct_thresh)
    #CheckModelScore('ams-nl', '20220215')
    #CheckModelScore_Par()
    #GetIPAttr('20220215', 'sao-br')
    #GetIPAttr_par()
    #get_mapping_status_v2('sao-br', '20220215')
    #get_mapping_status_v2_par()
    #ConstrTrainingData()
    #ConstrTrainingData_par()
    #CalScores_ForValidate('sao-br', '20220215')
    #CalScores_ForValidate_par()
    #debug_write_f1()
    #return
    # DebugModify4()
    # return
    #CheckFailedIP(vp)
    #return
    # # DebugCheck3257_6461()
    # # #DebugCmpScores()
    # # # DebugTest5(False)
    # # # # # #DebugCheckCandScores()
    # DebugCheckMM(vp)
    # return
    #DebugCheckModifiedMappings()
    # DebugTest4()
    # with open('/home/slt/code/ana_c_d_incongruity/ipattr_score_ams-nl.json', 'r') as rf:
    #     scores = json.load(rf)            
    #     ips = ['192.42.115.1', '145.145.17.88', '145.145.176.43', '82.98.195.123', '82.98.195.122', '64.125.31.104', '50.242.151.213', '96.110.34.29', '96.110.35.126', '96.110.38.65', '96.110.35.21', '96.110.35.6', '96.110.37.150', '96.110.39.73', '96.110.43.250', '96.110.65.114', '50.217.4.186']
    #     for ip in ips:
    #         print(scores.get(ip, ''), end = ', ')
    #return
   
    ipattr_names = sorted([elem for elem in dir(ipattr) if not elem.startswith('_')])
    vps = {'sjc2-us', 'syd-au', 'nrt-jp', 'sao-br'} #'ams-nl', 
    for vp in ['sao-br']:
        if not os.path.exists('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/' %vp):
            os.mkdir('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/' %vp)
        fns = glob.glob('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ori_bdr_filtersndmm/trace_stat_%s.*' %(vp, vp))
        dates = [fn.split('.')[-1] for fn in fns]
        for date in dates:
            if date != '20220215':
                continue
            if os.path.exists('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/model_modified_ip_mappings_%s_%s_%s_%s.json' %(vp, vp, date, g_max_thresh, g_correct_thresh)):
                continue
            print(vp + date)
            if not os.path.exists('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/ipattr_trip_AS_%s_%s.json' %(vp, vp, date)):
                GetIPAttr(date, vp)
            collect_cands = CollectCandidates(date, vp)
            # trips = ['58453 18403 51964', '58453 4862 51964']
            # for trip in trips:
            #     prev, cur, succ = trip.split(' ')
            #     print(collect_cands.GetRel(int(prev), int(cur)), end='')
            #     print(collect_cands.GetRel(int(cur), int(succ)))
            # return
            get_res_attrs = GetResAttr(date, vp, collect_cands)
            if not os.path.exists('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/ipattr_score_%s_%s.json' %(vp, vp, date)):
                print('begin construct attr scores in %s' %vp)
                ips = None
                with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/ipattr_trip_AS_%s_%s.json' %(vp, vp, date), 'r') as rf:
                    data = json.load(rf)
                    ips = data.keys()
                map_score = {}
                stat = Counter()
                ips_not_in_bdr = set()
                tmp_attrs = []
                tmp_ips = []
                for ip in ips:
                    if ip not in get_res_attrs.cache.keys() or not collect_cands.IsLegalASN(get_res_attrs.cache[ip]):
                        map_score[ip] = 0
                        ips_not_in_bdr.add(ip)
                        continue
                    rib_res, trip_asn_counter, prev_asn_stat, succ_counter = get_res_attrs.GetIPAttr_Prepare_ForACand(ip)
                    attr = get_res_attrs.GetIPAttr_ForACand(ip, get_res_attrs.cache.get(ip), rib_res, trip_asn_counter, prev_asn_stat, succ_counter)
                    tmp_attrs.append(attr)
                    tmp_ips.append(ip)
                    if len(tmp_attrs) % 10000 == 0:
                        print(len(tmp_attrs))
                tmp_scores = get_pred_result_per_attr_v3(tmp_attrs, get_res_attrs.model, ipattr_names)
                for tmp_i in range(len(tmp_scores)):
                    map_score[tmp_ips[tmp_i]] = tmp_scores[tmp_i].tolist()
                    stat[map_score[tmp_ips[tmp_i]] > 0.5] += 1
                with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/ipattr_score_%s_%s.json' %(vp, vp, date), 'w') as wf:
                    json.dump(map_score, wf, indent=1)
                get_res_attrs.map_score.update(map_score)
                print('ips_not_in_bdr: %d' %(len(ips_not_in_bdr)))
                print(stat)
            # a = collect_cands.checksiblings.bgp.reltype(3356, 399782)
            # print(a)
            # print(get_res_attrs.map_score.get('168.197.23.145'))
            # ip = '100.127.5.113'
            # cands = ['?']
            # if False:
            #     rib_res, trip_asn_counter, prev_asn_stat, succ_counter = get_res_attrs.GetIPAttr_Prepare_ForACand(ip)
            #     cand, prb = collect_cands.CheckCands(get_res_attrs, ip, cands, rib_res, trip_asn_counter, prev_asn_stat, succ_counter)
            # else:
            #     cand, prb = GetIpCand(ip, collect_cands, get_res_attrs, cands[0], get_res_attrs.map_score, True)
            # ip = "145.145.176.43"
            # cur_map = get_res_attrs.cache.get(ip)
            # a = get_res_attrs.CheckIfModifyNow(ip, cur_map)
            # return
            prev_fails = {}
            #related_ips = set()
            related_ips = set()
            #get_res_attrs.ReCalScoreBasedOnOld()
            # #先处理IXP
            # for ip, score in get_res_attrs.map_score.items():
            #     if score < collect_cands.score_threshold:
            #         if ip in collect_cands.ixp_ip_asn.keys():
            #             cand = str(collect_cands.ixp_ip_asn[ip])        
            #             if cand:
            #                 get_res_attrs.cache[ip] = cand
            #                 get_res_attrs.is_ixp[ip] = True
            #                 get_res_attrs.map_score[ip] = 1
            ori_scores = {ip:val for ip, val in get_res_attrs.map_score.items()}
            # if considerscores:
            #     ReConstrAttrScores_ConsiderScores(vp, collect_cands, get_res_attrs)
            nouse_times = 0
            try_times = 0
            while True:
                fail_info = {ip:score for ip, score in get_res_attrs.map_score.items() if score < collect_cands.score_threshold}
                s_fails = sorted(fail_info.keys(), key=lambda x:fail_info[x])
                print('cur fail: {}, prev_fail: {}'.format(len(s_fails), len(prev_fails)))
                try_times += 1
                if len(prev_fails) > 0 and len(prev_fails) - len(fail_info) <= 100:
                    nouse_times += 1
                else:
                    nouse_times = 0
                if len(fail_info) < 100 or nouse_times >= 2 or try_times >= 10:
                    if try_times >= 10:
                        print('try_times too much!')
                    break
                if not prev_fails:
                    prev_fails = [elem for elem in s_fails]
                else:                
                    new_fails = set(s_fails).difference(set(prev_fails))
                    print('new fails: {}'.format(len(new_fails)))
                    prev_fails = [elem for elem in s_fails]
                modified = {}
                i = 0
                debug_ip = "109.105.98.109"
                for ip in s_fails:#need_recounts:
                    if i % 500 == 0: print(i)
                    cur_map = get_res_attrs.cache.get(ip)
                    if ip == debug_ip:
                        print('original: {}:{}, {}'.format(ip, cur_map, get_res_attrs.map_score[ip], get_res_attrs.CheckIfModifyNow(ip, cur_map)))
                    if not get_res_attrs.CheckIfModifyNow(ip, cur_map):
                        continue
                    [cand, prb] = GetIpCand(ip, collect_cands, get_res_attrs, cur_map, ori_scores, ipattr_names)
                    if cand:
                        if cand == cur_map:
                            continue
                        #if prb - get_res_attrs.map_score[ip] > 0.3:
                        if not isinstance(prb, float) and not isinstance(prb, int):
                            prb = prb.tolist()
                        #if prb - ori_scores[ip] > 0.3:
                        if prb - ori_scores[ip] > g_correct_thresh:
                            #print(prb)
                            modified[ip] = [cand, prb]
                    if ip == debug_ip:
                        if ip in modified.keys():
                            print('{}:{}'.format(ip, modified[ip]))
                    i += 1
                print('solved: %d' %len(modified))
                # if len(modified) < 100:
                #     break
                tmp_modified_trace_flags = [0 for i in range(len(get_res_attrs.ip_traces))]
                for ip, val in modified.items():
                    if ip == debug_ip:
                        a = 1
                    cand, prb = val                
                    get_res_attrs.cache[ip] = cand
                    get_res_attrs.is_ixp[ip] = (ip in get_res_attrs.collect_cands.ixp_ip_asn.keys() or IsIxpAs(cand))
                    # if isinstance(prb, float) or isinstance(prb, int):
                    #     get_res_attrs.map_score[ip] = prb
                    # else:
                    #     get_res_attrs.map_score[ip] = prb.tolist()
                    #modified_trace_idxs = modified_trace_idxs | set(get_res_attrs.prev_ips[ip].keys())
                    for trace_idx in get_res_attrs.prev_ips[ip].keys():
                        if trace_idx.__contains__('.'):
                            trace_idx = trace_idx[:-2]
                        tmp_modified_trace_flags[int(trace_idx)] = 1                        
                modified_trace_idxs = [str(i) for i in range(len(tmp_modified_trace_flags)) if tmp_modified_trace_flags[i] == 1]
                print('affected traces: %d' %len(modified_trace_idxs))
                i = 0
                need_recal_scores = {ip:0 for ip in get_res_attrs.trip_AS.keys()}
                print(debug_ip in need_recal_scores.keys())                    
                for trace_idx in modified_trace_idxs:
                    if i % 10000 == 0: print(i)
                    i += 1
                    if trace_idx == '108171':
                        a = 1
                    ModifyRelatedIps(trace_idx, get_res_attrs, need_recal_scores)
                need_recal_scores = [ip for ip, flag in need_recal_scores.items() if flag==1 and get_res_attrs.map_score[ip] < 1]
                print(debug_ip in need_recal_scores)
                print('begin recalculate attr: %d' %len(need_recal_scores))
                i = 0
                tmp_attrs = []
                r_ips = []
                for r_ip in need_recal_scores:
                    if i % 500 == 0: print(i)
                    i += 1        
                    if r_ip == debug_ip:
                        a = 1
                    # rib_res, trip_asn_counter, prev_asn_stat, succ_counter = get_res_attrs.GetIPAttr_Prepare_ForACand(r_ip)
                    # tmp_attr = get_res_attrs.GetIPAttr_ForACand(r_ip, get_res_attrs.cache.get(r_ip), rib_res, trip_asn_counter, prev_asn_stat, succ_counter)
                    tmp_attr = None
                    #if considerscores:
                    if False:
                        rib_res, trip_asn_counter, prev_asn_stat, succ_counter = get_res_attrs.GetIPAttr_Prepare_ForACand_ConsiderScores(r_ip)
                        tmp_attr = get_res_attrs.GetIPAttr_ForACand_ConsiderScores(r_ip, get_res_attrs.cache.get(r_ip), rib_res, trip_asn_counter, prev_asn_stat, succ_counter)
                    else:
                        rib_res, trip_asn_counter, prev_asn_stat, succ_counter = get_res_attrs.GetIPAttr_Prepare_ForACand(r_ip)
                        tmp_attr = get_res_attrs.GetIPAttr_ForACand(r_ip, get_res_attrs.cache.get(r_ip), rib_res, trip_asn_counter, prev_asn_stat, succ_counter)
                        if r_ip == debug_ip:
                            print('{}:{}'.format(r_ip, tmp_attr))
                            print('{}'.format(get_pred_result_per_attr_v3_old(tmp_attr, get_res_attrs.model)))
                    if tmp_attr:
                        tmp_attrs.append(tmp_attr)
                        r_ips.append(r_ip)
                    else:
                        get_res_attrs.map_score[r_ip] = 0    
                tmp_scores = get_pred_result_per_attr_v3(tmp_attrs, get_res_attrs.model, ipattr_names)
                for i in range(len(tmp_scores)):
                    get_res_attrs.map_score[r_ips[i]] = tmp_scores[i].tolist()
                for ip, val in modified.items():
                    if val[1] == 1:
                        get_res_attrs.map_score[ip] = 1 #IXP敲死！
                # with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/ipattr_score_recal_%s_%s.json' %(vp, vp, date), 'w') as wf:
                #     json.dump(get_res_attrs.map_score, wf, indent=1)
            with open('/mountdisk1/ana_c_d_incongruity/new_out_my_anatrace/%s/ml_map_filtersndmm/model_modified_ip_mappings_%s_%s_%s_%s.json' %(vp, vp, date, g_max_thresh, g_correct_thresh), 'w') as wf:
                json.dump(get_res_attrs.cache, wf, indent=1)

def main_func_atlas():   
    ipattr_names = sorted([elem for elem in dir(ipattr) if not elem.startswith('_')])
    fns = glob.glob('/mountdisk2/common_vps/20*15')
    dates = [fn.split('/')[-1] for fn in fns]
    for date in dates:        
        #'/mountdisk2/common_vps/%s/atlas/mapped_%s' %(date, date)
        if date[:6] == '202208':
            continue
        if os.path.exists('/mountdisk2/common_vps/%s/atlas/model_modified_ip_mappings_%s.json' %(date, date)):
            continue
        print(date)
        if not os.path.exists('/mountdisk2/common_vps/%s/atlas/ipattr_trip_AS_%s.json' %(date, date)):
            GetIPAttr(date, None)
        collect_cands = CollectCandidates(date, None)
        # trips = ['58453 18403 51964', '58453 4862 51964']
        # for trip in trips:
        #     prev, cur, succ = trip.split(' ')
        #     print(collect_cands.GetRel(int(prev), int(cur)), end='')
        #     print(collect_cands.GetRel(int(cur), int(succ)))
        # return
        get_res_attrs = GetResAttr(date, None, collect_cands)
        if not os.path.exists('/mountdisk2/common_vps/%s/atlas/ipattr_score_%s.json' %(date, date)):
            print('begin construct attr scores on %s' %date)
            ips = None
            with open('/mountdisk2/common_vps/%s/atlas/ipattr_trip_AS_%s.json' %(date, date), 'r') as rf:
                data = json.load(rf)
                ips = data.keys()
            map_score = {}
            stat = Counter()
            ips_not_in_bdr = set()
            tmp_attrs = []
            tmp_ips = []
            for ip in ips:
                if ip not in get_res_attrs.cache.keys() or not collect_cands.IsLegalASN(get_res_attrs.cache[ip]):
                    map_score[ip] = 0
                    ips_not_in_bdr.add(ip)
                    continue
                rib_res, trip_asn_counter, prev_asn_stat, succ_counter = get_res_attrs.GetIPAttr_Prepare_ForACand(ip)
                attr = get_res_attrs.GetIPAttr_ForACand(ip, get_res_attrs.cache.get(ip), rib_res, trip_asn_counter, prev_asn_stat, succ_counter)
                tmp_attrs.append(attr)
                tmp_ips.append(ip)
                if len(tmp_attrs) % 10000 == 0:
                    print(len(tmp_attrs))
            tmp_scores = get_pred_result_per_attr_v3(tmp_attrs, get_res_attrs.model, ipattr_names)
            for tmp_i in range(len(tmp_scores)):
                map_score[tmp_ips[tmp_i]] = tmp_scores[tmp_i].tolist()
                stat[map_score[tmp_ips[tmp_i]] > 0.5] += 1
            with open('/mountdisk2/common_vps/%s/atlas/ipattr_score_%s.json' %(date, date), 'w') as wf:
                json.dump(map_score, wf, indent=1)
            get_res_attrs.map_score.update(map_score)
            print('ips_not_in_bdr: %d' %(len(ips_not_in_bdr)))
            print(stat)
        # a = collect_cands.checksiblings.bgp.reltype(3356, 399782)
        # print(a)
        # print(get_res_attrs.map_score.get('168.197.23.145'))
        # ip = '100.127.5.113'
        # cands = ['?']
        # if False:
        #     rib_res, trip_asn_counter, prev_asn_stat, succ_counter = get_res_attrs.GetIPAttr_Prepare_ForACand(ip)
        #     cand, prb = collect_cands.CheckCands(get_res_attrs, ip, cands, rib_res, trip_asn_counter, prev_asn_stat, succ_counter)
        # else:
        #     cand, prb = GetIpCand(ip, collect_cands, get_res_attrs, cands[0], get_res_attrs.map_score, True)
        # return
        prev_fails = {}
        #related_ips = set()
        related_ips = set()
        #get_res_attrs.ReCalScoreBasedOnOld()
        # #先处理IXP
        # for ip, score in get_res_attrs.map_score.items():
        #     if score < collect_cands.score_threshold:
        #         if ip in collect_cands.ixp_ip_asn.keys():
        #             cand = str(collect_cands.ixp_ip_asn[ip])        
        #             if cand:
        #                 get_res_attrs.cache[ip] = cand
        #                 get_res_attrs.is_ixp[ip] = True
        #                 get_res_attrs.map_score[ip] = 1
        ori_scores = {ip:val for ip, val in get_res_attrs.map_score.items()}
        # if considerscores:
        #     ReConstrAttrScores_ConsiderScores(vp, collect_cands, get_res_attrs)
        nouse_times = 0
        try_times = 0
        while True:
            fail_info = {ip:score for ip, score in get_res_attrs.map_score.items() if score < collect_cands.score_threshold}
            s_fails = sorted(fail_info.keys(), key=lambda x:fail_info[x])
            print('cur fail: {}, prev_fail: {}'.format(len(s_fails), len(prev_fails)))
            try_times += 1
            #if len(prev_fails) > 0 and len(prev_fails) - len(fail_info) <= 100:
            if len(prev_fails) > 0 and len(prev_fails) - len(fail_info) <= 20:
                nouse_times += 1
            else:
                nouse_times = 0
            #if len(fail_info) < 100 or nouse_times >= 2 or try_times >= 10:
            if len(fail_info) < 20 or nouse_times >= 2 or try_times >= 10:
                if try_times >= 10:
                    print('try_times too much!')
                break
            if not prev_fails:
                prev_fails = [elem for elem in s_fails]
            else:                
                new_fails = set(s_fails).difference(set(prev_fails))
                print('new fails: {}'.format(len(new_fails)))
                prev_fails = [elem for elem in s_fails]
            modified = {}
            i = 0
            for ip in s_fails:#need_recounts:
                #print(ip)
                if i % 500 == 0: print(i)
                # if not get_res_attrs.CheckIfModifyNow(ip):
                #     continue
                cur_map = get_res_attrs.cache.get(ip)
                [cand, prb] = GetIpCand(ip, collect_cands, get_res_attrs, cur_map, ori_scores, ipattr_names)
                # if ip == "80.249.212.241" or ip == "217.170.0.252":
                #     print('{}:{}, {}|{}'.format(ip, cand, prb, ori_scores[ip]))
                if cand:
                    #if prb - get_res_attrs.map_score[ip] > 0.3:
                    if not isinstance(prb, float) and not isinstance(prb, int):
                        prb = prb.tolist()
                    #if prb - ori_scores[ip] > 0.3:
                    if prb - ori_scores[ip] > 0.2:
                        #print(prb)
                        modified[ip] = [cand, prb]
                # if ip == "80.249.212.241" or ip == "217.170.0.252":
                #     if ip in modified.keys():
                #         print('{}:{}'.format(ip, modified[ip]))
                i += 1
            print('solved: %d' %len(modified))
            # if len(modified) < 100:
            #     break
            tmp_modified_trace_flags = [0 for i in range(len(get_res_attrs.ip_traces))]
            for ip, val in modified.items():
                cand, prb = val                
                get_res_attrs.cache[ip] = cand
                get_res_attrs.is_ixp[ip] = (ip in get_res_attrs.collect_cands.ixp_ip_asn.keys() or IsIxpAs(cand))
                # if isinstance(prb, float) or isinstance(prb, int):
                #     get_res_attrs.map_score[ip] = prb
                # else:
                #     get_res_attrs.map_score[ip] = prb.tolist()
                #modified_trace_idxs = modified_trace_idxs | set(get_res_attrs.prev_ips[ip].keys())
                for trace_idx in get_res_attrs.prev_ips[ip].keys():
                    tmp_modified_trace_flags[int(trace_idx)] = 1
            modified_trace_idxs = [str(i) for i in range(len(tmp_modified_trace_flags)) if tmp_modified_trace_flags[i] == 1]
            print('affected traces: %d' %len(modified_trace_idxs))
            i = 0
            need_recal_scores = {ip:0 for ip in get_res_attrs.trip_AS.keys()}
            for trace_idx in modified_trace_idxs:
                if i % 10000 == 0: print(i)
                i += 1
                ModifyRelatedIps(trace_idx, get_res_attrs, need_recal_scores)
            need_recal_scores = [ip for ip, flag in need_recal_scores.items() if flag==1]
            print('begin recalculate attr: %d' %len(need_recal_scores))
            i = 0
            tmp_attrs = []
            r_ips = []
            for r_ip in need_recal_scores:
                if i % 500 == 0: print(i)
                i += 1        
                # rib_res, trip_asn_counter, prev_asn_stat, succ_counter = get_res_attrs.GetIPAttr_Prepare_ForACand(r_ip)
                # tmp_attr = get_res_attrs.GetIPAttr_ForACand(r_ip, get_res_attrs.cache.get(r_ip), rib_res, trip_asn_counter, prev_asn_stat, succ_counter)
                tmp_attr = None
                #if considerscores:
                if False:
                    rib_res, trip_asn_counter, prev_asn_stat, succ_counter = get_res_attrs.GetIPAttr_Prepare_ForACand_ConsiderScores(r_ip)
                    tmp_attr = get_res_attrs.GetIPAttr_ForACand_ConsiderScores(r_ip, get_res_attrs.cache.get(r_ip), rib_res, trip_asn_counter, prev_asn_stat, succ_counter)
                else:
                    rib_res, trip_asn_counter, prev_asn_stat, succ_counter = get_res_attrs.GetIPAttr_Prepare_ForACand(r_ip)
                    tmp_attr = get_res_attrs.GetIPAttr_ForACand(r_ip, get_res_attrs.cache.get(r_ip), rib_res, trip_asn_counter, prev_asn_stat, succ_counter)
                if tmp_attr:
                    tmp_attrs.append(tmp_attr)
                    r_ips.append(r_ip)
                else:
                    get_res_attrs.map_score[r_ip] = 0    
            tmp_scores = get_pred_result_per_attr_v3(tmp_attrs, get_res_attrs.model, ipattr_names)
            for i in range(len(tmp_scores)):
                get_res_attrs.map_score[r_ips[i]] = tmp_scores[i].tolist()
            with open('/mountdisk2/common_vps/%s/atlas/ipattr_score_recal_%s.json' %(date, date), 'w') as wf:
                json.dump(get_res_attrs.map_score, wf, indent=1)
        with open('/mountdisk2/common_vps/%s/atlas/model_modified_ip_mappings_%s.json' %(date, date), 'w') as wf:
            json.dump(get_res_attrs.cache, wf, indent=1)
    
if __name__ == '__main__':
    #main_func_atlas()
    main_func()
    