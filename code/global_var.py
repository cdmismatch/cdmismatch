
par_path = '/mountdisk1/ana_c_d_incongruity/'

prefix2as_dir = 'prefix2as_data/'
as2org_dir = 'as_org_data/'
rel_cc_dir = 'as_rel_cc_data/'
peeringdb_dir = 'peeringdb_data/'
out_ip2as_dir = 'out_ip2as_data/'
out_bdrmapit_dir = 'out_bdrmapit/'
traceroute_dir = 'traceroute_data/'
rib_dir = 'rib_data/'
out_my_anatrace_dir = 'out_my_anatrace'
midar_dir = 'midar_data/'
irr_dir = 'irr_data/'
other_middle_data_dir = 'other_middle_data/'

prefix2as_flag = 'pfxas'
as2org_flag = 'as2org'
rel_flag = 'asrel'
cone_flag = 'ascone'
peeringdb_flag = 'peeringdb'
asrank_flag = 'asrank'


vps = ['sjc2-us', 'nrt-jp', 'per-au', 'syd-au', 'zrh2-ch']
#vps = ['per-au']
#map_methods = ['ribs', 'midar', 'bdrmapit', 'ribs_midar', 'ribs_bdrmapit', 'ribs_midar_bdrmapit', 'bdrmapit2', 'bdrmapit3', 'ribs_midar_bdrmapit2', 'ribs_midar_bdrmapit3']
map_methods = ['ribs', 'midar', 'bdrmapit']
#map_methods = ['bdrmapit2', 'bdrmapit3', 'ribs_midar_bdrmapit2', 'ribs_midar_bdrmapit3']

trace_as_dict = dict()
trace_as_dict['hkg-cn'] = '3257'
trace_as_dict['sjc2-us'] = '6939'
trace_as_dict['nrt-jp'] = '7660'
trace_as_dict['per-au'] = '7575'
trace_as_dict['syd-au'] = '7575'
trace_as_dict['scl-cl'] = '27678'
trace_as_dict['zrh2-ch'] = '34288'
trace_as_dict['sjc2-us'] = '6939'
trace_as_dict['ord-us'] = '54728'
trace_as_dict['test'] = '7660'

irrs = ['apnic', 'ripe', 'afrinic', 'arin', 'lacnic']
irr_filename_pre = 'irrdata_'
irr_filename_default = 'irrdata'
irr_dbname = 'irr.db'
ip_ranges_filename = 'ip_ranges'


all_trace_par_path = '/mountdisk3/'
all_trace_download_dir = 'traceroute_download_all/'
all_trace_trace_as_res_dir = 'trace_as_res/'
all_trace_out_data_dir = 'out_data/'
all_trace_out_all_trace_filename = 'all_traces'
all_trace_out_all_trace_uni_as_filename = 'all_traces_uni_as'
all_trace_out_all_trace_links_filename = 'all_trace_links'