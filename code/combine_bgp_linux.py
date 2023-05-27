
import os

if __name__ == '__main__':
    asn_list = ['3257', '7660', '6939', '7575', '34288']
    for asn in asn_list:
        cmd = "cat rib_" + asn + " updates_ " + asn + " > bgp_" + asn
        print(cmd)
        os.system(cmd) 
