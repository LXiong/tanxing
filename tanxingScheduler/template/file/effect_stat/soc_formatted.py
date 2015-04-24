# -*- coding: utf-8 -*-
import sys 
import os
import codecs

def load_file(filename, encode='utf-8'):
    if not os.path.isfile(filename):
        print "File %s not exists." % filename
        exit(1)
    return codecs.open(filename, encoding=encode)

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print "Usage: python %s <month_total_clk_pc_wise> <account_percent_info> <result_file>" % sys.argv[0]
        exit(1)
    fp_stat = load_file(sys.argv[1])
    fp_info = load_file(sys.argv[2])
    soc_out = open(sys.argv[3], 'w')
    #add the 1st column name line    
    soc_out.write("账户\t行业\t品类\t广告主\t品牌\tpc_total_click\twise_total_click\ttotal_click\n")
    trans = {}
    i = 0
    for line in fp_stat:
        i += 1
        if i == 1:
            continue
        line = line.strip()
        if line == '': continue
        arr = line.replace('NULL', '0').split("\t")
        username = arr[0]
        pc = int(arr[1])
        wise = int(arr[2])
        al = int(arr[3])        
        trans[username] = [pc, wise, al]    
    i = 0
    for line in fp_info:
        i += 1
        if i == 1:
            continue
        line = line.strip()
        if line == '': continue
        arr = line.split(",")
        perc = int(arr[-1].rstrip('%'))
        if arr[0] not in trans:
            str = "%s\t%d\t%d\t%d\n" % ("\t".join(arr[:-1]), 0, 0, 0) 
        else:
            ll = trans[arr[0]]
            str = "%s\t%d\t%d\t%d\n" % ("\t".join(arr[:-1]), round(ll[0]/100.0*perc), round(ll[1]/100.0*perc), round(ll[2]/100.0*perc))
        soc_out.write(str.encode('utf-8'))
