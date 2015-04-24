##################################################################
# Wu Tong on 20140718
# Abstract film's info from ori.txt. Abstract total line.
# The name of films to be abstracted are in file query.txt.
# The result is saved to jiaoji.txt
##################################################################

import os

import sys

import math

def exit_with_help():
    print("Usage: python py_grep.py ori.txt col_index_from_0 query.txt grep_result.txt")
    exit(1)

def check_para(argv):
    argc = len(argv)
    if argc != 5:
        exit_with_help()

def load_file2map(in_file, col_index):
    fn_value = {}
    fv_file = open(in_file, 'r')
    if fv_file is None:
        return None
    for line in fv_file:
        ele = line.strip().split('\t')
        if ele is None:
            return None
        object = ele[col_index]
        if object in fn_value:
            fn_value[object].append(line.strip())
        else:
            lines = []
            lines.append(line.strip())
            fn_value[object] = lines			
    fv_file.close()
    return fn_value

def load_file2list(in_file):
    s_list = []
    f = open(in_file, 'r')
    if f is None:
        return None
    for line in f:
        s_list.append(line.strip())
    f.close()
    return s_list

def get_intersection(all_map, search_list, out_file):
    f = open(out_file, 'w')
    for query in search_list:
        if query in all_map:            
            for line in all_map[query]:
                f.write(line + '\n')
        else:
            f.write(query+'\t0\n')           
    f.flush()
    f.close()

def main(argv = sys.argv):
    check_para(argv)
    all_map = load_file2map(argv[1], (int)(argv[2]))
    if all_map is None:
        exit_with_help()
	search_list = []
    search_list = load_file2list(argv[3])
    if search_list is None:
        exit_with_help()
    get_intersection(all_map, search_list, argv[4])
	
if __name__ == '__main__':
    main(sys.argv)
