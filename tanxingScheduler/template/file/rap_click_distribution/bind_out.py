#!/bin/python

#encoding=gb2312

import sys

def main(argv):
    channel=argv[1]
    fn=argv[2]
    f = open(fn,'r')

    dic={}
    for line in f:
        lines=line.strip().split('\t',1)
	if lines[1] not in dic.keys():
	    dic[lines[1]] = int(lines[0])
	else:
            dic[lines[1]] += int(lines[0])

    for key in dic.keys():
    	print ','.join([x for x in key.split('\t')])+','+str(dic[key])

if __name__=="__main__":
    main(sys.argv)

