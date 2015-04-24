#!/bin/python
#encoding=gb2312

import os
import sys
import re
import time, datetime

def write_conf(dir,subtaskid,wordbag,channel,date_list,group_by_attrs,pid):
    #output example:
    #subtaskid	110
    #wordbag	wordbag.txt
    #channel	pc
    #partiotion_stat_date	20140101,20140102,20140103,20140104,20140105,...,20140114
    #group_by	province,city,url_domain
    output_fn = str(dir)+'/'+str(subtaskid)+'_'+channel+'_'+str(pid)+'-para'
    f = open(output_fn,'a')
    f.write('subtaskid'+'\t'+subtaskid+'\n')
    f.write('wordbag'+'\t'+wordbag+'\n')
    f.write('channel'+'\t'+channel+'\n')
    f.write('partition_stat_date'+'\t'+"'"+"','".join([d for d in date_list])+"'"+'\n')
    f.write('group_by'+'\t'+group_by_attrs+'\n')
    f.close()
    
    return 0

def get_date_list(date_range):
    #input: 20140101,20140105
    #output: [20140101,20140102,20140103,20140104,20140105]
    fromday = date_range.split(',')[0]
    today = date_range.split(',')[1]
    format='%Y%m%d'
    res=[]
    fd = datetime.datetime(*time.strptime(fromday, format)[:6])
    td = datetime.datetime(*time.strptime(today, format)[:6])
    newday = fd 
    while newday <= td:
	res.append(newday.strftime('%Y%m%d'))
        newday = newday + datetime.timedelta(1)

    return res


def main(argv):
    #$1 taskid $2 pc|wise $3 partition_stat_date:date1,date2 $4 wordbag $5 select_attributes
    subtaskid = argv[1]  #110
    channels = argv[2].split(',')  #[pc,wise]
    date_range = argv[3] #20140101,20140110
    wordbag = argv[4] #wordbag.txt
    group_by_attrs = re.sub('[\"\[\]]','',argv[5]) #province, url_domain
    dir = argv[6]

    date_list = get_date_list(date_range)
    nparts = int((len(date_list)-1)/14)+1
    for i in range(len(channels)):
	channel = channels[i]
	for pid in range(nparts):
	    write_conf(dir,subtaskid, wordbag, channel, date_list[(pid*14):(pid*14+14)], group_by_attrs, pid)
    
    return 0

if __name__ == "__main__":
    main(sys.argv)

