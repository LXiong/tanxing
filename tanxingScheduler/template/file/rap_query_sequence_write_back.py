#!/bin/env python

##**************************************************************************
## 
## Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
## 
##************************************************************************
## @file query_all_write_back.## @author susidian(com@baidu.com)
## @author susidian(com@baidu.com)
## @brief 
##  

import sys
import MySQLdb
import json
import time

cur_time="\""+time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))+"\""

with open('conf/default.conf','r') as confFile:
    confStr = confFile.read()
conf = json.JSONDecoder().decode(confStr);
dbStaticResult = conf['database']['db_bim_rap_result_db'];

conn = MySQLdb.connect(host=dbStaticResult['host'],\
        user=dbStaticResult['user'],\
        passwd=dbStaticResult['password'],\
        db=dbStaticResult['database'],\
        port=dbStaticResult['port']);
cur = conn.cursor();

rap_query_sequence_random_url = sys.stdin.readline().strip()
rap_query_sequence_filtered_url = sys.stdin.readline().strip()


if len(rap_query_sequence_filtered_url) > 0:
    result1=rap_query_sequence_filtered_url.split(','); 
    insertion = 'insert into query_sequence(task_id, result_type, sequence_file_url, insert_datetime, update_datetime ) values ("' + sys.argv[1] + '", "filtered", "' + result1[1] + '",' + cur_time + ' ,'+ cur_time + ' )'
    try:
        cur.execute(insertion)
    except:
        print "exception on inserting filtered result"

if len(rap_query_sequence_random_url) > 0:
    result2= rap_query_sequence_random_url.split(',');
    insertion = 'insert into query_sequence(task_id, result_type, sequence_file_url, insert_datetime, update_datetime ) values ("' + sys.argv[1] + '", "random", "' + result2[1] + '",' + cur_time + ' ,'+ cur_time + ' )'
    try:
        cur.execute(insertion)
    except:
        print "exception on inserting random result"
    
conn.commit()
conn.close();

## vim: set expandtab ts=4 sw=4 sts=4 tw=0: */
