#!/bin/env python

##**************************************************************************
## 
## Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
## 
##*************************************************************************/
##
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
dbStaticResult = conf['database']['dbStaticResult'];

conn = MySQLdb.connect(host=dbStaticResult['host'],\
        user=dbStaticResult['user'],\
        passwd=dbStaticResult['password'],\
        db=dbStaticResult['database'],\
        port=dbStaticResult['port']);
cur = conn.cursor();

get_query_account_url = sys.stdin.readline().strip()
if len(get_query_account_url) > 0:
    result=get_query_account_url.split(',');
    insertion = 'insert into query_file(task_id,category_id, url, insert_datetime, update_datetime) values (' + sys.argv[1] + ','+ result[0] +',"' + result[1] + '",' + cur_time + ' ,'+ cur_time + ' )'
    try:
        cur.execute(insertion)
    except:
        print "exception on inserting get_query_account result"

conn.commit()
conn.close();

## vim: set expandtab ts=4 sw=4 sts=4 tw=0: */
