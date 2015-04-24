#!/bin/env python

##**************************************************************************
## 
## Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
## 
##*************************************************************************/
##
## @file query_all_write_back.py
## @author zhouyuefeng(com@baidu.com)
## @date 2014/06/19 13:06:33
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

tagged_query_dict_url = sys.stdin.readline().strip()
tagged_query_dict_detail_url = sys.stdin.readline().strip()

insertion = 'insert into tagged_query_dict(task_id, dict_file_url, insert_datetime, update_datetime ) values ("' + sys.argv[1] + '","' + tagged_query_dict_url + '",' + cur_time + ' ,'+ cur_time + ' )'
cur.execute(insertion)

#insertion = 'insert into tagged_query_dict_detail (task_id, report_file_url, insert_datetime, update_datetime ) values ("' + sys.argv[1] + '","' + tagged_query_dict_detail_url + '",' + cur_time + ' ,'+ cur_time + ' )'
#cur.execute(insertion)

conn.commit()
conn.close();

## vim: set expandtab ts=4 sw=4 sts=4 tw=0: */
