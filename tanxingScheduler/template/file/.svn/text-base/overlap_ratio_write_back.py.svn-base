#!/bin/env python

##**************************************************************************
## 
## Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
## 
##*************************************************************************/
##
## @file overlap_ratio_write_back.py
## @author zhouyuefeng(com@baidu.com)
## @date 2014/06/19 13:06:33
## @brief 
##  

import sys
import MySQLdb
import json
import time

task_id = sys.argv[1]

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
insertion = "insert into market_overlap(task_id, brand_map_ids, overlap, insert_datetime, update_datetime ) values (" + task_id + ", %s, %s, " + cur_time + " ,"+ cur_time + " )"

for line in sys.stdin:
    values = line.strip().split("\t")
    cur.execute(insertion, values);

conn.commit()
conn.close();

## vim: set expandtab ts=4 sw=4 sts=4 tw=0: */
