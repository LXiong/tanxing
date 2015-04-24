#!/bin/env python

##**************************************************************************
## 
## Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
## 
##*************************************************************************/
##
## @file channel_stat_write_back.py
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
dbStaticResult = conf['database']['dbStaticResult'];

conn = MySQLdb.connect(host=dbStaticResult['host'],\
        user=dbStaticResult['user'],\
        passwd=dbStaticResult['password'],\
        db=dbStaticResult['database'],\
        port=dbStaticResult['port']);
cur = conn.cursor();
insertion = "insert into user_behavior_summary_by_channel(task_id, industry_id, channel_id, residence_time, page_count, unique_visitor, insert_datetime, update_datetime ) values (" + sys.argv[1] + ", " + sys.argv[2] + ", %s, %s, %s, %s, " + cur_time + " ,"+ cur_time + ")"

for line in sys.stdin:
    values = line.strip().split("\t")
    cur.execute(insertion, values);

conn.commit()
conn.close();

## vim: set expandtab ts=4 sw=4 sts=4 tw=0: */
