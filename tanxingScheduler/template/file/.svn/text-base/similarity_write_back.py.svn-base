#!/bin/env python

##**************************************************************************
## 
## Copyright (c) 2014 Baidu.com, Inc. All Rights Reserved
## 
##*************************************************************************/
##
## @file similarity_write_back.py
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
insertion = "insert into market_competition(task_id, axes, stress, insert_datetime, update_datetime ) values (" + task_id + ", %s, %s, " + cur_time + " ,"+ cur_time + " )"

line = sys.stdin.readline().strip()
values = line.split("\t")
stress = values[3]
ids = values[0].split(",")
xvalues = values[1].split(",")
yvalues = values[2].split(",")
for i in xrange(len(ids)):
    if i == 0:
        axes = ids[i] + ":(" + xvalues[i] + "," + yvalues[i] + ")"
    else:
        axes += "," + ids[i] + ":(" + xvalues[i] + "," + yvalues[i] + ")"
cur.execute(insertion, [axes, stress])

conn.commit()
conn.close()

## vim: set expandtab ts=4 sw=4 sts=4 tw=0: */
