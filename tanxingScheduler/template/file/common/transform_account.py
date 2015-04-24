#!/bin/env python

import sys
import MySQLdb
import json
import time
import string

cur_time="\""+time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))+"\""

with open('../../conf/default.conf','r') as confFile:
    confStr = confFile.read()
    conf = json.JSONDecoder().decode(confStr);
    dbStaticResult = conf['database']['db_bim_dict_db'];

    conn = MySQLdb.connect(host=dbStaticResult['host'],\
            user=dbStaticResult['user'],\
            passwd=dbStaticResult['password'],\
            db=dbStaticResult['database'],\
            port=dbStaticResult['port']);
    cur = conn.cursor();

file= open(sys.argv[1]);
while 1:
    line= file.readline()
    if len(line) == 0:
        break
    queryaccount=line.split('\t')
    selection= 'select a.name from advertiser a join advertiser_account b on a.id=b.advertiser_id where b.id='+queryaccount[1];
    cur.execute(selection)
    accountname = cur.fetchone();
    print queryaccount[0],queryaccount[1].rstrip(),accountname[0];
file.close()
conn.commit()
conn.close();

## vim: set expandtab ts=4 sw=4 sts=4 tw=0: */
