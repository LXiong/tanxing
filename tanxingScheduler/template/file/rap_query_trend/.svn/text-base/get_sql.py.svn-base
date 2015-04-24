#!/usr/bin/python
#-*- coding=utf-8 -*-

import sys,os
import re
def exit_with_help(info):
    print info
    exit(1)

def get_sql_head(fn):
    f = open(fn, 'r')
    sql_head = ''
    for line in f:
        sql_head += line
    f.close()
    return sql_head + '\n'

def get_sql_para_map(fn):
    f = open(fn, 'r')
    sql_para_map = {}
    for line in f:
        ele = line.strip().split('\t',1)
        if len(ele) == 2:
            #two default input parameters
            sql_para_map[ele[0]] = ele[1]
        else:
            continue
    f.close()
    return sql_para_map

def get_sql_wordbag(fn,table_name):
    sql_wordbag = ''
    if fn=='':
        return sql_wordbag
	exit_with_help("Wordbag! is missing")
    else:
        sql_wordbag = sql_wordbag + "drop table if exists "+ table_name +";\n"+ \
                                    "create table "+ table_name +"(word string)\n"+ \
                                    "row format delimited fields terminated by " + "'\\t'" + ";\n\n"
        sql_wordbag = sql_wordbag + "add file "+ fn +";\n"+ \
	                            "load data local inpath '"+ fn + \
                                    "' overwrite into table " + table_name + ";\n\n"
    return sql_wordbag


def get_sql_zone(fn,table_name):
    sql_zone = ''
    if fn=='':
        return sql_zone
        exit_with_help("Wordbag! is missing")
    else:
        sql_zone = sql_zone + "drop table if exists "+ table_name +";\n"+ \
                            "create table "+ table_name +"(pid bigint, cid bigint, pidcid bigint, region string)\n"+ \
                            "row format delimited fields terminated by " + "'\\t'" + ";\n\n"
        sql_zone = sql_zone + "add file "+ fn +";\n"+ \
                            "load data local inpath '"+ fn + \
                            "' overwrite into table " + table_name + ";\n\n"
    return sql_zone


def get_sql_body(sql_para_map,word_table_name,tmp_table_name,zone_table_name):
    region_info = sql_para_map['group_by'].split(',')
    group_by_region = False
    if ('cid' in region_info) or ('pid' in region_info):
        group_by_region = True
		
    if 'group_by' in sql_para_map.keys():
	attributes=sql_para_map['group_by'].split(',')
	for i in range(len(attributes)):
	    if (attributes[i]=='day'):
		attributes[i] = 'a.stat_date as day'
	    if (attributes[i]=='month'):
		attributes[i] = 'substr(a.stat_date,1,6) as month'
    else:
	exit_with_help("no group_by attributes selected!")
	
    sql_body = ''
    if group_by_region:
        sql_body += get_sql_zone('zoneList',zone_table_name)
	sql_body += "drop table if exists "+ tmp_table_name +";\n"+ \
		    "create table "+ tmp_table_name + \
		    "\nrow format delimited fields terminated by " + "'\\t'\n" + "as\n"

    sql_body += 'select \n\t' + ', '.join([add_prefix_1(x) for x in attributes])+', sum(a.pv) as pv, sum(a.pvad) as epv'
    
    sql_body += '\nfrom \n\t' + word_table_name + ' b\n' + 'join \n\tmydb.sem_bl_all_baidu_search_theme a\n' 
    
    sql_body += 'on \n\tb.word=a.normalized_query\nwhere \n\t'
    
    if 'partition_stat_date' in sql_para_map:
	if len(sql_para_map['partition_stat_date'].split(','))==1:
	    sql_body += 'a.partition_stat_date ='+sql_para_map['partition_stat_date']+' '
	else:
	    sql_body += 'a.partition_stat_date in ('+sql_para_map['partition_stat_date']+') '
    else:
        exit_with_help('no partition_val given')
    
    sql_body += 'and \n\ta.channel_id='+sql_para_map['channel']
    
    if 'cid' in region_info:
        sql_body += ' and \n\ta.cid <> 0'
    if 'pid' in region_info:
        sql_body += ' and \n\ta.cid = 0'

    if 'pidcid' in sql_para_map:
        if len(sql_para_map['pidcid'].split(',')) == 1:
            sql_body += ' and \n\ta.pidcid' '=' + sql_para_map['pidcid']
        else:
            sql_body += ' and \n\ta.pidcid in (' + sql_para_map['pidcid']+ ')'

    if 'group_by' in sql_para_map.keys():
	sql_body += '\ngroup by '+', '.join([add_prefix_2(x) for x in attributes]) + ';\n\n'
    else:
	exit_with_help('no attribues selected!')    
    
    if group_by_region:
	sql_body += 'select\n\t' + ', '.join([add_prefix_3(x) for x in attributes])+', a.pv, a.epv' + '\n' + \
		    'from \n\t' + zone_table_name + ' b\n' + \
		    'join \n\t' + tmp_table_name + ' a\n'
        if 'cid' in region_info:
	    sql_body += 'on \n\tb.cid = a.cid \nwhere\n\t b.cid <> 0;\n\n'
        if 'pid' in region_info:
	    sql_body += 'on \n\tb.pid = a.pid \nwhere\n\t b.cid = 0;\n\n'
			
    sql_body += '\n'
    
    return sql_body

def add_prefix_3(x):
    if x in ['cid','pid']:
	return 'b.region'
    if x.split(' ')[-1] in ['month','day']:
        return 'a.' + x.split(' ')[-1]
    return 'a.'+x

def add_prefix_1(x):
    if x.split(' ')[-1] in ['month','day']:
        return x
    return 'a.'+x

def add_prefix_2(x):
    if x.split(' ')[-1] in ['month','day']:
        return x.split(' ')[0]
    return 'a.'+x

def save_to_sql_file(sql_body, outf):
    f = open(outf, 'w')
    f.write(sql_body)
    f.close()

def main(argv):
    #output example:
    #subtaskid  110
    #wordbag    wordbag.txt
    #channel    11011
    #partiotion_stat_date       20140101,20140102,20140103,20140104,20140105,...,20140114
    #region	null
    #group_by   province,city,url_domain
    sql_head = get_sql_head(argv[1])
    sql_para_map = get_sql_para_map(argv[2])
    output_fn = re.sub('-para','.sql',argv[2])
    wordbag_name = argv[2].split('/')[-1].split('-')[0]+'_word_table'
    zone_name = argv[2].split('/')[-1].split('-')[0]+'_zone'
    wordbag_fn = sql_para_map['wordbag']
    tmp_name = argv[2].split('/')[-1].split('-')[0]+'_tmp'

    sql_wordbag = get_sql_wordbag(wordbag_fn,wordbag_name)

    sql_body = get_sql_body(sql_para_map,wordbag_name,tmp_name,zone_name)
    sql_body = sql_head + sql_wordbag+sql_body
    save_to_sql_file(sql_body, output_fn)

if __name__ == '__main__':
    main(sys.argv)
