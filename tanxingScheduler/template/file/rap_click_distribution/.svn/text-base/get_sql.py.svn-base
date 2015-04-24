#!/usr/bin/python

#generate a sql file
import sys,os

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

def add_prefix(x):
    if x.split(' ')[-1] in ['domain','hour']:
	return x.split(' ')[0]
    return 'a.'+x

def get_sql_body(sql_para_map,table_name):
    
    if 'group_by' in sql_para_map.keys():
	attributes=sql_para_map['group_by'].split(',')
	for i in range(len(attributes)):
	    if (attributes[i]=='domain'):
		attributes[i] = "parse_url(a.clk_url,'HOST') as domain"
            if (attributes[i]=='hour'):
                attributes[i] = "hour(a.action_time) as hour"
    else:
	exit_with_help("no group_by attributes selected!")
    
    sql_body = 'select ' + ', '.join([add_prefix(x) for x in attributes]) + ', count(*)'
    
    sql_body += '\nfrom ' + table_name + ' b\n' + 'join '
    
    if 'channel' in sql_para_map:
	if sql_para_map['channel']=='pc':
            sql_body += 'default.ud_al_ps_session_data' + ' a\n'
	else:
            sql_body += 'default.ud_al_wise_session_data'+' a\n'
    else :
        exit_with_help('no channel selected!')
        
    sql_body += 'on b.word=a.normalized_query\nwhere '
    
    if 'partition_stat_date' in sql_para_map:
	if len(sql_para_map['partition_stat_date'].split(','))==1:
	    sql_body += 'a.partition_stat_date ='+sql_para_map['partition_stat_date']+''
	else:
	    sql_body += 'a.partition_stat_date in ('+sql_para_map['partition_stat_date']+')'
    else:
        exit_with_help('no partition_val given')
    
    if sql_para_map['channel']=='wise':
        sql_body += ' and a.is_filter=0'
    
    sql_body += '\n'

    if 'group_by' in sql_para_map.keys():
	sql_body += 'group by '+', '.join([add_prefix(x) for x in attributes]) + ';\n'
    else:
	exit_with_help('no attribues selected!')    
    
    sql_body += '\n'
    
    return sql_body
    

def save_to_sql_file(sql_body, outf):
    f = open(outf, 'w')
    f.write(sql_body)
    f.close()

def main(argv):
    #output example:
    #subtaskid  110
    #wordbag    wordbag.txt
    #channel    pc
    #partiotion_stat_date       20140101,20140102,20140103,20140104,20140105,...,20140114
    #group_by   province,city,url_domain
    sql_head = get_sql_head(argv[1])
    sql_para_map = get_sql_para_map(argv[2])
    output_fn = argv[2].split('-')[0]+'.sql'
    wordbag_name = argv[2].split('/')[-1].split('-')[0]+'_word_table'
    wordbag_fn = sql_para_map['wordbag']

    sql_wordbag = get_sql_wordbag(wordbag_fn,wordbag_name)
    sql_body = get_sql_body(sql_para_map,wordbag_name)
    sql_body = sql_head + sql_wordbag+sql_body
    save_to_sql_file(sql_body, output_fn)

if __name__ == '__main__':
    main(sys.argv)
