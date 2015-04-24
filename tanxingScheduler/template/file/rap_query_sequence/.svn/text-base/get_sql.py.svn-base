#!/usr/bin/python

import sys
import os
import string
import datetime

# get sql_head that sql_file starts with 
def get_sql_head(fn):
    f = open(fn, 'r')
    sql_head = ''
    for line in f:
        sql_head += line
    f.close()
    return sql_head + '\n'

def save_to_sql_file(sql_body, outf):
    f = open(outf, 'w')
    f.write(sql_body)
    f.close()

# process query select rule 
def get_rule(in_rule):
    all_rule = ''
    in_rule = in_rule.replace(';',',').replace('\n',',').replace('\t',',')
    if ',' in in_rule:
        rule_list = in_rule.split(',')
    else:
        rule_list = [in_rule]
    for every_rule in rule_list:
        rule = ''
        if '&' in every_rule:
            every_rule = every_rule.replace('&&','&')
            and_list = every_rule.split('&')
        else:
            and_list = [every_rule]
        for A in and_list:
            A = A.replace('(','').replace(')','')
            if '|' in A:
                A = A.replace('||','|')
                or_list = A.split('|')
                tmp_rule = '.*('
                for query in or_list:
                    tmp_rule += query+'|'
                tmp_rule = tmp_rule[0:-1]+').*'
            else:
                tmp_rule = '.*('+A+').*'
            rule += "a.normalized_query rlike '"+tmp_rule+"' and "
        rule = rule[0:-4]
        all_rule += '( '+rule+' )'+' or '
    all_rule = all_rule[0:-3]
    if len(rule_list) >=2:
        all_rule = '('+all_rule+')'
    return all_rule

def main(argv):
    #python get_sql.py $path_run $taskid $channel $cookie_date_range /
    #   $cookie_active_length $flag_wordbag_or_rule $wordbag_or_rule /
    #   $minimum_pv $cookie_num $rand_cookie_num $log_date_range
    
    cur_path = argv[1]  # current task dir
    taskid = argv[2]   
    channel = argv[3].strip().split(',') # ps : 10011 wise : 11000
    cookie_date_range = argv[4].strip().split(',') 
    cookie_active_length = string.atoi(argv[5]) 
    flag_wordbag_or_rule = string.atoi(argv[6]) #  0 for wordbag ,1 for rule
    wordbag_or_rule = argv[7] # file(wordbag) when flag_wordbag_or_rule=0 , rule when = 1
    minimum_pv = string.atoi(argv[8]) # filter :select pv >= minimum_pv
    cookie_num = string.atoi(argv[9]) # filter :the num of selected cookie that pv >= minimum_pv
    rand_cookie_num = string.atoi(argv[10]) # the num of random selected cookie without other filter
    log_date_range = argv[11].strip().split(',') 
    
    sql_head = get_sql_head('head.sql')
    
    Cur_Step = 1
    
    # load wordbag.txt into to table
    if flag_wordbag_or_rule == 0:
        # wordbag 
        # Step : get table wordbag 
        sql_body = ""
        sql_body += sql_head
        
        sql_body += 'drop table if exists rqs_'+taskid+'_query;\n'
        sql_body += 'create table rqs_'+taskid+'_query(query string)\n'
        sql_body += "row format delimited fields terminated by '\\t';\n"
        sql_body += 'add file '+wordbag_or_rule+';\n'
        sql_body += "load data local inpath '"+wordbag_or_rule+"' overwrite into table rqs_"+taskid+"_query;\n"
        save_to_sql_file(sql_body, cur_path+'Step'+str(Cur_Step)+'/rqs_'+taskid+'_query.sql')
        
    # Step : create cookie table  
    if '10011' in channel:
        #ps
        sql_body = ""
        sql_body += sql_head
        sql_body += 'drop table if exists rqs_'+taskid+'_ps_cookie;\n'
        if minimum_pv >= 2:
            # pv needed now
            sql_body += 'create table rqs_'+taskid+'_ps_cookie(cookie string,pv int)\n'
            sql_body += "row format delimited fields terminated by '\\t';\n"
        else:
            # pv not necessary
            sql_body += 'create table rqs_'+taskid+'_ps_cookie(cookie string);\n'
        save_to_sql_file(sql_body, cur_path+'Step'+str(Cur_Step)+'/rqs_'+taskid+'_ps_cookie.sql')
    if '11000' in channel:
        #wise
        sql_body = ""
        sql_body += sql_head
        sql_body += 'drop table if exists rqs_'+taskid+'_wise_uid;\n'
        if minimum_pv >= 2:
            # pv needed now
            sql_body += 'create table rqs_'+taskid+'_wise_uid(wise_uid string,pv int)\n'
            sql_body += "row format delimited fields terminated by '\\t';\n"
        else:
            # pv not necessary
            sql_body += 'create table rqs_'+taskid+'_wise_uid(wise_uid string);\n'
        save_to_sql_file(sql_body, cur_path+'Step'+str(Cur_Step)+'/rqs_'+taskid+'_wise_uid.sql')
    Cur_Step += 1
    
    
    # Step : get all cookie 
    start_date = datetime.datetime.strptime( cookie_date_range[0],'%Y%m%d')
    end_date = datetime.datetime.strptime( cookie_date_range[1],'%Y%m%d')
    cur_date = start_date
    N_part = 0 # split into N part when duration longer then 14 days,every part last 14 days
    while end_date >= cur_date :
        N_part += 1
        day_count = 1
        # generate date string first
        partition_stat_date = "("
        while cur_date <= end_date and day_count <= 14:
            partition_stat_date += "'"+datetime.datetime.strftime(cur_date,'%Y%m%d')+"',"
            day_count += 1
            cur_date += datetime.timedelta(1)
        partition_stat_date = partition_stat_date[0:-1]+')'
        # generate whole sql
        if flag_wordbag_or_rule == 0:
            if '10011' in channel:
                #ps
                sql_body = ""
                sql_body += sql_head
                sql_body += 'insert into table rqs_'+taskid+'_ps_cookie\n'
                if minimum_pv >= 2:
                    sql_body += 'select a.cookie,count(*) as pv\n'
                else:
                    sql_body += 'select distinct a.cookie\n'
                sql_body += 'from rqs_'+taskid+'_query b\n'
                sql_body += 'join default.ud_al_ps_session_data a\n'
                sql_body += 'on b.query = a.normalized_query\n'
                sql_body += "where a.partition_stat_date in "+partition_stat_date+'\n'
                if minimum_pv >= 2:
                    sql_body += 'group by a.cookie\n'
                sql_body += ';\n'
                save_to_sql_file(sql_body, cur_path+'Step'+str(Cur_Step)+'/rqs_'+taskid+'_get_ps_cookie_'+str(N_part)+'.sql')
                
            if '11000' in channel:
                #wise
                sql_body = ""
                sql_body += sql_head
                sql_body += 'insert into table rqs_'+taskid+'_wise_uid\n'
                if minimum_pv >= 2:
                    sql_body += 'select a.wise_uid,count(*) as pv\n'
                else:
                    sql_body += 'select distinct a.wise_uid\n'
                sql_body += 'from rqs_'+taskid+'_query b\n'
                sql_body += 'join default.ud_al_wise_session_data a\n'
                sql_body += 'on b.query = a.normalized_query\n'
                sql_body += "where a.partition_stat_date in "+partition_stat_date+'\n'
                sql_body += 'and  a.is_filter=0\n'
                if minimum_pv >= 2:
                    sql_body += 'group by a.wise_uid\n'
                sql_body += ';\n'
                save_to_sql_file(sql_body, cur_path+'Step'+str(Cur_Step)+'/rqs_'+taskid+'_get_wise_uid_'+str(N_part)+'.sql')
        else:
            #generate rule first
            rule = get_rule(wordbag_or_rule)
            
            if '10011' in channel:
                #ps
                sql_body = ""
                sql_body += sql_head
                sql_body += 'insert into table rqs_'+taskid+'_ps_cookie\n'
                if minimum_pv >= 2:
                    sql_body += 'select a.cookie,count(*) as pv\n'
                else:
                    sql_body += 'select distinct a.cookie\n'
                sql_body += 'from default.ud_al_ps_session_data a\n'
                sql_body += "where a.partition_stat_date in "+partition_stat_date+'\n'
                sql_body += 'and '+rule+'\n'
                if minimum_pv >= 2:
                    sql_body += 'group by a.cookie\n'
                sql_body += ';\n'
                save_to_sql_file(sql_body, cur_path+'Step'+str(Cur_Step)+'/rqs_'+taskid+'_get_ps_cookie_'+str(N_part)+'.sql')
                
            if '11000' in channel:
                #wise
                sql_body = ""
                sql_body += sql_head
                sql_body += 'insert into table rqs_'+taskid+'_wise_uid\n'
                if minimum_pv >= 2:
                    sql_body += 'select a.wise_uid,count(*) as pv\n'
                else:
                    sql_body += 'select distinct a.wise_uid\n'
                sql_body += 'from default.ud_al_wise_session_data a\n'
                sql_body += "where a.partition_stat_date in "+partition_stat_date+'\n'
                sql_body += 'and '+rule+'\n'
                sql_body += 'and  a.is_filter=0\n'
                if minimum_pv >= 2:
                    sql_body += 'group by a.wise_uid\n'
                sql_body += ';\n'
                save_to_sql_file(sql_body, cur_path+'Step'+str(Cur_Step)+'/rqs_'+taskid+'_get_wise_uid_'+str(N_part)+'.sql')
    Cur_Step += 1
    
    # Step : cookie active ensure
    if cookie_active_length > 0:
        # generate date info first
        active_date_a = '('
        active_date_b = ''
        for n in range(0,cookie_active_length):
            active_date_a += "'"+datetime.datetime.strftime(start_date+datetime.timedelta(n),'%Y%m%d')+"',"
            active_date_b = "'"+datetime.datetime.strftime(end_date-datetime.timedelta(n),'%Y%m%d')+"'," + active_date_b
        active_date = active_date_a + active_date_b
        active_date = active_date[0:-1]+')'
    if '10011' in channel:
        #ps
        sql_body = ""
        sql_body += sql_head
        sql_body += 'drop table if exists rqs_'+taskid+'_ps_cookie_active;\n'
        if cookie_active_length > 0:
            if minimum_pv >= 2:
                sql_body += 'create table rqs_'+taskid+'_ps_cookie_active \n'
                sql_body += "row format delimited fields terminated by '\\t' as\n"
                sql_body += 'select x.cookie ,sum(x.pv) as pv\n'
                sql_body += 'from (select b.cookie,b.pv \n'
                sql_body += '      from rqs_'+taskid+'_ps_cookie b\n'
                sql_body += '      join default.ud_al_ps_session_data a\n'
                sql_body += '      on a.cookie = b.cookie\n'
                sql_body += '      where a.partition_stat_date in '+active_date+'\n'
                sql_body += '      ) x\n'
                sql_body += 'group by x.cookie;\n'
            else:
                sql_body += 'create table rqs_'+taskid+'_ps_cookie_active \n'
                sql_body += "row format delimited fields terminated by '\\t' as\n"
                sql_body += 'select b.cookie\n'
                sql_body += 'from rqs_'+taskid+'_ps_cookie b\n'
                sql_body += 'join default.ud_al_ps_session_data a\n'
                sql_body += 'on a.cookie = b.cookie\n'
                sql_body += 'where a.partition_stat_date in '+active_date+'\n'
                sql_body += ';\n'
        else:
            if minimum_pv >= 2:
                sql_body += 'create table rqs_'+taskid+'_ps_cookie_active \n'
                sql_body += "row format delimited fields terminated by '\\t' as\n"
                sql_body += 'select cookie ,sum(pv) as pv\n'
                sql_body += 'from rqs_'+taskid+'_ps_cookie b\n'
                sql_body += 'group by cookie;\n'
            else:
                sql_body += 'create table rqs_'+taskid+'_ps_cookie_active as\n'
                sql_body += 'select cookie\n'
                sql_body += 'from rqs_'+taskid+'_ps_cookie b\n'
                sql_body += ';\n'
        
        save_to_sql_file(sql_body, cur_path+'Step'+str(Cur_Step)+'/rqs_'+taskid+'_ps_cookie_active.sql')
    if '11000' in channel:
        #wise
        sql_body = ""
        sql_body += sql_head
        sql_body += 'drop table if exists rqs_'+taskid+'_wise_uid_active;\n'
        if cookie_active_length > 0:
            if minimum_pv >= 2:
                sql_body += 'create table rqs_'+taskid+'_wise_uid_active \n'
                sql_body += "row format delimited fields terminated by '\\t' as\n"
                sql_body += 'select x.wise_uid ,sum(x.pv) as pv\n'
                sql_body += 'from (select b.wise_uid,b.pv \n'
                sql_body += '      from rqs_'+taskid+'_wise_uid b\n'
                sql_body += '      join default.ud_al_wise_session_data a\n'
                sql_body += '      on a.wise_uid = b.wise_uid\n'
                sql_body += '      where a.partition_stat_date in '+active_date+'\n'
                sql_body += '      and a.is_filter=0\n'
                sql_body += '      ) x\n'
                sql_body += 'group by x.wise_uid;\n'
            else:
                sql_body += 'create table rqs_'+taskid+'_wise_uid_active\n'
                sql_body += "row format delimited fields terminated by '\\t' as\n"
                sql_body += 'select b.wise_uid\n'
                sql_body += 'from rqs_'+taskid+'_wise_uid b\n'
                sql_body += 'join default.ud_al_wise_session_data a\n'
                sql_body += 'on a.wise_uid = b.wise_uid\n'
                sql_body += 'where a.partition_stat_date in '+active_date+'\n'
                sql_body += 'and  a.is_filter=0\n'
                sql_body += ';\n'
        else:
            if minimum_pv >= 2:
                sql_body += 'create table rqs_'+taskid+'_wise_uid_active \n'
                sql_body += "row format delimited fields terminated by '\\t' as\n"
                sql_body += 'select wise_uid ,sum(pv) as pv\n'
                sql_body += 'from rqs_'+taskid+'_wise_uid b\n'
                sql_body += 'group by wise_uid;\n'
            else:
                sql_body += 'create table rqs_'+taskid+'_wise_uid_active as\n'
                sql_body += 'select wise_uid\n'
                sql_body += 'from rqs_'+taskid+'_wise_uid b\n'
                sql_body += ';\n'
        save_to_sql_file(sql_body, cur_path+'Step'+str(Cur_Step)+'/rqs_'+taskid+'_wise_uid_active.sql')
    Cur_Step += 1

    
    # Step : cookie filter 
    field_ps = 'cookie string,user_id string,normalized_query string,partition_stat_date string,action_time string,\n'
    field_ps+= 'province string,city string,matched_action_name string,clk_url string,url_host string,query_level1_type string,\n'
    field_ps+= 'query_level2_type string,attributes string,goal_id bigint,goal_step int'
    
    field_wise = 'wise_uid string,normalized_query string,partition_stat_date string,action_time string,\n'
    field_wise+= 'province string,city string,matched_action_name string,clk_url string,url_host string,attributes string,\n'
    field_wise+= 'goal_id string,goal_step bigint'
    
    if '10011' in channel:
        sql_body = ""
        sql_body += sql_head
        sql_body += 'drop table if exists rqs_'+taskid+'_ps_cookie_filter;\n'
        sql_body += 'create table rqs_'+taskid+'_ps_cookie_filter as\n'
        sql_body += 'select cookie \n'
        sql_body += 'from rqs_'+taskid+'_ps_cookie_active\n'
        if rand_cookie_num >= 1:
            sql_body += 'distribute by rand()\nsort by rand()\n'
            sql_body += 'limit '+str(rand_cookie_num)
        sql_body += ';\n'
        
        sql_body += 'drop table if exists rqs_'+taskid+'_ps_log;\n'
        sql_body += 'create table rqs_'+taskid+'_ps_log('+field_ps+')\n'
        sql_body += "row format delimited fields terminated by '\\t';\n"
        save_to_sql_file(sql_body, cur_path+'Step'+str(Cur_Step)+'/rqs_'+taskid+'_ps_cookie_filter.sql')
    if '11000' in channel:
        sql_body = ""
        sql_body += sql_head
        sql_body += 'drop table if exists rqs_'+taskid+'_wise_uid_filter;\n'
        sql_body += 'create table rqs_'+taskid+'_wise_uid_filter as\n'
        sql_body += 'select wise_uid \n'
        sql_body += 'from rqs_'+taskid+'_wise_uid_active\n'
        if rand_cookie_num >= 1:
            sql_body += 'distribute by rand()\nsort by rand()\n'
            sql_body += 'limit '+str(rand_cookie_num)
        sql_body += ';\n'
        
        sql_body += 'drop table if exists rqs_'+taskid+'_wise_log;\n'
        sql_body += 'create table rqs_'+taskid+'_wise_log('+field_wise+')\n'
        sql_body += "row format delimited fields terminated by '\\t';\n"
        save_to_sql_file(sql_body, cur_path+'Step'+str(Cur_Step)+'/rqs_'+taskid+'_wise_uid_filter.sql')

    
    if minimum_pv >=2 and  cookie_num >= 1:
        if '10011' in channel:
            sql_body = ""
            sql_body += sql_head
            sql_body += 'drop table if exists rqs_'+taskid+'_ps_cookie_filter_pv;\n'
            sql_body += 'create table rqs_'+taskid+'_ps_cookie_filter_pv as\n'
            sql_body += 'select x.cookie \n'
            sql_body += 'from ( select cookie,pv from rqs_'+taskid+'_ps_cookie_active where pv >= '+str(minimum_pv)+' \n'
            sql_body += '       order by pv desc limit '+str(cookie_num)+'0 ) x\n'
            sql_body += 'distribute by rand()\nsort by rand()\n'
            sql_body += 'limit '+str(cookie_num)+';\n'
            
            sql_body += 'drop table if exists rqs_'+taskid+'_ps_log_pv;\n'
            sql_body += 'create table rqs_'+taskid+'_ps_log_pv('+field_ps+')\n'
            sql_body += "row format delimited fields terminated by '\\t';\n"
            save_to_sql_file(sql_body, cur_path+'Step'+str(Cur_Step)+'/rqs_'+taskid+'_ps_cookie_filter_pv.sql')
        if '11000' in channel:
            sql_body = ""
            sql_body += sql_head
            sql_body += 'drop table if exists rqs_'+taskid+'_wise_uid_filter_pv;\n'
            sql_body += 'create table rqs_'+taskid+'_wise_uid_filter_pv as\n'
            sql_body += 'select x.wise_uid \n'
            sql_body += 'from ( select wise_uid,pv from rqs_'+taskid+'_wise_uid_active where pv >= '+str(minimum_pv)+' \n'
            sql_body += '       order by pv desc limit '+str(cookie_num)+'0 ) x\n'
            sql_body += 'distribute by rand()\nsort by rand()\n'
            sql_body += 'limit '+str(cookie_num)+';\n'
            
            sql_body += 'drop table if exists rqs_'+taskid+'_wise_log_pv;\n'
            sql_body += 'create table rqs_'+taskid+'_wise_log_pv('+field_wise+')\n'
            sql_body += "row format delimited fields terminated by '\\t';\n"
            save_to_sql_file(sql_body, cur_path+'Step'+str(Cur_Step)+'/rqs_'+taskid+'_wise_uid_filter_pv.sql')
    Cur_Step += 1
    
    # Step : get log
    start_date = datetime.datetime.strptime( log_date_range[0],'%Y%m%d')
    end_date = datetime.datetime.strptime( log_date_range[1],'%Y%m%d')
    cur_date = start_date
    N_part = 0
    while end_date >= cur_date:
        N_part += 1
        day_count = 1
        partition_stat_date = "("
        while cur_date <= end_date and day_count <= 14:
            partition_stat_date += "'"+datetime.datetime.strftime(cur_date,'%Y%m%d')+"',"
            day_count += 1
            cur_date += datetime.timedelta(1)
        partition_stat_date = partition_stat_date[0:-1]+')'
        
        if '10011' in channel:
            #ps
            sql_body = ""
            sql_body += sql_head
            sql_body += 'insert into table rqs_'+taskid+'_ps_log\n'
            sql_body += 'select  a.cookie,\n'
            sql_body += '        a.user_id,\n'
            sql_body += '        a.normalized_query,\n'
            sql_body += '        a.partition_stat_date,\n'
            sql_body += '        a.action_time,\n'
            sql_body += '        a.province,\n'
            sql_body += '        a.city,\n'
            sql_body += '        a.matched_action_name,\n'
            sql_body += '        a.clk_url,\n'
            sql_body += "        parse_url(a.clk_url,'HOST') AS url_host,\n"
            sql_body += '        a.query_level1_type,\n'
            sql_body += '        a.query_level2_type,\n'
            sql_body += '        a.attributes,\n'
            sql_body += '        a.goal_id,\n'
            sql_body += '        a.goal_step\n'
            sql_body += 'from rqs_'+taskid+'_ps_cookie_filter b\n'
            sql_body += 'join default.ud_al_ps_session_data a\n'
            sql_body += 'on b.cookie = a.cookie\n'
            sql_body += "where a.partition_stat_date in "+partition_stat_date+'\n'
            sql_body += ';\n'
            save_to_sql_file(sql_body, cur_path+'Step'+str(Cur_Step)+'/rqs_'+taskid+'_ps_log_'+str(N_part)+'.sql')
                
            if minimum_pv >=2 and  cookie_num >= 1:
                sql_body = ""
                sql_body += sql_head
                sql_body += 'insert into table rqs_'+taskid+'_ps_log_pv\n'
                sql_body += 'select  a.cookie,\n'
                sql_body += '        a.user_id,\n'
                sql_body += '        a.normalized_query,\n'
                sql_body += '        a.partition_stat_date,\n'
                sql_body += '        a.action_time,\n'
                sql_body += '        a.province,\n'
                sql_body += '        a.city,\n'
                sql_body += '        a.matched_action_name,\n'
                sql_body += '        a.clk_url,\n'
                sql_body += "        parse_url(a.clk_url,'HOST') AS url_host,\n"
                sql_body += '        a.query_level1_type,\n'
                sql_body += '        a.query_level2_type,\n'
                sql_body += '        a.attributes,\n'
                sql_body += '        a.goal_id,\n'
                sql_body += '        a.goal_step\n'
                sql_body += 'from rqs_'+taskid+'_ps_cookie_filter_pv b\n'
                sql_body += 'join default.ud_al_ps_session_data a\n'
                sql_body += 'on b.cookie = a.cookie\n'
                sql_body += "where a.partition_stat_date in "+partition_stat_date+'\n'
                sql_body += ';\n'
                save_to_sql_file(sql_body, cur_path+'Step'+str(Cur_Step)+'/rqs_'+taskid+'_ps_log_pv_'+str(N_part)+'.sql')
            
        if '11000' in channel:
            #wise
            sql_body = ""
            sql_body += sql_head
            sql_body += 'insert into table rqs_'+taskid+'_wise_log\n'
            sql_body += 'select a.wise_uid,\n'
            sql_body += '       a.normalized_query,\n'
            sql_body += '       a.partition_stat_date,\n'
            sql_body += '       a.action_time,\n'
            sql_body += '       a.province,\n'
            sql_body += '       a.city,\n'
            sql_body += '       a.matched_action_name,\n'
            sql_body += '       a.clk_url,\n'
            sql_body += "       parse_url(a.clk_url,'HOST') AS url_host,\n"
            sql_body += '       a.attributes,\n'
            sql_body += '       a.goal_id,\n'
            sql_body += '       a.goal_step\n'
            sql_body += 'from rqs_'+taskid+'_wise_uid_filter b\n'
            sql_body += 'join default.ud_al_wise_session_data a\n'
            sql_body += 'on b.wise_uid = a.wise_uid\n'
            sql_body += "where a.partition_stat_date in "+partition_stat_date+'\n'
            sql_body += 'and  a.is_filter=0\n'
            sql_body += ';\n'
            save_to_sql_file(sql_body, cur_path+'Step'+str(Cur_Step)+'/rqs_'+taskid+'_wise_log_'+str(N_part)+'.sql')
                
            if minimum_pv >=2 and  cookie_num >= 1:
                sql_body = ""
                sql_body += sql_head
                sql_body += 'insert into table rqs_'+taskid+'_wise_log_pv\n'
                sql_body += 'select a.wise_uid,\n'
                sql_body += '       a.normalized_query,\n'
                sql_body += '       a.partition_stat_date,\n'
                sql_body += '       a.action_time,\n'
                sql_body += '       a.province,\n'
                sql_body += '       a.city,\n'
                sql_body += '       a.matched_action_name,\n'
                sql_body += '       a.clk_url,\n'
                sql_body += "       parse_url(a.clk_url,'HOST') AS url_host,\n"
                sql_body += '       a.attributes,\n'
                sql_body += '       a.goal_id,\n'
                sql_body += '       a.goal_step\n'
                sql_body += 'from rqs_'+taskid+'_wise_uid_filter_pv b\n'
                sql_body += 'join default.ud_al_wise_session_data a\n'
                sql_body += 'on b.wise_uid = a.wise_uid\n'
                sql_body += "where a.partition_stat_date in "+partition_stat_date+'\n'
                sql_body += 'and  a.is_filter=0\n'
                sql_body += ';\n'
                save_to_sql_file(sql_body, cur_path+'Step'+str(Cur_Step)+'/rqs_'+taskid+'_wise_log_pv_'+str(N_part)+'.sql')
    Cur_Step += 1
    
    
    # Step : get uv pv
    if '10011' in channel:
        sql_body = ""
        sql_body += sql_head
        sql_body += 'select count(*)\n'
        sql_body += 'from (select distinct cookie from rqs_'+taskid+'_ps_log) x;\n'
        sql_body += 'select count(*)\n'
        sql_body += 'from rqs_'+taskid+'_ps_log;\n'
        save_to_sql_file(sql_body, cur_path+'Step'+str(Cur_Step)+'/uv_pv_rqs_'+taskid+'_ps_get_log.sql')
    if '11000' in channel:
        sql_body = ""
        sql_body += sql_head
        sql_body += 'select count(*)\n'
        sql_body += 'from (select distinct wise_uid from rqs_'+taskid+'_wise_log) x;\n'
        sql_body += 'select count(*)\n'
        sql_body += 'from rqs_'+taskid+'_wise_log;\n'
        save_to_sql_file(sql_body, cur_path+'Step'+str(Cur_Step)+'/uv_pv_rqs_'+taskid+'_wise_get_log.sql')
    
    if minimum_pv >=2 and  cookie_num >= 1:
        if '10011' in channel:
            sql_body = ""
            sql_body += sql_head
            sql_body += 'select count(*)\n'
            sql_body += 'from (select distinct cookie from rqs_'+taskid+'_ps_log_pv) x;\n'
            sql_body += 'select count(*)\n'
            sql_body += 'from rqs_'+taskid+'_ps_log_pv;\n'
            save_to_sql_file(sql_body, cur_path+'Step'+str(Cur_Step)+'/uv_pv_rqs_'+taskid+'_ps_get_log_pv.sql')
        if '11000' in channel:
            sql_body = ""
            sql_body += sql_head
            sql_body += 'select count(*)\n'
            sql_body += 'from (select distinct wise_uid from rqs_'+taskid+'_wise_log_pv) x;\n'
            sql_body += 'select count(*)\n'
            sql_body += 'from rqs_'+taskid+'_wise_log_pv;\n'
            save_to_sql_file(sql_body, cur_path+'Step'+str(Cur_Step)+'/uv_pv_rqs_'+taskid+'_wise_get_log_pv.sql')
    
    Cur_Step += 1
    
    
    # Step : get output log
    if '10011' in channel:
        sql_body = ""
        sql_body += sql_head
        sql_body += 'select *\n'
        sql_body += 'from rqs_'+taskid+'_ps_log;\n'
        save_to_sql_file(sql_body, cur_path+'Step'+str(Cur_Step)+'/rqs_'+taskid+'_ps_get_log.sql')
    if '11000' in channel:
        sql_body = ""
        sql_body += sql_head
        sql_body += 'select *\n'
        sql_body += 'from rqs_'+taskid+'_wise_log;\n'
        save_to_sql_file(sql_body, cur_path+'Step'+str(Cur_Step)+'/rqs_'+taskid+'_wise_get_log.sql')
    
    if minimum_pv >=2 and  cookie_num >= 1:
        if '10011' in channel:
            sql_body = ""
            sql_body += sql_head
            sql_body += 'select *\n'
            sql_body += 'from rqs_'+taskid+'_ps_log_pv;\n'
            save_to_sql_file(sql_body, cur_path+'Step'+str(Cur_Step)+'/rqs_'+taskid+'_ps_get_log_pv.sql')
        if '11000' in channel:
            sql_body = ""
            sql_body += sql_head
            sql_body += 'select *\n'
            sql_body += 'from rqs_'+taskid+'_wise_log_pv;\n'
            save_to_sql_file(sql_body, cur_path+'Step'+str(Cur_Step)+'/rqs_'+taskid+'_wise_get_log_pv.sql')
    
    print Cur_Step
    return Cur_Step

if __name__ == '__main__':
    main(sys.argv)
