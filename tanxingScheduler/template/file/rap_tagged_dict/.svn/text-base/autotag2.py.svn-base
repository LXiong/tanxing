#!/bin/python
# -*- coding=utf-8 -*-

import sys
import os
import re

def read_rules(fn):
    if fn=='null':
	return []
    f = open(fn,'r')
    rules = []
    r = 0
    for line in f:
        if r==0:
            r=1
            continue
        rules.append(line.strip())
    f.close()
    return rules

def tagged_by_rules(rules_array,query,account):	
    if len(rules_array)==0:
        return [query+','+account],[]

    dic={')':')', '(':'(', '!':' not ', '&':' and ', '|':' or ','（':'(','）':')'}
    res_tag=[]
    num_match = 0
    wrongRule=[]
    for rule_array in rules_array:
        try:
            if not rule_array.startswith('"'):
                tmp = rule_array.split(',')
            else:
                tmp_array = rule_array.split('"')[1:]
                tmp_array[0] = re.sub(',', '', tmp_array[0])
                tmp = ''.join(tmp_array)

            rule,rule_account,rule_tag = tmp[0],tmp[1],tmp[2:]
            str_cmd = ""
            search_word = ""
            #flag: 0 - return if in acount_list, 1 - return if not in account_list 
            flag = rule_account.startswith('!')
            rule_account_list = re.sub('[!()]','',rule_account).split(';')
            for i in range(len(rule)-1):
                if rule[i] in dic.keys():
                    str_cmd += dic[rule[i]]
                else:
                    if rule[i+1] in dic.keys():
                        search_word += rule[i]
                        ind = query.find(search_word)>=0
                        str_cmd += str(ind)
                        search_word = ""
                    else:
                        search_word += rule[i]                    
            str_cmd += dic[rule[-1]]
            #print str_cmd
            if (eval(str_cmd)==1) & (flag^((account=="") or (account in rule_account_list))):
	        num_match += 1
	        if num_match==1:
           	    res_tag.append(query+','+account+','+','.join([tag for tag in rule_tag]))
	        else:
		    res_tag.append(query+','+account+','+','.join([tag for tag in rule_tag]))
        except:
            wrongRule.append(rule)

    if len(res_tag)>0:
        return res_tag,wrongRule
    else:
        return [query+','+account],wrongRule

def read_dict(fn):
    if fn=='null':
	return {}
    f = open(fn,'r')
    r = 0
    dic = {}
    for line in f:
	line = line.strip()
	if r==0:
            r=1
	    continue
        if not line.startswith('"'):
	    lines = line.split(',')
	else:
	    tmp_line = line.split('"')[1:]
	    tmp_line[0] = re.sub(',','',tmp_line[0])
	    lines = ''.join(tmp_line).split(',')
	query = lines[0]
	account = lines[1]
	tag = lines[2:]
	flag = account.startswith('!')
	account_list = re.sub('[!()]','',account).split(';')
	if query not in dic.keys():
	    dic[query] = [[flag, account_list, tag]]
	else:
	    dic[query].append([flag, account_list, tag])
        
    return dic

def tagged_by_dict(query_dict, query, account):
    res = []
    if query_dict == {}:
	return []
    for match in query_dict[query]:
	if (match[0])^(account=="" or (account in match[1])):
            res.append(query+','+account+','+','.join(match[2]))

    return res
    
def main(argv):
    query_dict = read_dict(argv[1])
    rules_array = read_rules(argv[2])
    f = open(argv[3],'r')
    fout = open(argv[4],'w')
    r = 0
    AllwrongRule=[]
    for line in f:
        if r==0:
	    r=1
	    continue
        line = line.strip()
	if not line.startswith('"'):
            lines = line.split(',')
        else:
            tmp_line = line.split('"')[1:]
            tmp_line[0] = re.sub(',', '', tmp_line[0])
            lines = ''.join(tmp_line).split(',')
        query = lines[0]
        account = lines[1]
	if query in query_dict.keys():
	    res_tag= tagged_by_dict(query_dict, query, account)
            for tag in res_tag:
		fout.write(tag)
                fout.write('\n')
	else:
            res_tag, wrongRule = tagged_by_rules(rules_array, query, account)
            AllwrongRule+=wrongRule
            for tag in res_tag:
                fout.write(tag)
                fout.write('\n')
    for rule in list(set(AllwrongRule)):
        print rule
    f.close()
    fout.close()
    return 0
    

if __name__ == "__main__":
    main(sys.argv)
