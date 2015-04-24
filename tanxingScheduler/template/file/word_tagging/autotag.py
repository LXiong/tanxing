#!/usr/bin/python

# -*- coding: gb2312 -*-
import urllib2
import re
import sys
import os
import string

def usage():
    print 'Usage : python $dictionary $rules $query > output'
	
def main(argv):
    if len(argv)<4:
        usage()
        os._exit(1)
        
    #load cidian_file
    cidian = read_dict(argv[1])
    #load rules
    regulars,rules,labels,acct,ind = load_rules(argv[2])
    
    #load query_file
    f = open(argv[3])
    r = 0
    for line in f:
        if r==0:
            r = 1
            continue
        wordset = {}
        lines = line.strip().split(',')
        qry = lines[0]
        account = lines[1]
        
        ############################
        ###########step 1###########
        #check if query in dictinary
        ############################
        re_lab = cidian_label(cidian,lines)
        if len(re_lab) > 0:
            lab = ','.join(re_lab)
            print (bytes(qry)+','+bytes(account)+','+bytes(lab))
            continue      
        #######################
        ########step 2#########
        #else label it by rules
        #######################
        print label_by_rules(regulars,rules,labels,acct,ind,lines,qry,account)
        

def label_by_rules(regulars,rules,labels,acct,ind,lines,qry,account):
    label = []
    for k in range(len(rules)): 
        hasLabel = False
        regular = regulars[k]
        wordset={}
        #words: [a,b,c,d]
        words = re.findall(r'[^|&!()]+',rules[k])
        for w in words:
            wordset[w] = False
        #regular: ['a|b', '&!', 'c|d']
        TorF = []
        for i in range(0,len(regular),2):
            #arr: ['a', '|', 'b']
            arr = re.split('([|&!])', regular[i])
            for j in range(0,len(arr),2):
                if lines[0].find(arr[j]) >= 0:
                    wordset[arr[j]] = True
            for j in range(1,len(arr),2):
                if (arr[j]=='|'):
                    wordset[arr[j+1]] |= wordset[arr[j-1]]
                else:
                    wordset[arr[j+1]] &= wordset[arr[j-1]]
            TorF.append(wordset[arr[-1]])
                
        for i in range(1,len(regular),2):
            if len(regular[i]) == 1:
                if regular[i]=='|':
                    TorF[2*i-1] |= TorF[2*i-2]
                else:
                    TorF[2*i-1] &= TorF[2*i-2]
            else:
                if regular[i]=='|':
                    TorF[2*i-1] = TorF[2*i-2] | (not TorF[2*i-1])
                else:
                    TorF[2*i-1] = TorF[2*i-2] & (not TorF[2*i-1])
        if TorF[-1]:
            if acct[k]=='null':
                hasLabel = True
            else:
                hasLabel = (ind[k]==1)^(account in acct[k])                    
        wordset.clear()
        if hasLabel:
            rlab = ','.join(labels[k])
            return (bytes(qry)+','+bytes(account)+','+bytes(rlab))
    if not hasLabel:
        return (bytes(qry)+','+bytes(account)+','+'null'+','+'null')


def load_rules(url):
    frules = open(url)
    regulars = []; rules = []; labels = []; acct = []; ind = []
    #generate rules
    r=0
    for liner in frules:
        if r==0:
            r=1
            continue
        indx = 0
        linesr = liner.strip().split(',')
        if linesr[1].startswith('!'):
            indx = 1
        # regular: ['a|b', '&!', 'c|d']
        regulars.append(re.split('[()]',linesr[0])[1:-1])
        rules.append(linesr[0])
        labels.append(linesr[2:])
        acct.append(re.findall(r'[^!();]+',linesr[1]))
        ind.append(indx)
    return regulars,rules,labels,acct,ind

#check if query in dictinaty
def cidian_label(dic,query):
    qry = query[0]
    account = query[1]
    for dd in dic:
        #dd: qry, ind, [account...], [label...]
        if (qry==dd[0]) & (dd[1]==0):
            if account in dd[2]:
                return dd[3]
        if (qry==dd[0]) & (dd[1]==1):
            if account not in dd[2]:
                return dd[3]
    return []

def read_dict(filename):
    #input:  query (account1,account2,...) label1 label2
    #output: [query, ind, [account1,account2,...], [label1,label2,...]]
    f = open(filename)
    dic = []
    r=0
    for line in f:
        if r==0:
            r=1
            continue
        d = []
        ind = 0
        lines = line.strip().split(',')
        qry = lines[0]
        if lines[1].startswith('!'):
            ind = 1
        accts = re.findall(r'[^!();]+',lines[1])
        labels = lines[2:]
        d.append(qry)
        d.append(ind)
        d.append(accts)
        d.append(labels)
        dic.append(d)
    return dic

if __name__ == "__main__":
    main(sys.argv)

