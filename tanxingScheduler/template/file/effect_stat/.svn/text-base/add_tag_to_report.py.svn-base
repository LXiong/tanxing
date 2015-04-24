import os
import sys

def set_null_tags(col_name_line):
    null_tags = ""
    ele = col_name_line.split("\t")
    i = 0
    for e in ele:
        i += 1
        if i <= 2:
            continue
        null_tags += "\tnull"
    return null_tags
    
def main(argv = sys.argv):
    ori_report_file = open(argv[1], 'r')
    group_by_col_file = open(argv[2], 'r')  #kwd,account_name,tag1,tag2,tag3,...,...
    res_file = open(argv[3], 'w')
        
    res_lines = []
    res_lines.append("")
    str_tag_map = {}
    
    kwd_idx = 0
    account_name_idx = 1
    col_name_line_p2 = ""
    i = -1
    add_accname_as_key = 1
    for line in group_by_col_file:
        i += 1
        if i == 1:
            accname = line.strip().split(",")[account_name_idx]
            if accname == '' or accname == 'null' or accname == 'NULL' :
                add_accname_as_key = 0
            break
    
    group_by_col_file.seek(0)
    i = -1
    for line in group_by_col_file: 
        line = line.strip() 
        i += 1
        if i == 0:
            col_name_line_p2 = line.replace(",", "\t")
            continue
        ele = line.strip().split(",")
        key = ele[kwd_idx]
        if add_accname_as_key == 1:
            key += ele[account_name_idx]
        str_tag_map[key] = line.replace(",", "\t")
        #print key
        #print str_tag_map[key]        
    
    null_tags = set_null_tags(col_name_line_p2)
    
    kwd_idx = 9
    account_name_idx = 2
    col_name_line_p1 = ""
    i = -1
    for line in ori_report_file:
        line = line.strip()        
        i += 1
        if i == 0:
            col_name_line_p1 = line
            continue
        ele = line.split("\t")
        #print ele
        str = ele[kwd_idx] 
        if add_accname_as_key == 1:
            str += ele[account_name_idx]
        #print "str=" + str
        tags = ""
        if str in str_tag_map.keys():
            tags = str_tag_map[str]
            res_lines.append(line + "\t" + tags)
        else:
            res_lines.append(line + "\t" + ele[kwd_idx] + "\t" + ele[account_name_idx] + null_tags)
	
    res_lines[0] = col_name_line_p1 + "\t" + col_name_line_p2
    for line in res_lines:
        res_file.write(line + "\n")
    res_file.close()
    
if __name__ == "__main__":
    main(sys.argv)
