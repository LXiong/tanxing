#coding=utf-8

import traceback as tb

import urllib2

import sys

from sms_v3_ReportService import *

def get_obj_report(service, request, acc_name, report_file):
    try:
        service.setUsername(acc_name)
        service.setTarget(acc_name)
        
        newres = service.getProfessionalReportId(request)
        print newres
        rid = newres["body"]["reportId"]
        print rid
	
        request = {"reportId":rid}
        while 1:
            newres = service.getReportState(request)
            report_stat = newres["body"]["isGenerated"]
            if report_stat == 3:
                break
        			
        request = {"reportId":rid}
        newres = service.getReportFileUrl(request)
        print newres
        report_url = newres["body"]["reportFilePath"]
        print report_url        
        
        report = urllib2.urlopen(report_url)
        report_file.write(report.read())
    except Exception, e:
        print e
        tb.print_exc()

def main(argv = sys.argv):
    acc_name_file = open(argv[1], "r")
    start_date = argv[2]
    end_date = argv[3]
    report_type = argv[4]
    report_file = open(argv[5], "wa")
    
    service = sms_v3_ReportService()
    request = {}
	#performanceData这个里面的顺序和报告的结果顺序无关，可以加其他的字段，且不需要修改其他脚本
    if report_type == "keyword":
        request = {"reportRequestType":{"performanceData":["click", "impression", "cost", "position"], "levelOfDetails":11, "reportType":14, "startDate":start_date, "endDate":end_date, "unitOfTime":8}}
    elif report_type == "keyword_pc":    
        request = {"reportRequestType":{"performanceData":["click", "impression", "cost", "position"], "levelOfDetails":11, "reportType":14, "startDate":start_date, "endDate":end_date, "unitOfTime":8, "device":1}}
    elif report_type == "keyword_wise":        
        request = {"reportRequestType":{"performanceData":["click", "impression", "cost", "position"], "levelOfDetails":11, "reportType":14, "startDate":start_date, "endDate":end_date, "unitOfTime":8, "device":2}}
    elif report_type == "account":
        request = {"reportRequestType":{"reportType":2, "startDate":start_date, "endDate":end_date, "levelOfDetails":2, "statRange":2, "unitOfTime":8, "performanceData":["click", "impression", "cost"]}}
    elif report_type == "account_pc":
        request = {"reportRequestType":{"reportType":2, "startDate":start_date, "endDate":end_date, "levelOfDetails":2, "statRange":2, "unitOfTime":8, "performanceData":["click", "impression", "cost"], "device": 1}}
    elif report_type == "account_wise":        
        request = {"reportRequestType":{"reportType":2, "startDate":start_date, "endDate":end_date, "levelOfDetails":2, "statRange":2, "unitOfTime":8, "performanceData":["click", "impression", "cost"], "device": 2}}
    else:
        print "ERROR: Invalid report_type!"
        sys.exit()
    
    for acc_name in acc_name_file:
        get_obj_report(service, request, acc_name.strip(), report_file)  
	
    print "SUCCESS: Get account report from sms API!"
    
if __name__ == "__main__":
    main(sys.argv)
