#!/bin/bash -

source ../conf/sys.conf

download_file_http_loc=$1
download_file_ori=$2

log=${log_dir}/summary.log

if [ $# != 2 ]
then
	echo "ERROR: There should be 2 input parameters, sh download.sh $download_file_http_loc $download_file_ori" >> ${log}
    exit -1
fi

${PY_PATH} download.py ${download_file_http_loc} ${download_file_ori}

if [ ! -f "${download_file_ori}" ]
then
    echo "ERROR: Failed in download ${download_file_http_loc}!" >> ${log}
	exit -1	
else
    echo "SUCCESS: download ${download_file_http_loc} !" >> ${log}
fi

lines=`cat ${download_file_ori} | wc -l`
if((${lines} == 0))
then
	echo "ERROR: ${download_file_ori} is empty!" >> ${log}
	exit -1
else
	echo "SUCCESS: get ${download_file_ori} !" >> ${log}
fi

exit 0
