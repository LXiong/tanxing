#!/bin/sh

TSTAT_RUN=3
TSTAT_DONE=4
TSTAT_FAIL=5

rm checkDone.result

#cat $1/iter/head/* >> checkDone.log

statStr=`tail -n1 $1/iter/head/* | awk -F: '{print $1}'`

if [ "${statStr:0:13}" = "[STAT]=Failed" ]
then
  echo ${TSTAT_FAIL} > checkDone.result
elif [ "${statStr:0:14}" = "[STAT]=Success" ]
then
  echo ${TSTAT_DONE} > checkDone.result
else
  echo ${TSTAT_RUN} > checkDone.result
fi
