#!/bin/sh

lockfile=${1-daemonize.lock};

wcLock=(`wc -l ${lockfile}`);
if [ ${wcLock[0]} -gt 0 ]
then
  echo "terminate daemon:";
  cat ${lockfile};
  cat ${lockfile} | xargs kill;
else
  echo 'tanxingScheduler is not running';
fi
