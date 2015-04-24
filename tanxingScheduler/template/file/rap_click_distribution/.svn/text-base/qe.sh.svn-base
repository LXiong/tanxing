#!/bin/sh

$1 -f $2.sql 1>$2.out 2>$2.log

times=0

#restart at most 3 times if error occurs 
while [ $? -ne 0 ]
do
    times=$((times+1))
    if [ $times -eq 3 ]
    then
        echo "" > $2.error
        exit -1
    fi
    $1 -f $2.sql 1>$2.out 2>$2.log
done

echo "" > $2.done

exit 0
