#!/bin/sh


times=0
$1 -f $2$3.sql 1>$2$3.out 2>$2$3.log

#restart at most 3 times if error occurs 
while [ $? -ne 0 ] && [ $times -lt 3 ]
do
	times=$((times+1))
	if [ $times -eq 3 ]
	then 
		echo "" > $2.error
		exit 1
	fi
	$1 -f $2$3.sql 1>$2$3.out 2>$2$3.log
done

echo "" > $2$3.done

exit 0
