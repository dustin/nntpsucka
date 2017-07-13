#!/bin/bash

cd /usr/src/nntpsucka-master

if test -z ${1}; then echo "usage $0 group"; exit 1; fi

pidfile=nntpsucka_${1}.pid
if test -f ${pidfile}; then
 pid=`cat ${pidfile}`
 ps aux |grep "${pid}"|grep python|grep "${1}\.conf" > /dev/null
 test $? -eq 0 && echo "nntpsucka_${1}.pid ${pid} runnning!" && exit 1
fi;

test -x filter.sh && ./filter.sh
test -f doneList.${1} && sort -u doneList.${1} > doneList.${1}.tmp; mv -v doneList.${1}.tmp doneList.${1}

gzip *_${1}.log
mkdir -p log.old/${1}
mv -v *_${1}.log.gz log.old/${1}/
LOGFILE="sucka`date +%s`_${1}.log"
if test -f "sucka_${1}.conf"; then
 ./nntpsucka.py "sucka_${1}.conf" > ${LOGFILE} &
 echo "tail -f ${LOGFILE}"
else
 echo "sucka_${1}.conf not found"
 exit 1
fi;

