#!/bin/bash
TMP_DIR=/tmp/bnc
mkdir -p $TMP_DIR
WORK_DIR=/home/trader/datafeed
cd $WORK_DIR
PID_FILE=$TMP_DIR/candles_dl.pid
TS=`date +'[%Y-%m-%d %H:%M]'`
if [ -e $PID_FILE ];
then
 PID=`cat $PID_FILE`
 echo "$TS. Previous PID = $PID, waiting exit..." > $TMP_DIR/start.log
 timeout 300 tail --pid=$PID -f /dev/null
 kill $PID
else
 sleep 3
fi

date > $TMP_DIR/candles_dl.start
SCREEN_LOG=$TMP_DIR/screen.out
truncate --size=0 $SCREEN_LOG
chmod +x $WORK_DIR/bnc_candles_dl.php
bzip2 -f --best logs/bnc_ca*.log
screen -L -Logfile $SCREEN_LOG -qdmS BNCCandleDL  $WORK_DIR/bnc_candles_dl.php
tail -n 100 /var/log/php*.log | grep -C 20 bnc
echo "SCREEN RESULT=$? "`  date` > $TMP_DIR/candles_dl.end
ps aux | grep candles_dl | grep php
