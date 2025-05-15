#!/bin/bash
TMP_DIR=/tmp/bfx
mkdir -p $TMP_DIR
WORK_DIR=/home/trader/datafeed
cd $WORK_DIR
PID_FILE=$TMP_DIR/bfx_candles_dl.pid
TS=`date +'[%Y-%m-%d %H:%M]'`
if [ -e $PID_FILE ];
then
 PID=`cat $PID_FILE`
 echo "$TS. Previous PID = $PID, waiting exit..."  > /tmp/bfx/start.log
 timeout 300 tail --pid=$PID -f /dev/null
else
 sleep 3
fi


SCREEN_LOG=$TMP_DIR/screen.out
truncate --size=0 $SCREEN_LOG
date > $TMP_DIR/candles_dl.start
chmod +x $WORK_DIR/bfx_candles_dl.php
bzip2 -f --best logs/bfx_candle*.log
screen -L -Logfile $SCREEN_LOG  -qdmS BFXCandleDL $WORK_DIR/bfx_candles_dl.php
echo "SCREEN RESULT=$? "`  date` > $TMP_DIR/candles_dl.end
sleep 5
tail -n 50 /var/log/php*.log
ps aux | grep candles_dl | grep php
