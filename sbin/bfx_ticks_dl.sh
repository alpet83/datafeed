#!/bin/bash
TMP_DIR=/tmp/bfx
mkdir -p $TMP_DIR
WORK_DIR=/home/trader/datafeed
cd $WORK_DIR
PID_FILE=$TMP_DIR/ticks_dl@all.pid
if [ -e $PID_FILE ];
then
 PID=`cat $PID_FILE`
 echo " Previous PID = $PID, waiting exit..."
 kill -SIGQUIT $PID
 timeout 300 tail --pid=$PID -f /dev/null
else
 sleep 3
fi

bzip2 --best -f logs/bfx_tick*.log

date > $TMP_DIR/ticks_dl.start
truncate --size=0 $TMP_DIR/t_screen.out
screen -L -Logfile $TMP_DIR/t_screen.out -qdmS BFXTicksDL $WORK_DIR/bfx_ticks_dl.php
echo "SCREEN RESULT=$? "`  date` > $TMP_DIR/ticks_dl.end
sleep 10
tail -n 50 /var/log/php*.log

ps aux | grep ticks_dl.php
