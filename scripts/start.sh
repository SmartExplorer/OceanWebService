#ident  "%W%"
#!/bin/sh

cd `dirname $0` > /dev/null

. ../../../common/current/scripts/setBase.sh

MAIN_CLASS_NAME=com.citi.ocean.restapi.service.VertxRunner
LOG_FILE_NAME=${LOG_DIR}/ocean_web_service_`date '+%Y%m%d_%H_%M_%S'`.log

export CLASSPATH=$PWD/../lib/*:$PWD/../config/${APP_ENV}

echo "CMD: nohup ${JAVA_HOME}/bin/java -cp ${CLASSPATH} -Dport=${OCEAN_WEB_BASE_PORT} ${MAIN_CLASS_NAME} > ${LOG_FILE_NAME} 2>&1 &"
nohup ${JAVA_HOME}/bin/java -cp ${CLASSPATH} -Dport=${OCEAN_WEB_BASE_PORT} ${MAIN_CLASS_NAME} > ${LOG_FILE_NAME} 2>&1 &

sleep 5

PID="`ps -ef | grep "${MAIN_CLASS_NAME}" | grep -v grep | awk '{print $2}'`"
if [ -z "$PID" ] 
then
    echo "Failed to start"
    exit 1
else
    echo "Started successful"
    exit 0
fi
