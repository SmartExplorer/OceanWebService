cd `dirname $0` > /dev/null
ps -ef | grep "com.citi.ocean.restapi.service.VertxRunner" | grep "port=${OCEAN_WEB_BASE_PORT}" | grep -v grep | awk '{ print $2 }' | xargs kill
cd - > /dev/null