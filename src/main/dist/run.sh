#!/usr/bin/env bash
#
# store hrdp page data
#
. /etc/profile
APPNAME="hrdp-portal-cache-pipeline"
APPDIR=/home/rgddata/pipelines/$APPNAME
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`
EMAIL_LIST=akundurthi@mcw.edu
if [ "$SERVER" = "REED" ]; then
  EMAIL_LIST=rgd.devops@mcw.edu
fi

cd $APPDIR
java -Dspring.config=$APPDIR/../properties/default_db2.xml \
    -Dlog4j.configurationFile=file://$APPDIR/properties/log4j2.xml \
    -jar lib/$APPNAME.jar "$@" > run.log 2>&1

mailx -s "[$SERVER]  hrdp-portal-cache-pipeline Run" $EMAIL_LIST < $APPDIR/logs/status.log
