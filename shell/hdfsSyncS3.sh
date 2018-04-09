#!/bin/bash

now=`date +%s`
now55=`expr $now + $1`
expectFinishTimeStr=`date -u '+%Y-%m-%d %H:%M' --date=@$now55`

echo "expectFinishTime ${expectFinishTimeStr}"

hdfsPath="hdfs://harunava/i18n/recommend/data/tiktokMusicalCount/onemin"
s3Path="s3://musically-datacenter-useast/task-data/view-counter-2/onemin"

mkdir tmpfileOne
touch _OK

function resetOffsetTime(){

    offset=$(/opt/tiger/yarn_deploy/hadoop/bin/hadoop fs -ls hdfs://harunava/i18n/recommend/data/tiktokMusicalCount/offsets/offset.json)
        offsetTimeStr=$(echo ${offset} | grep -P '\d{4}(\-\d{2}){2} \d{2}:\d{2}' -o)
    echo "offsetTimeStr $offsetTimeStr"
    offsetTime=`date -u --date="$offsetTimeStr" +%s`

}

resetOffsetTime

checkTimeStr=$(/opt/tiger/yarn_deploy/hadoop/bin/hadoop fs -cat hdfs://harunava/i18n/recommend/data/tiktokMusicalCount/path_checkpoint/_checkOne)
echo "checkTime $checkTimeStr"
checkTime=`date -u --date="$checkTimeStr" +%s`
checkTime=`expr $checkTime + 60`

while [ `date +%s` -le $now55 ]; do
    checkTimeStr=`date -u '+%Y-%m-%d %H:%M' --date=@$checkTime`
    offsetTimeStr=`date -u '+%Y-%m-%d %H:%M' --date=@$offsetTime`

    echo "checkTime = $checkTimeStr, offsetTime = $offsetTimeStr"

    checkTimeUtc8=`expr $checkTime + \( 8 \* 3600 \)`

    day=`date -u '+%Y%m%d' --date=@$checkTimeUtc8`
    min=`date -u '+%H%M' --date=@$checkTimeUtc8`

    echo "/opt/tiger/yarn_deploy/hadoop/bin/hadoop fs -test -e hdfs://harunava/i18n/recommend/data/tiktokMusicalCount/onemin/dt=${day}/hm=${min}/_SUCCESS"
    /opt/tiger/yarn_deploy/hadoop/bin/hadoop fs -test -e hdfs://harunava/i18n/recommend/data/tiktokMusicalCount/onemin/dt=${day}/hm=${min}/_SUCCESS
    exec_result=$?

    if [ ${exec_result} = 0 ]; then

        rm -rf tmpfileOne/*
        /opt/tiger/yarn_deploy/hadoop/bin/hadoop fs -copyToLocal ${hdfsPath}/dt=${day}/hm=${min}/* tmpfileOne/
        /usr/bin/aws s3 cp tmpfileOne/ ${s3Path}/dt=${day}/hm=${min}/ --recursive --acl bucket-owner-full-control
        exec_result=$?
        if [ ${exec_result} = 0 ]; then
            /usr/bin/aws s3 cp _OK ${s3Path}/dt=${day}/hm=${min}/ --acl bucket-owner-full-control
            echo $checkTimeStr > _checkOne
            /opt/tiger/yarn_deploy/hadoop/bin/hadoop fs -put -f _checkOne hdfs://harunava/i18n/recommend/data/tiktokMusicalCount/path_checkpoint/
            checkTime=`expr $checkTime + 60`
        fi

    else

        if [ "$checkTime" -lt "$offsetTime" ]; then
            checkTime=`expr $checkTime + 60`
        else
            echo "checkTime up to offsetTime, sleep 1 min"
            sleep 60
            resetOffsetTime
        fi
    fi

done

finishTimeStr=`date -u '+%Y-%m-%d %H:%M'`
echo "expectFinished: $expectFinishTimeStr realFinishTime: $finishTimeStr"
exit 0
