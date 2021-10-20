#!/bin/bash

bashurl=$(cd `dirname $0`; pwd)/..


if [ ! -n "$1" ] ;then
    echo "[请传入 commit time]"
    exit 1
fi

if [ ! -n "$2" ] ;then
    echo "[请传入 base path]"
    exit 1
fi

if [ ! -n "$3" ] ;then
    echo "[请传入 table name]"
    exit 1
fi

if [ ! -f "$1" ];then
    echo "[$1 配置文件不存在]"
    exit 1
fi
jobid=$(echo $2 | sed 's/,/\n/g' | grep "jobid" | sed 's/:/\n/g' | sed '1d' | sed 's/}//g')
echo "[ 配置文件: $1 ]"
echo "[ 配置json: $2 ]"
echo "[ jobid: $jobid ]"
echo "[当前路径: $bashurl]"


s=`ls -d  ${bashurl}/libs/*`

strjar=`echo $s | sed "s/ /,/g"`

# principal=`sed '/^spark.submit.principal=/!d;s/.*=//' ${bashurl}/conf/spark_config_template.properties | sed 's/\r//'`
# keytab=`sed '/^spark.submit.keytab=/!d;s/.*=//' ${bashurl}/conf/spark_config_template.properties | sed 's/\r//'`


echo "[spark执行命令:]"

# spark 版本: spark-2.4.4-bin-hadoop2.7
echo "nohop spark-submit
        --master local
        --num-executors 1
        --executor-memory 2g
        --driver-cores 1
        --class cn.hoodie.compact.HoodieClean hoodie-option-jar-with-dependencies.jar 20210617162438 /tmp/hoodie_callback/test test

# echo "nohup /opt/spark/bin/spark-submit  --class SparkCunsumeKafka \\
#           --principal ${principal} \\
#           --keytab ${keytab}   \\
#           --driver-java-options '-Djava.security.auth.login.config=/home/xiahu/jaas.conf -XX:+PrintTenuringDistribution -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime -XX:+PrintGCTimeStamps -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/hoodie-heapdump.hprof' \\
#           --file '/home/xiahu/jaas.conf' \\
#           --master yarn \\
#           --deploy-mode client \\
#           --jars ${strjar}  \\
#           ${bashurl}/libs/fuxi-spark-consumer-1.0.0-SNAPSHOT.jar $1 '$2' & "

execute=$_

echo "[任务开始执行]"

if [ ! -d "${bashurl}/log" ];then
mkdir ${bashurl}/log
fi

time=$(date "+%Y-%m-%d_%H:%M:%S")
echo ${time}
execute=$(echo $execute | sed  "s:\\\::g")
eval $execute >${bashurl}/log/spark_consumer_${time}.log 2>&1