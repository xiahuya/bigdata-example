#!/bin/bash



bashurl=$(cd `dirname $0`; pwd)/..



echo "[当前路径:$bashurl]"

s=`ls -d  ${bashurl}/libs/*`

strjar=`echo $s | sed "s/ /,/g"`

kerberosIsOpen=`sed '/^nuwa.kerberos.enable=/!d;s/.*=//' ${bashurl}/conf/spark-shell-test.propreties | sed 's/\r//'`
principal=`sed '/^nuwa.import.spark.submit.principal=/!d;s/.*=//' ${bashurl}/conf/spark-shell-test.propreties | sed 's/\r//'`
keytab=`sed '/^nuwa.import.spark.submit.keytab=/!d;s/.*=//' ${bashurl}/conf/spark-shell-test.propreties | sed 's/\r//'`

logname=sparkILoop_`date '+%Y%m%d_%H%M%S'`
echo "[spark执行命令:]"

if [ "true" = kerberosIsOpen ] ;then
  echo "spark-submit  --class com.clin.spark.loop.SparkShellTest \\
          --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer'
          --master yarn \\
          --deploy-mode client \\
          --principal spark/master@HADOOP.COM \\
          --keytab yarn.keytab \\
          --files ${bashurl}/conf/nuwa-import.properties \\
          --jars ${strjar}  \\
          ${bashurl}/libs/spark-shell-test-1.0-SNAPSHOT.jar  ${bashurl}/conf/nuwa-import.properties"
  execute=$_
  else
  echo "spark-submit  --class com.clin.spark.loop.SparkShellTest \\
          --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer'
          --master yarn \\
          --deploy-mode client \\
          --files ${bashurl}/conf/nuwa-import.properties \\
          --jars ${strjar}  \\
          ${bashurl}/libs/spark-shell-test-1.0-SNAPSHOT.jar  ${bashurl}/conf/nuwa-import.properties"
  execute=$_
fi


echo "[任务开始执行]"

if [ ! -d "${bashurl}/log" ];then
mkdir ${bashurl}/log
fi

execute=$(echo $execute | sed  "s:\\\::g")
#eval $execute

eval nohup $execute  1>${bashurl}/log/"${logname}".log 2>${bashurl}/log/"${logname}"_error.log &

echo ${bashurl}/log/"${logname}".log