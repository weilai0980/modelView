#!/bin/bash

rm MyFile.4.log.db
rm MyFile.data.db
rm MyFile.index.db
rm MyFile.trace.db
rm -r csv-check-points

ant gsn



#PATH1='/home/guo/workspace/tsdb_cloud/hbase/hbase-0.92.1.jar'
#CLASSPATH=$PATH1:"$CLASSPATH"
#PATH2='/home/guo/workspace/tsdb_cloud/hbase/hbase-0.92.1-tests.jar'
#CLASSPATH=$PATH2:"$CLASSPATH"

#for i in /home/guo/workspace/tsdb_cloud/hbase/lib/*.jar; 
#do CLASSPATH=/$i:"$CLASSPATH"; 
#done 

#for i in /home/guo/workspace/tsdb_cloud/hbase/conf/*.*; 
#do CLASSPATH=/$i:"$CLASSPATH"; 
#done 

#for i in /home/guo/workspace/tsdb_cloud/hadoop/*.jar; 
#do CLASSPATH=/$i:"$CLASSPATH"; 
#done 

#for i in /home/guo/workspace/tsdb_cloud/hadoop/hadoop-conf/*.*; 
#do CLASSPATH=/$i:"$CLASSPATH"; 
#done 

#PATH1='/home/guo/storm-0.8.2/storm-0.8.2.jar'

#for i in /home/guo/storm-0.8.2/lib/*.jar; 
#do CLASSPATH=/$i:"$CLASSPATH"; 
#done 

#CLASSPATH=$PATH1:"$CLASSPATH"

#for i in /home/guo/workspace/tsdb_cloud/hadoop/lib/jsp-2.1/*.jar; 
#do CLASSPATH=/$i:"$CLASSPATH"; 
#done 

#export CLASSPATH=.:$CLASSPATH 

#echo $CLASSPATH

#rm streamqry.jar
#rm sources_list
#rm bin/*.class
#rm -r bin/basetool

#find ./src/ -name *.java >sources_list
#javac -classpath $CLASSPATH -d bin @sources_list
#jar -cvf streamqry.jar -C bin/ .

# $1 $2

#/home/guo/storm-0.8.2/bin/storm jar streamqry.jar main/TopologyMain $1 conqry 
#echo @@@@@@@@@@@@@@@@@@@@@@@@@return
