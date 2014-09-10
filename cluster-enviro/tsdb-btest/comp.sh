#!/bin/bash

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


for i in /home/guo/tsdb-btest/bin/lib/*.jar; 
do CLASSPATH=/$i:"$CLASSPATH"; 
done 

#for i in /home/guo/workspace/tsdb_cloud/hadoop/lib/jsp-2.1/*.jar; 
#do CLASSPATH=/$i:"$CLASSPATH"; 
#done 
export CLASSPATH=.:$CLASSPATH 

rm qry.jar
rm sources_list
#rm bin/*.class
#rm -r bin/basetool

find ./src/ -name *.java >sources_list
javac -classpath $CLASSPATH -d bin @sources_list
jar -cvf qry.jar -C bin/ .

hadoop jar qry.jar QueryManager $1 $2 

