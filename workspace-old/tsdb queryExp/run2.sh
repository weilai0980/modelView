#!/bin/bash
rm tp ti vp vi
PATH1='/home/guo/Downloads/hbase/hbase-0.92.1.jar'
CLASSPATH=$PATH1:"$CLASSPATH"
PATH2='/home/guo/Downloads/hbase/hbase-0.92.1-tests.jar'
CLASSPATH=$PATH2:"$CLASSPATH"

for i in /home/guo/Downloads/hbase/lib/*.jar; 
do CLASSPATH=/$i:"$CLASSPATH"; 
done 
export CLASSPATH=.:$CLASSPATH 

java -cp $CLASSPATH bin/QueryPro

