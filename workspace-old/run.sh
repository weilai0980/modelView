#!/bin/bash

PATH1='/home/guo/workspace/tsdb_cloud/hbase/hbase-0.92.1.jar'
CLASSPATH=$PATH1:"$CLASSPATH"
PATH2='/home/guo/workspace/tsdb_cloud/hbase/hbase-0.92.1-tests.jar'
CLASSPATH=$PATH2:"$CLASSPATH"

for i in /home/guo/workspace/tsdb_cloud/hbase/lib/*.jar; 
do CLASSPATH=/$i:"$CLASSPATH"; 
done 

for i in /home/guo/workspace/tsdb_cloud/hbase/conf/*.*; 
do CLASSPATH=/$i:"$CLASSPATH"; 
done 

for i in /home/guo/workspace/tsdb_cloud/hadoop/*.jar; 
do CLASSPATH=/$i:"$CLASSPATH"; 
done 

for i in /home/guo/workspace/tsdb_cloud/hadoop/lib/*.jar; 
do CLASSPATH=/$i:"$CLASSPATH"; 
done 

for i in /home/guo/workspace/tsdb_cloud/hadoop/lib/jsp-2.1/*.jar; 
do CLASSPATH=/$i:"$CLASSPATH"; 
done 
export CLASSPATH=.:$CLASSPATH 

#print "$i"

#java -cp $CLASSPATH QueryManager

#java -cp $CLASSPATH QueryManager c0

if [ $1 == "c0" ]
then
echo "c0"
java -cp $CLASSPATH QueryManager c0
elif [ $1 == "c1" ]
then 
echo "c1"
java -cp $CLASSPATH QueryManager c1
elif [ $1 == "c2"  ]
then
echo "c2"
java -cp $CLASSPATH QueryManager c2
elif [ $1 == "expti" ]
then 
java -cp $CLASSPATH QueryManager expti
echo "ti"
elif [ $1 == "exptp" ]
then 
java -cp $CLASSPATH QueryManager exptp
echo "tp"
elif [ $1 == "expvi" ]
then 
java -cp $CLASSPATH QueryManager expvi
echo "vi"
elif [ $1 == "expvp" ]
then
java -cp $CLASSPATH QueryManager expvp
echo "vp"
elif [ $1 == "l0" ]
then
echo "l0"
java -cp $CLASSPATH QueryManager l0
elif [ $1 == "l1" ]
then 
echo "l1"
java -cp $CLASSPATH QueryManager l1
elif [ $1 == "l2"  ]
then
echo "l2"
java -cp $CLASSPATH QueryManager l2
#else 
#echo "sdsd"
fi


