#!/bin/bash

#...............path configuration..............
#for i in ../tsdb_cloud/lib/*.jar; 
#do CLASSPATH=/$i:"$CLASSPATH"; 
#done 

PATH1='/home/puneet/hadoop/conf/*.*'
CLASSPATH=$PATH1:"$CLASSPATH"
PATH2='/home/puneet/hbase/conf/*.*'
CLASSPATH=$PATH2:"$CLASSPATH" 

for i in /home/puneet/hadoop/lib/*.jar; 
do CLASSPATH=/$i:"$CLASSPATH"; 
done 

for i in /home/puneet/hadoop/*.jar; 
do CLASSPATH=/$i:"$CLASSPATH"; 
done 


for i in /home/puneet/hbase/*.jar; 
do CLASSPATH=/$i:"$CLASSPATH"; 
done 

for i in /home/puneet/hbase/lib/*.jar; 
do CLASSPATH=/$i:"$CLASSPATH"; 
done 

#for i in /home/puneet/hbase/jsp-2.1/*.jar; 
#do CLASSPATH=/$i:"$CLASSPATH"; 
#done 

export CLASSPATH=.:$CLASSPATH 
#...............................................

echo $1 $2

rm sources_list
find ./src/ -name *.java >sources_list
javac -classpath $CLASSPATH -d bin @sources_list


if [ $1 == "query" ]
then 

rm qry.jar
jar -cvf qry.jar -C bin/ .
hadoop jar qry.jar QueryManager $2

elif [ $1 == "load" ]
then 

 java -cp $CLASSPATH:'./bin/' QueryManager $2

fi
