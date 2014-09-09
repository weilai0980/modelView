#!/bin/bash

CLASSPATH= '/usr/lib/hbase/bin/../conf:/usr/lib/jvm/j2sdk1.6-oracle/lib/tools.jar:/usr/lib/hbase/bin/..:/usr/lib/hbase/bin/../hbase-0.94.2-cdh4.2.0-security.jar:/usr/lib/hbase/bin/../hbase-0.94.2-cdh4.2.0-security-tests.jar:/usr/lib/hbase/bin/../hbase.jar:/usr/lib/hbase/bin/../lib/activation-1.1.jar:/usr/lib/hbase/bin/../lib/aopalliance-1.0.jar:/usr/lib/hbase/bin/../lib/asm-3.2.jar:/usr/lib/hbase/bin/../lib/avro-1.7.3.jar:/usr/lib/hbase/bin/../lib/commons-beanutils-1.7.0.jar:/usr/lib/hbase/bin/../lib/commons-beanutils-core-1.8.0.jar:/usr/lib/hbase/bin/../lib/commons-cli-1.2.jar:/usr/lib/hbase/bin/../lib/commons-codec-1.4.jar:/usr/lib/hbase/bin/../lib/commons-collections-3.2.1.jar:/usr/lib/hbase/bin/../lib/commons-configuration-1.6.jar:/usr/lib/hbase/bin/../lib/commons-daemon-1.0.3.jar:/usr/lib/hbase/bin/../lib/commons-digester-1.8.jar:/usr/lib/hbase/bin/../lib/commons-el-1.0.jar:/usr/lib/hbase/bin/../lib/commons-httpclient-3.1.jar:/usr/lib/hbase/bin/../lib/commons-io-2.1.jar:/usr/lib/hbase/bin/../lib/commons-lang-2.5.jar:/usr/lib/hbase/bin/../lib/commons-logging-1.1.1.jar:/usr/lib/hbase/bin/../lib/commons-net-3.1.jar:/usr/lib/hbase/bin/../lib/core-3.1.1.jar:/usr/lib/hbase/bin/../lib/gmbal-api-only-3.0.0-b023.jar:/usr/lib/hbase/bin/../lib/grizzly-framework-2.1.1.jar:/usr/lib/hbase/bin/../lib/grizzly-framework-2.1.1-tests.jar:/usr/lib/hbase/bin/../lib/grizzly-http-2.1.1.jar:/usr/lib/hbase/bin/../lib/grizzly-http-server-2.1.1.jar:/usr/lib/hbase/bin/../lib/grizzly-http-servlet-2.1.1.jar:/usr/lib/hbase/bin/../lib/grizzly-rcm-2.1.1.jar:/usr/lib/hbase/bin/../lib/guava-11.0.2.jar:/usr/lib/hbase/bin/../lib/guice-3.0.jar:/usr/lib/hbase/bin/../lib/guice-servlet-3.0.jar:/usr/lib/hbase/bin/../lib/high-scale-lib-1.1.1.jar:/usr/lib/hbase/bin/../lib/httpclient-4.1.3.jar:/usr/lib/hbase/bin/../lib/httpcore-4.1.3.jar:/usr/lib/hbase/bin/../lib/jackson-core-asl-1.8.8.jar:/usr/lib/hbase/bin/../lib/jackson-jaxrs-1.8.8.jar:/usr/lib/hbase/bin/../lib/jackson-mapper-asl-1.8.8.jar:/usr/lib/hbase/bin/../lib/jackson-xc-1.8.8.jar:/usr/lib/hbase/bin/../lib/jamon-runtime-2.3.1.jar:/usr/lib/hbase/bin/../lib/jasper-compiler-5.5.23.jar:/usr/lib/hbase/bin/../lib/jasper-runtime-5.5.23.jar:/usr/lib/hbase/bin/../lib/javax.inject-1.jar:/usr/lib/hbase/bin/../lib/javax.servlet-3.0.jar:/usr/lib/hbase/bin/../lib/jaxb-api-2.1.jar:/usr/lib/hbase/bin/../lib/jaxb-impl-2.2.3-1.jar:/usr/lib/hbase/bin/../lib/jersey-client-1.8.jar:/usr/lib/hbase/bin/../lib/jersey-core-1.8.jar:/usr/lib/hbase/bin/../lib/jersey-grizzly2-1.8.jar:/usr/lib/hbase/bin/../lib/jersey-guice-1.8.jar:/usr/lib/hbase/bin/../lib/jersey-json-1.8.jar:/usr/lib/hbase/bin/../lib/jersey-server-1.8.jar:/usr/lib/hbase/bin/../lib/jersey-test-framework-core-1.8.jar:/usr/lib/hbase/bin/../lib/jersey-test-framework-grizzly2-1.8.jar:/usr/lib/hbase/bin/../lib/jets3t-0.6.1.jar:/usr/lib/hbase/bin/../lib/jettison-1.1.jar:/usr/lib/hbase/bin/../lib/jetty-6.1.26.cloudera.2.jar:/usr/lib/hbase/bin/../lib/jetty-util-6.1.26.cloudera.2.jar:/usr/lib/hbase/bin/../lib/jruby-complete-1.6.5.jar:/usr/lib/hbase/bin/../lib/jsch-0.1.42.jar:/usr/lib/hbase/bin/../lib/jsp-2.1-6.1.14.jar:/usr/lib/hbase/bin/../lib/jsp-api-2.1-6.1.14.jar:/usr/lib/hbase/bin/../lib/jsp-api-2.1.jar:/usr/lib/hbase/bin/../lib/jsr305-1.3.9.jar:/usr/lib/hbase/bin/../lib/junit-4.10-HBASE-1.jar:/usr/lib/hbase/bin/../lib/kfs-0.3.jar:/usr/lib/hbase/bin/../lib/libthrift-0.9.0.jar:/usr/lib/hbase/bin/../lib/log4j-1.2.17.jar:/usr/lib/hbase/bin/../lib/management-api-3.0.0-b012.jar:/usr/lib/hbase/bin/../lib/metrics-core-2.1.2.jar:/usr/lib/hbase/bin/../lib/netty-3.2.4.Final.jar:/usr/lib/hbase/bin/../lib/paranamer-2.3.jar:/usr/lib/hbase/bin/../lib/protobuf-java-2.4.0a.jar:/usr/lib/hbase/bin/../lib/servlet-api-2.5-6.1.14.jar:/usr/lib/hbase/bin/../lib/servlet-api-2.5.jar:/usr/lib/hbase/bin/../lib/slf4j-api-1.6.1.jar:/usr/lib/hbase/bin/../lib/snappy-java-1.0.4.1.jar:/usr/lib/hbase/bin/../lib/stax-api-1.0.1.jar:/usr/lib/hbase/bin/../lib/xmlenc-0.52.jar:/usr/lib/hbase/bin/../lib/zookeeper.jar:/etc/hadoop/conf/*:/lib/*:/usr/lib/zookeeper/*:/usr/lib/zookeeper/lib/*::/etc/hadoop/conf:/usr/lib/hadoop/lib/*:/usr/lib/hadoop/.//*:/usr/lib/hadoop-hdfs/./:/usr/lib/hadoop-hdfs/lib/*:/usr/lib/hadoop-hdfs/.//*:/usr/lib/hadoop-yarn/lib/*:/usr/lib/hadoop-yarn/.//*:/usr/lib/hadoop-0.20-mapreduce/./:/usr/lib/hadoop-0.20-mapreduce/lib/*:/usr/lib/hadoop-0.20-mapreduce/.//*:/etc/hadoop/conf:/usr/lib/hadoop/lib/*:/usr/lib/hadoop/.//*:/usr/lib/hadoop-hdfs/./:/usr/lib/hadoop-hdfs/lib/*:/usr/lib/hadoop-hdfs/.//*:/usr/lib/hadoop-yarn/lib/*:/usr/lib/hadoop-yarn/.//*:/usr/lib/hadoop-0.20-mapreduce/./:/usr/lib/hadoop-0.20-mapreduce/lib/*:/usr/lib/hadoop-0.20-mapreduce/.//*'

#   QueryManager expti 1


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
java -cp $CLASSPATH QueryManager expti $2
echo "ti"
elif [ $1 == "exptp" ]
then 
java -cp $CLASSPATH QueryManager exptp $2
echo "tp"
elif [ $1 == "expvi" ]
then 
java -cp $CLASSPATH QueryManager expvi $2
echo "vi"
elif [ $1 == "expvp" ]
then
java -cp $CLASSPATH QueryManager expvp $2
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
fi
