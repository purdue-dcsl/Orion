#!/bin/bash
#SEARCH_REDUCE_TASKS
#OFFSET
#QUERY_FRAG_LENGTH
#QUERY_ACTUAL_LENGTH
#THRESHOLD
#DB_ACTUAL_LENGTH
#K
#LAMBDA
#H
#MATCH
#MISMATCH
#GAP_INIT
#GAP_EXTEND
#E_THRESHOLD

#MAP_INPUTDIR
#OUTPUT_DIR
#SORT REDUCE TASKS



if [ "$1" == "-h" ]; then
  echo -e "Usage: `basename $0` \nProvide:\nSEARCH_REDUCE_TASKS\nOFFSET\nQUERY_FRAG_LENGTH\nQUERY_ACTUAL_LENGTH\nTHRESHOLD\nMAP_INPUTDIR\nOUTPUT_DIR\nSORT REDUCE TASKS\n in this order "
  exit 0
fi

$HADOOP_HOME/bin/hadoop fs -copyFromLocal ${15} inputq

$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-streaming-1.2.1.jar -input inputq -output ${16}_i  -mapper $ORION_HOME/scripts/blast.py 

$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-streaming-1.2.1.jar -libjars $ORION_HOME/jar_files/KReducer.jar -D mapred.reduce.tasks=$1 -D stream.map.output.field.separator=,  -D stream.num.map.output.key.fields=1 -D OFFSET=$2 -D QUERY_FRAG_LENGTH=$3 -D QUERY_ACTUAL_LENGTH=$4 -D THRESHOLD=$5 -D DB_ACTUAL_LENGTH=$6 -D K=$7 -D LAMBDA=$8 -D H=$9 -D MATCH=${10} -D MISMATCH=${11} -D GAP_INIT=${12} -D GAP_EXTEND=${13} -D E_THRESHOLD=${14}  -input inputq -output ${16}_i  -mapper $ORION_HOME/scripts/blast.py -reducer org.myorg.KReducer

$HADOOP_HOME/bin/hadoop fs -rmr ${16}_i/_logs
$HADOOP_HOME/bin/hadoop fs -rmr ${16}_i/_SUCCESS

$HADOOP_HOME/bin/hadoop jar $ORION_HOME/jar_files/KSorter.jar org.myorg.KSorter ${16}_i ${16}
#mapred.reduce.tasks


if [ "$#" -ne 16 ]; then
    echo "Illegal number of parameters"
    exit 0
fi




