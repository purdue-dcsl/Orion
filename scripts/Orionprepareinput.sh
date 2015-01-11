#!/bin/bash
#
# MAP_INPUTDIR_NAME 
# DB_SHARDS
#QUERY_FRAG_NUMBER
#DATABASE_DIR_LOC
#DB_NAME
# QUERY_PART_DIR

if [ "$1" == "-h" ]; then
  echo -e "Usage: `basename $0` \nProvide:\nMAP_INPUTDIR_NAME DB_SHARDS QUERY_FRAG_NUMBER DATABASE_DIR_LOC DB_NAME QUERY_PART_DIR in this order "
  exit 0
fi

if [ "$#" -ne 6 ]; then
    echo "Illegal number of parameters"
    exit 0
fi

mkdir $1
rm $1/*
cd $1
DB_SHARDS=$(($2-1))
$ORION_HOME/scripts/prepare_input.sh $DB_SHARDS $3 $4 $5 $6
#db_shards_count query_fragment_count database_dir database_name query_dir

