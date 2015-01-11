#!/bin/bash
# QUERY_PATH : Pathname of the query file
# FORMATTED_QUERY_NAME
# FRAGMENT_LENGTH 
# DATABASE_LENGTH 
# QUERY_FRAGMENTS_PATH

if [ "$1" == "-h" ]; then
  echo -e "Usage: `basename $0` \nProvide:\nQUERY_PATH: Pathname of the query file\nFORMATTED_QUERY_NAME:New name of query\nFRAGMENT_LENGTH\nDATABASE_LENGTH\nQUERY_FRAGMENTS_PATH\n in this order "
  exit 0
fi

if [ "$#" -ne 5 ]; then
    echo "Illegal number of parameters"
    exit 0
fi

bin/parser -query $1  $2
bin/partition -query $2 -frag_length $3 -database $4
mkdir temp_$2
mv QueryPart* temp_$2/
rm  $5/temp_$2
mv temp_$2 $5/


echo -e "Query fragments are placed in temporary directory in $5"
