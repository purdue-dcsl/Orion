#!/bin/sh
#Script file to generate inputs the way hadoop mapper expects it to be path:(of binary),program:blastn,query:path_to_query,database:path_to_database,output:path_to_output
#Query files start with QueryPart_num and databaseshard, the arguments to the script file specifiy the number of shards  and number of query fragmengts to generate required file. $1-shards.$2-db_shards
#Enter location of blast binary

BLAST_BIN=/home/min/a/kmahadik/mpiblast/mpiblast-1.6.0/ncbi/bin/blastall
#QUERY_PART_DIR=/home/min/a/kmahadik/Personal/Queryparts
if [ "$#" -ne 5 ]; then
    echo "Illegal number of parameters. Usage : $0 db_shards_count query_fragment_count database_dir database_name query_dir"
    exit
fi
for i in $(eval echo {0..$1}); 
do	
	if  [ 10 -gt $i ]; then
                dbase="$3/$4.00$i"
        elif [ 100 -gt $i ]; then
                dbase="$3/$4.0$i"
        elif [ 1000 -gt $i ]; then
                dbase="$3/$4.$i"
        fi
      
	for j in $(eval echo {1..$2});
	do
	echo "path:$BLAST_BIN,program:blastn,query:$5/QueryPart$j,database:$dbase,output:/tmp/kmahadik/output" > i${j}_${i}
	done
done


