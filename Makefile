# the compiler: gcc for C program, define as g++ for C++
CC = cc
JCC = javac  
HADOOP_HOME = /home/min/a/kmahadik/hadoop
# compiler flags:
# -g    adds debugging information to the executable file
#  -Wall turns on most, but not all, compiler warnings
CFLAGS  = 
LIBS = -lm
#  the build target executable:
all: parser partition reducer sorter

parser: src/parser.c 
	$(CC) $(CFLAGS) -o bin/parser src/parser.c
  
partition: src/partition.c 
	$(CC) $(CFLAGS) $(LIBS) -o bin/partition src/partition.c

clean:
	$(RM) -r bin/* jar_files/*

reducer: src/org/myorg/KReducer.java
	$(JCC) -classpath $(HADOOP_HOME)/hadoop-core-1.2.1.jar -d jar_files src/org/myorg/KReducer.java && jar -cvf jar_files/KReducer.jar -C jar_files/ .

sorter: src/org/myorg/KSorter.java
	$(JCC) -classpath $(HADOOP_HOME)/hadoop-core-1.2.1.jar -d jar_files src/org/myorg/KSorter.java && jar -cvf jar_files/KSorter.jar -C jar_files/ .

