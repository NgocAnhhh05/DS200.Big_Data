#!/bin/bash

# CONFIG
JAR_PATH="target/MovieAnalysis-1.0-SNAPSHOT.jar"
MAIN_CLASS="com.ngocanh.hadoop"

INPUT_DIR="/lab01/input"
OUTPUT_DIR="/lab01/output"

# CHECK INPUT
if [ -z "$1" ]; then
  echo "Usage: ./run.sh ass01 | ass02 | ass03 | ass04"
  exit 1
fi

ASSIGN=$1

echo "Running $ASSIGN..."


# BUILD
echo "Building project..."
mvn clean package 

# REMOVE OLD OUTPUT
echo "Removing old output..."
hdfs dfs -rm -r $OUTPUT_DIR >/dev/null 2>&1

# RUN JOB
echo "Running Hadoop job..."



if [ "$ASSIGN" == "ass01" ] || [ "$ASSIGN" == "ass02" ]; then
  hadoop jar $JAR_PATH $MAIN_CLASS.$ASSIGN \
  $INPUT_DIR/movies.txt \
  $INPUT_DIR/ratings_1.txt \
  $INPUT_DIR/ratings_2.txt \
  $OUTPUT_DIR
else
  hadoop jar $JAR_PATH $MAIN_CLASS.$ASSIGN \
  $INPUT_DIR/movies.txt \
  $INPUT_DIR/ratings_1.txt \
  $INPUT_DIR/ratings_2.txt \
  $INPUT_DIR/users.txt \
  $OUTPUT_DIR
fi

CHECK RESULT
if [ $? -ne 0 ]; then
  echo "Job failed!"
  exit 1
fi

echo "Job completed!"

SHOW OUTPUT
echo "First 15 lines of output:"
hdfs dfs -cat $OUTPUT_DIR/part-r-00000 | head -n 15