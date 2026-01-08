#!/bin/bash
# calculate and export the HADOOP CLASSPATH dynamically
export CLASSPATH=$(hdfs classpath --glob)
echo "CLASSPATH set to: $CLASSPATH"

# execute the main command passed to the container (the original CMD)
exec "$@"
