# Word Count Chained MapReduce

## Input

The input file is an HDFS file with "the quick sly fox jumped over the other fox" written inside. Feel free to change the content to any text splitted by one or more whitespaces or tabs.

## How to run
1. Load this project in IntelliJ, then Maven -> Lifecycle -> Install. This will generate a jar file in the target folder.
2. Upload the jar file to the cluster and run the following command in the cluster:
   `yarn jar <jar file path> WordCount <input file path> <directory path for the first-round output> <directory path for the second-round(final) output>`
