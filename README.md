# TinyHadoop
Scratchpad for working out kinks with Hadoop

## What does this do?
 
Takes in a number of raw text files of different sizes; mapper does nothing. Before the default reduce is applied, they are merged into one sequence file.

## Todo


a) Guarantee files are read in order (numerically)

b) Split merged sequence file into several of equal sized

c) Read merged file; sort data before split



## Usage

Prerequites:


* Hadoop 2.7.2 locally installed and running.

* Java openJDK 1.8.0_91

* Eclipse

* MapReduce project configured as Maven in Eclipse

* Some sort of input

1. Export project as a .jar

* Make sure that the class application entry point is set!

2. Execute on hadoop

> hadoop jar TinyHadoop.jar HDFS_input_dir HDFS_output_dir

## Notes

You can dump sequence file results to terminal with:

>  hadoop fs -cat /user/hduser/TinyHadoop/out/seq/part-r-00000
