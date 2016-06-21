# TinyHadoop
Scratchpad for working out kinks with Hadoop

## What does this do? / Milestones Hit
 
1) Takes in a number of raw text files of different sizes; Mapper does nothing. Before the default Reducer is applied, they are merged into one sequence file.

2) Output (reduce) file is named according to first item in the input

## Todo


a) Guarantee files are read in order (numerically)

b) Split merged sequence file into several of equal size

c) Read merged file; sort data before split (Needs new sample input: e.g., all numbers)

d) Try creating at least two reducer output files (e.g., two files, each named with first item)

## Usage

Prerequites:


* Java openJDK 1.8.0_91

* Hadoop 2.7.2 locally installed and running

* Eclipse

* MapReduce project configured as Maven in Eclipse; All external JARS are on build path

* Some sort of input set (See /SampleInputFiles)

1. Export project as a .jar (Make sure that the class application entry point is set!)

2. Execute on hadoop

> hadoop jar TinyHadoop.jar HDFS_input_dir HDFS_output_dir

## Notes

You can dump sequence file results to terminal with:

>  hadoop fs -cat /user/hduser/TinyHadoop/out/seq/part-r-00000
