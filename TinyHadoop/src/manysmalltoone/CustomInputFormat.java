/*
 * @eburdon
 * 
 * Custom import format 
 * 		1. Merges files created from previous mappers
 * 		2. Splits equally based on size
 */

package manysmalltoone;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class CustomInputFormat extends FileInputFormat<NullWritable, Text> {

	@Override
	// TODO: I want to merge all the input files, then resplit?
	protected boolean isSplitable(JobContext context, Path filename) {
		// called once for each split (e.g., each input file)
		return false;
	}


	@Override
	// public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
	public RecordReader<NullWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException {
		
		System.out.println("Creating a recordReader for: ");
		System.out.println(split.toString());
		
		CustomRecordReader reader = new CustomRecordReader();
		
		try {
			reader.initialize(split, context);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		return reader;
	}
}
