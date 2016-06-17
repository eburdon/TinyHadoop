/*
 * @eburdon
 * 
 * Custom import format 
 * 		1. Merges files created from previous mappers
 * 		2. Splits equally based on size
 * 
 * 	Todo:
 * 		* Custom record reader -- to sort merged data??
 * 		* Custom output writer -- to sort each file's data??
 */

package manysmalltoone;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class CustomInputFormat extends FileInputFormat<NullWritable, BytesWritable> {

	// In order to process all records as a "single record", define as false
	// TODO: I do want to split again...
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}

	// Define my custom record reader
	@Override
	public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException {
		
		CustomRecordReader reader = new CustomRecordReader();
		
		try {
			// TODO: This was auto-generated. Need?
			reader.initialize(split, context);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return reader;
	}
}
