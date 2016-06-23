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
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}


	@Override
	public RecordReader<NullWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException {
		
		// breaks data into K/V pairs for the mapper
		CustomRecordReader reader = new CustomRecordReader();
		
		try {
			reader.initialize(split, context);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		return reader;
	}
}
