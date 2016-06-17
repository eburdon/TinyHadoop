/*
 * Custom output format
 * 		Names output sequence files based on first item passed in
 * 
 * TODO: Implement requred methods
 */

package manysmalltoone;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;

public class CustomOutputFormat extends OutputFormat<NullWritable, BytesWritable> {

	@Override
	public void checkOutputSpecs(JobContext arg0) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext arg0) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RecordWriter<NullWritable, BytesWritable> getRecordWriter(TaskAttemptContext arg0)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

}
