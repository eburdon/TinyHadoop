/*
 * Custom output format
 * 		Names output sequence files based on first item passed in
 * 
 * TODO: Implement requred methods
 */

package manysmalltoone;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

import org.apache.hadoop.io.Text;


public class CustomOutputFormat extends SequenceFileOutputFormat<Text, Text> {
	
//	TODO: Am I supposed to use this format, instead of record writer?
//	protected Writer getSequenceWriter(TaskAttemptContext context, Class<?> key, Class<?> value) throws IOException {
//		return super.getSequenceWriter(context, key, value);
//	}

	@Override
	public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		
		System.out.println("Get Record Writer");
		
		return new CustomRecordWriter(context);
	}
	
}
