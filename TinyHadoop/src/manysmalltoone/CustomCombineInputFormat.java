package manysmalltoone;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;



public class CustomCombineInputFormat extends CombineFileInputFormat<LongWritable, Text> {
	
	// Constructor; I must include the max split size
	// TODO: change max size to force-create more files?
	public CustomCombineInputFormat() {
		super();
//		setMaxSplitSize(67108864);
	}
	
	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		return false;
	}

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException {
		
		return new CombineFileRecordReader<LongWritable, Text>((CombineFileSplit) split, context, CustomCombineRecordReader.class);
	}

}
