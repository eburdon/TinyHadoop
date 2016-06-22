package manysmalltoone;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;


// http://www.idryman.org/blog/2013/09/22/process-small-files-on-hadoop-using-combinefileinputformat-1/
// https://gist.github.com/airawat/6647007
// http://stackoverflow.com/questions/10380200/how-to-use-combinefileinputformat-in-hadoop
// http://hadoopsupport.blogspot.ca/2015/02/combinefileinputformat-mapreduce-code.html
// http://stackoverflow.com/questions/14270317/implementation-for-combinefileinputformat-hadoop-0-20-205
public class CustomCombineInputFormat extends CombineFileInputFormat<NullWritable, Text> {
	
	// Constructor; I must include the max split size
	// TODO: change max size to force-create more files?
	public CustomCombineInputFormat() {
		super();
		setMaxSplitSize(67108864);
	}
	
	// TODO: Do I need to add handling to split again, or does my constructor handle this?
	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		return false;
	}

	@Override
	public RecordReader<NullWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException {
		// instead of calling reader.initialize, use a detailed constructor
		return new CombineFileRecordReader<NullWritable, Text>((CombineFileSplit) split, context, CustomCombineRecordReader.class);
	}

}
