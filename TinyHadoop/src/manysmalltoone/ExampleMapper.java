package manysmalltoone;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ExampleMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {

	private Text filename;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		InputSplit split = context.getInputSplit();
		FileSplit fileSplit = (FileSplit) split;
		Path path = fileSplit.getPath();
		filename = new Text(path.toString());
		
	}
	
	@Override
	protected void map(NullWritable key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		
		// does nothing except write FILENAME,V
		context.write(filename, value);
	}
}
