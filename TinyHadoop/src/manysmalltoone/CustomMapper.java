package manysmalltoone;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CustomMapper extends Mapper<
	NullWritable,  	// input
	Text, 			// input
	Text,  			// output
	Text			// output
	> {

	private Text filename;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		System.out.println("Setting up Mapper");
		
		InputSplit split = context.getInputSplit();
		FileSplit fileSplit = (FileSplit) split;
		Path path = fileSplit.getPath();
		filename = new Text(path.toString());
		
	}
	
	@Override
	protected void map(NullWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		// key is null
		// file name is hdfs://localhost:9000/user/hduser/TinyHadoop/in/in5.txt
		// value is bytes
		
		// this is the K, V sent to the shuffle -> reducer
		context.write(filename, value);
	}
}
