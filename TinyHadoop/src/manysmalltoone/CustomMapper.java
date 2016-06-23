package manysmalltoone;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
//import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CustomMapper extends Mapper<
	LongWritable,  	// input
	Text, 			// input
	Text,  			// output
	Text			// output
	> {

//	private Text filename;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		System.out.println("Setting up Mapper - not do anything?");
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		// key is null
		// file name is hdfs://localhost:9000/user/hduser/TinyHadoop/in/in5.txt
		// value is bytes
		
		Text testKey = new Text();
		testKey.set("Hello");
		
		System.out.println("Mapper: writing!");
		System.out.println(value.toString());
		System.out.println(testKey.toString());
		
		// this is the K, V sent to the shuffle -> reducer
		context.write(testKey, value);
	}
}
