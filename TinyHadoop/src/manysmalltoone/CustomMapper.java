package manysmalltoone;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CustomMapper extends Mapper<
	LongWritable,  	// input
	Text, 			// input
	Text,  			// output
	Text			// output
	> {

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		System.out.println("Setting up Mapper - not do anything?");
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		Text testKey = new Text();
		testKey.set("Hello");
		
		// this is the K, V sent to the shuffle -> reducer
		context.write(testKey, value);
	}
}
