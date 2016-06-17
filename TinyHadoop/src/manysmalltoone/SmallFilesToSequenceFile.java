/*
 * Runnable file
 * Converts several small files into one sequence file 
 */

package manysmalltoone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class SmallFilesToSequenceFile extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(SmallFilesToSequenceFile.class);
		
		job.setJobName("SmallFilesToOneSequenceFile");
		
		job.setInputFormatClass(CustomInputFormat.class);
//		job.setOutputFormatClass(CustomOutputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
	
		job.setNumReduceTasks(1);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		
		job.setMapperClass(ExampleMapper.class);
		
		return job.waitForCompletion(true)? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SmallFilesToSequenceFile(), args);
		System.exit(exitCode);
	}

}
