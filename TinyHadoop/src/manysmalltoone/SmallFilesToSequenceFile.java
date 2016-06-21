/*
 * Runnable file
 * Converts several small files into one sequence file 
 */

package manysmalltoone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.BytesWritable;
//import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SmallFilesToSequenceFile extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(SmallFilesToSequenceFile.class);
		
		job.setJobName("SmallFilesToOneSequenceFile");
		
		job.setNumReduceTasks(1);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// This is the magic between initialization --> mapper
		// TODO: Change type
		job.setInputFormatClass(CustomInputFormat.class);
		job.setOutputFormatClass(CustomOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		
		// TODO: Change type (all files!!)
		// For testing, let's do a pure text -- text implementation
		// We need to be able to read the text value, and name our file based
		// off that.
		// job.setOutputValueClass(BytesWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(CustomMapper.class);
		
		// run job
		return job.waitForCompletion(true)? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SmallFilesToSequenceFile(), args);
		System.exit(exitCode);
	}

}
