package manysmalltoone;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient.Conf;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

public class CustomCombineRecordReader extends RecordReader<NullWritable, Text> {
	
	private Configuration conf;
	private Text value = new Text();
	
	CustomCombineRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException {
		FileSplit fileSplit = new FileSplit(split.getPath(index), split.getOffset(index), split.getLength(index), split.getLocations());
		
		conf = context.getConfiguration();
	}
	
	@Override
	public void close() throws IOException {
		// do nothing?
	}

	@Override
	public NullWritable getCurrentKey() throws IOException, InterruptedException {
		return NullWritable.get();
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0;
	}
	
	/*
	 * Where the magic happens!
	 * What do I want to do here?
	 */
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		System.out.println("what");
		return true;
		
	}
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		// Do nothing / not called [detailed constructor instead]
	}
}
