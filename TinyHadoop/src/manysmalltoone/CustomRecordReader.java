/*
 * Custom Record Reader
 * 		This reader will read in and merge all files into one sequence file
 * 
 * 	NOTE:
 * 		* Can I read and sort files here??	
 */

package manysmalltoone;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

//public class CustomRecordReader extends RecordReader<NullWritable, BytesWritable> {
public class CustomRecordReader extends RecordReader<NullWritable, Text> {
	private FileSplit fileSplit;
	private Configuration conf;
	private Text value = new Text();
	private boolean processed = false;
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		
		System.out.println("Initializing custom record reader");
		System.out.println(split.toString()); 		// this is the name of my file
		
		this.fileSplit = (FileSplit) split;
		this.conf = context.getConfiguration();
	}
	
	@Override
	public void close() throws IOException {
		// do nothing
	}

	@Override
	public NullWritable getCurrentKey() throws IOException, InterruptedException {
		return NullWritable.get();
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return processed ? 1.0f : 0.0f;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!processed) {
			byte[] contents = new byte[(int) fileSplit.getLength()];
			Path file = fileSplit.getPath();
			FileSystem fs = file.getFileSystem(conf);
			FSDataInputStream in = null;
			
			try {
				in = fs.open(file);
				IOUtils.readFully(in, contents, 0, contents.length);
				value.set(contents, 0, contents.length);
			} finally {
				IOUtils.closeStream(in);
			}
			
			processed = true;
			return true;
		}
		
		return false;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

}
