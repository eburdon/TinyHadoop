package manysmalltoone;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
//import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;

/* This is the one custom record reader for the entire input */
public class CustomCombineRecordReader extends RecordReader<LongWritable, Text> {

	// TODO: Will have to make this a sequence file record reader for BigHadoop
//	private SequenceFileRecordReader<NullWritable, Text> recordReader;
	private LineRecordReader recordReader;
	private Configuration conf;
	private FileSplit fileSplit;
	private boolean processed = false;
	private Text value = new Text();
	
	CustomCombineRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException, InterruptedException {
		
		fileSplit = new FileSplit(split.getPath(index), split.getOffset(index), split.getLength(index), split.getLocations());
		
		conf = context.getConfiguration();
		
//		recordReader = new SequenceFileRecordReader<NullWritable, Text>();
		recordReader = new LineRecordReader();
		
		recordReader.initialize(fileSplit, context);
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		System.out.println("Current key: ");
		System.out.println(recordReader.getCurrentKey());
		return recordReader.getCurrentKey();
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		System.out.println("Current value: ");
		System.out.println(recordReader.getCurrentValue());
		return recordReader.getCurrentValue();
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return recordReader.getProgress();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		System.out.println("Next key value:");
		System.out.println(recordReader.nextKeyValue());
//		return recordReader.nextKeyValue();
		if (!processed) {
			
			System.out.println("Hhola");
			
			byte[] contents = new byte[(int) fileSplit.getLength()];
			Path file = fileSplit.getPath();
			FileSystem fs = file.getFileSystem(conf);
			FSDataInputStream in = null;
			
			try {
				System.out.println("Opening file:");
				System.out.println(file.toString());
				
				in = fs.open(file);
				IOUtils.readFully(in, contents, 0, contents.length);
				
				System.out.println("Setting value!");
				
				value.set(contents, 0, contents.length);
				
			} finally {
				
				System.out.println("Finished processing");
				
				IOUtils.closeStream(in);
			}
			
			processed = true;
			return true;
		}
		
		return false;
	}
	
	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
		//
	}
	
	@Override
	public void close() throws IOException {
		recordReader.close();
	}
	
}
