package manysmalltoone;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;


public class CustomCombineRecordReader extends RecordReader<LongWritable, Text> {

	// TODO: Will have to make this a sequence file record reader for BigHadoop
	private LineRecordReader recordReader;
	private Configuration conf;
	private FileSplit fileSplit;
	private boolean processed = false;
	private Text value = new Text();
	
	CustomCombineRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException, InterruptedException {
		
		fileSplit = new FileSplit(split.getPath(index), split.getOffset(index), split.getLength(index), split.getLocations());
		
		conf = context.getConfiguration();
		
		recordReader = new LineRecordReader();
		recordReader.initialize(fileSplit, context);
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return recordReader.getCurrentKey();
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return recordReader.getProgress();
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
				
				// TODO: Sort content & remove EOF here before setting
				
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
