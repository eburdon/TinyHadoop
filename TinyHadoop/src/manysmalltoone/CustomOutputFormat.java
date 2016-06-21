/*
 * Custom output format
 * 		Names output sequence files based on first item passed in
 * 
 * TODO: Implement requred methods
 */

package manysmalltoone;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.BytesWritable;
//import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;


//public class CustomOutputFormat extends SequenceFileOutputFormat<Text, BytesWritable> {
public class CustomOutputFormat extends SequenceFileOutputFormat<Text, Text> {
	
//	@Override
//	protected Writer getSequenceWriter(TaskAttemptContext context, Class<?> key, Class<?> value) throws IOException {
//		// TODO Can I use this instead of RecordWriter?
//		return super.getSequenceWriter(context, key, value);
//	}

	@Override
//	public RecordWriter<Text, BytesWritable> getRecordWriter(TaskAttemptContext context)
	public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		
		// todo: can I prune this function? If I'm initializing elsewhere, do I need this?
		
		// get target directory path
		Path path = SequenceFileOutputFormat.getOutputPath(context);
		
		// create our file [name]
		// TODO: Read the first input item and name the file based on that.
		Path fullpath = new Path(path, "DEFAULT");
		
		// create the file in our file system
		FileSystem fs = path.getFileSystem(context.getConfiguration());
		
		FSDataOutputStream fileOut = fs.create(fullpath, context);
		
		// todo: later
		// stream will be initialized on first item read
//		FSDataOutputStream fileOut = null;
		
		return new CustomRecordWriter(fileOut, context);

	}
	
}
