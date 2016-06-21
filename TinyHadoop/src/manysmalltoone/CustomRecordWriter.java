/*
 * @eburdon
 * 
 * TODO: Custom RecordWriter to create files based on first item
 */

package manysmalltoone;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.BytesWritable;
//import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


// TODO: Make sure this fits for a SEQUENCE FILE 
// http://johnnyprogrammer.blogspot.ca/2012/01/custom-file-output-in-hadoop.html
//public class CustomRecordWriter extends RecordWriter<Text, BytesWritable> {
public class CustomRecordWriter extends RecordWriter<Text, Text> {

	private FSDataOutputStream out;
	 private TaskAttemptContext context;
	
	private char SPACE;
	private char NEW_LINE;
	private String name;
	
	// Constructor; called once for each split?
	public CustomRecordWriter(FSDataOutputStream stream, TaskAttemptContext arg1) {
		out = stream;
		context = arg1;
		
		// parsing constants
		SPACE = 32;
		NEW_LINE = 10;
		
		// initialize file name
		name = "";
	}
	
	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		out.close();
	}

	// TODO: needs exception handling?
	// iterate until we build the file name; only on first file
	private String parseFileName(Text value) {
		String tmp = "";
		
		for (int i = 0; i < value.getLength(); i++) {
			int item = value.charAt(i);
			
			if (item != SPACE && item != NEW_LINE) {
				tmp = tmp + String.valueOf((char)item);
			} else {
				break;
			}
		}
		
		System.out.println(tmp);
		
		return tmp;
	}
	
	// TODO: add exception handling
	public FSDataOutputStream createOutputFile() throws IOException {
		// get existing target directory path
		Path path = SequenceFileOutputFormat.getOutputPath(context);
		
		// create file based on our discovered name
		Path fullpath = new Path(path, name);
		
		// create the file in our file system
		FileSystem fs = path.getFileSystem(context.getConfiguration());
			
		FSDataOutputStream fileOut = fs.create(fullpath, context);
		
		return fileOut;
	}
	
	/*
	* @@param: Text is the file name (e.g., where value found)
	* @@param: Value is the text from the input;
	*/
	@Override
	public void write(Text text, Text value) throws IOException, InterruptedException {
		
		if (out.equals(null)) {
			// delay creating out file until here
			System.out.println("MERMAID -- NULL OUT STREAM\n\n");			
			// get target directory path
			// Path path = SequenceFileOutputFormat.getOutputPath(context);			
		} else {
			if (name == "") {
				name = parseFileName(value);
				
				// create file based on this name
				System.out.println(name);
				
				System.out.println("Creating new outfile on this name");
				System.out.println(out.toString());
				out = createOutputFile();
				System.out.println(out.toString());
			}
		}
		
		// write key
		out.writeBytes(text.toString() + ": ");
		
		// write values
		out.writeBytes(value.toString());
		
		// EOF
		out.writeBytes("\n");
	}
	
}
