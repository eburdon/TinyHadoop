/*
 * @eburdon
 * 
 * TODO: Custom RecordWriter to create files based on first item
 */

package manysmalltoone;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.BytesWritable;
//import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
//import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


// TODO: Make sure this fits for a SEQUENCE FILE 
// http://johnnyprogrammer.blogspot.ca/2012/01/custom-file-output-in-hadoop.html
//public class CustomRecordWriter extends RecordWriter<Text, BytesWritable> {
public class CustomRecordWriter extends RecordWriter<Text, Text> {

	private FSDataOutputStream out;
	// private TaskAttemptContext context;
	
	private char SPACE;
	private char NEW_LINE;
	private String name;
	
	// Constructor; called once for each split?
	public CustomRecordWriter(FSDataOutputStream stream, TaskAttemptContext arg1) {
		out = stream;
		// context = arg1;
		
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
			// iterate until we build the file name; only on first file
			
			if (name == "") {
				for (int i = 0; i < value.getLength(); i++) {
				
					int item = value.charAt(i);
					
					// essentially a splitter; parse the input until the first space or new line
					if (item != SPACE && item != NEW_LINE) {
						name = name + String.valueOf((char)item);
					} else {
						break;
					}
				}
				
				// CREATE THE OUTPUT FILE BASED ON THIS NAME
				System.out.println(name);
			}
		}
		
		// write key
		System.out.println(name);
		
		out.writeBytes(text.toString() + ": ");
		
//		// Loop through and append all values
//		for (int i = 0; i < bytes.getLength(); i++) {
//			out.writeBytes(bytes.toString());
//		}
		
		out.writeBytes("\n");
	}
	
}
