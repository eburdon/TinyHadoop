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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


// TODO: Make sure this fits for a SEQUENCE FILE
public class CustomRecordWriter extends RecordWriter<Text, Text> {

	private FSDataOutputStream out;
	 private TaskAttemptContext context;
	
	private char SPACE;
	private char NEW_LINE;
	private String name;
	
	public CustomRecordWriter(TaskAttemptContext arg1) {
		out = null;
		context = arg1;
		
		// parsing constants
		SPACE = 32;
		NEW_LINE = 10;
		
		// initialize file name
		name = "";
	}
	
	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		if (out != null) {
			out.close();
		}
	}

	/*
	 * Parses text value until the first whitespace (space or new line).
	 * @@params: Value from mapper
	 * @@return: String of first item
	 * */
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
		
		if (out == null) {
			
			System.out.println("MERMAID -- NULL OUT STREAM\n\n");			
			
			if (name == "") {
				// set new file name
				name = parseFileName(value);
				
				// create output file
				out = createOutputFile();
			}
		}
		
		// write values
		out.writeBytes(value.toString());
		
		// EOF (split)
		out.writeBytes("\n");
	}
	
}
