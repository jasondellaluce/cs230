package edu.uci.cs230.toy_cdn.hadoop.mapreduce;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import edu.uci.cs230.toy_cdn.hadoop.MapReduceOperation;
import edu.uci.cs230.toy_cdn.hadoop.ResultVisitor;

public abstract class TextMapReduceOperation implements MapReduceOperation {

	private String inputDirectory;
	private String outputDirectory;

	public TextMapReduceOperation(String inputDirectory, String outputDirectory) {
		this.inputDirectory = inputDirectory;
		this.outputDirectory = outputDirectory;
	}
	
	public String getInputDirectory() {
		return inputDirectory;
	}

	public String getOutputDirectory() {
		return outputDirectory;
	}

	@Override
	public void acceptResultVisitor(ResultVisitor visitor) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(conf);
		RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(
				new Path(getLastOperationOutputDirectory()), true);
		
	    while(iterator.hasNext()){
	        LocatedFileStatus fileStatus = iterator.next();
	        BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(fileStatus.getPath())));
	        String line = null;
	        while((line = reader.readLine()) != null) {
	        	visitor.visit(line);
	        }
	        reader.close();
	    }
	    
	    fileSystem.close();
	}
	
	protected abstract String getLastOperationOutputDirectory();

}
