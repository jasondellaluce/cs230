package edu.uci.cs230.toy_cdn.hadoop.mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import edu.uci.cs230.toy_cdn.hadoop.MapReduceOperation;
import edu.uci.cs230.toy_cdn.hadoop.MapReduceVisitor;

public abstract class AbstractMapReduceOperation implements MapReduceOperation {
	
	private final static Logger Log = LogManager.getLogger(AbstractMapReduceOperation.class);
	private String inputDirectory;
	private String outputDirectory;
	private String lastOperationOutputDirectory;

	public AbstractMapReduceOperation(String inputDirectory, String outputDirectory) {
		this.inputDirectory = inputDirectory;
		this.outputDirectory = outputDirectory;
	}
	
	public String getInputDirectory() {
		return inputDirectory;
	}

	public String getOutputDirectory() {
		return outputDirectory;
	}
	
	protected String getLastOperationOutputDirectory() {
		return lastOperationOutputDirectory;
	}
	
	@Override
	public boolean run() throws Exception {
		Log.info("Preparing to initialize the job...");
		
		LocalDateTime currentDateTime = LocalDateTime.now();
		int year = currentDateTime.atOffset(ZoneOffset.UTC).getYear();
		int month = currentDateTime.atOffset(ZoneOffset.UTC).getMonthValue();
		int day = currentDateTime.atOffset(ZoneOffset.UTC).getDayOfMonth();
		int hour = currentDateTime.atOffset(ZoneOffset.UTC).getHour();
		int minute = currentDateTime.atOffset(ZoneOffset.UTC).getMinute();
		int second = currentDateTime.atOffset(ZoneOffset.UTC).getSecond();
		lastOperationOutputDirectory = getOutputDirectory() + "/" + getJobDirectoryName() + "-" 
				+ month + "-" + day + "-" + year + "-" + hour + "-" + minute + "-" + second;
		
		Configuration conf = new Configuration();	
		Job job = configureMapReduceJob(Job.getInstance(conf));
		FileInputFormat.addInputPath(job, new Path(getInputDirectory()));
		FileOutputFormat.setOutputPath(job, new Path(getLastOperationOutputDirectory()));	
		
		try {
			Log.info("Starting job...");
			boolean result = job.waitForCompletion(true);
			Log.info("Job completed!");
			return result;
		}
		catch (ClassNotFoundException | InterruptedException e) {
			throw new IOException(e);
		}
	}
	
	@Override
	public void acceptResultVisitor(MapReduceVisitor visitor) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(conf);
		RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(
				new Path(getLastOperationOutputDirectory()), true);
		
	    while(iterator.hasNext()){
	        LocatedFileStatus fileStatus = iterator.next();
	        BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(fileStatus.getPath())));
	        String line = null;
	        while((line = reader.readLine()) != null) {
	        	visitor.visitResult(line);
	        }
	        reader.close();
	    }
	    
	    fileSystem.close();
	}
	
	protected abstract Job configureMapReduceJob(Job job);
	protected abstract String getJobDirectoryName();

}
