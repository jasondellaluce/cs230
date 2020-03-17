package edu.uci.cs230.toy_cdn.hadoop.impl;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.uci.cs230.toy_cdn.hadoop.LogConsumer;

public class HdfsDailyLogConsumer implements LogConsumer {

	private String hdfsFileDirectory;
	private FileSystem fileSystem;
	
	public HdfsDailyLogConsumer(String hdfsFileDirectory) {
		this.hdfsFileDirectory = hdfsFileDirectory;
	}
	
	private String getOutputFileName() {
		LocalDateTime currentDateTime = LocalDateTime.now();
		int year = currentDateTime.atOffset(ZoneOffset.UTC).getYear();
		int month = currentDateTime.atOffset(ZoneOffset.UTC).getMonthValue();
		int day = currentDateTime.atOffset(ZoneOffset.UTC).getDayOfMonth();
		return hdfsFileDirectory + "/" + month + "-" + day + "-" + year + ".log";
	}

	@Override
	public void initTask() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		fileSystem = FileSystem.get(conf);
		System.out.println(this.getClass().getSimpleName() + ": FileSystem initialized!");
	}

	@Override
	public void onReceivedLine(String newLine) throws Exception {
		Path outFilePath = new Path(getOutputFileName());
		Path tmpPath = new Path(hdfsFileDirectory + "/tempfile");
		FSDataOutputStream outputStream = null;
		boolean toRename = false;
		
        if (!fileSystem.exists(outFilePath)) {
        	outputStream = fileSystem.create(outFilePath);
        	System.out.println(this.getClass().getSimpleName() + ": Output file created!");
        }
        else {
        	FSDataInputStream inputStream = fileSystem.open(outFilePath);
        	outputStream = fileSystem.create(tmpPath);
        	IOUtils.copy(inputStream, outputStream);
        	toRename = true;
        }
		outputStream.write((newLine + "\n").getBytes());
		outputStream.flush();
		outputStream.hflush();
		outputStream.close();
		
		if(toRename) {
			fileSystem.delete(outFilePath, false);
			fileSystem.rename(tmpPath, outFilePath);			
		}
	}

}
