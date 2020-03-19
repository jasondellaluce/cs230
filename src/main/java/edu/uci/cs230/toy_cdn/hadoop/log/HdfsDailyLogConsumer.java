package edu.uci.cs230.toy_cdn.hadoop.log;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import edu.uci.cs230.toy_cdn.hadoop.LogConsumer;

public class HdfsDailyLogConsumer implements LogConsumer {

	private final static Logger Log = LogManager.getLogger(HdfsDailyLogConsumer.class);
	private String hdfsFileDirectory;
	private Configuration conf;
	
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
		conf = new Configuration();
		Log.info("FileSystem initialized!");
	}

	@Override
	public void onReceivedLine(String newLine) throws Exception {
		FileSystem fileSystem = FileSystem.newInstance(conf);
		Path outFilePath = new Path(getOutputFileName());
		Path tmpPath = new Path(hdfsFileDirectory + "/tempfile");
		FSDataOutputStream outputStream = null;
		boolean toRename = false;
		
        if (!fileSystem.exists(outFilePath)) {
        	outputStream = fileSystem.create(outFilePath);
        	Log.info("Output file created!");
        }
        else {
        	FSDataInputStream inputStream = fileSystem.open(outFilePath);
        	outputStream = fileSystem.create(tmpPath);
        	IOUtils.copy(inputStream, outputStream);
        	inputStream.close();
        	toRename = true;
        }
		outputStream.write((newLine + "\n").getBytes());
		outputStream.close();
		
		if(toRename) {
			fileSystem.delete(outFilePath, false);
			fileSystem.rename(tmpPath, outFilePath);			
		}
		Log.debug("Wrote log line!");
	}

}
