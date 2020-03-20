package edu.uci.cs230.toy_cdn.hadoop.log;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import edu.uci.cs230.toy_cdn.hadoop.LogConsumer;

public class AppendHdfsDailyLogConsumer implements LogConsumer {

	private final static Logger Log = LogManager.getLogger(AppendHdfsDailyLogConsumer.class);
	private String hdfsFileDirectory;
	private Configuration conf;
	
	public AppendHdfsDailyLogConsumer(String hdfsFileDirectory) {
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
		Path outFilePath = new Path(getOutputFileName());
		
		try (FileSystem fileSystem = FileSystem.newInstance(conf)) {
			FSDataOutputStream outputStream = null;
			if(fileSystem.exists(outFilePath)) {
				outputStream = fileSystem.append(outFilePath);
			}
			else {
				outputStream = fileSystem.create(outFilePath);
			}
			
			outputStream.write((newLine + "\n").getBytes());
			outputStream.close();
			Log.info("Wrote new record in log file!");
		}
	}

}
