package edu.uci.cs230.toy_cdn.hadoop.log;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Random;

import edu.uci.cs230.toy_cdn.hadoop.LogReceiver;

public class MockRandomLogReceiver implements LogReceiver {

	private final String[] files = {
			"script.js", "style.css", "index.html",
			"library.js", "section.css", "about.html"
	};
	
	private final String[] folders = {
			"it", "en", "jp",
			"news", "common", "private"
	};
	
	private final String[] cacheStatuses = {
			"miss", "hit"
	};
	
	private LocalDateTime actualTime;
	private Random random;
	
	public MockRandomLogReceiver() {
		actualTime = LocalDateTime.now();
		random = new Random(System.currentTimeMillis());
	}
	
	@Override
	public void initTask() throws Exception {
		actualTime = LocalDateTime.now();
		random = new Random(System.currentTimeMillis());
	}

	@Override
	public String receiveLine() throws Exception {
		String folder = folders[random.nextInt(folders.length)];
		String file = files[random.nextInt(files.length)];
		String fileId = "/" + folder + "/" + file;
		
		long timestamp = actualTime.toEpochSecond(ZoneOffset.UTC);
		actualTime = actualTime.plusSeconds((random.nextInt(5) + 1));
		
		String cacheStatus = cacheStatuses[random.nextInt(cacheStatuses.length)];
		
		String logLine = "<timestamp>" + timestamp + "</timestamp>"
				+ "<fileId>" + fileId + "</fileId>"
				+ "<cacheStatus>" + cacheStatus + "</cacheStatus>";
		
		Thread.sleep(1000);
		return logLine;
	}

}
