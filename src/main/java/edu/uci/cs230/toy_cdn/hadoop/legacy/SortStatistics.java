package edu.uci.cs230.toy_cdn.hadoop.legacy;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Random;

public class SortStatistics {

	/* Generates random logs for a CDN node.
	 * Agreed log format:
	 * <timestamp>123</timestamp>
	 * <fileId>XYZ</fileId>
	 * <cacheStatus>Miss</cacheStatus>
	 * */
	public static void main(String[] args) {
		if(args.length != 1) {
			System.out.println("Usage: edu.uci.cs230.toy_cdn.hadoop.LogGenerator <line-count>");
			System.exit(0);
		}
		
		LocalDateTime actualTime = LocalDateTime.now();
		int totalCount = Integer.parseInt(args[0]);	
		String[] files = {
				"script.js", "style.css", "index.html",
				"library.js", "section.css", "about.html"
		};
		String[] folders = {
				"it", "en", "jp",
				"news", "common", "private"
		};
		String[] cacheStatuses = {
				"miss", "hit"
		};
		
		Random random = new Random(System.currentTimeMillis());
		for(int i = 0; i < totalCount; i++) {
			String folder = folders[random.nextInt(folders.length)];
			String file = files[random.nextInt(files.length)];
			String fileId = "/" + folder + "/" + file;
			
			long timestamp = actualTime.toEpochSecond(ZoneOffset.UTC);
			actualTime = actualTime.plusSeconds((random.nextInt(5) + 1));
			
			String cacheStatus = cacheStatuses[random.nextInt(cacheStatuses.length)];
			
			String logLine = "<timestamp>" + timestamp + "</timestamp>"
					+ "<fileId>" + fileId + "</fileId>"
					+ "<cacheStatus>" + cacheStatus + "</cacheStatus>";
			System.out.println(logLine);
		}
		
	}

}
