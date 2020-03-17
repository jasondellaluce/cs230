package edu.uci.cs230.toy_cdn.hadoop;

import edu.uci.cs230.toy_cdn.hadoop.impl.HdfsDailyLogConsumer;
import edu.uci.cs230.toy_cdn.hadoop.impl.RandomMockLogReceiver;

public class DaemonApplication {

	public static void main(String[] args) {
		System.out.println(" ------ ToyCDN - Hadoop Daemon ------\n");
		
		System.out.println("Initializing LogHandler routine...");
		LogConsumer logConsumer = new HdfsDailyLogConsumer("toycdn/logs");
		LogReceiver logReceiver = new RandomMockLogReceiver();
		LogHandlerThread logHandler = new LogHandlerThread(logConsumer, logReceiver);
		logHandler.start();
		System.out.println("LogHandler has been started!");
	}
	
}
