package edu.uci.cs230.toy_cdn.hadoop;

import edu.uci.cs230.toy_cdn.hadoop.impl.RandomMockLogReceiver;
import edu.uci.cs230.toy_cdn.hadoop.impl.StandardOutLogConsumer;

public class DaemonApplication {

	public static void main(String[] args) {
		System.out.println(" ------ ToyCDN - Hadoop Daemon ------\n");
		
		System.out.println("Initializing LogHandler routine...");
		LogConsumer logConsumer = new StandardOutLogConsumer();
		LogReceiver logReceiver = new RandomMockLogReceiver();
		LogHandlerThread logHandler = new LogHandlerThread(logConsumer, logReceiver);
		logHandler.start();
		System.out.println("LogHandler has been started!");
	}
	
}
