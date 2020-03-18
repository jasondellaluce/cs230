package edu.uci.cs230.toy_cdn.hadoop;

import java.nio.file.Paths;

import edu.uci.cs230.toy_cdn.hadoop.log.HdfsDailyLogConsumer;
import edu.uci.cs230.toy_cdn.hadoop.log.RandomMockLogReceiver;
import edu.uci.cs230.toy_cdn.hadoop.mapreduce.CacheHitRateOperation;
import edu.uci.cs230.toy_cdn.hadoop.mapreduce.StandardOutResultVisitor;

public class DaemonApplication {

	public static void main(String[] args) {
		String inputDirectory = "toycdn/logs";
		String outputDirectory = "toycdn/mapreduce";
		
		System.out.println(" ------ ToyCDN - Hadoop Daemon ------\n");
			
		System.out.println("Initializing LogHandler routine...");
		LogConsumer logConsumer = new HdfsDailyLogConsumer(inputDirectory);
		LogReceiver logReceiver = new RandomMockLogReceiver();
		LogHandlerThread logHandler = new LogHandlerThread(logConsumer, logReceiver);
		logHandler.start();
		System.out.println("LogHandler has been started!");
		
		System.out.println("Initializing OperationHandler routine...");
		int period = 2;
		MapReduceOperation operation = new CacheHitRateOperation(inputDirectory, outputDirectory + "/cache-hit");
		ResultVisitor resultVisitor = new StandardOutResultVisitor();
		OperationHandlerThread operationHandler = new OperationHandlerThread(period, operation, resultVisitor);
		operationHandler.start();
		System.out.println("OperationHandler Handler has been started!");
		
	}
	
}
