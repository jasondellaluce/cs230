package edu.uci.cs230.toy_cdn.hadoop;

import edu.uci.cs230.toy_cdn.hadoop.log.HdfsDailyLogConsumer;
import edu.uci.cs230.toy_cdn.hadoop.log.RandomMockLogReceiver;
import edu.uci.cs230.toy_cdn.hadoop.mapreduce.CacheHitRateOperation;
import edu.uci.cs230.toy_cdn.hadoop.mapreduce.StandardOutResultVisitor;
import edu.uci.cs230.toy_cdn.hadoop.zeromq.ZMQHitRateResultVisitor;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class DaemonApplication {

	public static void main(String[] args) {
		InputStream configStream = DaemonApplication.class.getClassLoader().getResourceAsStream("daemon.properties");
		assert configStream != null;
		Properties configProp = new Properties();
		try {
			configProp.load(configStream);
		} catch (IOException e) {
			System.out.println("Failed to read properties file");
			System.out.println(e.getMessage());
			return;
		}

		String inputDirectory = configProp.getProperty("daemon.hadoop.input_directory");
		String outputDirectory = configProp.getProperty("daemon.hadoop.output_directory");
		String cdnAddress = configProp.getProperty("daemon.cdn.address");
		String cdnPort = configProp.getProperty("daemon.cdn.port");
		String visitorType = configProp.getProperty("daemon.result_visitor");
		
		System.out.println(" ------ ToyCDN - Hadoop Daemon ------\n");
			
		System.out.println("Initializing LogHandler routine...");
		LogConsumer logConsumer = new HdfsDailyLogConsumer(inputDirectory);
		LogReceiver logReceiver = new RandomMockLogReceiver();
		LogHandlerThread logHandler = new LogHandlerThread(logConsumer, logReceiver);
		logHandler.start();
		System.out.println("LogHandler has been started!");
		
		System.out.println("Initializing OperationHandler routine...");
		int period = 3; // 3 seconds
		MapReduceOperation operation = new CacheHitRateOperation(inputDirectory, outputDirectory + "/cache-hit");
		ResultVisitor resultVisitor = null;
		switch (visitorType) {
			case "stdout":
				resultVisitor = new StandardOutResultVisitor();
				break;
			case "zmq": {
				String cdnEndPoint = String.format("tcp://%s:%s", cdnAddress, cdnPort);
				resultVisitor = new ZMQHitRateResultVisitor(cdnEndPoint, cdnEndPoint);
				break;
			}
			default:
				System.out.println(String.format("Unrecognized visitor type %s", visitorType));
		}
		OperationHandlerThread operationHandler = new OperationHandlerThread(period, operation, resultVisitor);
		operationHandler.start();
		System.out.println("OperationHandler Handler has been started!");
		
	}
	
}
