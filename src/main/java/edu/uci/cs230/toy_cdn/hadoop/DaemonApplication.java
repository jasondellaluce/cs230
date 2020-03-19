package edu.uci.cs230.toy_cdn.hadoop;

import edu.uci.cs230.toy_cdn.hadoop.log.HdfsDailyLogConsumer;
import edu.uci.cs230.toy_cdn.hadoop.log.MockRandomLogReceiver;
import edu.uci.cs230.toy_cdn.hadoop.mapreduce.CacheHitRateOperation;
import edu.uci.cs230.toy_cdn.hadoop.mapreduce.StdoutMapReduceVisitor;
import edu.uci.cs230.toy_cdn.hadoop.zeromq.ZmqHitRateMapReduceVisitor;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class DaemonApplication {

	private final static Logger Log = LogManager.getLogger(DaemonApplication.class);
	
	public static void main(String[] args) {
		InputStream configStream = DaemonApplication.class.getClassLoader()
				.getResourceAsStream("src/main/resources/daemon.properties");
		assert configStream != null;
		Properties configProp = new Properties();
		try {
			configProp.load(configStream);
		} catch (IOException e) {
			Log.error("Failed to read properties file");
			Log.error(e.getMessage());
			return;
		}

		String inputDirectory = configProp.getProperty("daemon.hadoop.input_directory");
		String outputDirectory = configProp.getProperty("daemon.hadoop.output_directory");
		String cdnAddress = configProp.getProperty("daemon.cdn.address");
		String cdnPort = configProp.getProperty("daemon.cdn.port");
		String visitorType = configProp.getProperty("daemon.result_visitor");
		
		Log.info(" ------ ToyCDN - Hadoop Daemon ------\n");
			
		Log.info("Initializing LogHandler routine...");
		LogConsumer logConsumer = new HdfsDailyLogConsumer(inputDirectory);
		LogReceiver logReceiver = new MockRandomLogReceiver();
		LogHandlerThread logHandler = new LogHandlerThread(logConsumer, logReceiver);
		logHandler.start();
		Log.info("LogHandler has been started!");
		
		Log.info("Initializing OperationHandler routine...");
		int period = Integer.parseInt(configProp.getProperty("daemon.hadoop.period"));
		MapReduceOperation operation = new CacheHitRateOperation(inputDirectory, outputDirectory);
		MapReduceVisitor resultVisitor = null;
		switch (visitorType) {
			case "stdout":
				resultVisitor = new StdoutMapReduceVisitor();
				break;
			case "zmq": {
				String cdnEndPoint = String.format("tcp://%s:%s", cdnAddress, cdnPort);
				resultVisitor = new ZmqHitRateMapReduceVisitor(cdnEndPoint, cdnEndPoint);
				break;
			}
			default:
				Log.error(String.format("Unrecognized visitor type %s", visitorType));
		}
		OperationHandlerThread operationHandler = new OperationHandlerThread(period, operation, resultVisitor);
		operationHandler.start();
		Log.info("OperationHandler Handler has been started!");
		
	}
	
}
