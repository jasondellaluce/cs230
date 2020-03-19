package edu.uci.cs230.toy_cdn.hadoop;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class OperationHandlerThread extends Thread {

	private final static Logger Log = LogManager.getLogger(OperationHandlerThread.class);
	private int period;
	private MapReduceOperation mapReduceOperation;
	private MapReduceVisitor resultVisitor;

	public OperationHandlerThread(int period, MapReduceOperation mapReduceOperation,
			MapReduceVisitor resultVisitor) {
		this.period = period;
		this.mapReduceOperation = mapReduceOperation;
		this.resultVisitor = resultVisitor;
	}

	@Override
	public void run() {
		while(true) {
			
			/* Run the MapReduce task */
			try {
				if(!mapReduceOperation.run()) {
					Log.error("ERROR in MapReduce: " + mapReduceOperation.getClass().getSimpleName());
					return;
				}
			}
			catch (Exception e) {
				e.printStackTrace();
				return;
			};
			
			/* Visit MapReduce results */
			Log.info("Operation completed, starting visitation...");
			try {
				mapReduceOperation.acceptResultVisitor(resultVisitor);
			}
			catch (Exception e) {
				e.printStackTrace();
				return;
			}
			Log.info("Visitation completed!");
			
			/* Slee a little till next computation */
			try {
				Thread.sleep(period * 1000);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
				return;
			}
		}
	}

}
