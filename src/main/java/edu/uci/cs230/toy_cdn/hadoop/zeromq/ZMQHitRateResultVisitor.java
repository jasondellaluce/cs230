package edu.uci.cs230.toy_cdn.hadoop.zeromq;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.StringTokenizer;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import edu.uci.cs230.toy_cdn.hadoop.ResultVisitor;

public class ZMQHitRateResultVisitor implements ResultVisitor {

	public static class DataEntry implements Comparable<DataEntry> {
		
		public static final Comparator<DataEntry> comparator = Comparator
				.comparingLong(DataEntry::getTotalCounter).reversed();
		private String fileName;
		private long totalCounter;
		private double hitRate;
		
		public DataEntry(String fileName, long totalCounter, double hitRate) {
			this.fileName = fileName;
			this.totalCounter = totalCounter;
			this.hitRate = hitRate;
		}

		public String getFileName() {
			return fileName;
		}
		
		public void setFileName(String fileName) {
			this.fileName = fileName;
		}
		
		public long getTotalCounter() {
			return totalCounter;
		}
		
		public void setTotalCounter(long totalCounter) {
			this.totalCounter = totalCounter;
		}
		
		public double getHitRate() {
			return hitRate;
		}
		
		public void setHitRate(double hitRate) {
			this.hitRate = hitRate;
		}

		@Override
		public int compareTo(DataEntry o) {
			return comparator.compare(this, o);
		}
			
	}
	
	private List<DataEntry> entryList;
	private String serviceEndpoint;
	private String syncEndpoint;
	private ZContext mInternalCtx;
	private ZMQ.Socket mSocketInternal;

	public ZMQHitRateResultVisitor(String serviceEndpoint, String syncEndpoint) {
		this.serviceEndpoint = serviceEndpoint;
		this.syncEndpoint = syncEndpoint;
		this.entryList = new ArrayList<>();
	}

	public void init() {
		mInternalCtx = new ZContext();
		mSocketInternal = mInternalCtx.createSocket(SocketType.PUSH);
		mSocketInternal.bind(serviceEndpoint);
		Socket syncInternal = mInternalCtx.createSocket(SocketType.PAIR);
		syncInternal.connect(syncEndpoint);
		syncInternal.send("READY", 0);
		syncInternal.close();
	}
	
	@Override
	public void visit(String line) {
		StringTokenizer stk = new StringTokenizer(line, "\t");
		String fileName = stk.nextToken();
		long totalCount = Long.parseLong(stk.nextToken());
		double hitRate = Double.parseDouble(stk.nextToken());
		entryList.add(new DataEntry(fileName, totalCount, hitRate));
	}
	
	public void pushMessages() {
		entryList.sort(DataEntry.comparator);
		for(DataEntry entry : entryList) {
			// TODO: Min, choose how you prefer ZMQ messages to be pushed
			mSocketInternal.send(formatDataEntry(entry));
		}	
	}

	@Override
	public void beforeVisit() throws Exception {
		entryList.clear();
	}

	@Override
	public void afterVisit() throws Exception {
		pushMessages();
	}
	
	private String formatDataEntry(DataEntry entry) {
		// TODO: Min, choose your string format for ZMQ messages
		return entry.getFileName();
	}

}
