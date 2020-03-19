package edu.uci.cs230.toy_cdn.hadoop.zeromq;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

import edu.uci.cs230.toy_cdn.hadoop.MapReduceVisitor;

public class ZmqHitRateMapReduceVisitor implements MapReduceVisitor {

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

	public ZmqHitRateMapReduceVisitor(String serviceEndpoint, String syncEndpoint) {
		this.serviceEndpoint = serviceEndpoint;
		this.syncEndpoint = syncEndpoint;
		this.entryList = new ArrayList<>();
		mInternalCtx = new ZContext();
		init();
	}

	public void init() {
		mSocketInternal = mInternalCtx.createSocket(SocketType.PUSH);
		mSocketInternal.bind(serviceEndpoint);
		Socket syncInternal = mInternalCtx.createSocket(SocketType.PAIR);
		syncInternal.connect(syncEndpoint);
		syncInternal.send("READY", 0);
		syncInternal.close();
	}
	
	@Override
	public void visitResult(String line) {
		StringTokenizer stk = new StringTokenizer(line, "\t");
		String fileName = stk.nextToken();
		long totalCount = Long.parseLong(stk.nextToken());
		double hitRate = Double.parseDouble(stk.nextToken());
		entryList.add(new DataEntry(fileName, totalCount, hitRate));
	}

	@Override
	public void beforeVisit() throws Exception {
		entryList.clear();
	}

	@Override
	public void afterVisit() throws Exception {
		pushMessageOnSocket();
	}
	
	private String formatDataEntry(DataEntry entry) {
		return entry.getFileName();
	}
	
	public void pushMessageOnSocket() {
		Stream<String> stream = entryList
				.stream()
				.sorted()
				.map(this::formatDataEntry)
				.limit(10);
		List<String> rank = stream.collect(Collectors.toList());
		ZMsg message = new ZMsg();
		for(String file : rank) {
			message.add(file);
		}
		message.send(mSocketInternal);
	}

}
