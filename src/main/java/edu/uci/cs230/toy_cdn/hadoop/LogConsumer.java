package edu.uci.cs230.toy_cdn.hadoop;

public interface LogConsumer {

	public void initTask() throws Exception;
	public void onReceivedLine(String newLine) throws Exception;
	
}
