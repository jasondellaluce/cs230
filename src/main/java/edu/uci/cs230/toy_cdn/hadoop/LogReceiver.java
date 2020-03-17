package edu.uci.cs230.toy_cdn.hadoop;

public interface LogReceiver {
	
	public void initTask() throws Exception;
	public String receiveLine() throws Exception;
	
}
