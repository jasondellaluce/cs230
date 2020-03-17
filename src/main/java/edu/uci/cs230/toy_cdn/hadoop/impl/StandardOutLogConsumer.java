package edu.uci.cs230.toy_cdn.hadoop.impl;

import edu.uci.cs230.toy_cdn.hadoop.LogConsumer;

public class StandardOutLogConsumer implements LogConsumer {

	@Override
	public void initTask() throws Exception {
		
	}

	@Override
	public void onReceivedLine(String newLine) throws Exception {
		System.out.println(newLine);
	}

}
