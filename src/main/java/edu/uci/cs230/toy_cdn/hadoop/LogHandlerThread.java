package edu.uci.cs230.toy_cdn.hadoop;

public class LogHandlerThread extends Thread {

	private LogConsumer logConsumer;
	private LogReceiver logReceiver;
	private boolean waiting;
	
	public LogHandlerThread(LogConsumer logConsumer, LogReceiver logReceiver) {
		this.logConsumer = logConsumer;
		this.logReceiver = logReceiver;
		this.waiting = false;
	}

	@Override
	public void run() {
		try {
			logReceiver.initTask();
			logConsumer.initTask();
		}
		catch (Exception e) {
			e.printStackTrace();
			return;
		}
		
		while(true) {
			if(!isWaiting()) {
				String line = "";
				
				try {
					line = logReceiver.receiveLine();
					logConsumer.onReceivedLine(line);
				}
				catch (Exception e) {
					e.printStackTrace();
					return;
				}
			}		
		}
	}

	public synchronized boolean isWaiting() {
		return waiting;
	}

	public synchronized void setWaiting(boolean waiting) {
		this.waiting = waiting;
	}
	
}
