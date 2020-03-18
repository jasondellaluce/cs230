package edu.uci.cs230.toy_cdn.hadoop;

public interface MapReduceOperation {

	public boolean run() throws Exception;
	public void acceptResultVisitor(ResultVisitor visitor) throws Exception;
	
}
