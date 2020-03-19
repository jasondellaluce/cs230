package edu.uci.cs230.toy_cdn.hadoop;

public interface MapReduceVisitor {

	public void beforeVisit() throws Exception;
	public void visitResult(String line) throws Exception;
	public void afterVisit() throws Exception;
}
