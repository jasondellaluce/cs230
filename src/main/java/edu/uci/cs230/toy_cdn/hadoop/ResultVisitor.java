package edu.uci.cs230.toy_cdn.hadoop;

public interface ResultVisitor {

	public void beforeVisit() throws Exception;
	public void visit(String line) throws Exception;
	public void afterVisit() throws Exception;
}
