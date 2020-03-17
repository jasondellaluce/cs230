package edu.uci.cs230.toy_cdn.hadoop.mapreduce;

public interface ResultVisitor {

	public void visit(String line);
	
}
