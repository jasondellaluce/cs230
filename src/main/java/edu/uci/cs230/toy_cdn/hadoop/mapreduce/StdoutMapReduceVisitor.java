package edu.uci.cs230.toy_cdn.hadoop.mapreduce;

import edu.uci.cs230.toy_cdn.hadoop.MapReduceVisitor;

public class StdoutMapReduceVisitor implements MapReduceVisitor {

	@Override
	public void visitResult(String line) throws Exception {
		System.out.println(line);
	}

	@Override
	public void beforeVisit() throws Exception {
		
	}

	@Override
	public void afterVisit() throws Exception {
		
	}

}
