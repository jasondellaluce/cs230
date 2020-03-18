package edu.uci.cs230.toy_cdn.hadoop.mapreduce;

import edu.uci.cs230.toy_cdn.hadoop.ResultVisitor;

public class StandardOutResultVisitor implements ResultVisitor {

	@Override
	public void visit(String line) throws Exception {
		System.out.println(line);
	}

	@Override
	public void beforeVisit() throws Exception {
		
	}

	@Override
	public void afterVisit() throws Exception {
		
	}

}
