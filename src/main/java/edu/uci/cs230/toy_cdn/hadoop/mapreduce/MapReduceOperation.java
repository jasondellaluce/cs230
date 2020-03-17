package edu.uci.cs230.toy_cdn.hadoop.mapreduce;

import java.io.IOException;

public interface MapReduceOperation {

	public boolean run() throws IOException;
	public void acceptResultVisitor(ResultVisitor visitor);
	
}
