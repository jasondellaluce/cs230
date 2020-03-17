package edu.uci.cs230.toy_cdn.hadoop.mapreduce;

public abstract class TextMapReduceOperation implements MapReduceOperation {

	private String inputDirectory;
	private String outputDirectory;

	public TextMapReduceOperation(String inputDirectory, String outputDirectory) {
		this.inputDirectory = inputDirectory;
		this.outputDirectory = outputDirectory;
	}
	
	public String getInputDirectory() {
		return inputDirectory;
	}

	public String getOutputDirectory() {
		return outputDirectory;
	}

	@Override
	public void acceptResultVisitor(ResultVisitor visitor) {
		// TODO Auto-generated method stub
		
	}

}
