package edu.uci.cs230.toy_cdn.hadoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.uci.cs230.toy_cdn.hadoop.MapReduceOperation;
import edu.uci.cs230.toy_cdn.hadoop.ResultVisitor;

public class HyperLinkCountOperation implements MapReduceOperation {
	
	public static class HyperLink implements Writable {
		
		private long count;

		public HyperLink() {
			
		}
		
		public HyperLink(long count) {
			if(count < 0 )
				throw new IllegalArgumentException();
			this.count = count;
		}

		public void write(DataOutput out) throws IOException {
			out.writeLong(count);
		}

		public void readFields(DataInput in) throws IOException {
			count = in.readLong();
		}
		
		@Override
		public String toString() {
			return String.valueOf(count);
		}
		
		public long getCount() {
			return count;
		}

	}
	
	public static class HyperLinkMapper extends Mapper<Object, Text, Text, HyperLink> {
		
		private static final Pattern regexPattern = Pattern.compile(
			"(href=\"http){1}(.*?)(.html){1}");
		private Text hyperLinkId = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Matcher regexMatcher = regexPattern.matcher(value.toString());
			if(regexMatcher.find()) {
				String strHyperLinkId = regexMatcher.group();
				strHyperLinkId = strHyperLinkId.substring(6,strHyperLinkId.length());
				hyperLinkId.set(strHyperLinkId);
				HyperLink hlink = new HyperLink(1);
				context.write(hyperLinkId, hlink);
			}
		}
	}

	public static class HyperLinkReducer extends Reducer<Text, HyperLink, Text, HyperLink> {

		public void reduce(Text key, Iterable<HyperLink> values, Context context) throws IOException, InterruptedException {
			long count = 0;

			for(HyperLink hlink : values) {
				count += hlink.getCount();
			}

			HyperLink hlink = new HyperLink(count);
			context.write(key, hlink);
		}
	}

	private String inputDirectory;
	private String outputDirectory;

	public HyperLinkCountOperation(String inputDirectory, String outputDirectory) {
		this.inputDirectory = inputDirectory;
		this.outputDirectory = outputDirectory;
	}
	
	@Override
	public boolean run() throws Exception {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "CDN Hyperlink Count");
		job.setJarByClass(HyperLinkCountOperation.class);
		job.setMapperClass(HyperLinkMapper.class);
		job.setCombinerClass(HyperLinkReducer.class);
		job.setReducerClass(HyperLinkReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(HyperLink.class);
		
		FileInputFormat.addInputPath(job, new Path(inputDirectory));
		FileOutputFormat.setOutputPath(job, new Path(outputDirectory));
		
		try {
			return job.waitForCompletion(true);
		}
		catch (ClassNotFoundException | InterruptedException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void acceptResultVisitor(ResultVisitor visitor) {
		// TODO Auto-generated method stub
		
	}
	
}
