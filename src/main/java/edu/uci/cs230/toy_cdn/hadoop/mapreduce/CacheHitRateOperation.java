package edu.uci.cs230.toy_cdn.hadoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class CacheHitRateOperation extends AbstractMapReduceOperation {
	
	public static class HitStat implements Writable {
		
		private long hitCounter;
		private long totalCounter;
		
		public HitStat() {
			
		}
		
		public HitStat(long hitCounter, long totalCounter) {
			if(hitCounter < 0 || totalCounter <= 0 || hitCounter > totalCounter)
				throw new IllegalArgumentException();
			this.hitCounter = hitCounter;
			this.totalCounter = totalCounter;
		}

		public void write(DataOutput out) throws IOException {
			out.writeLong(hitCounter);
			out.writeLong(totalCounter);
		}

		public void readFields(DataInput in) throws IOException {
			hitCounter = in.readLong();
			totalCounter = in.readLong();
		}
		
		@Override
		public String toString() {
			return String.valueOf(getTotalCounter()) + "\t" + String.valueOf(getHitRatio());
		}
		
		public long getHitCounter() {
			return hitCounter;
		}
		
		public long getTotalCounter() {
			return totalCounter;
		}
		
		public double getHitRatio() {
			return ((double) hitCounter) / totalCounter;
		}
		
	}
	
	public static class HitStatMapper extends Mapper<Object, Text, Text, HitStat> {
		
		private static final Pattern regexPattern = Pattern.compile(
			"(<timestamp>){1}([0-9]*)(<\\/timestamp>){1}"
			+ "(<fileId>){1}(.*)(<\\/fileId>){1}"
			+ "(<cacheStatus>){1}(.*)(<\\/cacheStatus>){1}");
		private Text fileId = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Matcher regexMatcher = regexPattern.matcher(value.toString());
			if(regexMatcher.find()) {
				// String strTimestamp = regexMatcher.group(2);
				String strFileId = regexMatcher.group(5);
				String strCacheStatus = regexMatcher.group(8);
				
				fileId.set(strFileId);
				HitStat hitStat = new HitStat(strCacheStatus.toLowerCase().contains("miss") ? 0 : 1, 1);
				context.write(fileId, hitStat);
			}
		}
	}

	public static class HitStatReducer extends Reducer<Text, HitStat, Text, HitStat> {

		public void reduce(Text key, Iterable<HitStat> values, Context context) throws IOException, InterruptedException {
			long hitCounter = 0;
			long totalCounter = 0;
			
			for(HitStat stat : values) {
				hitCounter += stat.getHitCounter();
				totalCounter += stat.getTotalCounter();
			}
			
			HitStat hitStat = new HitStat(hitCounter, totalCounter);
			context.write(key, hitStat);
		}
	}
	
	public CacheHitRateOperation(String inputDirectory, String outputDirectory) {
		super(inputDirectory, outputDirectory);
	}

	@Override
	protected Job configureMapReduceJob(Job job) {
		job.setJobName("CDN Cache Hit Rate");
		job.setJarByClass(CacheHitRateOperation.class);
		job.setMapperClass(HitStatMapper.class);
		job.setCombinerClass(HitStatReducer.class);
		job.setReducerClass(HitStatReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(HitStat.class);
		return job;
	}

	@Override
	protected String getJobDirectoryName() {
		return "cache-hit";
	}

}
