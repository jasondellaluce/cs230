package edu.uci.cs230.toy_cdn.hadoop;

import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WeightedEdgeStatistics {
	
	private static class HitStat implements Writable {
		private long hitCounter;
		private long totalCounter;
		
		protected HitStat() {
			
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

		public static HitStat read(DataInput in) throws IOException {
			HitStat w = new HitStat();
			w.readFields(in);
			return w;
		}
		
		@Override
		public String toString() {
			return String.valueOf(getHitRatio());
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
				String strTimestamp = regexMatcher.group(2);
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

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf, "CDN Hit Statistics");
		job1.setJarByClass(WeightedEdgeStatistics.class);
		job1.setMapperClass(HitStatMapper.class);
		job1.setCombinerClass(HitStatReducer.class);
		job1.setReducerClass(HitStatReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(HitStat.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);
//		System.exit(job1.waitForCompletion(true) ? 0 : 1);


		//sort and send message
//		try (ZContext context = new ZContext()) {
		try {
			FileSystem fs = FileSystem.get(conf);
			Path inFile = new Path(args[1]+"/part-r-00000");
			if (!fs.exists(inFile)) {
				System.out.println("Input file not found");
				throw new IOException("Input file not found");
			}

			FSDataInputStream reader = fs.open(inFile);
			List<Pair<String, Double>> list = new ArrayList<>();
			String line = reader.readLine();
			while (line != null) {
				line = line.replaceAll("\\s", ",");
				String[] kvp = line.split(",", 2);
				list.add(new Pair<String, Double>(kvp[0], Double.parseDouble(kvp[1])));
				line = reader.readLine();
			}
			reader.close();
			list.sort((a, b) -> a.getValue().compareTo(b.getValue()));
			for (Pair x : list) {
				System.out.println(x);
			}

			// Socket to talk to clients
//			ZMQ.Socket socket = context.createSocket(SocketType.PUSH);
//			socket.bind("tcp://*:5555");
//
//			var msg = new ZMsg();
//			for (int i = 0; i < list.size() || i < 10; i++){
//				msg.add(list.get(i).getKey());
//			}
//			msg.send(socket);
//
//			var syncInternal = context.createSocket(SocketType.PAIR);
//			syncInternal.connect("tcp:cdn");
//			syncInternal.send("READY", 0);
//			syncInternal.close();

		} catch (IOException e) {
			e.printStackTrace();

//
//			var daemon = new Thread(new Runnable() {
//				@Override
//				public void run() {
//					//1. Fetch data from hdfs
//
//
//					//2. Sorting
//					//3. Send using ZMQ
//				}
//			});
//			daemon.start();
//			daemon.join();

		}
	}
	
}
