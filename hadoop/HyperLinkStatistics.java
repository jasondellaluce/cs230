import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;
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

public class HyperLinkStatistics {
	
	private static class HyperLink implements Writable {
		private long count;

		protected HyperLink() {
			
		}
		
		public HyperLink(long count) {
			if(count < 0 )
				throw new IllegalArgumentException();
			this.count = count;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(count);
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
			count = in.readLong();
		}

		public static HyperLink read(DataInput in) throws IOException {
			HyperLink w = new HyperLink();
			w.readFields(in);
			return w;
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
			"(href=\"){1}(.*?)(.html){1}");
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

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "CDN Hyperlink Count");
		job.setJarByClass(HyperLinkStatistics.class);
		job.setMapperClass(HyperLinkMapper.class);
		job.setCombinerClass(HyperLinkReducer.class);
		job.setReducerClass(HyperLinkReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(HyperLink.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
