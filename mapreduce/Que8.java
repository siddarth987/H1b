import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//8) Find the average Prevailing Wage for each Job for each Year (take part time and full time separate)

public class Que8 {

	public static class map extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context con)
				throws IOException, InterruptedException {
			String[] rec = value.toString().split("\t");
			String prevailing_wage = (rec[6]);
			if (rec[5].equals("Y")) {
				con.write(new Text(rec[6]), new Text(prevailing_wage));
			} else if (rec[5].equals("N")) {
				con.write(new Text(rec[6]), new Text(prevailing_wage));
			}
			con.write(new Text(rec[5]),new Text("1"));
			
			
		}
	}

	/*
	 * public static class year_partitionerclass extends Partitioner<Text,
	 * IntWritable> {
	 * 
	 * @Override public int getPartition(Text key, IntWritable value, int
	 * numReduceTasks) {
	 * 
	 * String[] rec = key.toString().split("\t"); String year = rec[0]; if
	 * (year.equals("2011")) { return 0; } else if (year.equals("2012")) {
	 * return 1; } else if (year.equals("2013")) { return 2; } else if
	 * (year.equals("2014")) { return 3; } else if (year.equals("2015")) {
	 * return 4; } else { return 5; } } }
	 */

	public static class red_loc extends
			Reducer<Text, Text, Text, FloatWritable> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			float sum = 0;
			for (Text val : values) {
				sum = sum + Float.parseFloat(val.toString());
				
			}
			context.write(key, new FloatWritable(sum));

		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "");
		job.setJarByClass(Que8.class);
		job.setMapperClass(map.class);
		// job.setPartitionerClass(year_partitionerclass.class);
		job.setReducerClass(red_loc.class);
		// job.setNumReduceTasks(6);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}