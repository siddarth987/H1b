import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//6) Find the percentage and the count of each case status on total applications for each year.
public class Que6 {
	public static class map_total_appl extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context con)
				throws IOException, InterruptedException {
			String[] rec = value.toString().split("\t");
			String applications = rec[3];
			String case_status = rec[1];
			String year = rec[7];
			con.write(new Text(year + '\t' + case_status), new IntWritable(1));
			con.write(new Text(year), new IntWritable(1));
		}
	}

	public static class red_appl extends
			Reducer<Text, IntWritable, Text, Text> {
		public void reduce(Text key, Iterable<IntWritable> value, Context con)
				throws IOException, InterruptedException {
			int count = 0;
			int total=0;
			for (IntWritable val : value) {
				count++;
				
			}
			int year = 0;
			for(IntWritable val1:value)
			{
				year++;
			}
			float avg =count/year;
			String totalSum = String.format("%d", year);
			String count1 = String.format("%d", count);
			String avg1 = String.format("%f", avg);
			String sam= totalSum+'\t'+count1+'\t'+avg1;
			
			con.write(key, new Text(sam));
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "");
		job.setJarByClass(Que6.class);
		job.setMapperClass(map_total_appl.class);
//		job.setPartitionerClass(year_partitionerclass.class);
		job.setReducerClass(red_appl.class);
//		job.setNumReduceTasks(6);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
