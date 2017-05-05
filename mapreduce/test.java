import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;;


public class test {
	public static class map extends Mapper<LongWritable,Text,Text,FloatWritable>
	{
		public void map(LongWritable key,Text value,Context con) throws IOException, InterruptedException
		{
			String[] str=value.toString().split("#");
			float pre=Float.parseFloat(str[6]);
			String tme=str[5];
			con.write(new Text(tme), new FloatWritable(pre));
		}
	}
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException
	{
		Configuration c =new Configuration();
		Job job=Job.getInstance(c,"");
		job.setJarByClass(test.class);
		job.setMapperClass(map.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
