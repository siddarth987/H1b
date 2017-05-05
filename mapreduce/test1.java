import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class test1 {
	public static class pet_Mapper extends
			Mapper<LongWritable, Text, Text, FloatWritable> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] str = value.toString().split("\t");

			String name = str[4];
			float val = Float.parseFloat(str[0]);
			// if(val!=="/N")

			context.write(new Text("\"ALL\""), new FloatWritable(val));
			context.write(new Text(name), new FloatWritable(val));

		}
	}

	public static class reduc_dat extends
			Reducer<Text, FloatWritable, Text, Text> {
		float sum = 0;

		public void reduce(Text key, Iterable<FloatWritable> value, Context con)
				throws IOException, InterruptedException {

			float con_sum = 0;
			float per = 0;
			for (FloatWritable val : value) {
				if (key.toString().equals("\"ALL\"")) {
					sum = sum + val.get();
				} else {
					con_sum = con_sum + val.get();
				}
				// per=con_sum/sum;
			}
			/*
			 * String st1 = String.format("%f", con_sum); String st11 =
			 * String.format("%f", per); String st = String.format("%f", sum);
			 * con.write(key, new Text(st1 + ',' + st + ',' + st11));
			 */
			con.write(key, new Text(String.valueOf(con_sum +"   "+sum)));
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration con = new Configuration();
		Job job = Job.getInstance(con, "");
		job.setJarByClass(test1.class);
		job.setMapperClass(pet_Mapper.class);
		// job.setNumReduceTasks(0);
		job.setReducerClass(reduc_dat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
