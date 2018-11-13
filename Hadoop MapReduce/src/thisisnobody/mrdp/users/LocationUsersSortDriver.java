package thisisnobody.mrdp.users;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class LocationUsersSortDriver {

	public static class LocationUsersMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if (value != null) {
				String[] strs = value.toString().split("\t");
				if (strs.length == 2) {
					context.write(new LongWritable(Long.parseLong(strs[1])), new Text(strs[0]));
				}
			}
		}
	}

	public static class LocationUsersReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

		@Override
		protected void reduce(LongWritable key, Iterable<Text> ite, Context context)
				throws IOException, InterruptedException {
			for (Text t : ite) {
				context.write(key, t);// 具有相同数量的不同location写在不同行
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		// String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String[] otherArgs = new GenericOptionsParser(conf,
				new String[] { "C:/Users/ZLP/eclipse-workspace/mrdp/locationusers",
						"C:/Users/ZLP/eclipse-workspace/mrdp/locationusers2" }).getRemainingArgs();

		if (otherArgs.length != 2) {
			System.err.println("Usage: CountNumUsersByState <users> <out>");
			System.exit(2);
		}

		Path input = new Path(otherArgs[0]);
		Path outputDir = new Path(otherArgs[1]);

		FileSystem fs = FileSystem.get(conf);

		if (fs.exists(outputDir)) {
			fs.delete(outputDir, true);
		}

		Job job = Job.getInstance(conf, "Count Num Users By State");
		job.setJarByClass(LocationUsersSortDriver.class);

		job.setMapperClass(LocationUsersMapper.class);
		job.setReducerClass(LocationUsersReducer.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, outputDir);

		job.waitForCompletion(true);

	}
}
