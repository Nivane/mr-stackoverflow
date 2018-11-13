package thisisnobody.mrdp.users;

import java.io.IOException;
import java.util.Map;

import thisisnobody.mrdp.utils.MRDPUtils;

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

public class LocationUsersDriver {

	public static class LocationUsersMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

		private static final Long one = new Long(1);

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// Parse the input into a nice map.
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());

			// Get the value for the Location attribute
			String location = parsed.get("Location");
			if (location != null && !location.isEmpty()) {
				context.write(new Text(location), new LongWritable(one));
			}
		}
	}

	public static class LocationUsersReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

		@Override
		protected void reduce(Text text, Iterable<LongWritable> ite, Context context)
				throws IOException, InterruptedException {
			long res = 0;

			for (LongWritable l : ite) {
				res += Long.parseLong(l.toString());
			}

			context.write(text, new LongWritable(res));

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		// String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String[] otherArgs = new GenericOptionsParser(conf, new String[] { "C:/Users/ZLP/eclipse-workspace/Users.xml",
				"C:/Users/ZLP/eclipse-workspace/mrdp/locationusers" }).getRemainingArgs();

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
		job.setJarByClass(LocationUsersDriver.class);

		job.setMapperClass(LocationUsersMapper.class);
		job.setReducerClass(LocationUsersReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, outputDir);

		job.waitForCompletion(true);

	}
}
