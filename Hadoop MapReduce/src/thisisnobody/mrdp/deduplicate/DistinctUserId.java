package thisisnobody.mrdp.deduplicate;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import thisisnobody.mrdp.utils.MRDPUtils;

public class DistinctUserId {

	public static class UserMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {

			Map<String, String> map = MRDPUtils.transformXmlToMap(value.toString());
			if (map == null) {
				return;
			}

			String user = map.get("UserId");
			if (user != null && !user.isEmpty()) {
				context.write(new Text(user), NullWritable.get());
			}
		}

	}

	public static class UserReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
		@Override
		protected void reduce(Text text, Iterable<NullWritable> ite,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {

			context.write(text, NullWritable.get());
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);

		job.setJarByClass(DistinctUserId.class);

		job.setMapperClass(UserMapper.class);
		job.setCombinerClass(UserReducer.class);
		job.setReducerClass(UserReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		Path in = new Path("c:/users/zlp/downloads/stackoverflow/stackoverflow/comments.xml");
		Path out = new Path("c:/users/zlp/downloads/stackoverflow/stackoverflow/distinctuserid");

		FileSystem fs = FileSystem.newInstance(conf);
		if (fs.exists(out)) {
			fs.delete(out, true);
		}

		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);
		job.waitForCompletion(true);
	}

}
