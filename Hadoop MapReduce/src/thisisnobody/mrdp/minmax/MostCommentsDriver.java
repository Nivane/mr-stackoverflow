package thisisnobody.mrdp.minmax;

import java.io.IOException;

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

public class MostCommentsDriver {

	public static class MostCommentsMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.write(parseText(value), value);
		}

		private LongWritable parseText(Text line) {
			String[] res = line.toString().split(" ");
			return new LongWritable(Long.parseLong(res[res.length - 1]));
		}
	}

	public static class MostCommentsReducer extends Reducer<LongWritable, Text, NullWritable, Text> {

		@Override
		protected void reduce(LongWritable key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			for (Text t : value) {
				context.write(NullWritable.get(), t);
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(MostCommentsDriver.class);

		job.setMapperClass(MostCommentsMapper.class);
		job.setReducerClass(MostCommentsReducer.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		Path in = new Path("C:/Users/ZLP/Downloads/stackoverflow/stackoverflow/comments/part-r-00000");
		Path out = new Path("C:/Users/ZLP/Downloads/stackoverflow/stackoverflow/comments/mostcommentusers");

		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(out)) {
			fs.delete(out, true);
		}

		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.waitForCompletion(true);
	}

}
