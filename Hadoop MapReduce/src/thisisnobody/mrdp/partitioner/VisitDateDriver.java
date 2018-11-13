package thisisnobody.mrdp.partitioner;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import thisisnobody.mrdp.utils.MRDPUtils;

public class VisitDateDriver {

	public static class VisitDateMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

		private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

		@SuppressWarnings("deprecation")
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Map<String, String> map = MRDPUtils.transformXmlToMap(value.toString());
			if (map == null) {
				return;
			}

			String date = map.get("LastAccessDate");
			if (date != null && !date.isEmpty()) {

				try {

					frmt.parse(date).getYear();
					context.write(value, NullWritable.get());
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

		}

	}

	public static class VisitDateReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

		@Override
		protected void reduce(Text text, Iterable<NullWritable> ite,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(text, NullWritable.get());
		}

	}

	public static class VisitDatePartitioner extends Partitioner<Text, NullWritable> {

		private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

		@SuppressWarnings("deprecation")
		@Override
		public int getPartition(Text key, NullWritable value, int numPartitions) {

			Map<String, String> map = MRDPUtils.transformXmlToMap(key.toString());

			String date = map.get("LastAccessDate");
			int year = 0;
			try {
				year = frmt.parse(date).getYear();
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			return year % 4;
		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(VisitDateDriver.class);

		job.setMapperClass(VisitDateMapper.class);
		job.setReducerClass(VisitDateReducer.class);
		job.setPartitionerClass(VisitDatePartitioner.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setNumReduceTasks(4);

		Path in = new Path("C:/Users/ZLP/Downloads/stackoverflow/stackoverflow/users.xml");
		Path out = new Path("C:/Users/ZLP/Downloads/stackoverflow/stackoverflow/lastvisitdate");

		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(out)) {
			fs.delete(out, true);
		}

		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.waitForCompletion(true);

	}

}
