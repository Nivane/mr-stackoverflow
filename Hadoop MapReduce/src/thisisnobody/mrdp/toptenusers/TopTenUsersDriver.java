package thisisnobody.mrdp.toptenusers;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

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

public class TopTenUsersDriver {

	public static class TopTenMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

		private TreeMap<Integer, Text> tops = new TreeMap<Integer, Text>();

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {

			Map<String, String> map = MRDPUtils.transformXmlToMap(value.toString());

			if (map == null) {
				return;
			}

			String strUser = map.get("Id");
			String strRep = map.get("Reputation");

			if (strUser != null || strRep != null) {
				try {
					Text rep = new Text(strRep);
					tops.put(Integer.parseInt(strRep), rep);

					if (tops.size() > 10) {
						tops.remove(tops.firstKey());
					}

				} catch (Exception e) {
					e.printStackTrace();

				}

			}

		}

		@Override
		protected void cleanup(Mapper<LongWritable, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {

			for (Entry<Integer, Text> e : tops.entrySet()) {
				context.write(NullWritable.get(), e.getValue());
			}

		}

	}

	public static class TopTenReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

		private TreeMap<Integer, Text> tops = new TreeMap<Integer, Text>();

		@Override
		protected void reduce(NullWritable nu, Iterable<Text> ite,
				Reducer<NullWritable, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {

			for (Text t : ite) {
				tops.put(Integer.parseInt(t.toString()), t);
				if (tops.size() > 10) {
					tops.remove(tops.firstKey());
				}

			}

			for (Entry<Integer, Text> e : tops.entrySet()) {
				context.write(NullWritable.get(), new Text(e.getKey() + ""));
			}
		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);

		job.setJarByClass(TopTenUsersDriver.class);

		job.setMapperClass(TopTenMapper.class);
		job.setReducerClass(TopTenReducer.class);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		Path in = new Path("C:/Users/ZLP/Downloads/stackoverflow/stackoverflow/Users.xml");
		Path out = new Path("C:/Users/ZLP/Downloads/stackoverflow/stackoverflow/topten");

		FileSystem fs = FileSystem.newInstance(conf);

		if (fs.exists(out)) {
			fs.delete(out, true);
		}

		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.waitForCompletion(true);

	}

}
