package thisisnobody.mrdp.average;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import thisisnobody.mrdp.utils.MRDPUtils;

public class AverageLengthDriver {

	public static class AverageMapper extends Mapper<LongWritable, Text, Text, AverageLength> {

		private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			Map<String, String> map = MRDPUtils.transformXmlToMap(value.toString());

			String date = map.get("CreationDate");
			String Text = map.get("Text");

			if (date != null && Text != null) {
				try {
					@SuppressWarnings("deprecation")
					int hour = frmt.parse(date).getHours();
					AverageLength al = new AverageLength();
					al.setAvgLength(Text.length());
					al.setCount(1);

					context.write(new Text(hour + ""), al);

				} catch (ParseException e) {

					e.printStackTrace();
				}
			}

		}

	}

	public static class AverageCombiner extends Reducer<Text, AverageLength, Text, AverageLength> {

		@Override
		protected void reduce(Text text, Iterable<AverageLength> ite, Context context)
				throws IOException, InterruptedException {

			long totalLength = 0;
			long count = 0;
			AverageLength res = new AverageLength();

			for (AverageLength al : ite) {
				totalLength += al.getCount() * al.getAvgLength();
				count += al.getCount();
			}

			res.setCount(count);
			res.setAvgLength(totalLength / count);
			context.write(text, res);
		}
	}

	public static class AverageReducer extends Reducer<Text, AverageLength, Text, Text> {

		@Override
		protected void reduce(Text text, Iterable<AverageLength> ite, Context context)
				throws IOException, InterruptedException {

			long totalLength = 0;
			long count = 0;
			AverageLength res = new AverageLength();

			for (AverageLength al : ite) {
				totalLength += al.getCount() * al.getAvgLength();
				count += al.getCount();
			}

			res.setCount(count);
			res.setAvgLength(totalLength / count);
			context.write(text, new Text("" + res.getAvgLength()));
		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();

		Job job = Job.getInstance();
		job.setJarByClass(AverageLengthDriver.class);
		job.setMapperClass(AverageMapper.class);
		job.setReducerClass(AverageReducer.class);
		job.setCombinerClass(AverageCombiner.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(AverageLength.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		Path in = new Path("C:/Users/ZLP/Downloads/stackoverflow/stackoverflow/Comments.xml");
		Path out = new Path("C:/Users/ZLP/Downloads/stackoverflow/stackoverflow/average");

		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(out)) {
			fs.delete(out, true);
		}

		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.waitForCompletion(true);

	}

}

class AverageLength implements Writable {

	private double avgLength;

	private long count;

	public double getAvgLength() {
		return avgLength;
	}

	public void setAvgLength(double avgLength) {
		this.avgLength = avgLength;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(avgLength);
		out.writeLong(count);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		avgLength = in.readDouble();
		count = in.readLong();
	}

}