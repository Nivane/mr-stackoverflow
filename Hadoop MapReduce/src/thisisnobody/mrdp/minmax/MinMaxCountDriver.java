package thisisnobody.mrdp.minmax;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
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

public class MinMaxCountDriver {

	public static class MinMaxCountMapper extends Mapper<LongWritable, Text, Text, CountComment> {

		private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());

			String creationDate = parsed.get("CreationDate");
			String user = parsed.get("UserId");

			if (user != null && !user.isEmpty() && creationDate != null && !creationDate.isEmpty()) {
				CountComment cc = new CountComment();
				try {
					Date date = frmt.parse(creationDate);
					cc.setCount(1);
					cc.setDate(date);
					context.write(new Text(user), cc);
				} catch (ParseException e) {
					e.printStackTrace();
				}

			}

		}

	}

	public static class MinMaxCountReducer extends Reducer<Text, CountComment, Text, Text> {
		private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

		@Override
		protected void reduce(Text user, Iterable<CountComment> comments, Context context)
				throws IOException, InterruptedException {
			long count = 0;
			Date minDate = null;
			Date maxDate = null;
			try {
				maxDate = frmt.parse("1970-12-31T00:00:00.001");
				minDate = frmt.parse("2099-12-31T00:00:00.001");
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			for (CountComment cc : comments) {
				if (cc.getDate().after(maxDate)) {
					maxDate = cc.getDate();
				}

				if (cc.getDate().before(minDate)) {
					minDate = cc.getDate();
				}

				count++;
			}

			context.write(user, new Text("MinDate: " + minDate.toString() + "\t" + "MaxDate: " + maxDate.toString()
					+ "\t" + "CommentCounts: " + count));

		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(MinMaxCountDriver.class);

		job.setMapperClass(MinMaxCountMapper.class);
		job.setReducerClass(MinMaxCountReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(CountComment.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		Path in = new Path("C:/Users/ZLP/Downloads/stackoverflow/stackoverflow/Comments.xml");
		Path out = new Path("C:/Users/ZLP/Downloads/stackoverflow/stackoverflow/comments");

		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(out)) {
			fs.delete(out, true);
		}

		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.waitForCompletion(true);
	}

}

class CountComment implements Writable {

	private Date date;
	private long count;

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(date.getTime());
		out.writeLong(count);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		date = new Date(in.readLong());
		count = in.readLong();
	}
}
