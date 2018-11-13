package thisisnobody.wordcount;

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
/**
 * 
 * @author ZLP
 * WordCount实例 提交集群
 * 提交命令
 * hadoop jar /usr/local/WordCount.jar /wordcount/core-site.xml /wordcount/output/
 * WCMapper和WCReducer都是内部类
 */
public class ClusterWordCount {

	/**
	 * @author ZLP
	 * 继承Mapper类，实现map方法
	 * 输入类型：<LongWritable, Text>	文件偏移量	文本
	 * 输出类型：<Text, LongWritable>	文本			数量
	 */
	public static class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		
		/*
		 * Called once for each key/value pair in the input split.
		 * 输入切片的每个键值对都执行
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] words = line.split(" ");
			for (String word : words) {
				/*
				 * Generate an output key/value pair.
				 * 生成中间结果
				 */
				context.write(new Text(word), new LongWritable(1));
			}
		}

	}
	
	/**
	 * @author ZLP
	 * 继承Reducer类，实现reduce方法
	 * 输入类型：<Text, Iterable<LongWritable>>	文本		数量
	 * 输出类型：<Text, LongWritable>			文本		数量
	 */
	public static class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		
		/*
		 * This method is called once for each key.
	     * 对每个键都执行
		 */
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			long count = 0;
			for (LongWritable value : values) {
				count += Long.parseLong(value.toString());
			}
			/*
			 * Generate an output key/value pair.
			 * 生成最终结果
			 */
			context.write(key, new LongWritable(count));
		}

	}


	public static void main(String[] args) throws Exception {
		
		//Provides access to configuration parameters.
		Configuration conf = new Configuration();

		/* The job submitter's view of the Job.
		 * 
		 * It allows the user to configure the
		 * job, submit it, control its execution, and query the state. The set methods
		 * only work until the job is submitted, afterwards they will throw an 
		 * IllegalStateException.
		 * 
		 * Normally the user creates the application, describes various facets of the
		 * job via {@link Job} and then submits the job and monitor its progress.
		 */
		Job job = Job.getInstance(conf);
		
		//Set the Jar by finding where a given class came from.
		job.setJarByClass(ClusterWordCount.class);
		
		job.setMapperClass(WCMapper.class);
		job.setReducerClass(WCReducer.class);
 
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		

		Path in = new Path(args[0]);
		/*
		 * Set the array of {@link Path}s as the list of inputs
		 * for the map-reduce job.
		 * Path是变长参数
		 */
		FileInputFormat.setInputPaths(job, in);

		Path out = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(out)) {
			fs.delete(out, true);
		}
		//Set the {@link Path} of the output directory for the map-reduce job.
		FileOutputFormat.setOutputPath(job, out);
		
		//Submit the job to the cluster and wait for it to finish.
		job.waitForCompletion(true);

	}
}

