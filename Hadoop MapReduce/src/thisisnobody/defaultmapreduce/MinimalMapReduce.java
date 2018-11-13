package thisisnobody.defaultmapreduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * @author ZLP
 * 默认MR作业，不显示默认设置
 */
public class MinimalMapReduce extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MinimalMapReduce(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());
		job.setJarByClass(getClass());

		Path in = new Path("c:/users/zlp/desktop/minimal.txt");
		Path out = new Path("c:/users/zlp/desktop/minimalmapreduce");

		FileSystem fs = FileSystem.get(getConf());
		if (fs.exists(out)) {
			fs.delete(out, true);
		}

		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
