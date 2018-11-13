package thisisnobody.defaultmapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * @author ZLP ��ʾĬ��Job������
 * Ĭ�ϴ������룺�ļ�ƫ���� + ��
 * Ĭ�ϴ���������ļ�ƫ���� + ��
 */
public class MinimalMapReduceWithDefaults extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MinimalMapReduceWithDefaults(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
		if (job == null)
			return -1;
		/*
		 * MapperĬ������
		 * �����ʽTextInputFormat����ΪLongWritable,ֵΪText
		 * Mapper��
		 * ���������LongWritable�����ֵ����Text
		 */
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(Mapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		/*
		 * ReducerĬ������
		 * Reduce��������1
		 * Reducer��
		 * �����TextOutputFormat�����ʹ��Tab����ֵ�ֿ�
		 * �����LongWritable�����ֵText
		 */
		job.setNumReduceTasks(1);
		job.setReducerClass(Reducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		/*
		 * PartitionerĬ������
		 * HashPartitioner �Լ�¼�ļ����й�ϣ����������¼������ÿ��������һ��reduce����������������reduce������
		 * ����ж��reduce������HashPartitioner����Ҫ��������
		 */
		return job.waitForCompletion(true) ? 0 : 1;
	}
}

class JobBuilder {

	public static Job parseInputAndOutput(Tool tool, Configuration conf, String[] args) throws IOException {

		Path in = new Path("c:/users/zlp/desktop/defaults.txt");
		Path out = new Path("c:/users/zlp/desktop/defaultmapreduce");

		Job job = Job.getInstance(conf);
		job.setJarByClass(tool.getClass());
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(out)) {
			fs.delete(out, true);
		}
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);
		return job;

	}

	public static void printUsage(Tool tool, String extraArgsUsage) {

		System.err.printf(tool.getClass().getSimpleName(), extraArgsUsage);
		GenericOptionsParser.printGenericCommandUsage(System.err);
	}
}
